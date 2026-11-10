// Package leader holds each member's lease, tells the control plane what changed and refuses to lead with an old epoch (spec #176).
package leader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/events"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
	"github.com/DeploCloud/deplo-cluster/internal/wire"
)

// ErrOlderEpoch is a roster from a control plane that holds an older epoch than this leader.
var ErrOlderEpoch = errors.New("the leader holds a newer epoch")

const (
	sendTimeout = 4 * time.Second
	// MaxEvents is how many events are kept for a control plane that is away.
	MaxEvents = 5000
)

// Config is what the leader needs from its process.
type Config struct {
	// Dial opens a stream to a member through the relay.
	Dial func(ctx context.Context, to wire.Party) (net.Conn, error)
	Now  func() time.Time
	// StateDir keeps the epochs and the event buffer across restarts; empty keeps nothing.
	StateDir string
	Logf     func(format string, args ...any)
}

// Member is a server of the cluster and the certificate it dials with.
type Member struct {
	ID          string `json:"id"`
	Fingerprint string `json:"fingerprint"`
}

type member struct {
	Member
	lastAck time.Time
	state   lease.State
}

type cluster struct {
	epoch      uint64
	members    map[string]*member
	placement  []lease.Entry
	superseded bool
}

// Leader serves every cluster whose roster the control plane pushed.
type Leader struct {
	cfg      Config
	log      *events.Log
	mu       sync.Mutex
	clusters map[string]*cluster
	epochs   map[string]uint64
}

// New returns a leader; Run starts renewing.
func New(cfg Config) *Leader {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	file := ""
	if cfg.StateDir != "" {
		_ = os.MkdirAll(cfg.StateDir, 0o700)
		file = filepath.Join(cfg.StateDir, "events.jsonl")
	}
	l := &Leader{cfg: cfg, log: events.Open(file, MaxEvents, cfg.Now), clusters: map[string]*cluster{}, epochs: map[string]uint64{}}
	if cfg.StateDir != "" {
		if raw, err := os.ReadFile(filepath.Join(cfg.StateDir, "epochs.json")); err == nil {
			_ = json.Unmarshal(raw, &l.epochs)
		}
	}
	return l
}

// Events is the buffer a watcher reads.
func (l *Leader) Events() *events.Log { return l.log }

func (l *Leader) saveEpochs() {
	if l.cfg.StateDir == "" {
		return
	}
	raw, _ := json.Marshal(l.epochs)
	tmp := filepath.Join(l.cfg.StateDir, "epochs.json.tmp")
	if os.WriteFile(tmp, raw, 0o600) == nil {
		_ = os.Rename(tmp, filepath.Join(l.cfg.StateDir, "epochs.json"))
	}
}

// Roster is the control plane's picture of one cluster: its epoch, its members and where it put each workload.
type Roster struct {
	Cluster   string        `json:"cluster"`
	Epoch     uint64        `json:"epoch"`
	Members   []Member      `json:"members"`
	Placement []lease.Entry `json:"placement"`
}

// SetRoster replaces a cluster's members and placement. An epoch older than the one held is refused, and a
// newer one is remembered. A member seen for the first time starts with a fresh lease.
func (l *Leader) SetRoster(r Roster) error {
	if r.Cluster == "" {
		return errors.New("a roster names its cluster")
	}
	if r.Epoch == 0 {
		r.Epoch = 1
	}
	l.mu.Lock()
	if held := l.epochs[r.Cluster]; r.Epoch < held {
		l.mu.Unlock()
		return fmt.Errorf("%w: %d, asked for %d", ErrOlderEpoch, held, r.Epoch)
	}
	c := l.clusters[r.Cluster]
	if c == nil {
		c = &cluster{members: map[string]*member{}}
		l.clusters[r.Cluster] = c
	}
	now := l.cfg.Now()
	next := map[string]*member{}
	for _, m := range r.Members {
		if old := c.members[m.ID]; old != nil && old.Fingerprint == m.Fingerprint {
			next[m.ID] = old
			continue
		}
		next[m.ID] = &member{Member: m, lastAck: now, state: lease.OK}
	}
	c.members, c.placement = next, r.Placement
	changed := r.Epoch != l.epochs[r.Cluster]
	if r.Epoch > c.epoch {
		c.superseded = false
	}
	c.epoch = r.Epoch
	l.epochs[r.Cluster] = r.Epoch
	if changed {
		l.saveEpochs()
	}
	l.mu.Unlock()
	if changed {
		l.log.Append("epoch", r.Cluster, map[string]uint64{"epoch": r.Epoch})
	}
	return nil
}

// Assign records where the leader put one workload and tells the control plane.
type Assign struct {
	Cluster      string   `json:"cluster"`
	Kind         string   `json:"kind"`
	ID           string   `json:"id"`
	Slug         string   `json:"slug"`
	SingleWriter bool     `json:"singleWriter"`
	Members      []string `json:"members"`
}

// Assign replaces the workload's entries; a cluster the leader does not serve is refused.
func (l *Leader) Assign(a Assign) error {
	l.mu.Lock()
	c := l.clusters[a.Cluster]
	if c == nil {
		l.mu.Unlock()
		return errors.New("this leader does not serve that cluster")
	}
	kept := c.placement[:0:0]
	for _, e := range c.placement {
		if e.Kind != a.Kind || e.ID != a.ID {
			kept = append(kept, e)
		}
	}
	for i, m := range a.Members {
		kept = append(kept, lease.Entry{Kind: a.Kind, ID: a.ID, Replica: i, Member: m, Slug: a.Slug, SingleWriter: a.SingleWriter})
	}
	c.placement = kept
	epoch := c.epoch
	l.mu.Unlock()
	l.log.Append("placement", a.Cluster, map[string]any{"kind": a.Kind, "workloadId": a.ID, "members": a.Members, "epoch": epoch})
	return nil
}

// Tick renews every member's lease once, then reports each member whose state changed.
func (l *Leader) Tick(ctx context.Context) {
	type job struct {
		cluster string
		m       *member
		order   lease.Order
	}
	var jobs []job
	l.mu.Lock()
	for id, c := range l.clusters {
		if c.superseded {
			continue
		}
		for _, m := range c.members {
			jobs = append(jobs, job{id, m, lease.Order{Cluster: id, Member: m.ID, Epoch: c.epoch, Placement: append([]lease.Entry(nil), c.placement...)}})
		}
	}
	l.mu.Unlock()

	var wg sync.WaitGroup
	for _, j := range jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.renew(ctx, j.cluster, j.m, j.order)
		}()
	}
	wg.Wait()
	l.settle()
}

func (l *Leader) renew(ctx context.Context, clusterID string, m *member, o lease.Order) {
	ctx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	c, err := l.cfg.Dial(ctx, wire.Party{Fingerprint: m.Fingerprint, Role: wire.RoleMember})
	if err != nil {
		return
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(sendTimeout))
	r, err := lease.Send(c, o)
	if err != nil {
		return
	}
	if r.Error == lease.StaleEpoch {
		l.mu.Lock()
		cl := l.clusters[clusterID]
		fresh := cl != nil && !cl.superseded
		if fresh {
			cl.superseded = true
		}
		l.mu.Unlock()
		if fresh {
			l.cfg.Logf("deplo-cluster: member %s holds epoch %d, newer than %d: stepping down", m.ID, r.Epoch, o.Epoch)
			l.log.Append("superseded", clusterID, map[string]uint64{"epoch": r.Epoch})
		}
		return
	}
	if r.OK {
		l.mu.Lock()
		m.lastAck = l.cfg.Now()
		l.mu.Unlock()
	}
}

// settle moves every member to the state its silence earns and records the changes, in cluster and member order.
func (l *Leader) settle() {
	type change struct{ cluster, member, state string }
	var out []change
	now := l.cfg.Now()
	l.mu.Lock()
	ids := make([]string, 0, len(l.clusters))
	for id := range l.clusters {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		c := l.clusters[id]
		if c.superseded {
			continue
		}
		mids := make([]string, 0, len(c.members))
		for mid := range c.members {
			mids = append(mids, mid)
		}
		sort.Strings(mids)
		for _, mid := range mids {
			m := c.members[mid]
			if s := lease.StateAfter(now.Sub(m.lastAck)); s != m.state {
				m.state = s
				out = append(out, change{id, mid, string(s)})
			}
		}
	}
	l.mu.Unlock()
	for _, c := range out {
		l.log.Append("member_state", c.cluster, map[string]string{"member": c.member, "state": c.state})
	}
}

// Run renews leases every lease.RenewEvery until ctx ends.
func (l *Leader) Run(ctx context.Context) {
	t := time.NewTicker(lease.RenewEvery)
	defer t.Stop()
	for {
		l.Tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// MemberView is one member as the snapshot shows it.
type MemberView struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

// ClusterView is one cluster as the snapshot shows it.
type ClusterView struct {
	Epoch      uint64        `json:"epoch"`
	Superseded bool          `json:"superseded"`
	Members    []MemberView  `json:"members"`
	Placement  []lease.Entry `json:"placement"`
}

// Snapshot is the whole truth now, for the control plane to reconcile against after a replay.
type Snapshot struct {
	Head     uint64                 `json:"head"`
	Clusters map[string]ClusterView `json:"clusters"`
}

// Snapshot reads every cluster the leader serves.
func (l *Leader) Snapshot() Snapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := Snapshot{Head: l.log.Head(), Clusters: map[string]ClusterView{}}
	for id, c := range l.clusters {
		v := ClusterView{Epoch: c.epoch, Superseded: c.superseded, Members: []MemberView{}, Placement: append([]lease.Entry{}, c.placement...)}
		for _, m := range c.members {
			v.Members = append(v.Members, MemberView{ID: m.ID, State: string(m.state)})
		}
		sort.Slice(v.Members, func(i, j int) bool { return v.Members[i].ID < v.Members[j].ID })
		s.Clusters[id] = v
	}
	return s
}
