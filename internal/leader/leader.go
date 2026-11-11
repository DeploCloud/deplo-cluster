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

	"github.com/DeploCloud/deplo-cluster/internal/bundles"
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
	// Bundles holds the sealed failover bundles; nil serves none and fails nothing over.
	Bundles *bundles.Store
	// PathOK says whether this leader can still reach the relays. A leader that lost its own path moves nothing.
	PathOK func() bool
}

// Member is a server of the cluster and the certificate it dials with. The rest is what the control plane last
// knew about it, which is all a failover has to go on while Deplo is away.
type Member struct {
	ID          string `json:"id"`
	Fingerprint string `json:"fingerprint"`

	Arch         string   `json:"arch,omitempty"`
	FreeMB       int      `json:"freeMb,omitempty"`
	Images       []string `json:"images,omitempty"`
	Emptying     bool     `json:"emptying,omitempty"`
	DatabaseEnvs []string `json:"databaseEnvs,omitempty"`
	// NoFailover is a member whose agent cannot start a moved workload: it keeps what it runs and takes over nothing.
	NoFailover bool `json:"noFailover,omitempty"`
	// NoRestore is a member whose agent cannot restore a backup before a start: it is given no workload that needs one.
	NoRestore bool `json:"noRestore,omitempty"`
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
	// moved marks a silent member whose workloads were already dealt with, until it answers again.
	moved map[string]bool
	// returning marks a member that came back from that, until it has said what it removed.
	returning map[string]bool
	// waiting is what found no room, retried every RetryEvery; auth is where the leader put a workload until
	// the control plane's picture agrees, so a roster that has not seen the move yet cannot undo it.
	waiting map[string]*Waiting
	auth    map[string]authority
	// extended is, per member, when a lease this leader extended before a restart runs out at the latest.
	extended map[string]time.Time
	// roster is the last one pushed, kept so a restarted leader serves its cluster before Deplo is back.
	roster Roster
}

type authority struct {
	At    time.Time   `json:"at"`
	Entry lease.Entry `json:"entry"`
	// From and Reason say what happened; Seq is the event that told the control plane, for one that missed it.
	From   string `json:"from,omitempty"`
	Reason string `json:"reason,omitempty"`
	Seq    uint64 `json:"seq,omitempty"`
	// BackupID and BackupAt name the backup a moved workload was restored from; RestoreMs is how long that took.
	BackupID  string `json:"backupId,omitempty"`
	BackupAt  string `json:"backupAt,omitempty"`
	RestoreMs int64  `json:"restoreMs,omitempty"`
}

// Leader serves every cluster whose roster the control plane pushed.
type Leader struct {
	cfg      Config
	log      *events.Log
	mu       sync.Mutex
	clusters map[string]*cluster
	epochs   map[string]uint64
	// foMu lets one failover pass run at a time; renewals never wait on it.
	foMu sync.Mutex
	// restored is the failover state read at start, handed to each cluster when its roster first arrives.
	restored map[string]persisted
	// pending is the moves a person asked for, made by the next failover pass.
	// ponytail: kept in memory, so a leader that restarts first forgets the ask and the person asks again.
	pending []pendingMove
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
		l.loadFailover()
		l.loadRosters()
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
	held := l.epochs[r.Cluster]
	if r.Epoch < held {
		l.mu.Unlock()
		return fmt.Errorf("%w: %d, asked for %d", ErrOlderEpoch, held, r.Epoch)
	}
	c := l.clusters[r.Cluster]
	if c == nil {
		c = l.newCluster(r.Cluster)
		l.clusters[r.Cluster] = c
	}
	// A newer epoch means another leader led in between: this one heard nobody meanwhile, and what it moved
	// before is that leader's to have changed. Every lease starts over and its own moves are dropped.
	again := c.epoch != 0 && r.Epoch > c.epoch
	if again {
		c.auth, c.waiting, c.moved = map[string]authority{}, map[string]*Waiting{}, map[string]bool{}
	}
	now := l.cfg.Now()
	next := map[string]*member{}
	for _, m := range r.Members {
		if old := c.members[m.ID]; old != nil && old.Fingerprint == m.Fingerprint && !again {
			old.Member = m
			next[m.ID] = old
			continue
		}
		next[m.ID] = &member{Member: m, lastAck: now, state: lease.OK}
	}
	c.members, c.placement = next, l.reconcilePlacement(c, r.Placement)
	if again || c.epoch == 0 {
		l.adoptWaiting(c, now)
	}
	changed := r.Epoch != c.epoch && (c.epoch != 0 || r.Epoch != held)
	if r.Epoch > c.epoch {
		c.superseded = false
	}
	c.epoch = r.Epoch
	l.epochs[r.Cluster] = r.Epoch
	if changed {
		l.saveEpochs()
	}
	c.roster = r
	l.saveRosters()
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
	for i := range c.waiting {
		if w := c.waiting[i]; w.Kind == a.Kind && w.ID == a.ID {
			delete(c.waiting, i)
		}
	}
	for i, m := range a.Members {
		kept = append(kept, lease.Entry{Kind: a.Kind, ID: a.ID, Replica: i, Member: m, Slug: a.Slug, SingleWriter: a.SingleWriter})
	}
	c.placement = kept
	epoch := c.epoch
	l.saveFailover()
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
			jobs = append(jobs, job{id, m, lease.Order{Cluster: id, Member: m.ID, Epoch: c.epoch, Placement: append([]lease.Entry(nil), c.placement...), Return: c.returning[m.ID]}})
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
		// A refusal of an order sent before a newer roster arrived says nothing about the epoch held now.
		fresh := cl != nil && !cl.superseded && r.Epoch > cl.epoch
		if fresh {
			cl.superseded = true
		}
		// The epoch that replaced this leader is another leader's: only a roster newer than it leads again here.
		if r.Epoch >= l.epochs[clusterID] {
			l.epochs[clusterID] = r.Epoch + 1
			l.saveEpochs()
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
		// A plain renewal starts the member's lease over at 60 s: what an earlier extension bought is gone.
		if cl := l.clusters[clusterID]; cl != nil && o.ExtendSeconds == 0 && !cl.extended[m.ID].IsZero() {
			delete(cl.extended, m.ID)
			l.saveFailover()
		}
		tell := l.cameBack(clusterID, m.ID, r.Returned)
		l.mu.Unlock()
		tell()
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
	go func() {
		f := time.NewTicker(lease.RenewEvery)
		defer f.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-f.C:
				l.Failover(ctx)
			}
		}
	}()
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
	// Moves are the placements the leader made on its own that no roster has repeated yet.
	Moves []Move `json:"moves"`
}

// Move is one replica the leader moved or left waiting (Member empty), and why.
type Move struct {
	lease.Entry
	From   string `json:"from"`
	Reason string `json:"reason"`
	Seq    uint64 `json:"seq"`
	// BackupID and BackupAt name the backup the workload was restored from, when it was; RestoreMs is how long that took.
	BackupID  string `json:"backupId,omitempty"`
	BackupAt  string `json:"backupAt,omitempty"`
	RestoreMs int64  `json:"restoreMs,omitempty"`
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
		v.Moves = []Move{}
		for _, a := range c.auth {
			v.Moves = append(v.Moves, Move{Entry: a.Entry, From: a.From, Reason: a.Reason, Seq: a.Seq, BackupID: a.BackupID, BackupAt: a.BackupAt, RestoreMs: a.RestoreMs})
		}
		sort.Slice(v.Moves, func(i, j int) bool {
			a, b := v.Moves[i], v.Moves[j]
			return entryKey(a.Kind, a.ID, a.Replica) < entryKey(b.Kind, b.ID, b.Replica)
		})
		s.Clusters[id] = v
	}
	return s
}
