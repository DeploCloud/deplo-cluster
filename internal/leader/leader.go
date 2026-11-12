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
	"github.com/DeploCloud/deplo-cluster/internal/statefile"
	"github.com/DeploCloud/deplo-cluster/internal/wire"
)

// ErrOlderEpoch is a roster from a control plane that holds an older epoch than this leader.
var ErrOlderEpoch = errors.New("the leader holds a newer epoch")

const (
	// sendTimeout is under SureFor: a round waits for its slowest member, and the live ones are renewed again
	// before their lease is old.
	sendTimeout = 2 * time.Second
	// SureFor is how long a round that a quorum answered lets this leader send orders without asking again:
	// longer than a round that waited out a silent member, so one does not make it ask every time.
	SureFor = 4 * lease.RenewEvery
	// DeploFor is how long the control plane's last beat breaks a tie for a leader that hears exactly half of
	// its members. Deplo waits longer than this before it lets another leader lead.
	DeploFor = 20 * time.Second
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
	// presumed is when the longest lease another leader may have given it runs out; zero once it answered this one.
	presumed time.Time
	// stoppedWithin is the member's own word on how soon after a renewal its fence is done; zero is an older one.
	stoppedWithin time.Duration
	// heldBack is a failover that left the single-writer workloads for a second pass.
	heldBack bool
	// startsOut are the start orders on their way to it, startEnded when the last one was over.
	startsOut  int
	startEnded time.Time
}

type cluster struct {
	epoch     uint64
	members   map[string]*member
	placement []lease.Entry
	// standbys are the copies that follow a workload from another member: never moved, promoted at most.
	standbys   []lease.Entry
	superseded bool
	// moved marks a silent member whose workloads were already dealt with, until it answers again.
	moved map[string]bool
	// returning marks a member that came back from that, until it has said what it removed.
	returning map[string]bool
	// left is, per member, the single-writer workloads that moved off it and when, until it has said what it
	// did with its copies: what it is told to save, and the time its backups are labelled with.
	left map[string]map[string]time.Time
	// kept is, per member, what it said it could not save: asked about, with its destination, until it keeps none.
	kept map[string]map[string]time.Time
	// waiting is what found no room, retried every RetryEvery; auth is where the leader put a workload until
	// the control plane's picture agrees, so a roster that has not seen the move yet cannot undo it.
	waiting map[string]*Waiting
	auth    map[string]authority
	// extended is, per member, when a lease this leader extended before a restart runs out at the latest.
	extended map[string]time.Time
	// roster is the last one pushed, kept so a restarted leader serves its cluster before Deplo is back.
	roster Roster
	// sure is when the last round began that a quorum answered at this epoch, and heard how many did.
	sure  time.Time
	heard int
	// deplo is when the control plane last named this epoch to this leader.
	deplo time.Time
	// starting is the workloads a start order is out for, until it has an outcome.
	starting map[string]bool
}

// quorum says whether that many members answering let this leader lead: more than half of them, or exactly
// half while the control plane names its epoch. The other half's leader has no such word, so only one leads.
func (c *cluster) quorum(heard int, now time.Time) bool {
	n := len(c.members)
	return heard*2 > n || (heard > 0 && heard*2 == n && now.Sub(c.deplo) < DeploFor)
}

// leads says this leader may renew, extend and move: a quorum took its epoch within SureFor. One that may
// have been replaced without hearing of it does none of the three (spec #176, never two writers).
func (c *cluster) leads(now time.Time) bool {
	return !c.superseded && now.Sub(c.sure) < SureFor && c.quorum(c.heard, now)
}

type authority struct {
	At    time.Time   `json:"at"`
	Entry lease.Entry `json:"entry"`
	Told
}

// Told is what happened to a replica the leader moved or left waiting, as the snapshot and the kept state hold it.
type Told struct {
	From   string `json:"from"`
	Reason string `json:"reason"`
	// Seq is the event that told the control plane, for one that missed it.
	Seq uint64 `json:"seq"`
	// Wait is why a waiting replica found no member (the placement's refusal).
	Wait string `json:"wait,omitempty"`
	// BackupID and BackupAt name the backup a moved workload was restored from; RestoreMs is how long that took.
	BackupID  string `json:"backupId,omitempty"`
	BackupAt  string `json:"backupAt,omitempty"`
	RestoreMs int64  `json:"restoreMs,omitempty"`
	// Promoted is a move that promoted the workload's standby copy and restored nothing.
	Promoted bool `json:"promoted,omitempty"`
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
	// tickMu keeps a drain behind the renewals already on their way, so its longer lease is what a member hears last.
	tickMu sync.Mutex
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
		raw, err := os.ReadFile(filepath.Join(cfg.StateDir, "epochs.json"))
		if err == nil {
			err = json.Unmarshal(raw, &l.epochs)
		}
		l.loadFailover()
		// Without the epochs it held, this leader cannot tell an old roster from the current one: it waits for Deplo's.
		if err != nil && !os.IsNotExist(err) {
			l.epochs = map[string]uint64{}
			cfg.Logf("deplo-cluster: epochs.json cannot be read (%v): serving no cluster until Deplo sends its roster", err)
			return l
		}
		l.loadRosters()
	}
	return l
}

// keep writes one state file whole; a failure is logged, since the state in memory is still right.
func (l *Leader) keep(name string, v any) {
	if l.cfg.StateDir == "" {
		return
	}
	raw, err := json.Marshal(v)
	if err == nil {
		err = statefile.Write(filepath.Join(l.cfg.StateDir, name), raw)
	}
	if err != nil {
		l.cfg.Logf("deplo-cluster: could not keep %s: %v", name, err)
	}
}

// Events is the buffer a watcher reads.
func (l *Leader) Events() *events.Log { return l.log }

func (l *Leader) saveEpochs() { l.keep("epochs.json", l.epochs) }

// Roster is the control plane's picture of one cluster: its epoch, its members and where it put each workload.
type Roster struct {
	Cluster   string        `json:"cluster"`
	Epoch     uint64        `json:"epoch"`
	Members   []Member      `json:"members"`
	Placement []lease.Entry `json:"placement"`
	// Standbys are the standby copies, each on another member than its workload; an older leader ignores them.
	Standbys []lease.Entry `json:"standbys,omitempty"`
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
	// before is that leader's to have changed. Every lease starts over and its own moves are dropped. A lease it
	// extended and a return it is still owed stay: who leads changes neither.
	again := c.epoch != 0 && r.Epoch > c.epoch
	if again {
		c.auth, c.waiting, c.moved = map[string]authority{}, map[string]*Waiting{}, map[string]bool{}
		c.sure, c.deplo = time.Time{}, time.Time{}
	}
	now := l.cfg.Now()
	next := map[string]*member{}
	for _, m := range r.Members {
		if old := c.members[m.ID]; old != nil && old.Fingerprint == m.Fingerprint && !again {
			old.Member = m
			next[m.ID] = old
			continue
		}
		next[m.ID] = &member{Member: m, lastAck: now, state: lease.OK, presumed: now.Add(lease.FenceAfter + lease.ExtendFor)}
	}
	for id := range c.left {
		if next[id] == nil {
			delete(c.left, id)
		}
	}
	for id := range c.kept {
		if next[id] == nil {
			delete(c.kept, id)
		}
	}
	c.members, c.placement = next, l.reconcilePlacement(c, r.Placement)
	c.standbys = c.keptStandbys(r.Standbys)
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

// Forget drops a cluster the control plane deleted, or stopped giving to this leader: its members are renewed no
// more, nothing of it moves, and its bundles and its epoch go. A pass in flight finds it gone and stops.
func (l *Leader) Forget(clusterID string) error {
	if clusterID == "" {
		return errors.New("a forget names its cluster")
	}
	if l.cfg.Bundles != nil {
		if err := l.cfg.Bundles.DropCluster(clusterID); err != nil {
			return err
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.clusters, clusterID)
	delete(l.epochs, clusterID)
	delete(l.restored, clusterID)
	l.saveEpochs()
	l.saveRosters()
	l.saveFailover()
	return nil
}

// Beat is the control plane saying, every few seconds, at which epoch each cluster is led.
type Beat struct {
	Epochs map[string]uint64 `json:"epochs"`
}

// Beat notes the control plane's word for the clusters led here at that epoch. A newer epoch means another
// leader was handed the cluster: this one stops, and leads again only from a roster at least that new.
func (l *Leader) Beat(b Beat) {
	now := l.cfg.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	for id, epoch := range b.Epochs {
		c := l.clusters[id]
		switch {
		case c == nil || epoch < c.epoch:
		case epoch == c.epoch:
			c.deplo = now
		case !c.superseded:
			c.superseded = true
			if epoch > l.epochs[id] {
				l.epochs[id] = epoch
				l.saveEpochs()
			}
			l.cfg.Logf("deplo-cluster: cluster %s is led at epoch %d now, newer than %d: stepping down", id, epoch, c.epoch)
		}
	}
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
	l.tellPlacement(a.Cluster, a.Kind, a.ID, a.Members, epoch)
	return nil
}

// tellPlacement is the one event that says where a workload's replicas are.
func (l *Leader) tellPlacement(clusterID, kind, id string, members []string, epoch uint64) events.Event {
	return l.log.Append("placement", clusterID, map[string]any{"kind": kind, "workloadId": id, "members": members, "epoch": epoch})
}

// place records which member runs one replica and returns its entry as it stands now; false when the placement
// does not hold that replica. The caller holds l.mu.
func (c *cluster) place(kind, id string, replica int, member string) (lease.Entry, bool) {
	for i := range c.placement {
		if p := &c.placement[i]; p.Kind == kind && p.ID == id && p.Replica == replica {
			p.Member = member
			return *p, true
		}
	}
	return lease.Entry{}, false
}

// Tick renews every member's lease once, then reports each member whose state changed.
func (l *Leader) Tick(ctx context.Context) {
	l.tickMu.Lock()
	defer l.tickMu.Unlock()
	// Told to stop: the drain's longer lease is the last order, and a dial does not look at a cancelled context.
	if ctx.Err() != nil {
		return
	}
	l.round(ctx, 0)
	l.settle()
}

// round asks the members of every cluster this leader is not sure it leads, then sends its orders where a
// quorum answered. extend is the longer lease of a drain; the caller holds l.tickMu.
func (l *Leader) round(ctx context.Context, extend time.Duration) {
	l.exchange(ctx, true, 0)
	l.exchange(ctx, false, extend)
}

// exchange sends one order to every member of the clusters that are due it: a probe where this leader is not
// sure it leads, a renewal where it is. A quorum of answers makes it sure for SureFor.
func (l *Leader) exchange(ctx context.Context, probe bool, extend time.Duration) {
	// id and fingerprint are read here, under the lock: a roster may replace them while the order is on its way.
	type job struct {
		cluster, id, fingerprint string
		m                        *member
		order                    lease.Order
	}
	var jobs []job
	epochs := map[string]uint64{}
	now := l.cfg.Now()
	l.mu.Lock()
	for id, c := range l.clusters {
		if c.superseded || c.leads(now) == probe {
			continue
		}
		epochs[id] = c.epoch
		for _, m := range c.members {
			o := lease.Order{Cluster: id, Member: m.ID, Epoch: c.epoch, Probe: probe}
			if !probe {
				o.Placement, o.Return, o.ExtendSeconds = c.orderPlacement(c.told()), c.owesReturn(m.ID), int(extend/time.Second)
			}
			if extend > 0 {
				// Kept before the order leaves: a member that took it and whose answer was lost is extended all the same.
				c.extended[m.ID] = now.Add(lease.FenceAfter + extend)
			}
			jobs = append(jobs, job{id, m.ID, m.Fingerprint, m, o})
		}
	}
	if extend > 0 {
		l.saveFailover()
	}
	l.mu.Unlock()

	answered := make(chan string, len(jobs))
	var wg sync.WaitGroup
	for _, j := range jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.renew(ctx, j.cluster, j.id, j.fingerprint, j.m, j.order) {
				answered <- j.cluster
			}
		}()
	}
	wg.Wait()
	close(answered)
	heard := map[string]int{}
	for id := range answered {
		heard[id]++
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for id, n := range heard {
		c := l.clusters[id]
		if c == nil || c.superseded || c.epoch != epochs[id] || !c.quorum(n, now) {
			continue
		}
		c.sure, c.heard = now, n
		if !probe {
			continue
		}
		// It was not sure until now, so an older leader may have extended a lease meanwhile: a member this one
		// never heard holds the longest one again, counted as from a roster one renewal before this round.
		for _, m := range c.members {
			if at := now.Add(lease.FenceAfter + lease.ExtendFor - lease.RenewEvery); !m.presumed.IsZero() && m.presumed.Before(at) {
				m.presumed = at
			}
		}
	}
}

// renew sends one order and says whether the member took it at this leader's epoch.
func (l *Leader) renew(ctx context.Context, clusterID, id, fingerprint string, m *member, o lease.Order) bool {
	ctx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	c, err := l.cfg.Dial(ctx, wire.Party{Fingerprint: fingerprint, Role: wire.RoleMember})
	if err != nil {
		return false
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(sendTimeout))
	// Opened only for a member that answers, never on a probe: a keep carries its destination's credentials.
	if !o.Probe {
		o.Keep, o.Stores = l.keepsFor(clusterID, id), l.storesFor(clusterID, id)
	}
	r, err := lease.Send(c, o)
	if err != nil {
		return false
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
			l.cfg.Logf("deplo-cluster: member %s holds epoch %d, newer than %d: stepping down", id, r.Epoch, o.Epoch)
			l.log.Append("superseded", clusterID, map[string]uint64{"epoch": r.Epoch})
		}
		return false
	}
	// A probe renewed nothing: the member's lease is as old as it was.
	if !r.OK || o.Probe {
		return r.OK
	}
	l.mu.Lock()
	m.lastAck, m.stoppedWithin = l.cfg.Now(), time.Duration(r.StoppedWithin)*time.Second
	// A plain renewal starts the member's lease over at 60 s: what an earlier extension bought is gone.
	if o.ExtendSeconds == 0 {
		m.presumed = time.Time{}
	}
	if cl := l.clusters[clusterID]; cl != nil && o.ExtendSeconds == 0 && !cl.extended[id].IsZero() {
		delete(cl.extended, id)
		l.saveFailover()
	}
	tell := l.cameBack(clusterID, id, r.Returned)
	has := l.stillHas(clusterID, id, r.Unplaced)
	later := l.heardLater(clusterID, id, r.Later)
	l.mu.Unlock()
	tell()
	has()
	later()
	return true
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
			s := lease.StateAfter(now.Sub(m.lastAck))
			if s == lease.NotResponding && m.stoppedWithin > 0 && now.Sub(m.lastAck) >= m.stoppedWithin {
				s = lease.Fenced
			}
			if s != m.state {
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
	Epoch      uint64 `json:"epoch"`
	Superseded bool   `json:"superseded"`
	// Leads says enough members answer for this leader to renew and move work right now.
	Leads     bool          `json:"leads"`
	Members   []MemberView  `json:"members"`
	Placement []lease.Entry `json:"placement"`
	// Moves are the placements the leader made on its own that no roster has repeated yet.
	Moves []Move `json:"moves"`
}

// Move is one replica the leader moved or left waiting (Member empty), and why.
type Move struct {
	lease.Entry
	Told
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
		v := ClusterView{Epoch: c.epoch, Superseded: c.superseded, Leads: c.leads(l.cfg.Now()), Members: []MemberView{}, Placement: append([]lease.Entry{}, c.placement...)}
		for _, m := range c.members {
			v.Members = append(v.Members, MemberView{ID: m.ID, State: string(m.state)})
		}
		sort.Slice(v.Members, func(i, j int) bool { return v.Members[i].ID < v.Members[j].ID })
		v.Moves = []Move{}
		for _, a := range c.auth {
			v.Moves = append(v.Moves, Move{Entry: a.Entry, Told: a.Told})
		}
		sort.Slice(v.Moves, func(i, j int) bool {
			a, b := v.Moves[i], v.Moves[j]
			return entryKey(a.Kind, a.ID, a.Replica) < entryKey(b.Kind, b.ID, b.Replica)
		})
		s.Clusters[id] = v
	}
	return s
}
