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
	"slices"
	"sort"
	"sync"
	"sync/atomic"
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
	// StartGrace is how long after this leader starts a member it has not heard yet is unknown, not silent: 0 is
	// DefaultStartGrace, below 0 none.
	StartGrace time.Duration
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
	// DiskFree, DiskTotal and Holds are its disk and the workloads whose data it holds whole (deplo ADR-0043).
	DiskFree  int64    `json:"diskFree,omitempty"`
	DiskTotal int64    `json:"diskTotal,omitempty"`
	Holds     []string `json:"holds,omitempty"`
	// NoFailover is a member whose agent cannot start a moved workload: it keeps what it runs and takes over nothing.
	NoFailover bool `json:"noFailover,omitempty"`
	// NoRestore is a member whose agent cannot restore a backup before a start: it is given no workload that needs one.
	NoRestore bool `json:"noRestore,omitempty"`
	// ShortFence is a member whose agent keeps to the fence its leader gives it, as Deplo knows from its version.
	ShortFence bool `json:"shortFence,omitempty"`
	// QuickStop is a member whose agent stops its stacks within quickMargin of its lease running out.
	QuickStop bool `json:"quickStop,omitempty"`
	// Succession is a member whose agent answers a pre-vote and gives each epoch to one leader only.
	Succession bool `json:"succession,omitempty"`
}

type member struct {
	Member
	lastAck time.Time
	state   lease.State
	// heard is a member that has answered this leader since it started; until then, see unknown. waitedUnknown
	// is its work waiting for that, said once.
	heard, waitedUnknown bool
	// other is the latest renewal another leader may have sent it; zero once it answered this one.
	other time.Time
	// stoppedWithin is the member's own word on how soon after a renewal its fence is done; zero is an older one.
	stoppedWithin time.Duration
	// heldBack is a failover that left the single-writer workloads for a second pass.
	heldBack bool
	// startsOut are the start orders on their way to it, startEnded when the last one was over.
	startsOut  int
	startEnded time.Time
	// running is what the member said it runs when this leader last probed it: see keepWhereRunning.
	running []lease.Writer
	// upSince is when it last began answering after a silence: copyBack waits for GiveCopyAfter of it.
	upSince time.Time
	// answered is its last answer of any kind, a probe's included: the line names a member that answers.
	answered time.Time
	// inherited is when the leader before this one last heard it, as the heir that took over was told.
	inherited time.Time
	// sending is an order of a probe or renewal on its way to it, not answered yet.
	sending bool
}

// lastHeard is when any leader of this cluster last heard the member, as far as this one knows; zero is never.
func (m *member) lastHeard() time.Time {
	if m.heard {
		return latest(m.lastAck, m.inherited)
	}
	return m.inherited
}

type cluster struct {
	epoch     uint64
	members   map[string]*member
	placement []lease.Entry
	// standbys are the copies that follow a workload from another member: never moved, promoted at most.
	standbys   []lease.Entry
	superseded bool
	// vouched is a member having said, at this epoch, how long an extension may keep a lease alive: unsure is the
	// latest such end any member named.
	vouched bool
	unsure  time.Time
	// moved marks a silent member whose workloads were already dealt with, until it answers again.
	moved map[string]bool
	// returning marks a member that came back from that, until it has said what it removed.
	returning map[string]bool
	// dropped marks a member asked the same to remove the copy of a move that was not recorded: see recheck.
	dropped map[string]bool
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
	// deploUntil is how long the control plane's last word for this epoch holds here.
	deploUntil time.Time
	// starting is the workloads a start order is out for, until it has an outcome: see told.
	starting map[string]underWay
	// gone is the members silent past GoneAfter that no longer count for a majority, and since when (ADR-0049).
	gone map[string]time.Time
	// succeed is an epoch this leader took without Deplo's word: it pre-votes before it probes (prevoted), until a
	// quorum takes it, and stands by again when none did by succeedUntil.
	succeed      bool
	prevoted     time.Time
	succeedUntil time.Time
	// order is the heirs as the line last stood: see line. since is the event that began this epoch here.
	order []string
	since uint64
	// ledAt is when a quorum took the epoch this leader took on its own: what Deplo dates the takeover with.
	ledAt time.Time
}

// GoneAfter is how long a member is silent before the others stop counting it. It is longer than a cut-off member
// takes to fence itself (FenceAfter for an older agent, plus fenceMargin), so it stopped its writers by then.
const GoneAfter = 2 * time.Minute

// voters is the members that count for a majority: every member but the gone ones.
func (c *cluster) voters() int {
	n := 0
	for id := range c.members {
		if _, out := c.gone[id]; !out {
			n++
		}
	}
	return n
}

// goneFingerprints is the certificates of the gone members, which every renewal names.
func (c *cluster) goneFingerprints() []string {
	var out []string
	for id := range c.gone {
		if m := c.members[id]; m != nil {
			out = append(out, m.Fingerprint)
		}
	}
	sort.Strings(out)
	return out
}

// quorum says whether that many members answering let this leader lead: more than half of them, or exactly
// half while the control plane names its epoch. The other half's leader has no such word, so only one leads.
func (c *cluster) quorum(heard int, now time.Time) bool {
	n := c.voters()
	return heard*2 > n || (heard > 0 && heard*2 == n && now.Before(c.deploUntil))
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
	// started is when this leader started: see unknown.
	started time.Time
	// standby is the rosters Deplo gave this leader to take a cluster over from, when its member is next in line.
	standby map[string]Roster
	// rotations are every roster's key rotations, read by the party without the lock.
	rotations atomic.Pointer[[]wire.Rotation]
	// files writes the state directory off l.mu: nil keeps nothing.
	files *statefile.Writer
	// said is the last line logged on each subject that a pass every second would repeat: see once.
	saidMu sync.Mutex
	said   map[string]string
	// kick has Run renew now: a leader handed a cluster at a newer epoch probes at once, not at its next tick.
	kick chan struct{}
	// ran is when this leader last noted it runs, and woke when it ran again after a gap past AsleepAfter: see awake.
	wakeMu    sync.Mutex
	ran, woke time.Time
}

// once logs a line a pass every second would repeat only when it differs from the last one on that subject.
func (l *Leader) once(subject, format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	l.saidMu.Lock()
	if l.said == nil {
		l.said = map[string]string{}
	}
	again := l.said[subject] == line
	l.said[subject] = line
	l.saidMu.Unlock()
	if !again {
		l.cfg.Logf("%s", line)
	}
}

// unsay forgets a subject's line: what it said is over, and the next one is news.
func (l *Leader) unsay(subject string) {
	l.saidMu.Lock()
	delete(l.said, subject)
	l.saidMu.Unlock()
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
	l := &Leader{cfg: cfg, log: events.Open(file, MaxEvents, cfg.Now), clusters: map[string]*cluster{}, epochs: map[string]uint64{}, started: cfg.Now(), standby: map[string]Roster{}, kick: make(chan struct{}, 1)}
	if cfg.StateDir != "" {
		l.files = statefile.NewWriter(cfg.StateDir, cfg.Logf)
		raw, err := os.ReadFile(filepath.Join(cfg.StateDir, "epochs.json"))
		if err == nil {
			err = json.Unmarshal(raw, &l.epochs)
		}
		l.loadFailover()
		// Without the epochs it held, this leader cannot tell an old roster from the current one: it waits for Deplo's.
		if err != nil && !os.IsNotExist(err) {
			l.epochs = map[string]uint64{}
			cfg.Logf("deplo-cluster: epochs.json cannot be read (%v): serving no cluster until Deplo sends its roster", err)
			l.forgetRestored()
			return l
		}
		l.loadRosters()
		l.forgetRestored()
		l.loadStandby()
	}
	return l
}

// keep has one state file written whole, off the lock; a failure is logged, since the state in memory is still right.
func (l *Leader) keep(name string, v any) {
	if l.files == nil {
		return
	}
	raw, err := json.Marshal(v)
	if err != nil {
		l.cfg.Logf("deplo-cluster: could not keep %s: %v", name, err)
		return
	}
	l.files.Put(name, raw)
}

// Flush returns once every state file asked for so far is on disk.
func (l *Leader) Flush() {
	if l.files != nil {
		l.files.Flush()
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
	// Rotations are its members' key rotations (ADR-0050): a member named by an old certificate answers with its new.
	Rotations []wire.Rotation `json:"rotations,omitempty"`
	// Self is the server this leader runs on, which every order names; Deplo is Deplo's machine, the end of every line.
	Self  string `json:"self,omitempty"`
	Deplo string `json:"deplo,omitempty"`
	// Heirs are the members that may lead after this one, in Deplo's order: each runs a leader that holds the bundles.
	Heirs []string `json:"heirs,omitempty"`
	// Succeed is an epoch taken without Deplo's word, by a member next in line or by Deplo's machine standing in: it
	// leads only once a quorum would take it from no leader it still hears (a pre-vote).
	Succeed bool `json:"succeed,omitempty"`
}

// Rotations is every key rotation the rosters announced, for the party that dials members.
func (l *Leader) Rotations() []wire.Rotation {
	if rs := l.rotations.Load(); rs != nil {
		return *rs
	}
	return nil
}

// keepRotations gathers what every roster announced; the caller holds l.mu.
func (l *Leader) keepRotations() {
	var all []wire.Rotation
	for _, c := range l.clusters {
		all = append(all, c.roster.Rotations...)
	}
	l.rotations.Store(&all)
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
	for _, rot := range r.Rotations {
		if from, to := (wire.Party{Fingerprint: rot.From, Role: wire.RoleMember}), (wire.Party{Fingerprint: rot.To, Role: wire.RoleMember}); !from.Valid() || !to.Valid() || rot.From == rot.To {
			return fmt.Errorf("%q to %q is not a key rotation", rot.From, rot.To)
		}
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
		c.sure, c.deploUntil = time.Time{}, time.Time{}
		c.vouched, c.unsure = false, time.Time{}
	}
	now := l.cfg.Now()
	next := map[string]*member{}
	for _, m := range r.Members {
		// A member that rotated its key is still the member it was.
		if old := c.members[m.ID]; old != nil && !again &&
			(old.Fingerprint == m.Fingerprint || slices.Contains(wire.Successors(r.Rotations, old.Fingerprint), m.Fingerprint)) {
			old.Member = m
			next[m.ID] = old
			continue
		}
		next[m.ID] = &member{Member: m, lastAck: now, state: lease.OK, other: now}
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
	var kept []lease.Entry
	takeover := again || c.epoch == 0
	c.placement, kept = l.reconcilePlacement(c, r.Placement, takeover)
	c.members = next
	c.standbys = c.keptStandbys(r.Standbys)
	if again || c.epoch == 0 {
		l.adoptWaiting(c, now)
	}
	changed := r.Epoch != c.epoch && (c.epoch != 0 || r.Epoch != held)
	if r.Epoch > c.epoch {
		c.superseded = false
	}
	if c.succeed = r.Succeed; c.succeed {
		c.succeedUntil = now.Add(SucceedFor)
	}
	c.epoch = r.Epoch
	// A try no quorum took yet promises nothing: its epoch is kept only once one does (c44).
	if !r.Succeed && l.epochs[r.Cluster] != r.Epoch {
		l.epochs[r.Cluster] = r.Epoch
		l.saveEpochs()
	}
	c.roster = r
	l.keepRotations()
	l.saveRosters()
	l.mu.Unlock()
	if changed {
		ev := l.log.Append("epoch", r.Cluster, map[string]uint64{"epoch": r.Epoch})
		l.mu.Lock()
		if c.epoch == r.Epoch {
			c.since = ev.Seq
			l.saveFailover()
		}
		l.mu.Unlock()
	}
	l.tellKept(r.Cluster, kept)
	if takeover {
		select {
		case l.kick <- struct{}{}:
		default:
		}
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
	if _, ok := l.standby[clusterID]; ok {
		delete(l.standby, clusterID)
		l.keep("standby.json", l.standby)
	}
	l.keepRotations()
	l.saveEpochs()
	l.saveRosters()
	l.saveFailover()
	return nil
}

// Beat is the control plane saying, every few seconds, at which epoch each cluster is led.
type Beat struct {
	Epochs map[string]uint64 `json:"epochs"`
	// TrustMs is how long the control plane asks its word to hold; never longer than DeploFor, which an older one gets.
	TrustMs int64 `json:"trustMs,omitempty"`
}

// BeatReply says how much longer this leader holds Deplo's word, an earlier and longer one included, so the
// control plane waits that out before it names another leader.
type BeatReply struct {
	OK      bool  `json:"ok"`
	HoldsMs int64 `json:"holdsMs"`
}

// Trust is how long a beat's word holds here.
func (b Beat) Trust() time.Duration {
	if t := time.Duration(b.TrustMs) * time.Millisecond; t > 0 && t < DeploFor {
		return t
	}
	return DeploFor
}

// Beat notes the control plane's word for the clusters led here at that epoch. A newer epoch means another
// leader was handed the cluster: this one renews its members one last time and stops, so the next one, handed the
// cluster after this answer, renews them within a second of it (c43: 1.49 s). It leads again only from a newer roster.
func (l *Leader) Beat(b Beat) BeatReply {
	if last := l.handing(b); len(last) > 0 {
		l.tickMu.Lock()
		defer l.tickMu.Unlock()
		l.exchange(context.Background(), pass{only: last})
	}
	now := l.cfg.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	held := b.Trust()
	for id, epoch := range b.Epochs {
		c := l.clusters[id]
		switch {
		case c == nil || epoch < c.epoch:
		case epoch == c.epoch:
			c.deploUntil = latest(c.deploUntil, now.Add(b.Trust()))
			held = max(held, c.deploUntil.Sub(now))
		case !c.superseded:
			c.superseded = true
			if epoch > l.epochs[id] {
				l.epochs[id] = epoch
				l.saveEpochs()
			}
			l.cfg.Logf("deplo-cluster: cluster %s is led at epoch %d now, newer than %d: stepping down", id, epoch, c.epoch)
		}
	}
	return BeatReply{OK: true, HoldsMs: held.Milliseconds()}
}

// handing is the clusters a beat hands to another leader that this one still leads.
func (l *Leader) handing(b Beat) map[string]bool {
	now := l.cfg.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[string]bool{}
	for id, epoch := range b.Epochs {
		if c := l.clusters[id]; c != nil && epoch > c.epoch && !c.superseded && c.leads(now) {
			out[id] = true
		}
	}
	return out
}

// Assign records where the leader put one workload and tells the control plane.
type Assign struct {
	Cluster      string   `json:"cluster"`
	Kind         string   `json:"kind"`
	ID           string   `json:"id"`
	Slug         string   `json:"slug"`
	SingleWriter bool     `json:"singleWriter"`
	Balanced     bool     `json:"balanced,omitempty"`
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
		kept = append(kept, lease.Entry{Kind: a.Kind, ID: a.ID, Replica: i, Member: m, Slug: a.Slug, SingleWriter: a.SingleWriter, Balanced: a.Balanced, Epoch: c.epoch})
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

// place records which member runs one replica, decided at this epoch, and returns its entry as it stands now; false
// when the placement does not hold that replica. The caller holds l.mu.
func (c *cluster) place(kind, id string, replica int, member string) (lease.Entry, bool) {
	for i := range c.placement {
		if p := &c.placement[i]; p.Kind == kind && p.ID == id && p.Replica == replica {
			p.Member, p.Epoch = member, c.epoch
			return *p, true
		}
	}
	return lease.Entry{}, false
}

// MemberView is one member as the snapshot shows it.
type MemberView struct {
	ID    string `json:"id"`
	State string `json:"state"`
	// Heard is a member that answered this leader since it took the epoch: until then its "ok" says nothing.
	Heard bool `json:"heard"`
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
	// Line is the members this leader names to lead after it, heir first; Succeed is an epoch it took without Deplo.
	Line    []string `json:"line"`
	Succeed bool     `json:"succeed,omitempty"`
	// Since is the event that began this epoch on this leader: a control plane that adopts it replays from there.
	Since uint64 `json:"since,omitempty"`
	// LedAt is when a quorum took the epoch this leader took without Deplo's word.
	LedAt string `json:"ledAt,omitempty"`
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
		v := ClusterView{Epoch: c.epoch, Superseded: c.superseded, Leads: c.leads(l.cfg.Now()), Members: []MemberView{}, Placement: append([]lease.Entry{}, c.placement...), Line: c.line(l.cfg.Now()), Succeed: c.succeed, Since: c.since}
		if !c.ledAt.IsZero() {
			v.LedAt = c.ledAt.UTC().Format(time.RFC3339Nano)
		}
		for _, m := range c.members {
			v.Members = append(v.Members, MemberView{ID: m.ID, State: string(m.state), Heard: m.heard})
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
