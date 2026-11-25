package leader

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/bundles"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
	"github.com/DeploCloud/deplo-cluster/internal/placement"
)

const (
	// FailoverAfter is how long a member is silent before its work moves; what must never run twice waits
	// until the member has stopped it as well (member.stopped).
	FailoverAfter = 5 * time.Second
	// fenceMargin is what an older member's fence takes to stop its stacks once its lease ran out.
	fenceMargin = 21 * time.Second
	// quickMargin is the same for an agent that kills its stacks all at once (QuickStop): 11 s, and 2 s to spare.
	quickMargin = 13 * time.Second
	// RetryEvery is how often work that found no room is placed again.
	RetryEvery = 30 * time.Second
	// maxStartTries bounds the members tried for one workload in one pass.
	maxStartTries = 3
	// StoppedAnswering is the reason a failover event carries.
	StoppedAnswering = "stopped_answering"
	// DefaultStartGrace is how long after a leader starts a member it has not heard yet waits for it: after a power
	// cut, servers come back minutes apart, and a slower one has not stopped answering (deplo-cluster#5).
	DefaultStartGrace = 2 * time.Minute
	// NotHeardYet is why the work of such a member waits.
	NotHeardYet = "not_heard_yet"
	// SourceAnswered and the two below are why a move that was started is not recorded: see recheck.
	SourceAnswered   = "source_answered"
	PlacementChanged = "placement_changed"
	TargetGone       = "target_gone"
)

// unknown says this leader started a moment ago and has not heard the member yet: it may still be booting, so it
// counts as neither answering nor silent. The caller holds l.mu.
func (l *Leader) unknown(m *member, now time.Time) bool {
	grace := l.cfg.StartGrace
	if grace == 0 {
		grace = DefaultStartGrace
	}
	return m != nil && !m.heard && now.Sub(l.started) < grace
}

// Waiting is a workload that found no room, and the member it left.
type Waiting struct {
	Kind    string    `json:"kind"`
	ID      string    `json:"id"`
	Replica int       `json:"replica"`
	From    string    `json:"from"`
	Reason  string    `json:"reason"`
	Slug    string    `json:"slug"`
	NextTry time.Time `json:"nextTry"`
	// Adopted is one this leader read as waiting in a roster: it does not know which member last had it.
	Adopted bool `json:"adopted,omitempty"`
}

// mayBeHeld says a single-writer workload this leader found waiting may still run on a member it has not
// heard: the old leader may have handed it back. Nobody starts it until each has answered or its lease is over.
func (c *cluster) mayBeHeld(w *Waiting, now time.Time) bool {
	if !w.Adopted || !slices.ContainsFunc(c.placement, func(e lease.Entry) bool {
		return e.Kind == w.Kind && e.ID == w.ID && e.Replica == w.Replica && e.SingleWriter
	}) {
		return false
	}
	for _, m := range c.members {
		if c.presumedAlive(m, now) {
			return true
		}
	}
	return false
}

func entryKey(kind, id string, replica int) string { return fmt.Sprintf("%s/%s/%d", kind, id, replica) }

// memberFacts is every member as the placement rule sees it; the caller holds l.mu.
func (c *cluster) memberFacts(now time.Time) []placement.Member {
	placed := map[string][]string{}
	for _, e := range c.placement {
		if e.Member != "" {
			placed[e.Member] = append(placed[e.Member], e.ID)
		}
	}
	ids := make([]string, 0, len(c.members))
	for id := range c.members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]placement.Member, 0, len(ids))
	for _, id := range ids {
		m := c.members[id]
		_, isGone := c.gone[id]
		// A gone member hosts nothing until it counts again.
		live := now.Sub(m.lastAck) < lease.NotRespondingAfter && !isGone
		out = append(out, placement.Member{ID: id, Online: live, Leased: live, Emptying: m.Emptying, Arch: m.Arch,
			Images: m.Images, FreeMB: m.FreeMB, Workloads: len(placed[id]), DatabaseEnvs: m.DatabaseEnvs, Placed: placed[id],
			DiskFree: m.DiskFree, DiskTotal: m.DiskTotal, Holds: m.Holds})
	}
	return out
}

// PlaceRequest asks which member runs a workload. Members, when given, are fresher facts than the roster's.
type PlaceRequest struct {
	Cluster  string             `json:"cluster"`
	Workload placement.Workload `json:"workload"`
	Members  []placement.Member `json:"members"`
	// Replicas asks for that many, spread across members; Current is where each ordinal runs now.
	Replicas int      `json:"replicas,omitempty"`
	Current  []string `json:"current,omitempty"`
	// Exclude names members never chosen: a standby copy never shares a member with its workload.
	Exclude []string `json:"exclude,omitempty"`
}

// Place answers with a member or a structured refusal; a member that is not leased by this leader is never chosen.
func (l *Leader) Place(r PlaceRequest) (placement.Answer, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	c := l.clusters[r.Cluster]
	if c == nil {
		return placement.Answer{}, errors.New("this leader does not serve that cluster")
	}
	facts := slices.DeleteFunc(c.memberFacts(l.cfg.Now()), func(f placement.Member) bool { return slices.Contains(r.Exclude, f.ID) })
	given := map[string]placement.Member{}
	for _, m := range r.Members {
		given[m.ID] = m
	}
	for i, f := range facts {
		if g, ok := given[f.ID]; ok {
			f.Arch, f.Images, f.FreeMB, f.Emptying, f.DatabaseEnvs = g.Arch, g.Images, g.FreeMB, g.Emptying, g.DatabaseEnvs
			if g.DiskTotal > 0 {
				f.DiskFree, f.DiskTotal, f.Holds = g.DiskFree, g.DiskTotal, g.Holds
			}
			if g.Workloads > f.Workloads {
				f.Workloads = g.Workloads
			}
			facts[i] = f
		}
	}
	if r.Replicas > 0 {
		return c.spread(r, facts), nil
	}
	return placement.Place(r.Cluster, r.Workload, facts), nil
}

type item struct {
	entry lease.Entry
	meta  bundles.Meta
}

func rank(it item) int {
	switch {
	case it.entry.Kind == "database":
		return 0
	case it.meta.HasDomain:
		return 1
	}
	return 2
}

// Order is the failover order: databases first, then workloads with a domain, then the rest, oldest first.
func Order(entries []lease.Entry, meta map[string]bundles.Meta) []lease.Entry {
	items := make([]item, 0, len(entries))
	for _, e := range entries {
		items = append(items, item{e, meta[e.Kind+"/"+e.ID]})
	}
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if ra, rb := rank(a), rank(b); ra != rb {
			return ra < rb
		}
		if a.meta.CreatedAt != b.meta.CreatedAt {
			return a.meta.CreatedAt < b.meta.CreatedAt
		}
		if a.entry.ID != b.entry.ID {
			return a.entry.ID < b.entry.ID
		}
		return a.entry.Replica < b.entry.Replica
	})
	out := make([]lease.Entry, len(items))
	for i, it := range items {
		out[i] = it.entry
	}
	return out
}

func mapIn[V any](keys []string, m map[string]V) []V {
	out := make([]V, 0, len(keys))
	for _, k := range keys {
		out = append(out, m[k])
	}
	return out
}

func replicaMembers(placement []lease.Entry, kind, id string) []string {
	var es []lease.Entry
	for _, e := range placement {
		if e.Kind == kind && e.ID == id {
			es = append(es, e)
		}
	}
	sort.Slice(es, func(i, j int) bool { return es[i].Replica < es[j].Replica })
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = e.Member
	}
	return out
}

// wait leaves a workload without a member and queues it for the next retry.
func (l *Leader) wait(clusterID string, e lease.Entry, from, reason, why string) {
	l.mu.Lock()
	c := l.clusters[clusterID]
	if c == nil {
		l.mu.Unlock()
		return
	}
	e.Member = ""
	c.place(e.Kind, e.ID, e.Replica, "")
	key := entryKey(e.Kind, e.ID, e.Replica)
	c.auth[key] = authority{At: l.cfg.Now(), Entry: e, Told: Told{From: from, Reason: reason, Wait: why}}
	c.waiting[key] = &Waiting{Kind: e.Kind, ID: e.ID, Replica: e.Replica, From: from, Reason: reason, Slug: e.Slug, NextTry: l.cfg.Now().Add(RetryEvery)}
	members := replicaMembers(c.placement, e.Kind, e.ID)
	epoch := c.epoch
	l.saveFailover()
	l.mu.Unlock()
	l.stamp(clusterID, key, l.tellPlacement(clusterID, e.Kind, e.ID, members, epoch).Seq)
}

type failoverJob struct {
	cluster string
	from    string
	entries []lease.Entry
}

type retryJob struct {
	cluster string
	w       Waiting
}

// Failover moves the work of members that have been silent for FailoverAfter and retries what found no room.
// Nothing moves while this leader cannot reach its relays, nor while more than half of a cluster went quiet at
// once: both look like this side's trouble, not the members'.
func (l *Leader) Failover(ctx context.Context) {
	l.foMu.Lock()
	defer l.foMu.Unlock()
	if ctx.Err() != nil || l.cfg.Bundles == nil || (l.cfg.PathOK != nil && !l.cfg.PathOK()) {
		return
	}
	now := l.cfg.Now()
	var jobs []failoverJob
	var retries []retryJob
	l.mu.Lock()
	moves := l.pending
	l.pending = nil
	ids := make([]string, 0, len(l.clusters))
	for id := range l.clusters {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		c := l.clusters[id]
		if !c.leads(now) {
			continue
		}
		silent := 0
		mids := make([]string, 0, len(c.members))
		for mid, m := range c.members {
			mids = append(mids, mid)
			if now.Sub(m.lastAck) < lease.NotRespondingAfter {
				delete(c.moved, mid)
			} else if _, out := c.gone[mid]; !out {
				silent++
			}
		}
		if silent*2 > c.voters() {
			continue
		}
		sort.Strings(mids)
		for _, mid := range mids {
			m := c.members[mid]
			// A lease extended before a restart may still be alive on a member this leader has not heard since.
			if now.Sub(m.lastAck) < FailoverAfter {
				continue
			}
			if l.unknown(m, now) {
				if !m.waitedUnknown {
					m.waitedUnknown = true
					l.cfg.Logf("deplo-cluster: %s has not answered since this leader started %s ago: its work waits, it may still be starting", mid, now.Sub(l.started).Round(time.Second))
				}
				continue
			}
			// What must never run twice waits until the member has stopped it, and for a lease this or another
			// leader may have extended: it moves in a second pass once both are over.
			held := c.presumedAlive(m, now) || now.Before(c.extended[mid].Add(fenceMargin)) || !c.stopped(m, now)
			again := c.moved[mid]
			if again && (held || !m.heldBack) {
				continue
			}
			var es []lease.Entry
			for _, e := range c.placement {
				// A single-writer workload moves only onto a standby copy or a backup another member can read; else it waits.
				if e.Member != mid {
					continue
				}
				// An App goes with its Environment's database, so it waits beside it.
				if e.SingleWriter && !held && (l.restorable(id, e, mid) || l.promotable(c, id, e, mid, now)) ||
					!e.SingleWriter && (!again && (!held || !l.withItsDatabase(id, c, e)) || again && l.withItsDatabase(id, c, e)) {
					es = append(es, e)
				}
			}
			c.moved[mid], m.heldBack = true, held
			if !held {
				m.other = time.Time{}
			}
			jobs = append(jobs, failoverJob{id, mid, es})
		}
		keys := make([]string, 0, len(c.waiting))
		for k := range c.waiting {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if w := c.waiting[k]; !now.Before(w.NextTry) && !c.mayBeHeld(w, now) {
				retries = append(retries, retryJob{id, *w})
			}
		}
	}
	l.mu.Unlock()

	for _, j := range jobs {
		l.runJob(ctx, j)
	}
	for _, r := range l.inOrder(retries) {
		l.retry(ctx, r)
	}
	for _, m := range moves {
		l.runMove(ctx, m)
	}
	for _, c := range l.copiesToGive() {
		l.copyBack(ctx, c)
	}
}

// withItsDatabase says a stateless workload shares an Environment with a database on its own member.
func (l *Leader) withItsDatabase(clusterID string, c *cluster, e lease.Entry) bool {
	m, err := l.cfg.Bundles.Meta(clusterID, e.Kind, e.ID)
	if err != nil || m.Environment == "" {
		return false
	}
	for _, d := range c.placement {
		if d.Member == e.Member && d.Kind == "database" && d.SingleWriter {
			if dm, err := l.cfg.Bundles.Meta(clusterID, d.Kind, d.ID); err == nil && dm.Environment == m.Environment {
				return true
			}
		}
	}
	return false
}

// presumedAlive says whether a lease another leader gave this member may still be running, margin included. Until
// a member vouches that no extension can, that is the longest one; then what the members say, or the plain lease.
func (c *cluster) presumedAlive(m *member, now time.Time) bool {
	if m.other.IsZero() {
		return false
	}
	end := m.other.Add(c.leaseFor(m))
	if !c.vouched {
		end = end.Add(lease.ExtendFor)
	} else if c.unsure.After(end) {
		end = c.unsure
	}
	return now.Before(end.Add(c.marginFor(m)))
}

// marginFor is how long after its lease ran out the member has surely stopped its stacks.
func (c *cluster) marginFor(m *member) time.Duration {
	if c.vouched && m.ShortFence && m.QuickStop {
		return quickMargin
	}
	return fenceMargin
}

// leaseFor is the longest lease another leader may have given a member, extensions aside: the short one only
// while a member vouches that its leaders announce, and Deplo that its agent keeps to the fence it is given.
func (c *cluster) leaseFor(m *member) time.Duration {
	if c.vouched && m.ShortFence {
		return lease.FenceFor
	}
	return lease.FenceAfter
}

// stopped says a silent member has surely stopped its single-writer workloads by now, by its own word or an
// older member's fence.
func (c *cluster) stopped(m *member, now time.Time) bool {
	within := c.leaseFor(m) + c.marginFor(m)
	if m.stoppedWithin > 0 {
		within = m.stoppedWithin
	}
	// ponytail: a member that dies mid-start keeps its databases until that start times out.
	last := m.lastAck
	if m.startEnded.After(last) {
		last = m.startEnded
	}
	return m.startsOut == 0 && now.Sub(last) >= max(within, FailoverAfter)
}

// restorable says whether the workload's bundle names a backup that did not go down with its member.
func (l *Leader) restorable(clusterID string, e lease.Entry, member string) bool {
	m, err := l.cfg.Bundles.Meta(clusterID, e.Kind, e.ID)
	return err == nil && m.Restore != nil && m.Restore.StoreMember != member
}

// inOrder puts each cluster's retries in the failover order: databases first, also the second time round.
func (l *Leader) inOrder(retries []retryJob) []retryJob {
	var clusters []string
	by := map[string][]retryJob{}
	for _, r := range retries {
		if by[r.cluster] == nil {
			clusters = append(clusters, r.cluster)
		}
		by[r.cluster] = append(by[r.cluster], r)
	}
	out := make([]retryJob, 0, len(retries))
	for _, id := range clusters {
		meta, jobs := map[string]bundles.Meta{}, map[string]retryJob{}
		var entries []lease.Entry
		for _, r := range by[id] {
			if m, err := l.cfg.Bundles.Meta(id, r.w.Kind, r.w.ID); err == nil {
				meta[r.w.Kind+"/"+r.w.ID] = m
			}
			jobs[entryKey(r.w.Kind, r.w.ID, r.w.Replica)] = r
			entries = append(entries, lease.Entry{Kind: r.w.Kind, ID: r.w.ID, Replica: r.w.Replica})
		}
		for _, e := range Order(entries, meta) {
			out = append(out, jobs[entryKey(e.Kind, e.ID, e.Replica)])
		}
	}
	return out
}

func (l *Leader) runJob(ctx context.Context, j failoverJob) {
	meta := map[string]bundles.Meta{}
	var movable []lease.Entry
	for _, e := range j.entries {
		if m, err := l.cfg.Bundles.Meta(j.cluster, e.Kind, e.ID); err == nil {
			meta[e.Kind+"/"+e.ID] = m
			movable = append(movable, e)
		}
	}
	type left struct {
		Kind     string `json:"kind"`
		ID       string `json:"workloadId"`
		Reason   string `json:"reason"`
		NeededMB int    `json:"neededMb"`
	}
	moved, waiting := []string{}, []left{}
	again := false
	var mu sync.Mutex
	do := func(e lease.Entry) {
		o := l.relocate(ctx, j.cluster, e.Kind, e.ID, e.Replica, j.from, false, nil)
		mu.Lock()
		defer mu.Unlock()
		switch {
		case o.skipped:
			again = again || o.again
		case o.refusal != nil:
			l.wait(j.cluster, e, j.from, StoppedAnswering, string(o.refusal.Reason))
			if !slices.ContainsFunc(waiting, func(w left) bool { return w.Kind == e.Kind && w.ID == e.ID }) {
				waiting = append(waiting, left{e.Kind, e.ID, string(o.refusal.Reason), o.refusal.NeededMB})
			}
		case !slices.Contains(moved, e.ID):
			moved = append(moved, e.ID)
		}
	}
	// Databases one at a time and first, as their Apps need them. Then each App with data on its own, so a slow
	// start holds none of the others (eu, 24 Nov 2026: lab-web waited 25 s behind fise), beside the Apps without
	// data, still one after another: those stop moving once their member answers again mid-pass.
	var plain []lease.Entry
	var ids []string
	writers := map[string][]lease.Entry{}
	for _, e := range Order(movable, meta) {
		switch {
		case e.Kind == "database":
			do(e)
		case e.SingleWriter:
			if writers[e.ID] == nil {
				ids = append(ids, e.ID)
			}
			writers[e.ID] = append(writers[e.ID], e)
		default:
			plain = append(plain, e)
		}
	}
	var wg sync.WaitGroup
	for _, chain := range append([][]lease.Entry{plain}, mapIn(ids, writers)...) {
		wg.Go(func() {
			for _, e := range chain {
				do(e)
			}
		})
	}
	wg.Wait()
	if again {
		// What this pass could not decide is not dealt with: the next pass looks at the member afresh.
		l.mu.Lock()
		if c := l.clusters[j.cluster]; c != nil {
			delete(c.moved, j.from)
		}
		l.mu.Unlock()
	}
	if len(moved)+len(waiting) > 0 {
		l.log.Append("failover", j.cluster, map[string]any{"member": j.from, "reason": StoppedAnswering, "moved": moved, "waiting": waiting})
	}
}

func (l *Leader) retry(ctx context.Context, r retryJob) {
	e := lease.Entry{Kind: r.w.Kind, ID: r.w.ID, Replica: r.w.Replica, Slug: r.w.Slug}
	o := l.relocate(ctx, r.cluster, e.Kind, e.ID, e.Replica, r.w.From, true, nil)
	if o.member != "" || (o.skipped && !o.again) {
		if o.skipped {
			l.mu.Lock()
			if c := l.clusters[r.cluster]; c != nil {
				delete(c.waiting, entryKey(e.Kind, e.ID, e.Replica))
				l.saveFailover()
			}
			l.mu.Unlock()
		}
		return
	}
	l.mu.Lock()
	if c := l.clusters[r.cluster]; c != nil {
		key := entryKey(e.Kind, e.ID, e.Replica)
		if w := c.waiting[key]; w != nil {
			w.NextTry = l.cfg.Now().Add(RetryEvery)
			// A retry may be refused for another reason than the first try was.
			if a, ok := c.auth[key]; ok && o.refusal != nil {
				a.Wait = string(o.refusal.Reason)
				c.auth[key] = a
			}
			l.saveFailover()
		}
	}
	l.mu.Unlock()
}

// Drain extends every member's lease before a planned restart, so the members do not stop their single-writer
// workloads while this leader is away. It asks a quorum again first: a leader that was replaced extends nothing.
func (l *Leader) Drain(ctx context.Context) {
	l.tickMu.Lock()
	defer l.tickMu.Unlock()
	l.mu.Lock()
	for _, c := range l.clusters {
		c.sure = time.Time{}
	}
	l.mu.Unlock()
	l.exchange(ctx, pass{probe: true})
	// Every member hears an extension may come before any is given, so a leader that takes over learns of it from
	// whichever member it reaches; a cluster where one does not answer gets none.
	told := l.exchange(ctx, pass{announce: true})
	l.exchange(ctx, pass{extend: lease.ExtendFor, only: told})
}
