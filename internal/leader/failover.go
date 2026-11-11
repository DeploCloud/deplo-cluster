package leader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/bundles"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
	"github.com/DeploCloud/deplo-cluster/internal/placement"
	"github.com/DeploCloud/deplo-cluster/internal/wire"
)

const (
	// FailoverAfter is how long a member is silent before its work moves: the 60 s fence plus a margin, so
	// the member has stopped what must never run twice before anything starts elsewhere.
	FailoverAfter = 81 * time.Second
	// RetryEvery is how often work that found no room is placed again.
	RetryEvery = 30 * time.Second
	// maxStartTries bounds the members tried for one workload in one pass.
	maxStartTries = 3
	// StoppedAnswering is the reason a failover event carries.
	StoppedAnswering = "stopped_answering"
)

// Waiting is a workload that found no room, and the member it left.
type Waiting struct {
	Kind    string    `json:"kind"`
	ID      string    `json:"id"`
	Replica int       `json:"replica"`
	From    string    `json:"from"`
	Reason  string    `json:"reason"`
	Slug    string    `json:"slug"`
	NextTry time.Time `json:"nextTry"`
}

func entryKey(kind, id string, replica int) string { return fmt.Sprintf("%s/%s/%d", kind, id, replica) }

type persisted struct {
	Waiting   map[string]*Waiting  `json:"waiting"`
	Auth      map[string]authority `json:"auth"`
	Extended  map[string]time.Time `json:"extended,omitempty"`
	Returning map[string]bool      `json:"returning,omitempty"`
}

func (l *Leader) newCluster(id string) *cluster {
	c := &cluster{members: map[string]*member{}, moved: map[string]bool{}, returning: map[string]bool{}, waiting: map[string]*Waiting{}, auth: map[string]authority{}, extended: map[string]time.Time{}}
	if p, ok := l.restored[id]; ok {
		if p.Extended != nil {
			c.extended = p.Extended
		}
		if p.Returning != nil {
			c.returning = p.Returning
		}
		if p.Waiting != nil {
			c.waiting = p.Waiting
		}
		if p.Auth != nil {
			c.auth = p.Auth
		}
		delete(l.restored, id)
	}
	return c
}

func (l *Leader) loadFailover() {
	l.restored = map[string]persisted{}
	if raw, err := os.ReadFile(filepath.Join(l.cfg.StateDir, "failover.json")); err == nil {
		_ = json.Unmarshal(raw, &l.restored)
	}
}

// saveFailover keeps what a restarted leader must not forget; the caller holds l.mu.
func (l *Leader) saveFailover() {
	if l.cfg.StateDir == "" {
		return
	}
	out := map[string]persisted{}
	for id, p := range l.restored {
		out[id] = p
	}
	for id, c := range l.clusters {
		out[id] = persisted{Waiting: c.waiting, Auth: c.auth, Extended: c.extended, Returning: c.returning}
	}
	raw, _ := json.Marshal(out)
	tmp := filepath.Join(l.cfg.StateDir, "failover.json.tmp")
	if os.WriteFile(tmp, raw, 0o600) == nil {
		_ = os.Rename(tmp, filepath.Join(l.cfg.StateDir, "failover.json"))
	}
}

// reconcilePlacement is the roster's placement with the leader's own moves laid over it. A move stands until the
// roster repeats it, however long that takes; a workload the roster no longer names is the control plane's to drop.
func (l *Leader) reconcilePlacement(c *cluster, roster []lease.Entry) []lease.Entry {
	out := append([]lease.Entry(nil), roster...)
	at := map[string]int{}
	for i, e := range out {
		at[entryKey(e.Kind, e.ID, e.Replica)] = i
	}
	for k, a := range c.auth {
		i, named := at[k]
		switch {
		case !named:
			delete(c.auth, k)
			delete(c.waiting, k)
		case out[i].Member == a.Entry.Member:
			delete(c.auth, k)
		default:
			out[i].Member = a.Entry.Member
		}
	}
	l.saveFailover()
	return out
}

func (l *Leader) loadRosters() {
	var saved map[string]Roster
	raw, err := os.ReadFile(filepath.Join(l.cfg.StateDir, "rosters.json"))
	if err != nil || json.Unmarshal(raw, &saved) != nil {
		return
	}
	for _, r := range saved {
		_ = l.SetRoster(r)
	}
}

// saveRosters keeps every cluster's last roster; the caller holds l.mu.
func (l *Leader) saveRosters() {
	if l.cfg.StateDir == "" {
		return
	}
	out := map[string]Roster{}
	for id, c := range l.clusters {
		out[id] = c.roster
	}
	raw, _ := json.Marshal(out)
	tmp := filepath.Join(l.cfg.StateDir, "rosters.json.tmp")
	if os.WriteFile(tmp, raw, 0o600) == nil {
		_ = os.Rename(tmp, filepath.Join(l.cfg.StateDir, "rosters.json"))
	}
}

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
		live := now.Sub(m.lastAck) < lease.NotRespondingAfter
		out = append(out, placement.Member{ID: id, Online: live, Leased: live, Emptying: m.Emptying, Arch: m.Arch,
			Images: m.Images, FreeMB: m.FreeMB, Workloads: len(placed[id]), DatabaseEnvs: m.DatabaseEnvs, Placed: placed[id]})
	}
	return out
}

// PlaceRequest asks which member runs a workload. Members, when given, are fresher facts than the roster's.
type PlaceRequest struct {
	Cluster  string             `json:"cluster"`
	Workload placement.Workload `json:"workload"`
	Members  []placement.Member `json:"members"`
}

// Place answers with a member or a structured refusal; a member that is not leased by this leader is never chosen.
func (l *Leader) Place(r PlaceRequest) (placement.Answer, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	c := l.clusters[r.Cluster]
	if c == nil {
		return placement.Answer{}, errors.New("this leader does not serve that cluster")
	}
	facts := c.memberFacts(l.cfg.Now())
	given := map[string]placement.Member{}
	for _, m := range r.Members {
		given[m.ID] = m
	}
	for i, f := range facts {
		if g, ok := given[f.ID]; ok {
			f.Arch, f.Images, f.FreeMB, f.Emptying, f.DatabaseEnvs = g.Arch, g.Images, g.FreeMB, g.Emptying, g.DatabaseEnvs
			if g.Workloads > f.Workloads {
				f.Workloads = g.Workloads
			}
			facts[i] = f
		}
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

type outcome struct {
	member  string
	refusal *placement.Refusal
	skipped bool
}

func (l *Leader) startOrder(b bundles.Bundle) *lease.Start {
	s := b.Stack
	auth := make([]lease.RegistryAuth, 0, len(s.RegistryAuth))
	for _, a := range s.RegistryAuth {
		auth = append(auth, lease.RegistryAuth(a))
	}
	mounts := make([]lease.MountFile, 0, len(s.Mounts))
	for _, m := range s.Mounts {
		mounts = append(mounts, lease.MountFile(m))
	}
	st := &lease.Start{DeploymentID: b.DeploymentID, Slug: s.Slug, ProjectID: s.ProjectID, ImageRef: s.ImageRef, ComposeYAML: s.ComposeYAML,
		Network: s.Network, Env: s.Env, RegistryAuth: auth, ComposeUpArgs: s.ComposeUpArgs, Ready: s.Ready, RestartRule: s.RestartRule,
		StartAfterReady: s.StartAfterReady, Mounts: mounts, FilesBindSources: s.FilesBindSources}
	if r := b.Restore; r != nil {
		st.Restore = &lease.Restore{BackupID: r.BackupID, TakenAt: r.TakenAt, Request: r.Request}
	}
	return st
}

// sendStart gives one member the order to bring a workload up and waits for it to answer.
func (l *Leader) sendStart(ctx context.Context, fp string, o lease.Order) error {
	// ponytail: one start at a time, so a long restore holds the Apps behind it; run restores beside them if that hurts.
	limit := lease.StartTimeout
	if o.Start.Restore != nil {
		limit = lease.RestoreTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	c, err := l.cfg.Dial(ctx, wire.Party{Fingerprint: fp, Role: wire.RoleMember})
	if err != nil {
		return err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(limit))
	r, err := lease.Send(c, o)
	switch {
	case err != nil:
		return err
	case !r.OK || !r.Started:
		if r.Error != "" {
			return errors.New(r.Error)
		}
		return errors.New("the member did not bring it up")
	case o.Start.Restore != nil && !r.Restored:
		return errors.New("the member brought it up without restoring its backup")
	}
	return nil
}

// relocate places one workload on a member that can take it and has it started there. It changes nothing
// on any failure: the workload stays where it was recorded, or waits.
func (l *Leader) relocate(ctx context.Context, clusterID, kind, id string, replica int, from string) outcome {
	meta, err := l.cfg.Bundles.Meta(clusterID, kind, id)
	if err != nil {
		l.cfg.Logf("deplo-cluster: %s %s has no bundle, so it does not fail over", kind, id)
		return outcome{skipped: true}
	}
	b, err := l.cfg.Bundles.Unseal(clusterID, kind, id)
	if err != nil {
		l.cfg.Logf("deplo-cluster: %s %s: %v", kind, id, err)
		return outcome{skipped: true}
	}
	exclude := map[string]bool{}
	for try := 0; try < maxStartTries; try++ {
		l.mu.Lock()
		c := l.clusters[clusterID]
		if c == nil || c.superseded {
			l.mu.Unlock()
			return outcome{skipped: true}
		}
		var entry lease.Entry
		for _, e := range c.placement {
			if e.Kind == kind && e.ID == id && e.Replica == replica {
				entry = e
			}
		}
		// What keeps data never starts empty: without a backup to restore it waits for its member.
		if entry.SingleWriter && b.Restore == nil {
			l.mu.Unlock()
			return outcome{skipped: true}
		}
		var facts []placement.Member
		for _, f := range c.memberFacts(l.cfg.Now()) {
			if f.ID != from && !exclude[f.ID] && !c.members[f.ID].NoFailover && canRestore(b.Restore, c.members[f.ID]) {
				facts = append(facts, f)
			}
		}
		w := placement.Workload{Kind: kind, ID: id, Name: meta.Name, Environment: meta.Environment, Image: meta.Image, Archs: meta.Archs, LimitMB: meta.LimitMB, PeakMB: meta.PeakMB}
		ans := placement.Place(clusterID, w, facts)
		if ans.Refusal != nil {
			l.mu.Unlock()
			return outcome{refusal: ans.Refusal}
		}
		target := c.members[ans.Member]
		next := append([]lease.Entry(nil), c.placement...)
		moved := false
		for i := range next {
			if next[i].Kind == kind && next[i].ID == id && next[i].Replica == replica {
				next[i].Member, moved = ans.Member, true
			}
		}
		if !moved {
			next = append(next, lease.Entry{Kind: kind, ID: id, Replica: replica, Member: ans.Member, Slug: b.Stack.Slug})
		}
		order := lease.Order{Cluster: clusterID, Member: ans.Member, Epoch: c.epoch, Placement: next, Start: l.startOrder(b), Return: c.returning[ans.Member]}
		fp := target.Fingerprint
		l.mu.Unlock()

		if err := l.sendStart(ctx, fp, order); err != nil {
			l.cfg.Logf("deplo-cluster: %s could not start %s: %v", ans.Member, b.Stack.Slug, err)
			exclude[ans.Member] = true
			continue
		}
		if entry.Slug == "" {
			entry.Slug = b.Stack.Slug
		}
		entry.Kind, entry.ID, entry.Replica = kind, id, replica
		entry.Member = ans.Member
		l.commit(clusterID, entry, placement.Needed(w), from, b.Restore)
		return outcome{member: ans.Member}
	}
	return outcome{refusal: &placement.Refusal{Reason: "start_failed", NeededMB: placement.Needed(placement.Workload{LimitMB: meta.LimitMB, PeakMB: meta.PeakMB}), Cluster: clusterID, Workload: id}}
}

// canRestore says whether a member can restore r: its agent knows how, and a backup on a member's disk is read there only.
func canRestore(r *bundles.Restore, m *member) bool {
	return r == nil || (!m.NoRestore && (r.StoreMember == "" || r.StoreMember == m.ID))
}

// commit records where a workload now runs, takes its memory off that member and tells the control plane.
func (l *Leader) commit(clusterID string, e lease.Entry, needMB int, from string, restored *bundles.Restore) {
	l.mu.Lock()
	c := l.clusters[clusterID]
	if c == nil {
		l.mu.Unlock()
		return
	}
	found := false
	for i := range c.placement {
		p := &c.placement[i]
		if p.Kind == e.Kind && p.ID == e.ID && p.Replica == e.Replica {
			p.Member, found = e.Member, true
		}
	}
	if !found {
		c.placement = append(c.placement, e)
	}
	if m := c.members[e.Member]; m != nil && needMB > 0 {
		m.FreeMB -= needMB
	}
	key := entryKey(e.Kind, e.ID, e.Replica)
	a := authority{At: l.cfg.Now(), Entry: e, From: from, Reason: StoppedAnswering}
	moved := map[string]any{"kind": e.Kind, "workloadId": e.ID, "from": from, "to": e.Member, "reason": StoppedAnswering}
	if restored != nil {
		a.BackupID, a.BackupAt = restored.BackupID, restored.TakenAt
		moved["backupId"], moved["backupAt"] = restored.BackupID, restored.TakenAt
	}
	c.auth[key] = a
	delete(c.waiting, key)
	members := replicaMembers(c.placement, e.Kind, e.ID)
	epoch := c.epoch
	l.saveFailover()
	l.mu.Unlock()
	l.log.Append("placement", clusterID, map[string]any{"kind": e.Kind, "workloadId": e.ID, "members": members, "epoch": epoch})
	ev := l.log.Append("failed_over", clusterID, moved)
	l.stamp(clusterID, key, ev.Seq)
}

// stamp notes which event told the control plane about a move, so one that missed the event can tell.
func (l *Leader) stamp(clusterID, key string, seq uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if c := l.clusters[clusterID]; c != nil {
		if a, ok := c.auth[key]; ok {
			a.Seq = seq
			c.auth[key] = a
			l.saveFailover()
		}
	}
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
func (l *Leader) wait(clusterID string, e lease.Entry, from, reason string) {
	l.mu.Lock()
	c := l.clusters[clusterID]
	if c == nil {
		l.mu.Unlock()
		return
	}
	e.Member = ""
	for i := range c.placement {
		p := &c.placement[i]
		if p.Kind == e.Kind && p.ID == e.ID && p.Replica == e.Replica {
			p.Member = ""
		}
	}
	key := entryKey(e.Kind, e.ID, e.Replica)
	c.auth[key] = authority{At: l.cfg.Now(), Entry: e, From: from, Reason: reason}
	c.waiting[key] = &Waiting{Kind: e.Kind, ID: e.ID, Replica: e.Replica, From: from, Reason: reason, Slug: e.Slug, NextTry: l.cfg.Now().Add(RetryEvery)}
	members := replicaMembers(c.placement, e.Kind, e.ID)
	epoch := c.epoch
	l.saveFailover()
	l.mu.Unlock()
	ev := l.log.Append("placement", clusterID, map[string]any{"kind": e.Kind, "workloadId": e.ID, "members": members, "epoch": epoch})
	l.stamp(clusterID, key, ev.Seq)
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
	if l.cfg.Bundles == nil || (l.cfg.PathOK != nil && !l.cfg.PathOK()) {
		return
	}
	now := l.cfg.Now()
	var jobs []failoverJob
	var retries []retryJob
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
		silent := 0
		mids := make([]string, 0, len(c.members))
		for mid, m := range c.members {
			mids = append(mids, mid)
			if now.Sub(m.lastAck) < lease.NotRespondingAfter {
				delete(c.moved, mid)
			} else {
				silent++
			}
		}
		if silent*2 > len(c.members) {
			continue
		}
		sort.Strings(mids)
		for _, mid := range mids {
			// A lease extended before a restart may still be alive on a member this leader has not heard since.
			if now.Sub(c.members[mid].lastAck) < FailoverAfter || c.moved[mid] || now.Before(c.extended[mid].Add(FailoverAfter-lease.FenceAfter)) {
				continue
			}
			var es []lease.Entry
			for _, e := range c.placement {
				// A single-writer workload moves only onto a backup another member can read; else it waits for its member.
				if e.Member == mid && (!e.SingleWriter || l.restorable(id, e, mid)) {
					es = append(es, e)
				}
			}
			c.moved[mid] = true
			jobs = append(jobs, failoverJob{id, mid, es})
		}
		keys := make([]string, 0, len(c.waiting))
		for k := range c.waiting {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if w := c.waiting[k]; !now.Before(w.NextTry) {
				retries = append(retries, retryJob{id, *w})
			}
		}
	}
	l.mu.Unlock()

	for _, j := range jobs {
		l.runJob(ctx, j)
	}
	for _, r := range retries {
		l.retry(ctx, r)
	}
}

// restorable says whether the workload's bundle names a backup that did not go down with its member.
func (l *Leader) restorable(clusterID string, e lease.Entry, member string) bool {
	m, err := l.cfg.Bundles.Meta(clusterID, e.Kind, e.ID)
	return err == nil && m.Restore != nil && m.Restore.StoreMember != member
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
	for _, e := range Order(movable, meta) {
		o := l.relocate(ctx, j.cluster, e.Kind, e.ID, e.Replica, j.from)
		switch {
		case o.skipped:
		case o.refusal != nil:
			l.wait(j.cluster, e, j.from, StoppedAnswering)
			waiting = append(waiting, left{e.Kind, e.ID, string(o.refusal.Reason), o.refusal.NeededMB})
		default:
			moved = append(moved, e.ID)
		}
	}
	if len(moved)+len(waiting) > 0 {
		l.log.Append("failover", j.cluster, map[string]any{"member": j.from, "reason": StoppedAnswering, "moved": moved, "waiting": waiting})
	}
}

func (l *Leader) retry(ctx context.Context, r retryJob) {
	e := lease.Entry{Kind: r.w.Kind, ID: r.w.ID, Replica: r.w.Replica, Slug: r.w.Slug}
	o := l.relocate(ctx, r.cluster, e.Kind, e.ID, e.Replica, r.w.From)
	if o.member != "" || o.skipped {
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
		if w := c.waiting[entryKey(e.Kind, e.ID, e.Replica)]; w != nil {
			w.NextTry = l.cfg.Now().Add(RetryEvery)
			l.saveFailover()
		}
	}
	l.mu.Unlock()
}

// Drain extends every member's lease before a planned restart, so the members do not stop their single-writer
// workloads while this leader is away. It returns once each member answered or the context ended.
func (l *Leader) Drain(ctx context.Context) {
	type job struct {
		cluster string
		m       *member
		order   lease.Order
	}
	var jobs []job
	l.mu.Lock()
	// Kept before the order leaves: a member that took it and whose answer was lost is extended all the same.
	until := l.cfg.Now().Add(lease.FenceAfter + lease.ExtendFor)
	for id, c := range l.clusters {
		if c.superseded {
			continue
		}
		for _, m := range c.members {
			c.extended[m.ID] = until
			jobs = append(jobs, job{id, m, lease.Order{Cluster: id, Member: m.ID, Epoch: c.epoch, Placement: append([]lease.Entry(nil), c.placement...), ExtendSeconds: int(lease.ExtendFor / time.Second), Return: c.returning[m.ID]}})
		}
	}
	l.saveFailover()
	l.mu.Unlock()
	done := make(chan struct{}, len(jobs))
	for _, j := range jobs {
		go func() {
			l.renew(ctx, j.cluster, j.m, j.order)
			done <- struct{}{}
		}()
	}
	for range jobs {
		select {
		case <-done:
		case <-ctx.Done():
			return
		}
	}
}
