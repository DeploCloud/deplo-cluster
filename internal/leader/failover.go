package leader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
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
		if m.presumedAlive(now) {
			return true
		}
	}
	return false
}

func entryKey(kind, id string, replica int) string { return fmt.Sprintf("%s/%s/%d", kind, id, replica) }

type persisted struct {
	Waiting   map[string]*Waiting             `json:"waiting"`
	Auth      map[string]authority            `json:"auth"`
	Extended  map[string]time.Time            `json:"extended,omitempty"`
	Returning map[string]bool                 `json:"returning,omitempty"`
	Left      map[string]map[string]time.Time `json:"left,omitempty"`
	Kept      map[string]map[string]time.Time `json:"kept,omitempty"`
}

func (l *Leader) newCluster(id string) *cluster {
	c := &cluster{members: map[string]*member{}, moved: map[string]bool{}, returning: map[string]bool{}, waiting: map[string]*Waiting{}, auth: map[string]authority{}, extended: map[string]time.Time{}, left: map[string]map[string]time.Time{}, kept: map[string]map[string]time.Time{}, starting: map[string]bool{}}
	if p, ok := l.restored[id]; ok {
		if p.Extended != nil {
			c.extended = p.Extended
		}
		if p.Returning != nil {
			c.returning = p.Returning
		}
		if p.Left != nil {
			c.left = p.Left
		}
		if p.Kept != nil {
			c.kept = p.Kept
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
	raw, err := os.ReadFile(filepath.Join(l.cfg.StateDir, "failover.json"))
	if err == nil {
		err = json.Unmarshal(raw, &l.restored)
	}
	// Safe to go on without: a member not heard since this start is presumed to hold the longest lease.
	if err != nil && !os.IsNotExist(err) {
		l.restored = map[string]persisted{}
		l.cfg.Logf("deplo-cluster: failover.json cannot be read (%v): what this leader had moved or left waiting is forgotten", err)
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
		out[id] = persisted{Waiting: c.waiting, Auth: c.auth, Extended: c.extended, Returning: c.returning, Left: c.left, Kept: c.kept}
	}
	l.keep("failover.json", out)
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

// adoptWaiting takes over what the roster says is waiting for room, so a leader that starts leading keeps
// trying what the one before it could not place; the caller holds l.mu.
func (l *Leader) adoptWaiting(c *cluster, now time.Time) {
	for _, e := range c.placement {
		key := entryKey(e.Kind, e.ID, e.Replica)
		if e.Member == "" && c.waiting[key] == nil {
			c.waiting[key] = &Waiting{Kind: e.Kind, ID: e.ID, Replica: e.Replica, Reason: StoppedAnswering, Slug: e.Slug, NextTry: now.Add(RetryEvery), Adopted: true}
		}
	}
	l.saveFailover()
}

func (l *Leader) loadRosters() {
	var saved map[string]Roster
	raw, err := os.ReadFile(filepath.Join(l.cfg.StateDir, "rosters.json"))
	if err == nil {
		err = json.Unmarshal(raw, &saved)
	}
	if err != nil {
		if !os.IsNotExist(err) {
			l.cfg.Logf("deplo-cluster: rosters.json cannot be read (%v): serving no cluster until Deplo sends its roster", err)
		}
		return
	}
	for _, r := range saved {
		// Refused: another leader took over while this one was down, so what it had moved is not its to keep.
		if errors.Is(l.SetRoster(r), ErrOlderEpoch) {
			delete(l.restored, r.Cluster)
		}
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
	l.keep("rosters.json", out)
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

type outcome struct {
	member  string
	refusal *placement.Refusal
	skipped bool
	// why says what skipped a move; again is a skip that the next pass may not repeat, so the work is tried again.
	why   string
	again bool
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
	case o.Start.Promote != nil && !r.Promoted:
		return errors.New("the member brought it up without promoting its standby copy")
	}
	return nil
}

// relocate places one workload on a member that can take it and has it started there. It changes nothing
// on any failure: the workload stays where it was recorded, or waits. asked is the bundle of a move a person
// ordered, used in place of the kept one. waited is a retry of what found no room. Whether the workload may
// still run where it was is asked again right before each start, and whether this leader still leads after it.
func (l *Leader) relocate(ctx context.Context, clusterID, kind, id string, replica int, from string, waited bool, asked *bundles.Bundle) outcome {
	meta, err := l.cfg.Bundles.Meta(clusterID, kind, id)
	if err != nil {
		l.cfg.Logf("deplo-cluster: %s %s has no bundle, so it does not fail over", kind, id)
		return outcome{skipped: true, why: NotPlaced}
	}
	var b bundles.Bundle
	if asked != nil {
		b = *asked
	} else if b, err = l.cfg.Bundles.Unseal(clusterID, kind, id); err != nil {
		l.cfg.Logf("deplo-cluster: %s %s: %v", kind, id, err)
		return outcome{skipped: true}
	}
	exclude := map[string]bool{}
	if asked != nil {
		// A promotion is a start too: the member a person's move leaves is checked before it, never two writers.
		left, why := l.leftNow(clusterID, kind, id)
		if why != "" {
			return outcome{skipped: true, why: why}
		}
		from = left
	}
	// An automatic move asks again, a person's move was asked above and is asked again below.
	gate := func(c *cluster) string {
		if asked != nil {
			return ""
		}
		_, why := c.mayStart(kind, id, replica, from, waited, l.cfg.Now())
		return why
	}
	// A standby copy that follows the workload is promoted where it is; only when that fails is a backup restored.
	if sb := l.standbyFor(clusterID, kind, id, from); sb != "" && b.Promote != nil {
		switch l.promote(ctx, clusterID, kind, id, replica, from, sb, b, gate) {
		case promoteOK:
			l.noteDatabaseEnv(clusterID, kind, sb, meta.Environment)
			return outcome{member: sb}
		case notNow:
			return outcome{skipped: true, again: true, why: StillAnswering}
		}
		exclude[sb] = true
	}
	for try := 0; try < maxStartTries; try++ {
		l.mu.Lock()
		c := l.clusters[clusterID]
		if c == nil || !c.leads(l.cfg.Now()) {
			l.mu.Unlock()
			return outcome{skipped: true, again: c != nil, why: NotLeading}
		}
		if asked != nil {
			left, why := c.leftBy(kind, id, l.cfg.Now())
			if why != "" {
				l.mu.Unlock()
				return outcome{skipped: true, why: why}
			}
			from = left
		}
		var entry lease.Entry
		for _, e := range c.placement {
			if e.Kind == kind && e.ID == id && e.Replica == replica {
				entry = e
			}
		}
		if why := gate(c); why != "" {
			l.mu.Unlock()
			return outcome{skipped: true, again: why == StillAnswering, why: why}
		}
		// What keeps data never starts empty: without a backup to restore it waits for its member.
		if entry.SingleWriter && b.Restore == nil {
			l.mu.Unlock()
			return outcome{skipped: true}
		}
		// Replicas kept on one member move as one stack: the first is started, the rest follow it.
		spread := b.Stack.Scale != ""
		if to := sibling(c.placement, kind, id, replica, from); to != "" && !spread {
			l.mu.Unlock()
			l.follow(clusterID, lease.Entry{Kind: kind, ID: id, Replica: replica, Member: to, Slug: b.Stack.Slug}, from)
			return outcome{member: to}
		}
		var facts []placement.Member
		for _, f := range c.memberFacts(l.cfg.Now()) {
			if f.ID != from && !exclude[f.ID] && !c.members[f.ID].NoFailover && canRestore(b.Restore, c.members[f.ID]) {
				facts = append(facts, f)
			}
		}
		w := placement.Workload{Kind: kind, ID: id, Name: meta.Name, Environment: meta.Environment, Image: meta.Image, Archs: meta.Archs, LimitMB: meta.LimitMB, PeakMB: meta.PeakMB}
		if n := countOn(c.placement, kind, id, ""); n > 1 && !spread {
			w.LimitMB = placement.Needed(w) * n
		}
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
		order := lease.Order{Cluster: clusterID, Member: ans.Member, Epoch: c.epoch, Placement: c.orderPlacement(next), Start: l.startOrder(b), Return: c.returning[ans.Member]}
		if spread {
			order.Start.ComposeYAML = strings.ReplaceAll(order.Start.ComposeYAML, b.Stack.Scale, strconv.Itoa(countOn(next, kind, id, ans.Member)))
		}
		fp := target.Fingerprint
		key := entryKey(kind, id, replica)
		c.starting[key] = true
		l.mu.Unlock()

		began := time.Now()
		if err := l.sendStart(ctx, fp, order); err != nil {
			l.endStart(clusterID, key)
			l.cfg.Logf("deplo-cluster: %s could not start %s: %v", ans.Member, b.Stack.Slug, err)
			exclude[ans.Member] = true
			continue
		}
		if entry.Slug == "" {
			entry.Slug = b.Stack.Slug
		}
		entry.Kind, entry.ID, entry.Replica = kind, id, replica
		entry.Member = ans.Member
		if !l.commit(clusterID, entry, placement.Needed(w), from, b.Restore, time.Since(began), false, order.Epoch) {
			return outcome{skipped: true, why: NotLeading}
		}
		l.noteDatabaseEnv(clusterID, kind, ans.Member, meta.Environment)
		return outcome{member: ans.Member}
	}
	return outcome{refusal: &placement.Refusal{Reason: "start_failed", NeededMB: placement.Needed(placement.Workload{LimitMB: meta.LimitMB, PeakMB: meta.PeakMB}), Cluster: clusterID, Workload: id}}
}

// canRestore says whether a member can restore r: its agent knows how, and a backup on a member's disk is read there only.
func canRestore(r *bundles.Restore, m *member) bool {
	return r == nil || (!m.NoRestore && (r.StoreMember == "" || r.StoreMember == m.ID))
}

// mayStart finds the entry an automatic move is about and says why it may not start now: the roster dropped it,
// it is no longer where the pass found it, or the member it leaves answers again or may still hold a lease.
// The caller holds l.mu.
func (c *cluster) mayStart(kind, id string, replica int, from string, waited bool, now time.Time) (lease.Entry, string) {
	var entry lease.Entry
	found := false
	for _, e := range c.placement {
		if e.Kind == kind && e.ID == id && e.Replica == replica {
			entry, found = e, true
		}
	}
	switch {
	case !found:
		return entry, NotPlaced
	case waited && entry.Member != "", !waited && entry.Member != from:
		return entry, Returned
	case waited:
		return entry, ""
	}
	m := c.members[from]
	if m == nil {
		return entry, ""
	}
	held := m.presumedAlive(now) || now.Before(c.extended[from].Add(FailoverAfter-lease.FenceAfter))
	if now.Sub(m.lastAck) < FailoverAfter || (entry.SingleWriter && held) {
		return entry, StillAnswering
	}
	return entry, ""
}

// endStart forgets a start that is over, so the workload is shown where it is recorded again.
func (l *Leader) endStart(clusterID, key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if c := l.clusters[clusterID]; c != nil {
		delete(c.starting, key)
	}
}

// told is the placement a renewal carries: a workload whose start is under way is shown as placed nowhere, so
// the member it left neither runs it again nor removes it before the start has an outcome. The caller holds l.mu.
func (c *cluster) told() []lease.Entry {
	out := append([]lease.Entry(nil), c.placement...)
	for i, e := range out {
		if c.starting[entryKey(e.Kind, e.ID, e.Replica)] {
			out[i].Member = ""
		}
	}
	return out
}

// noteDatabaseEnv has the Apps of a database's Environment prefer the member it just moved to, in this pass too.
func (l *Leader) noteDatabaseEnv(clusterID, kind, member, env string) {
	if kind != "database" || env == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if c := l.clusters[clusterID]; c != nil {
		if m := c.members[member]; m != nil && !slices.Contains(m.DatabaseEnvs, env) {
			m.DatabaseEnvs = append(slices.Clone(m.DatabaseEnvs), env)
		}
	}
}

// commit records where a workload now runs, takes its memory off that member and tells the control plane. A
// start that outlived this leader's epoch is not recorded: the cluster is another leader's to describe.
func (l *Leader) commit(clusterID string, e lease.Entry, needMB int, from string, restored *bundles.Restore, took time.Duration, promoted bool, epoch uint64) bool {
	l.mu.Lock()
	c := l.clusters[clusterID]
	if c == nil {
		l.mu.Unlock()
		return false
	}
	delete(c.starting, entryKey(e.Kind, e.ID, e.Replica))
	if c.superseded || c.epoch != epoch {
		l.mu.Unlock()
		l.cfg.Logf("deplo-cluster: %s started %s for epoch %d, which this leader no longer leads: not recorded", e.Member, e.Slug, epoch)
		return false
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
		// The whole recovery, start and restore: what the control plane's next estimate is measured from.
		a.RestoreMs = max(took.Milliseconds(), 1)
		moved["restoreMs"] = a.RestoreMs
	}
	if promoted {
		a.Promoted, moved["promoted"] = true, true
	}
	c.auth[key] = a
	delete(c.waiting, key)
	c.noteLeft(from, e, a.At)
	c.standbys = c.keptStandbys(c.standbys)
	members := replicaMembers(c.placement, e.Kind, e.ID)
	l.saveFailover()
	l.mu.Unlock()
	l.log.Append("placement", clusterID, map[string]any{"kind": e.Kind, "workloadId": e.ID, "members": members, "epoch": epoch})
	ev := l.log.Append("failed_over", clusterID, moved)
	l.stamp(clusterID, key, ev.Seq)
	return true
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
func (l *Leader) wait(clusterID string, e lease.Entry, from, reason, why string) {
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
	c.auth[key] = authority{At: l.cfg.Now(), Entry: e, From: from, Reason: reason, Wait: why}
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
			} else {
				silent++
			}
		}
		if silent*2 > len(c.members) {
			continue
		}
		sort.Strings(mids)
		for _, mid := range mids {
			m := c.members[mid]
			// A lease extended before a restart may still be alive on a member this leader has not heard since.
			if now.Sub(m.lastAck) < FailoverAfter || now.Before(c.extended[mid].Add(FailoverAfter-lease.FenceAfter)) {
				continue
			}
			// A member this leader never heard may hold a lease another leader extended: what must never run
			// twice waits that out, and moves in a second pass once it has.
			held := m.presumedAlive(now)
			if c.moved[mid] && (held || m.presumed.IsZero()) {
				continue
			}
			var es []lease.Entry
			for _, e := range c.placement {
				// A single-writer workload moves only onto a standby copy or a backup another member can read; else it waits.
				if e.Member == mid && (!e.SingleWriter || (!held && (l.restorable(id, e, mid) || l.promotable(c, id, e, mid, now)))) {
					es = append(es, e)
				}
			}
			c.moved[mid] = true
			if !held {
				m.presumed = time.Time{}
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
}

// presumedAlive says whether a lease another leader gave this member may still be running, margin included.
func (m *member) presumedAlive(now time.Time) bool {
	return now.Before(m.presumed.Add(FailoverAfter - lease.FenceAfter))
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
	for _, e := range Order(movable, meta) {
		o := l.relocate(ctx, j.cluster, e.Kind, e.ID, e.Replica, j.from, false, nil)
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
	l.round(ctx, lease.ExtendFor)
}
