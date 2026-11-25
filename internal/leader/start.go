package leader

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/bundles"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
	"github.com/DeploCloud/deplo-cluster/internal/placement"
	"github.com/DeploCloud/deplo-cluster/internal/wire"
)

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
		StartAfterReady: s.StartAfterReady, Mounts: mounts, FilesBindSources: s.FilesBindSources, Certificates: b.Certificates}
	if r := b.Restore; r != nil {
		st.Restore = &lease.Restore{BackupID: r.BackupID, TakenAt: r.TakenAt, Request: r.Request}
	}
	return st
}

// sendStart gives one member the order to bring a workload up and waits for it to answer.
func (l *Leader) sendStart(ctx context.Context, fp string, o lease.Order) error {
	// A start renews the member's lease too, so it names the gone members as every renewal does: without them the
	// member's name server would count a gone one again until the next renewal (seen on cluster eu).
	l.mu.Lock()
	if c := l.clusters[o.Cluster]; c != nil {
		o.Gone = c.goneFingerprints()
	}
	l.mu.Unlock()
	// A start renews the member's lease too, when it lands: until it is over, its fence counts from its end.
	l.startsTo(o, 1)
	defer l.startsTo(o, -1)
	// ponytail: a member's databases start one at a time, so a long restore holds the next database behind it.
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
	subject := "bundle " + clusterID + " " + kind + " " + id
	meta, err := l.cfg.Bundles.Meta(clusterID, kind, id)
	if err != nil {
		l.once(subject, "deplo-cluster: %s %s has no bundle, so it does not fail over", kind, id)
		return outcome{skipped: true, why: NotPlaced}
	}
	var b bundles.Bundle
	if asked != nil {
		b = *asked
	} else if b, err = l.cfg.Bundles.Unseal(clusterID, kind, id); err != nil {
		l.once(subject, "deplo-cluster: %s %s: %v", kind, id, err)
		return outcome{skipped: true}
	}
	l.unsay(subject)
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
		now := l.cfg.Now()
		if !waited && l.unknown(c.members[from], now) {
			return NotHeardYet
		}
		_, why := c.mayStart(kind, id, replica, from, waited, now)
		return why
	}
	// A standby copy that follows the workload is promoted where it is; only when that fails is a backup restored.
	promoteFailed := false
	if sb := l.standbyFor(clusterID, kind, id, from); sb != "" && b.Promote != nil {
		switch l.promote(ctx, clusterID, kind, id, replica, from, sb, b, gate, asked == nil) {
		case promoteOK:
			l.noteDatabaseEnv(clusterID, kind, sb, meta.Environment)
			return outcome{member: sb}
		case notNow:
			return outcome{skipped: true, again: true, why: StillAnswering}
		}
		exclude[sb], promoteFailed = true, true
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
			return outcome{skipped: true, again: why == StillAnswering || why == NotHeardYet, why: why}
		}
		// What keeps data never starts empty: without a backup to restore it waits for its member. One whose standby
		// could not be promoted is stuck, and says so: it waits, and the promotion is tried again (deplo#242).
		if entry.SingleWriter && b.Restore == nil {
			l.mu.Unlock()
			if promoteFailed {
				return outcome{refusal: &placement.Refusal{Reason: StartFailed, NeededMB: placement.Needed(placement.Workload{LimitMB: meta.LimitMB, PeakMB: meta.PeakMB}), Cluster: clusterID, Workload: id}}
			}
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
		w := placement.Workload{Kind: kind, ID: id, Name: meta.Name, Environment: meta.Environment, Image: meta.Image, Archs: meta.Archs, LimitMB: meta.LimitMB, PeakMB: meta.PeakMB, DiskBytes: meta.DiskBytes}
		if n := countOn(c.placement, kind, id, ""); n > 1 && !spread {
			w.LimitMB = placement.Needed(w) * n
		}
		ans := placement.Place(clusterID, w, facts)
		if ans.Refusal != nil {
			l.mu.Unlock()
			return outcome{refusal: ans.Refusal}
		}
		target := c.members[ans.Member]
		// Taken now, not at the commit: another start may be placed while this one runs.
		need := placement.Needed(w)
		target.FreeMB -= need
		// ponytail: a start that fails keeps this until Deplo's next roster; the leader alone then errs on room.
		if !slices.Contains(target.Holds, id) {
			target.DiskFree -= meta.DiskBytes
		}
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
		c.starting[key] = underWay{to: ans.Member, from: from}
		check := c.found(asked == nil, kind, id, replica, from)
		l.mu.Unlock()
		// Deplo shows the workload moving from here until its new member answers (ADR-0049).
		l.log.Append("moving", clusterID, map[string]any{"kind": kind, "workloadId": id, "from": from, "to": ans.Member})

		began := time.Now()
		if err := l.sendStart(ctx, fp, order); err != nil {
			l.endStart(clusterID, key)
			l.unreserve(clusterID, ans.Member, need)
			l.cfg.Logf("deplo-cluster: %s could not start %s: %v", ans.Member, b.Stack.Slug, err)
			exclude[ans.Member] = true
			continue
		}
		if entry.Slug == "" {
			entry.Slug = b.Stack.Slug
		}
		entry.Kind, entry.ID, entry.Replica = kind, id, replica
		entry.Member = ans.Member
		if why := l.commit(clusterID, entry, 0, from, b.Restore, time.Since(began), false, order.Epoch, check); why != "" {
			l.unreserve(clusterID, ans.Member, need)
			return outcome{skipped: true, why: why}
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
	held := c.presumedAlive(m, now) || now.Before(c.extended[from].Add(fenceMargin)) || !c.stopped(m, now)
	if now.Sub(m.lastAck) < FailoverAfter || (entry.SingleWriter && held) {
		return entry, StillAnswering
	}
	return entry, ""
}

// unreserve returns the memory a start that did not count had taken on its member.
func (l *Leader) unreserve(clusterID, member string, mb int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if c := l.clusters[clusterID]; c != nil && c.members[member] != nil {
		c.members[member].FreeMB += mb
	}
}

// endStart forgets a start that is over, so the workload is shown where it is recorded again.
func (l *Leader) endStart(clusterID, key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if c := l.clusters[clusterID]; c != nil {
		delete(c.starting, key)
	}
}

// underWay is a start order out: the member it brings the workload up on, and the one it leaves.
type underWay struct{ to, from string }

func (c *cluster) isStarting(key string) bool {
	_, ok := c.starting[key]
	return ok
}

// told is the placement a renewal to a member carries: a workload whose start is under way is placed where it starts,
// so the others pass visitors there once it answers, and nowhere for the two members the start is about: its start
// order alone brings it up, and the one it leaves neither runs it again nor removes it. The caller holds l.mu.
func (c *cluster) told(member string) []lease.Entry {
	out := append([]lease.Entry(nil), c.placement...)
	for i, e := range out {
		if s, ok := c.starting[entryKey(e.Kind, e.ID, e.Replica)]; ok {
			out[i].Member = s.to
			if member == s.from || member == s.to {
				out[i].Member = ""
			}
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
func (l *Leader) commit(clusterID string, e lease.Entry, needMB int, from string, restored *bundles.Restore, took time.Duration, promoted bool, epoch uint64, check *recheck) string {
	l.mu.Lock()
	c := l.clusters[clusterID]
	if c == nil {
		l.mu.Unlock()
		return NotLeading
	}
	delete(c.starting, entryKey(e.Kind, e.ID, e.Replica))
	if c.superseded || c.epoch != epoch {
		l.mu.Unlock()
		l.cfg.Logf("deplo-cluster: %s started %s for epoch %d, which this leader no longer leads: not recorded", e.Member, e.Slug, epoch)
		return NotLeading
	}
	stale := c.stale(check, e)
	if stale != "" && !e.SingleWriter {
		return l.drop(c, clusterID, e, from, stale)
	}
	e.Epoch = c.epoch
	if _, ok := c.place(e.Kind, e.ID, e.Replica, e.Member); !ok {
		c.placement = append(c.placement, e)
	}
	if m := c.members[e.Member]; m != nil && needMB > 0 {
		m.FreeMB -= needMB
	}
	key := entryKey(e.Kind, e.ID, e.Replica)
	a := authority{At: l.cfg.Now(), Entry: e, Told: Told{From: from, Reason: StoppedAnswering}}
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
	// What must never run twice is recorded all the same: see recheck.
	if stale != "" {
		moved["stale"] = stale
		l.cfg.Logf("deplo-cluster: %s took %s over, then %s; recorded, since %s had stopped it before the start", e.Member, e.Slug, staleSentence(stale, from), from)
	}
	c.auth[key] = a
	delete(c.waiting, key)
	c.noteLeft(from, e, a.At)
	c.standbys = c.keptStandbys(c.standbys)
	members := replicaMembers(c.placement, e.Kind, e.ID)
	l.saveFailover()
	l.mu.Unlock()
	l.tellPlacement(clusterID, e.Kind, e.ID, members, epoch)
	ev := l.log.Append("failed_over", clusterID, moved)
	l.stamp(clusterID, key, ev.Seq)
	return ""
}

// recheck is what an automatic move found when it decided, read again once its start is over (deplo-cluster#5):
// the member it leaves answered since, the workload is placed elsewhere now, or the member it started on left.
// A workload that may run twice then does not move, and its new copy is removed. One that must never run twice is
// recorded anyway: its member had stopped it before the start, and handing it back while the new copy still runs
// would make two writers.
type recheck struct {
	from string
	// ack is that member's last answer, and on where the workload was placed, when the move was decided.
	ack time.Time
	on  string
}

// found is what an automatic move finds before its start; nil for a person's move. The caller holds l.mu.
func (c *cluster) found(auto bool, kind, id string, replica int, from string) *recheck {
	if !auto {
		return nil
	}
	r := &recheck{from: from}
	if m := c.members[from]; m != nil {
		r.ack = m.lastAck
	}
	for _, p := range c.placement {
		if p.Kind == kind && p.ID == id && p.Replica == replica {
			r.on = p.Member
		}
	}
	return r
}

// stale says why a move no longer is what its decision found; "" when it still is. The caller holds l.mu.
func (c *cluster) stale(r *recheck, e lease.Entry) string {
	if r == nil {
		return ""
	}
	if c.members[e.Member] == nil {
		return TargetGone
	}
	on, named := "", false
	for _, p := range c.placement {
		if p.Kind == e.Kind && p.ID == e.ID && p.Replica == e.Replica {
			on, named = p.Member, true
		}
	}
	if !named || on != r.on {
		return PlacementChanged
	}
	if m := c.members[r.from]; m != nil && m.lastAck.After(r.ack) {
		return SourceAnswered
	}
	return ""
}

func staleSentence(why, from string) string {
	switch why {
	case SourceAnswered:
		return from + " answered again"
	case PlacementChanged:
		return "it was placed elsewhere meanwhile"
	}
	return "the server it started on left the cluster"
}

// drop leaves a move unrecorded and has the member that took its start remove that copy, the way a returning
// member removes what runs elsewhere. The caller holds l.mu; drop releases it.
func (l *Leader) drop(c *cluster, clusterID string, e lease.Entry, from, why string) string {
	if c.members[e.Member] != nil {
		c.returning[e.Member], c.dropped[e.Member] = true, true
	}
	l.saveFailover()
	l.mu.Unlock()
	l.cfg.Logf("deplo-cluster: %s started %s, then %s: the move is dropped, and %s removes its copy", e.Member, e.Slug, staleSentence(why, from), e.Member)
	l.log.Append("move_dropped", clusterID, map[string]any{"kind": e.Kind, "workloadId": e.ID, "from": from, "to": e.Member, "reason": why})
	return why
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

func (l *Leader) startsTo(o lease.Order, n int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if c := l.clusters[o.Cluster]; c != nil && c.members[o.Member] != nil {
		m := c.members[o.Member]
		m.startsOut += n
		m.startEnded = l.cfg.Now()
	}
}
