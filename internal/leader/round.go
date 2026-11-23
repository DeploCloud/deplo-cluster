package leader

import (
	"context"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/lease"
	"github.com/DeploCloud/deplo-cluster/internal/wire"
)

// Tick renews every member's lease once, then reports each member whose state changed.
func (l *Leader) Tick(ctx context.Context) {
	l.tickMu.Lock()
	defer l.tickMu.Unlock()
	// Told to stop: the drain's longer lease is the last order, and a dial does not look at a cancelled context.
	if ctx.Err() != nil {
		return
	}
	l.round(ctx)
	l.settle()
}

// round asks the members of every cluster this leader is not sure it leads, then sends its orders where a
// quorum answered. The caller holds l.tickMu.
func (l *Leader) round(ctx context.Context) {
	l.standDown(l.cfg.Now())
	l.exchange(ctx, pass{prevote: true})
	l.exchange(ctx, pass{probe: true})
	l.exchange(ctx, pass{})
}

// pass is one exchange: a probe, a renewal that may announce an extension, or one that extends, to only some clusters.
type pass struct {
	probe, announce, prevote bool
	extend                   time.Duration
	only                     map[string]bool
}

// exchange sends one order to every member of the clusters that are due it: a probe where this leader is not
// sure it leads, a renewal where it is. A quorum of answers makes it sure for SureFor. It says which clusters
// had every member answer.
func (l *Leader) exchange(ctx context.Context, p pass) map[string]bool {
	probe, extend := p.probe, p.extend
	// id and fingerprint are read here, under the lock: a roster may replace them while the order is on its way.
	type job struct {
		cluster, id, fingerprint string
		m                        *member
		order                    lease.Order
		votes                    bool
	}
	var jobs []job
	epochs := map[string]uint64{}
	now := l.cfg.Now()
	l.mu.Lock()
	for id, c := range l.clusters {
		if c.superseded || c.leads(now) == (probe || p.prevote) || p.only != nil && !p.only[id] || !c.due(p, now) {
			continue
		}
		epochs[id] = c.epoch
		line := c.line(now)
		for _, m := range c.members {
			if p.prevote && !m.Succession {
				continue
			}
			if probe {
				m.running = nil
			}
			o := lease.Order{Cluster: id, Member: m.ID, Epoch: c.epoch, Probe: probe, PreVote: p.prevote, From: c.roster.Self, Succeed: c.succeed}
			if !probe && !p.prevote {
				o.Placement, o.Return, o.ExtendSeconds = c.orderPlacement(c.told()), c.owesReturn(m.ID), int(extend/time.Second)
				o.Line, o.Last = line, c.lineEnd()
			}
			if p.announce {
				o.Announce = int(lease.AnnounceFor / time.Second)
			}
			o.Gone = c.goneFingerprints()
			if extend > 0 {
				// Kept before the order leaves: a member that took it and whose answer was lost is extended all the same.
				c.extended[m.ID] = now.Add(lease.FenceAfter + extend)
			}
			_, out := c.gone[m.ID]
			jobs = append(jobs, job{id, m.ID, m.Fingerprint, m, o, !out})
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
			// A gone member is renewed but does not vote until it is back and caught up.
			if l.renew(ctx, j.cluster, j.id, j.fingerprint, j.m, j.order) && j.votes {
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
	sent := map[string]int{}
	for _, j := range jobs {
		if j.votes {
			sent[j.cluster]++
		}
	}
	all := map[string]bool{}
	for id, n := range heard {
		all[id] = n == sent[id]
	}
	var tells []func()
	l.mu.Lock()
	for id, n := range heard {
		c := l.clusters[id]
		if c == nil || c.superseded || c.epoch != epochs[id] || !c.quorum(n, now) {
			continue
		}
		if p.prevote {
			c.prevoted = now
			continue
		}
		c.sure, c.heard = now, n
		if !probe {
			continue
		}
		if c.succeed {
			c.succeed, c.roster.Succeed = false, false
			l.saveRosters()
			tells = append(tells, l.succeeded(id, c.epoch))
		}
		// It was not sure until now, so an older leader may have extended a lease meanwhile: a member this one
		// never heard holds the longest one again, counted as from a roster one renewal before this round.
		for _, m := range c.members {
			if at := now.Add(-lease.RenewEvery); !m.other.IsZero() && m.other.Before(at) {
				m.other = at
			}
		}
		tells = append(tells, l.keepWhereRunning(id, c)...)
	}
	l.mu.Unlock()
	for _, tell := range tells {
		tell()
	}
	return all
}

// keepWhereRunning places each single-writer workload on the member that runs it, the newest epoch first, before
// this leader's first renewal: Deplo's roster can lag what the leader before moved, and a writer moved back to
// where it ran before runs twice (deplo#241). The caller holds l.mu; what it returns tells once l.mu is released.
func (l *Leader) keepWhereRunning(clusterID string, c *cluster) []func() {
	newest, runners := map[string]uint64{}, map[string][]string{}
	for id, m := range c.members {
		for _, w := range m.running {
			switch {
			case w.Epoch > newest[w.Slug] || runners[w.Slug] == nil:
				newest[w.Slug], runners[w.Slug] = w.Epoch, []string{id}
			case w.Epoch == newest[w.Slug]:
				runners[w.Slug] = append(runners[w.Slug], id)
			}
		}
		m.running = nil
	}
	var tells []func()
	for i, e := range c.placement {
		on := runners[e.Slug]
		if !e.SingleWriter || e.Slug == "" || len(on) == 0 || slices.Contains(on, e.Member) {
			continue
		}
		slices.Sort(on)
		from := e.Member
		c.placement[i].Member = on[0]
		e = c.placement[i]
		key := entryKey(e.Kind, e.ID, e.Replica)
		c.auth[key] = authority{At: l.cfg.Now(), Entry: e, Told: Told{From: from, Reason: RunsThere}}
		delete(c.waiting, key)
		members, epoch := replicaMembers(c.placement, e.Kind, e.ID), c.epoch
		moved := map[string]any{"kind": e.Kind, "workloadId": e.ID, "from": from, "to": e.Member, "reason": RunsThere}
		tells = append(tells, func() {
			l.cfg.Logf("deplo-cluster: %s runs on %s: kept there, not placed on %q as the roster says", e.Slug, e.Member, from)
			l.tellPlacement(clusterID, e.Kind, e.ID, members, epoch)
			ev := l.log.Append("failed_over", clusterID, moved)
			l.stamp(clusterID, key, ev.Seq)
		})
	}
	if len(tells) > 0 {
		l.saveFailover()
	}
	return tells
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
	if r.OK && r.Unsure != nil {
		l.mu.Lock()
		if cl := l.clusters[clusterID]; cl != nil && cl.epoch == o.Epoch {
			// Zero is no extension alive: only a later end moves the one already known.
			cl.vouched = true
			if u := l.cfg.Now().Add(time.Duration(*r.Unsure) * time.Second); *r.Unsure > 0 && u.After(cl.unsure) {
				cl.unsure = u
			}
		}
		l.mu.Unlock()
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
	if r.OK {
		l.mu.Lock()
		m.answered = l.cfg.Now()
		l.mu.Unlock()
	}
	// A probe renewed nothing, a pre-vote took nothing: the member's lease is as old as it was.
	if o.PreVote {
		return r.OK
	}
	if !r.OK || o.Probe {
		if r.OK {
			l.mu.Lock()
			m.running = r.Running
			l.mu.Unlock()
			if o.Probe && !r.Reports {
				l.cfg.Logf("deplo-cluster: %s's server agent does not say what it runs: the roster stands for its writers", id)
			}
		}
		return r.OK
	}
	l.mu.Lock()
	if now := l.cfg.Now(); m.upSince.IsZero() || now.Sub(m.lastAck) >= lease.NotRespondingAfter {
		m.upSince = now
	}
	m.lastAck, m.stoppedWithin, m.heard = l.cfg.Now(), time.Duration(r.StoppedWithin)*time.Second, true
	// A plain renewal starts the member's lease over at 60 s: what an earlier extension bought is gone.
	if o.ExtendSeconds == 0 {
		m.other = time.Time{}
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
	saved := false
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
			// Fenced as soon as its work may move: by the member's own word, or by what the members vouch for.
			if s == lease.NotResponding && (m.stoppedWithin > 0 || c.vouched) && c.stopped(m, now) && !c.presumedAlive(m, now) {
				s = lease.Fenced
			}
			if s != m.state {
				m.state = s
				out = append(out, change{id, mid, string(s)})
			}
		}
		for _, mid := range mids {
			if ev := c.regroup(mid, now); ev != "" {
				saved = true
				out = append(out, change{id, mid, ev})
			}
		}
	}
	if saved {
		l.saveFailover()
	}
	l.mu.Unlock()
	for _, c := range out {
		if c.state == "gone" || c.state == "rejoined" {
			l.log.Append("member_"+c.state, c.cluster, map[string]string{"member": c.member})
			continue
		}
		l.log.Append("member_state", c.cluster, map[string]string{"member": c.member, "state": c.state})
	}
}

// regroup takes a member silent past GoneAfter out of the majority while this leader leads the others, and counts
// it again once it has answered for GiveCopyAfter and said what it removed. It names what changed, or "".
func (c *cluster) regroup(mid string, now time.Time) string {
	m := c.members[mid]
	if _, out := c.gone[mid]; out {
		if now.Sub(m.lastAck) < lease.NotRespondingAfter && !m.upSince.IsZero() && now.Sub(m.upSince) >= GiveCopyAfter && !c.returning[mid] {
			delete(c.gone, mid)
			return "rejoined"
		}
		return ""
	}
	if !m.heard || now.Sub(m.lastAck) < GoneAfter || !c.leads(now) || c.voters() < 2 {
		return ""
	}
	if c.gone == nil {
		c.gone = map[string]time.Time{}
	}
	c.gone[mid] = now
	return "gone"
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
