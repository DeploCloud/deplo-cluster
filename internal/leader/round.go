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
func (l *Leader) Tick(ctx context.Context) { l.tick(ctx, time.Time{}) }

// tick is Tick, sending a renewal that reached no member again until again: see renewAgain.
func (l *Leader) tick(ctx context.Context, again time.Time) {
	asked := time.Now()
	l.tickMu.Lock()
	defer l.tickMu.Unlock()
	// Told to stop: the drain's longer lease is the last order, and a dial does not look at a cancelled context.
	if ctx.Err() != nil {
		return
	}
	l.round(ctx, again, asked)
	l.settle()
}

// round asks the members of every cluster this leader is not sure it leads, then sends its orders where a
// quorum answered. The caller holds l.tickMu.
func (l *Leader) round(ctx context.Context, again, asked time.Time) {
	l.standDown(l.cfg.Now())
	began := time.Now()
	l.exchange(ctx, pass{prevote: true})
	voted := time.Now()
	l.exchange(ctx, pass{probe: true})
	probed := time.Now()
	l.exchange(ctx, pass{again: again})
	// c48: every member's renewals 2 to 3 s apart, and nothing in the journal said why.
	if took := time.Since(asked); took >= slowRound {
		l.rarely("round", "deplo-cluster: a round took %d ms: %d waiting for its turn, pre-vote %d, probe %d, renewals %d",
			took.Milliseconds(), began.Sub(asked).Milliseconds(), voted.Sub(began).Milliseconds(),
			probed.Sub(voted).Milliseconds(), time.Since(probed).Milliseconds())
	}
}

// slowRound is a round that leaves the next renewal at risk of coming a whole tick late.
const slowRound = 900 * time.Millisecond

// pass is one exchange: a probe, a renewal that may announce an extension, or one that extends, to only some clusters.
// With again set, a plain renewal that reached no member is sent again until then.
type pass struct {
	probe, announce, prevote bool
	extend                   time.Duration
	only                     map[string]bool
	again                    time.Time
}

// orderFor is what a pass sends a member at now, with the cluster's line. The caller holds l.mu.
func (c *cluster) orderFor(id string, m *member, p pass, line []string, now time.Time) lease.Order {
	o := lease.Order{Cluster: id, Member: m.ID, Epoch: c.epoch, Probe: p.probe, PreVote: p.prevote, From: c.roster.Self, Succeed: c.succeed}
	if !p.probe && !p.prevote {
		o.Placement, o.Return, o.ExtendSeconds = c.orderPlacement(c.told(m.ID)), c.owesReturn(m.ID), int(p.extend/time.Second)
		o.Line, o.Last, o.Heard = line, c.lineEnd(), c.heardAgo(now)
		o.NoExtension = !p.announce && p.extend == 0 && c.noExtension(now)
	}
	if p.announce {
		o.Announce = int(lease.AnnounceFor / time.Second)
	}
	o.Gone = c.goneFingerprints()
	return o
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
	drain := p.announce || extend > 0
	if drain {
		// Every member must take an announcement or an extension: one still taking an earlier order takes it after.
		l.quiet(sendTimeout)
	}
	now := l.cfg.Now()
	l.mu.Lock()
	l.exchanges++
	seq := l.exchanges
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
			// A member still taking the last order gets the next one once it answered, never two at a time.
			if !p.prevote && m.sending {
				continue
			}
			if probe {
				m.running = nil
			}
			o := c.orderFor(id, m, p, line, now)
			if extend > 0 {
				// Kept before the order leaves: a member that took it and whose answer was lost is extended all the same.
				c.extended[m.ID] = now.Add(lease.FenceAfter + extend)
			}
			_, out := c.gone[m.ID]
			m.sending = !p.prevote
			jobs = append(jobs, job{id, m.ID, m.Fingerprint, m, o, !out})
		}
	}
	if extend > 0 {
		l.saveFailover()
	}
	l.mu.Unlock()
	if extend > 0 {
		l.Flush()
	}

	answered := make(chan string, len(jobs))
	var wg sync.WaitGroup
	for _, j := range jobs {
		wg.Add(1)
		go func() {
			// A gone member is renewed but does not vote until it is back and caught up.
			ok, reached := l.renew(ctx, j.cluster, j.id, j.fingerprint, j.m, j.order)
			if !p.prevote {
				l.mu.Lock()
				j.m.sending = false
				l.mu.Unlock()
			}
			if ok && j.votes {
				answered <- j.cluster
			}
			wg.Done()
			if !reached && !p.again.IsZero() {
				l.renewAgain(ctx, j.cluster, j.m, j.order.Epoch, seq, p.again)
			}
		}()
	}
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	heard := map[string]int{}
	// A pre-vote takes nothing, so it is over once a quorum said yes: a dead member is not waited out. A probe or a
	// renewal waits stragglerWait more for the rest, then goes on: a slow member holds up only its own renewal.
	var late <-chan time.Time
	for waiting := true; waiting; {
		select {
		case id := <-answered:
			heard[id]++
			if l.everyQuorum(epochs, heard, now) {
				waiting = !p.prevote
				if late == nil && !drain {
					late = time.After(stragglerWait)
				}
			}
		case <-late:
			waiting = false
		case <-finished:
			waiting = false
		}
	}
	for len(answered) > 0 {
		heard[<-answered]++
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
		if c != nil && c.succeed && p.prevote {
			l.once("tally "+id, "deplo-cluster: cluster %s: epoch %d: %d of %d members would take it, Deplo's word %t", id, c.epoch, n, c.voters(), now.Before(c.deploUntil))
		}
		if c == nil || c.superseded || c.epoch != epochs[id] || !c.quorum(n, now) {
			continue
		}
		if p.prevote {
			// When the answers are in, not when the pass began: a dead member alone takes sendTimeout to give up on.
			c.prevoted = l.cfg.Now()
			continue
		}
		c.sure, c.heard = now, n
		if !probe {
			continue
		}
		if c.succeed {
			c.succeed, c.roster.Succeed, c.ledAt = false, false, now
			if c.epoch > l.epochs[id] {
				l.epochs[id] = c.epoch
				l.saveEpochs()
			}
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

// stragglerWait is how long a probe or a renewal waits, once every cluster heard a quorum, for the members left.
const stragglerWait = 300 * time.Millisecond

// quiet waits, at most for d, until no member is still taking an order.
func (l *Leader) quiet(d time.Duration) {
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		l.mu.Lock()
		busy := false
		for _, c := range l.clusters {
			for _, m := range c.members {
				busy = busy || m.sending
			}
		}
		l.mu.Unlock()
		if !busy {
			return
		}
	}
}

// everyQuorum says whether every cluster of a pass heard a quorum at its epoch.
func (l *Leader) everyQuorum(epochs map[string]uint64, heard map[string]int, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for id, epoch := range epochs {
		if c := l.clusters[id]; c != nil && c.epoch == epoch && !c.quorum(heard[id], now) {
			return false
		}
	}
	return true
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
		c.placement[i].Member, c.placement[i].Epoch = on[0], c.epoch
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

// againAfter is how long a renewal that reached no member waits before each new try: an agent that restarts
// misses one renewal, not a whole tick of its lease.
var againAfter = []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 200 * time.Millisecond}

// againWithin is how far into its tick a renewal is sent again, so it is over before the next tick's.
const againWithin = 700 * time.Millisecond

// renewAgain sends a member its renewal anew after each of againAfter, until one reaches it or until passes. A later
// pass, another epoch or a leader no longer sure ends it: it never goes out beside or after a later pass's order.
func (l *Leader) renewAgain(ctx context.Context, clusterID string, m *member, epoch, seq uint64, until time.Time) {
	ctx, cancel := context.WithDeadline(ctx, until)
	defer cancel()
	for _, wait := range againAfter {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		l.mu.Lock()
		c, now := l.clusters[clusterID], l.cfg.Now()
		if ctx.Err() != nil || l.exchanges != seq || c == nil || c.epoch != epoch || c.members[m.ID] != m || !c.leads(now) {
			l.mu.Unlock()
			return
		}
		o := c.orderFor(clusterID, m, pass{}, c.line(now), now)
		id, fingerprint := m.ID, m.Fingerprint
		m.sending = true
		l.mu.Unlock()
		_, reached := l.renew(ctx, clusterID, id, fingerprint, m, o)
		l.mu.Lock()
		m.sending = false
		l.mu.Unlock()
		if reached {
			return
		}
	}
}

// dialWithin ends a dial that reaches nothing before the tick two after its own, which would skip the member while it
// lasts: a renewal sent while a member's agent was down left it 4 s without one (c48).
const dialWithin = 2*lease.RenewEvery - 100*time.Millisecond

// slowSend is a member reached, or answering, slowly enough to say so.
const slowSend = 500 * time.Millisecond

// renew sends one order and says whether the member took it at this leader's epoch, and whether it answered at all.
func (l *Leader) renew(ctx context.Context, clusterID, id, fingerprint string, m *member, o lease.Order) (took, reached bool) {
	end, bounded := ctx.Deadline()
	ctx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	began := time.Now()
	dctx, dcancel := context.WithTimeout(ctx, dialWithin)
	c, err := l.cfg.Dial(dctx, wire.Party{Fingerprint: fingerprint, Role: wire.RoleMember})
	dcancel()
	if err != nil {
		return false, false
	}
	defer c.Close()
	dialed := time.Now()
	defer func() {
		if d, a := dialed.Sub(began), time.Since(dialed); d >= slowSend || a >= slowSend {
			l.rarely("slow "+id, "deplo-cluster: member %s took %d ms to reach and %d ms to answer", id, d.Milliseconds(), a.Milliseconds())
		}
	}()
	dl := time.Now().Add(sendTimeout)
	if bounded && end.Before(dl) {
		dl = end
	}
	_ = c.SetDeadline(dl)
	// Opened only for a member that answers, never on a probe: a keep carries its destination's credentials.
	if !o.Probe {
		o.Keep, o.Stores = l.keepsFor(clusterID, id), l.storesFor(clusterID, id)
	}
	r, err := lease.Send(c, o)
	if err != nil {
		return false, false
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
		return false, true
	}
	if r.OK {
		l.mu.Lock()
		m.answered = l.cfg.Now()
		l.mu.Unlock()
	}
	// A probe renewed nothing, a pre-vote took nothing: the member's lease is as old as it was.
	if o.PreVote {
		return r.OK, true
	}
	if !r.OK || o.Probe {
		if r.OK {
			l.mu.Lock()
			m.running = r.Running
			l.mu.Unlock()
			if o.Probe && !r.Reports {
				l.once("reports "+id, "deplo-cluster: %s's server agent does not say what it runs: the roster stands for its writers", id)
			}
		}
		return r.OK, true
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
	return true, true
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
		// One still trying to lead renews nobody: what it would say of a member, itself included, is not so (eu, 23 Nov).
		if c.superseded || c.succeed && c.sure.IsZero() {
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

// heardAgo is how long ago each member was last heard, in milliseconds, for the next leader; the caller holds l.mu.
func (c *cluster) heardAgo(now time.Time) map[string]int64 {
	out := map[string]int64{}
	for id, m := range c.members {
		if at := m.lastHeard(); !at.IsZero() {
			out[id] = max(now.Sub(at).Milliseconds(), 0)
		}
	}
	return out
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
	// Counted from the last time any leader heard it, so a takeover never starts the clock over.
	if last := m.lastHeard(); last.IsZero() || now.Sub(last) < GoneAfter || !c.leads(now) || c.voters() < 2 {
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
		a := time.NewTicker(awakeEvery)
		defer a.Stop()
		for {
			l.Awake()
			select {
			case <-ctx.Done():
				return
			case <-a.C:
			}
		}
	}()
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
		l.tick(ctx, time.Now().Add(againWithin))
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-l.kick:
		}
	}
}

// noExtension says no lease of the cluster can live on an extension: every member took a plain renewal from this
// leader since its last extension, or has been silent past the longest lease another leader may have given it.
func (c *cluster) noExtension(now time.Time) bool {
	for id, m := range c.members {
		if !m.other.IsZero() && now.Before(m.other.Add(lease.FenceAfter+lease.ExtendFor+fenceMargin)) {
			return false
		}
		if e := c.extended[id]; !e.IsZero() && now.Before(e.Add(fenceMargin)) {
			return false
		}
	}
	return true
}
