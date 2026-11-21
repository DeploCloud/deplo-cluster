package leader

import (
	"context"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/lease"
	"github.com/DeploCloud/deplo-cluster/internal/placement"
)

// A failover starts the lost copy of a load-balanced App beside one a member already runs. Once the member it
// left answers again for GiveCopyAfter, it gets a copy of its own back: started there and nowhere else, so no
// other copy restarts. The extra copy stays where the failover put it until the App's next deploy.
// https://deplo.build/docs/operations/clusters/load-balancing

// GiveCopyAfter is longer than the longest time a cluster's zone takes to drop a stopped server (60 s), so a
// server that answers again and drops within it is given nothing.
const GiveCopyAfter = 2 * time.Minute

// CopyBack is the reason a failed_over event carries for a copy handed back to a member that came back.
const CopyBack = "copy_back"

type copyGift struct {
	cluster string
	e       lease.Entry
}

// copiesToGive is, per cluster this leader leads, one replica of each load-balanced App that runs twice on a
// member while another member that has answered long enough runs none.
func (l *Leader) copiesToGive() []copyGift {
	now := l.cfg.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []copyGift
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
		for _, w := range c.balancedWorkloads() {
			if e, to := c.copyFor(w, now); len(to) > 0 {
				out = append(out, copyGift{id, e})
			}
		}
	}
	return out
}

// balancedWorkloads is each load-balanced App of the placement once, by kind and id; the caller holds l.mu.
func (c *cluster) balancedWorkloads() [][2]string {
	seen := map[[2]string]bool{}
	var out [][2]string
	for _, e := range c.placement {
		k := [2]string{e.Kind, e.ID}
		if e.Balanced && !e.SingleWriter && !e.Standby && !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	return out
}

// copyFor finds the replica to hand back, the highest of the member that runs the most, and the members, by id,
// that run none and may take it. Nothing while one of its replicas waits or starts.
func (c *cluster) copyFor(w [2]string, now time.Time) (lease.Entry, []string) {
	count := map[string]int{}
	var from string
	var entry lease.Entry
	for _, e := range c.placement {
		if e.Kind != w[0] || e.ID != w[1] {
			continue
		}
		if e.Member == "" || c.starting[entryKey(e.Kind, e.ID, e.Replica)] || c.waiting[entryKey(e.Kind, e.ID, e.Replica)] != nil {
			return lease.Entry{}, nil
		}
		count[e.Member]++
	}
	for _, e := range c.placement {
		if e.Kind != w[0] || e.ID != w[1] {
			continue
		}
		n := count[e.Member]
		if n >= 2 && (n > count[from] || n == count[from] && (e.Member < from || e.Member == from && e.Replica > entry.Replica)) {
			from, entry = e.Member, e
		}
	}
	if from == "" {
		return lease.Entry{}, nil
	}
	var to []string
	for mid, m := range c.members {
		if count[mid] == 0 && c.mayTakeCopy(m, now) {
			to = append(to, mid)
		}
	}
	sort.Strings(to)
	return entry, to
}

// mayTakeCopy: the member answers, has for GiveCopyAfter, said what it removed when it came back, and takes work.
func (c *cluster) mayTakeCopy(m *member, now time.Time) bool {
	return !m.upSince.IsZero() && now.Sub(m.lastAck) < lease.NotRespondingAfter && now.Sub(m.upSince) >= GiveCopyAfter &&
		!c.owesReturn(m.ID) && !m.NoFailover && !m.Emptying
}

// copyBack starts one replica on the member it is handed back to and records it there once it is ready. It changes
// nothing on any failure: the replica stays where the failover put it, and the next pass looks again.
func (l *Leader) copyBack(ctx context.Context, g copyGift) {
	meta, err := l.cfg.Bundles.Meta(g.cluster, g.e.Kind, g.e.ID)
	if err != nil {
		return
	}
	b, err := l.cfg.Bundles.Unseal(g.cluster, g.e.Kind, g.e.ID)
	if err != nil || b.Stack.Scale == "" {
		return
	}
	now := l.cfg.Now()
	l.mu.Lock()
	c := l.clusters[g.cluster]
	if c == nil || !c.leads(now) {
		l.mu.Unlock()
		return
	}
	// Asked again under the lock: a pass, a roster or a failover may have moved things since.
	e, candidates := c.copyFor([2]string{g.e.Kind, g.e.ID}, now)
	if e.Replica != g.e.Replica || e.Member != g.e.Member {
		l.mu.Unlock()
		return
	}
	w := placement.Workload{Kind: g.e.Kind, ID: g.e.ID, Name: meta.Name, Environment: meta.Environment, Image: meta.Image, Archs: meta.Archs, LimitMB: meta.LimitMB, PeakMB: meta.PeakMB}
	to := ""
	for _, f := range c.memberFacts(now) {
		if to == "" && slices.Contains(candidates, f.ID) && placement.Place(g.cluster, w, []placement.Member{f}).Refusal == nil {
			to = f.ID
		}
	}
	if to == "" {
		l.mu.Unlock()
		return
	}
	next := append([]lease.Entry(nil), c.placement...)
	for i := range next {
		if next[i].Kind == e.Kind && next[i].ID == e.ID && next[i].Replica == e.Replica {
			next[i].Member = to
		}
	}
	order := lease.Order{Cluster: g.cluster, Member: to, Epoch: c.epoch, Placement: c.orderPlacement(next), Start: l.startOrder(b), Return: c.returning[to]}
	order.Start.ComposeYAML = strings.ReplaceAll(order.Start.ComposeYAML, b.Stack.Scale, strconv.Itoa(countOn(next, e.Kind, e.ID, to)))
	key := entryKey(e.Kind, e.ID, e.Replica)
	c.starting[key] = true
	fp, epoch := c.members[to].Fingerprint, c.epoch
	l.mu.Unlock()

	if err := l.sendStart(ctx, fp, order); err != nil {
		l.endStart(g.cluster, key)
		l.cfg.Logf("deplo-cluster: %s could not start its copy of %s again: %v", to, e.Slug, err)
		return
	}
	l.mu.Lock()
	c = l.clusters[g.cluster]
	if c == nil || c.superseded || c.epoch != epoch {
		if c != nil {
			delete(c.starting, key)
		}
		l.mu.Unlock()
		return
	}
	delete(c.starting, key)
	e, ok := c.place(g.e.Kind, g.e.ID, g.e.Replica, to)
	if !ok {
		l.mu.Unlock()
		return
	}
	if m := c.members[to]; m != nil {
		m.FreeMB -= placement.Needed(w)
	}
	c.auth[key] = authority{At: l.cfg.Now(), Entry: e, Told: Told{From: g.e.Member, Reason: CopyBack}}
	members := replicaMembers(c.placement, e.Kind, e.ID)
	l.saveFailover()
	l.mu.Unlock()
	l.tellPlacement(g.cluster, e.Kind, e.ID, members, epoch)
	ev := l.log.Append("failed_over", g.cluster, map[string]any{"kind": e.Kind, "workloadId": e.ID, "from": g.e.Member, "to": to, "reason": CopyBack})
	l.stamp(g.cluster, key, ev.Seq)
}
