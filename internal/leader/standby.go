package leader

import (
	"context"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/bundles"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

// A standby copy follows a workload from another member (spec #176). The leader never moves one: when the
// workload's member stops, it promotes the copy where it is, after the same wait as any failover.

// keptStandbys drops a copy that names no member or sits with the workload it follows: that one was promoted.
func (c *cluster) keptStandbys(standbys []lease.Entry) []lease.Entry {
	var out []lease.Entry
	for _, s := range standbys {
		with := false
		for _, e := range c.placement {
			if e.Kind == s.Kind && e.ID == s.ID && e.Member == s.Member {
				with = true
			}
		}
		if s.Member != "" && !with {
			s.Standby, s.SingleWriter = true, false
			out = append(out, s)
		}
	}
	return out
}

// orderPlacement is what a member is told: where everything runs, then the standby copies, marked.
func (c *cluster) orderPlacement(placement []lease.Entry) []lease.Entry {
	out := append([]lease.Entry(nil), placement...)
	for _, s := range c.standbys {
		with := false
		for _, e := range placement {
			if e.Kind == s.Kind && e.ID == s.ID && e.Member == s.Member {
				with = true
			}
		}
		if !with {
			out = append(out, s)
		}
	}
	return out
}

// standbyOn is the member holding a live standby copy of the workload, other than `from`; the caller holds l.mu.
func (c *cluster) standbyOn(kind, id, from string, now time.Time) string {
	for _, s := range c.standbys {
		m := c.members[s.Member]
		if s.Kind == kind && s.ID == id && s.Member != from && m != nil && !m.NoFailover && now.Sub(m.lastAck) < lease.NotRespondingAfter {
			return s.Member
		}
	}
	return ""
}

// promotable says whether the workload has a standby copy to promote and a bundle that says how.
func (l *Leader) promotable(c *cluster, clusterID string, e lease.Entry, member string, now time.Time) bool {
	if c.standbyOn(e.Kind, e.ID, member, now) == "" {
		return false
	}
	m, err := l.cfg.Bundles.Meta(clusterID, e.Kind, e.ID)
	return err == nil && m.Promotes
}

func (l *Leader) standbyFor(clusterID, kind, id, from string) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	c := l.clusters[clusterID]
	if c == nil || !c.leads(l.cfg.Now()) {
		return ""
	}
	return c.standbyOn(kind, id, from, l.cfg.Now())
}

// promote has the standby's member promote the copy and start the workload's stack over it. Nothing is restored.
func (l *Leader) promote(ctx context.Context, clusterID, kind, id string, replica int, from, to string, b bundles.Bundle) bool {
	l.mu.Lock()
	c := l.clusters[clusterID]
	if c == nil || !c.leads(l.cfg.Now()) || c.members[to] == nil {
		l.mu.Unlock()
		return false
	}
	entry := lease.Entry{Kind: kind, ID: id, Replica: replica, Slug: b.Stack.Slug, SingleWriter: true}
	next := append([]lease.Entry(nil), c.placement...)
	for i := range next {
		if next[i].Kind == kind && next[i].ID == id && next[i].Replica == replica {
			next[i].Member = to
			entry = next[i]
		}
	}
	start := l.startOrder(b)
	start.Restore = nil
	start.Promote = &lease.Promote{Container: b.Promote.Container, User: b.Promote.User, Command: b.Promote.Command}
	order := lease.Order{Cluster: clusterID, Member: to, Epoch: c.epoch, Placement: c.orderPlacement(next), Start: start, Return: c.returning[to]}
	fp := c.members[to].Fingerprint
	l.mu.Unlock()

	if err := l.sendStart(ctx, fp, order); err != nil {
		l.cfg.Logf("deplo-cluster: %s could not promote the standby copy of %s: %v", to, b.Stack.Slug, err)
		return false
	}
	entry.Member = to
	// The copy's memory was already counted on its member.
	l.commit(clusterID, entry, 0, from, nil, 0, true)
	return true
}
