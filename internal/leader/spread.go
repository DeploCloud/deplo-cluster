package leader

import (
	"github.com/DeploCloud/deplo-cluster/internal/lease"
	"github.com/DeploCloud/deplo-cluster/internal/placement"
)

// spread answers a request for several replicas. One the leader left waiting stays its own to place, so it
// is answered empty; the caller holds l.mu.
func (c *cluster) spread(r PlaceRequest, facts []placement.Member) placement.Answer {
	out := make([]string, r.Replicas)
	var open []int
	var current []string
	for i := range out {
		if c.waiting[entryKey(r.Workload.Kind, r.Workload.ID, i)] != nil {
			continue
		}
		open = append(open, i)
		at := ""
		if i < len(r.Current) {
			at = r.Current[i]
		}
		current = append(current, at)
	}
	members, refusal := placement.Spread(r.Cluster, r.Workload, facts, len(open), current)
	if refusal != nil {
		return placement.Answer{Refusal: refusal}
	}
	for j, i := range open {
		out[i] = members[j]
	}
	return placement.Answer{Members: out}
}

// sibling is the member another replica of the workload runs on, when it is not the one being left.
func sibling(all []lease.Entry, kind, id string, replica int, from string) string {
	for _, e := range all {
		if e.Kind == kind && e.ID == id && e.Replica != replica && e.Member != "" && e.Member != from {
			return e.Member
		}
	}
	return ""
}

// countOn is how many replicas of the workload the placement gives a member; an empty member counts them all.
func countOn(all []lease.Entry, kind, id, member string) int {
	n := 0
	for _, e := range all {
		if e.Kind == kind && e.ID == id && (member == "" || e.Member == member) {
			n++
		}
	}
	return n
}

// follow records a replica on the member its stack was already started on, and tells the control plane where.
func (l *Leader) follow(clusterID string, e lease.Entry, from string) {
	l.mu.Lock()
	c := l.clusters[clusterID]
	if c == nil {
		l.mu.Unlock()
		return
	}
	if p, ok := c.place(e.Kind, e.ID, e.Replica, e.Member); ok {
		e = p
	} else {
		c.placement = append(c.placement, e)
	}
	key := entryKey(e.Kind, e.ID, e.Replica)
	c.auth[key] = authority{At: l.cfg.Now(), Entry: e, Told: Told{From: from, Reason: StoppedAnswering}}
	delete(c.waiting, key)
	members, epoch := replicaMembers(c.placement, e.Kind, e.ID), c.epoch
	l.saveFailover()
	l.mu.Unlock()
	l.stamp(clusterID, key, l.tellPlacement(clusterID, e.Kind, e.ID, members, epoch).Seq)
}
