package leader

import (
	"slices"
	"sort"

	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

// Returned is the reason a workload that found no room went back to the member that came back with it.
const Returned = "returned"

// Stopped is one workload whose copy a returning member removed, because it runs elsewhere now.
type Stopped struct {
	Kind string `json:"kind"`
	ID   string `json:"workloadId"`
}

// cameBack handles a member answering again after its work was dealt with: what found no room goes back to
// it, and once it reports what it removed, one event says so. The caller holds l.mu and runs the result after.
func (l *Leader) cameBack(clusterID, memberID string, report *lease.Returned) func() {
	c := l.clusters[clusterID]
	if c == nil || !c.owesReturn(memberID) {
		return func() {}
	}
	var back []lease.Entry
	if c.moved[memberID] {
		delete(c.moved, memberID)
		c.returning[memberID] = true
		back = l.giveBack(c, memberID, func(w *Waiting) bool { return w.From == memberID })
	}
	// A leader that restarted since it moved this member's work still owes it the question.
	c.returning[memberID] = true
	tell := l.placementOf(clusterID, c, back)
	var stopped []Stopped
	var data map[string]any
	if report != nil {
		delete(c.returning, memberID)
		stopped = []Stopped{}
		for _, slug := range report.Removed {
			for _, e := range c.placement {
				if e.Slug == slug && e.Slug != "" && !containsStopped(stopped, e.Kind, e.ID) {
					stopped = append(stopped, Stopped{e.Kind, e.ID})
				}
			}
		}
		data = map[string]any{"member": memberID, "stopped": stopped}
		if saved, kept := c.savedAndKept(memberID, report); len(saved)+len(kept) > 0 {
			if len(saved) > 0 {
				data["saved"] = saved
			}
			if len(kept) > 0 {
				data["kept"] = kept
			}
		}
		delete(c.left, memberID)
	}
	l.saveFailover()
	return func() {
		tell()
		if data != nil {
			l.log.Append("member_returned", clusterID, data)
		}
	}
}

// giveBack places on a member every waiting entry that is its own again; the caller holds l.mu.
func (l *Leader) giveBack(c *cluster, memberID string, its func(*Waiting) bool) []lease.Entry {
	var back []lease.Entry
	for key, w := range c.waiting {
		if !its(w) {
			continue
		}
		if p, ok := c.place(w.Kind, w.ID, w.Replica, memberID); ok {
			c.auth[key] = authority{At: l.cfg.Now(), Entry: p, Reason: Returned}
			back = append(back, p)
		}
		delete(c.waiting, key)
	}
	sort.Slice(back, func(i, j int) bool {
		return entryKey(back[i].Kind, back[i].ID, back[i].Replica) < entryKey(back[j].Kind, back[j].ID, back[j].Replica)
	})
	return back
}

// stillHas handles a member naming stacks its order placed nowhere: what waits for room is that member's
// again, so no second copy starts beside it. The caller holds l.mu and runs the result after.
func (l *Leader) stillHas(clusterID, memberID string, slugs []string) func() {
	c := l.clusters[clusterID]
	if c == nil || len(slugs) == 0 {
		return func() {}
	}
	back := l.giveBack(c, memberID, func(w *Waiting) bool { return w.Slug != "" && slices.Contains(slugs, w.Slug) })
	if len(back) == 0 {
		return func() {}
	}
	tell := l.placementOf(clusterID, c, back)
	l.saveFailover()
	return tell
}

// placementOf reads where the replicas of each given-back workload are now and returns what tells the control
// plane, to run once l.mu is released. The caller holds l.mu.
func (l *Leader) placementOf(clusterID string, c *cluster, back []lease.Entry) func() {
	type placed struct {
		kind, id string
		members  []string
	}
	var told []placed
	for _, e := range back {
		told = append(told, placed{e.Kind, e.ID, replicaMembers(c.placement, e.Kind, e.ID)})
	}
	epoch := c.epoch
	return func() {
		for _, p := range told {
			l.tellPlacement(clusterID, p.kind, p.id, p.members, epoch)
		}
	}
}

func containsStopped(all []Stopped, kind, id string) bool {
	for _, s := range all {
		if s.Kind == kind && s.ID == id {
			return true
		}
	}
	return false
}
