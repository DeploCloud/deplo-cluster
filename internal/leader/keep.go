package leader

import (
	"sort"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

// What a member still holds of a database or a Volume App that moved off it is saved before it is removed
// (spec #176): the leader remembers what moved and when, and hands the member the bundle's keep when it returns.

// Saved is one archive a returning member wrote, and when the workload it belongs to had failed over.
type Saved struct {
	Kind               string `json:"kind"`
	ID                 string `json:"workloadId"`
	Ref                string `json:"ref,omitempty"`
	ObjectKey          string `json:"objectKey"`
	SizeBytes          int64  `json:"sizeBytes"`
	DecryptedSizeBytes int64  `json:"decryptedSizeBytes,omitempty"`
	Sha256             string `json:"sha256,omitempty"`
	At                 string `json:"at"`
	FailedOverAt       string `json:"failedOverAt,omitempty"`
}

// Kept is data a returning member could not save, so it stays on that member.
type Kept struct {
	Kind   string `json:"kind"`
	ID     string `json:"workloadId"`
	Ref    string `json:"ref,omitempty"`
	Reason string `json:"reason"`
	Error  string `json:"error,omitempty"`
}

// owesReturn says a member's work was moved and it has not said yet what it did with its copies.
func (c *cluster) owesReturn(memberID string) bool {
	return c.returning[memberID] || c.moved[memberID] || len(c.left[memberID]) > 0
}

// noteLeft remembers a single-writer workload that started elsewhere than the member it ran on.
func (c *cluster) noteLeft(from string, e lease.Entry, at time.Time) {
	if !e.SingleWriter || from == "" || from == e.Member {
		return
	}
	if c.left[from] == nil {
		c.left[from] = map[string]time.Time{}
	}
	c.left[from][entryKey(e.Kind, e.ID, e.Replica)] = at
}

// heldBy is what a member still holds of the workloads that moved off it: the entry, and when it failed over.
type heldBy struct {
	e  lease.Entry
	at time.Time
	// kept is data the member already said it could not save: it is asked about until it keeps none.
	kept bool
}

// heldOn is what moved off a member and it has not said is gone. A workload the cluster no longer names is
// forgotten here; the caller holds l.mu.
func (c *cluster) heldOn(memberID string) []heldBy {
	var out []heldBy
	seen := map[string]bool{}
	for _, e := range c.placement {
		key := entryKey(e.Kind, e.ID, e.Replica)
		if e.Slug == "" {
			continue
		}
		if at, ok := c.kept[memberID][key]; ok {
			seen[key] = true
			out = append(out, heldBy{e, at, true})
		} else if at, ok := c.left[memberID][key]; ok && e.Member != "" && e.Member != memberID {
			out = append(out, heldBy{e, at, false})
		}
	}
	for key := range c.kept[memberID] {
		if !seen[key] {
			delete(c.kept[memberID], key)
		}
	}
	return out
}

// keepsFor opens the bundle of each workload a member still holds data of and passes its keep on, unread.
// What the member kept past its return is named even with nowhere to save it, so it can say when it is gone.
func (l *Leader) keepsFor(clusterID, memberID string) []lease.Keep {
	if l.cfg.Bundles == nil {
		return nil
	}
	l.mu.Lock()
	c := l.clusters[clusterID]
	if c == nil {
		l.mu.Unlock()
		return nil
	}
	held := c.heldOn(memberID)
	prints := map[string]string{}
	for id, m := range c.members {
		prints[id] = m.Fingerprint
	}
	l.mu.Unlock()
	var out []lease.Keep
	for _, h := range held {
		k := lease.Keep{Slug: h.e.Slug, FailedOverAt: h.at.UTC().Format(time.RFC3339)}
		b, err := l.cfg.Bundles.Unseal(clusterID, h.e.Kind, h.e.ID)
		if err == nil && b.Keep != nil && len(b.Keep.Request) > 0 && h.e.Member != "" && h.e.Member != memberID {
			k.Ref, k.StoreMember, k.Request = b.Keep.Ref, b.Keep.StoreMember, b.Keep.Request
			k.StoreFingerprint = prints[b.Keep.StoreMember]
		} else if !h.kept {
			continue
		}
		out = append(out, k)
	}
	return out
}

// storesFor is the archives a member is told to accept: what each other member still holds of a workload whose
// destination is on this member's disk. Nothing of another cluster's is ever in it.
func (l *Leader) storesFor(clusterID, memberID string) []lease.Store {
	if l.cfg.Bundles == nil {
		return nil
	}
	type from struct {
		id, print string
		held      []heldBy
	}
	var senders []from
	l.mu.Lock()
	if c := l.clusters[clusterID]; c != nil && c.members[memberID] != nil {
		for id, m := range c.members {
			if id == memberID || len(c.left[id])+len(c.kept[id]) == 0 {
				continue
			}
			senders = append(senders, from{id, m.Fingerprint, c.heldOn(id)})
		}
	}
	l.mu.Unlock()
	var out []lease.Store
	for _, s := range senders {
		for _, h := range s.held {
			if h.e.Member == "" || h.e.Member == s.id {
				continue
			}
			b, err := l.cfg.Bundles.Unseal(clusterID, h.e.Kind, h.e.ID)
			if err != nil || b.Keep == nil || b.Keep.StoreMember != memberID || b.Keep.StoreStem == "" {
				continue
			}
			out = append(out, lease.Store{From: s.print, Root: b.Keep.StoreRoot, ObjectKey: b.Keep.StoreStem + lease.LeftName(h.at, s.id, true)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].From+out[i].ObjectKey < out[j].From+out[j].ObjectKey })
	return out
}

// entryBySlug is the placed entry of a stack; the caller holds l.mu.
func (c *cluster) entryBySlug(slug string) (lease.Entry, bool) {
	for _, e := range c.placement {
		if e.Slug == slug && slug != "" {
			return e, true
		}
	}
	return lease.Entry{}, false
}

// savedOf names the workloads of the archives a member wrote, with the failover each belongs to.
func (c *cluster) savedOf(list []lease.Saved, at map[string]time.Time) []Saved {
	var saved []Saved
	for _, s := range list {
		e, ok := c.entryBySlug(s.Slug)
		if !ok {
			continue
		}
		v := Saved{Kind: e.Kind, ID: e.ID, Ref: s.Ref, ObjectKey: s.ObjectKey, SizeBytes: s.SizeBytes, DecryptedSizeBytes: s.DecryptedSizeBytes, Sha256: s.Sha256, At: s.At}
		if t, ok := at[entryKey(e.Kind, e.ID, e.Replica)]; ok {
			v.FailedOverAt = t.UTC().Format(time.RFC3339)
		}
		saved = append(saved, v)
	}
	return saved
}

// savedAndKept names the workloads of a member's report, and remembers what it kept; the caller holds l.mu.
func (c *cluster) savedAndKept(memberID string, r *lease.Returned) ([]Saved, []Kept) {
	saved := c.savedOf(r.Saved, c.left[memberID])
	var kept []Kept
	for _, k := range r.Kept {
		e, ok := c.entryBySlug(k.Slug)
		if !ok {
			continue
		}
		kept = append(kept, Kept{Kind: e.Kind, ID: e.ID, Ref: k.Ref, Reason: k.Reason, Error: k.Error})
		key := entryKey(e.Kind, e.ID, e.Replica)
		if at, ok := c.left[memberID][key]; ok {
			if c.kept[memberID] == nil {
				c.kept[memberID] = map[string]time.Time{}
			}
			c.kept[memberID][key] = at
		}
	}
	return saved, kept
}

// heardLater takes a member's word on data it kept past its return: what it saved since is told once, and
// nothing it no longer keeps is asked about again. The caller holds l.mu and runs the result after.
func (l *Leader) heardLater(clusterID, memberID string, later *lease.Later) func() {
	c := l.clusters[clusterID]
	if c == nil || later == nil || len(c.kept[memberID]) == 0 {
		return func() {}
	}
	var mine []lease.Saved
	for _, s := range later.Saved {
		if e, ok := c.entryBySlug(s.Slug); ok {
			if _, ok := c.kept[memberID][entryKey(e.Kind, e.ID, e.Replica)]; ok {
				mine = append(mine, s)
			}
		}
	}
	saved := c.savedOf(mine, c.kept[memberID])
	for _, slug := range later.Gone {
		if e, ok := c.entryBySlug(slug); ok {
			delete(c.kept[memberID], entryKey(e.Kind, e.ID, e.Replica))
		}
	}
	if len(c.kept[memberID]) == 0 {
		delete(c.kept, memberID)
	}
	l.saveFailover()
	return func() {
		if len(saved) > 0 {
			l.log.Append("left_saved", clusterID, map[string]any{"member": memberID, "saved": saved})
		}
	}
}
