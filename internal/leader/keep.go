package leader

import (
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

// leftOn is the entries that moved off a member and run on another one now.
func (c *cluster) leftOn(memberID string) []lease.Entry {
	var out []lease.Entry
	for _, e := range c.placement {
		if _, ok := c.left[memberID][entryKey(e.Kind, e.ID, e.Replica)]; ok && e.Member != "" && e.Member != memberID && e.Slug != "" {
			out = append(out, e)
		}
	}
	return out
}

// keepsFor opens the bundle of each workload that moved off a member and passes its keep on, unread.
func (l *Leader) keepsFor(clusterID, memberID string) []lease.Keep {
	if l.cfg.Bundles == nil {
		return nil
	}
	l.mu.Lock()
	var entries []lease.Entry
	if c := l.clusters[clusterID]; c != nil {
		entries = c.leftOn(memberID)
	}
	l.mu.Unlock()
	var out []lease.Keep
	for _, e := range entries {
		b, err := l.cfg.Bundles.Unseal(clusterID, e.Kind, e.ID)
		if err != nil || b.Keep == nil || len(b.Keep.Request) == 0 {
			continue
		}
		out = append(out, lease.Keep{Slug: e.Slug, Ref: b.Keep.Ref, StoreMember: b.Keep.StoreMember, Request: b.Keep.Request})
	}
	return out
}

// savedAndKept names the workloads of a member's report; the caller holds l.mu.
func (c *cluster) savedAndKept(memberID string, r *lease.Returned) ([]Saved, []Kept) {
	find := func(slug string) (lease.Entry, bool) {
		for _, e := range c.placement {
			if e.Slug == slug && slug != "" {
				return e, true
			}
		}
		return lease.Entry{}, false
	}
	var saved []Saved
	for _, s := range r.Saved {
		e, ok := find(s.Slug)
		if !ok {
			continue
		}
		v := Saved{Kind: e.Kind, ID: e.ID, Ref: s.Ref, ObjectKey: s.ObjectKey, SizeBytes: s.SizeBytes, DecryptedSizeBytes: s.DecryptedSizeBytes, Sha256: s.Sha256, At: s.At}
		if at, ok := c.left[memberID][entryKey(e.Kind, e.ID, e.Replica)]; ok {
			v.FailedOverAt = at.UTC().Format(time.RFC3339)
		}
		saved = append(saved, v)
	}
	var kept []Kept
	for _, k := range r.Kept {
		if e, ok := find(k.Slug); ok {
			kept = append(kept, Kept{Kind: e.Kind, ID: e.ID, Ref: k.Ref, Reason: k.Reason, Error: k.Error})
		}
	}
	return saved, kept
}
