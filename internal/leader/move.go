package leader

import (
	"context"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/bundles"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

// Why a move a person asked for is not made.
const (
	// StillAnswering is a member that answers, or whose lease may not have run out yet: its copy may be running.
	StillAnswering = "still_answering"
	NoBackup       = "no_backup"
	Unreadable     = "unreadable"
	NotPlaced      = "not_placed"
	NotLeading     = "not_leading"
	NoPath         = "no_path"
	StartFailed    = "start_failed"
)

// MoveRequest asks for one workload that waits for its member to be brought up elsewhere. Sealed is its bundle
// with the backup to restore, for this one move: the bundle the leader keeps names none, which is why it waits.
type MoveRequest struct {
	Cluster string `json:"cluster"`
	Kind    string `json:"kind"`
	ID      string `json:"id"`
	Sealed  string `json:"sealed"`
}

// MoveAnswer is OK once the move is taken; a move that then fails is a move_failed event.
type MoveAnswer struct {
	OK     bool   `json:"ok"`
	Reason string `json:"reason,omitempty"`
}

type pendingMove struct {
	cluster string
	bundle  bundles.Bundle
}

// leftBy is the member a workload last ran on, and why it may not be started elsewhere yet: never two writers.
// The caller holds l.mu.
func (c *cluster) leftBy(kind, id string, now time.Time) (from, why string) {
	found := false
	for _, e := range c.placement {
		if e.Kind == kind && e.ID == id && e.Replica == 0 {
			from, found = e.Member, true
		}
	}
	if !found {
		return "", NotPlaced
	}
	if from == "" {
		if w := c.waiting[entryKey(kind, id, 0)]; w != nil {
			from = w.From
		}
		return from, ""
	}
	if m := c.members[from]; m != nil && (now.Sub(m.lastAck) < FailoverAfter || m.presumedAlive(now) || now.Before(c.extended[from].Add(FailoverAfter-lease.FenceAfter))) {
		return from, StillAnswering
	}
	return from, ""
}

func (l *Leader) leftNow(clusterID, kind, id string) (from, why string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	c := l.clusters[clusterID]
	if c == nil || !c.leads(l.cfg.Now()) {
		return "", NotLeading
	}
	return c.leftBy(kind, id, l.cfg.Now())
}

// Move takes a person's order to bring a waiting workload back from a backup. It answers at once; the next
// failover pass makes the move, so a long restore never holds the call.
func (l *Leader) Move(r MoveRequest) MoveAnswer {
	no := func(why string) MoveAnswer { return MoveAnswer{Reason: why} }
	if l.cfg.Bundles == nil {
		return no(Unreadable)
	}
	b, err := l.cfg.Bundles.OpenBase64(r.Sealed)
	if err != nil || b.Kind != r.Kind || b.Workload != r.ID {
		return no(Unreadable)
	}
	if b.Restore == nil {
		return no(NoBackup)
	}
	if l.cfg.PathOK != nil && !l.cfg.PathOK() {
		return no(NoPath)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	c := l.clusters[r.Cluster]
	if c == nil || !c.leads(l.cfg.Now()) {
		return no(NotLeading)
	}
	if _, why := c.leftBy(r.Kind, r.ID, l.cfg.Now()); why != "" {
		return no(why)
	}
	l.pending = append(l.pending, pendingMove{r.Cluster, b})
	return MoveAnswer{OK: true}
}

// runMove makes one asked-for move. One ask is one try: a failure is reported, never retried on its own.
func (l *Leader) runMove(ctx context.Context, m pendingMove) {
	b := m.bundle
	o := l.relocate(ctx, m.cluster, b.Kind, b.Workload, 0, "", &b)
	if o.member != "" {
		return
	}
	why := o.why
	if o.refusal != nil {
		why = string(o.refusal.Reason)
	}
	if why == "" {
		why = StartFailed
	}
	l.log.Append("move_failed", m.cluster, map[string]any{"kind": b.Kind, "workloadId": b.Workload, "reason": why})
}
