package leader

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

type persisted struct {
	Waiting   map[string]*Waiting             `json:"waiting"`
	Auth      map[string]authority            `json:"auth"`
	Extended  map[string]time.Time            `json:"extended,omitempty"`
	Returning map[string]bool                 `json:"returning,omitempty"`
	Dropped   map[string]bool                 `json:"dropped,omitempty"`
	Left      map[string]map[string]time.Time `json:"left,omitempty"`
	Kept      map[string]map[string]time.Time `json:"kept,omitempty"`
	Gone      map[string]time.Time            `json:"gone,omitempty"`
}

func (l *Leader) newCluster(id string) *cluster {
	c := &cluster{members: map[string]*member{}, moved: map[string]bool{}, returning: map[string]bool{}, dropped: map[string]bool{}, waiting: map[string]*Waiting{}, auth: map[string]authority{}, extended: map[string]time.Time{}, left: map[string]map[string]time.Time{}, kept: map[string]map[string]time.Time{}, starting: map[string]bool{}}
	if p, ok := l.restored[id]; ok {
		if p.Extended != nil {
			c.extended = p.Extended
		}
		if p.Returning != nil {
			c.returning = p.Returning
		}
		if p.Dropped != nil {
			c.dropped = p.Dropped
		}
		if p.Left != nil {
			c.left = p.Left
		}
		if p.Kept != nil {
			c.kept = p.Kept
		}
		if p.Waiting != nil {
			c.waiting = p.Waiting
		}
		if p.Auth != nil {
			c.auth = p.Auth
		}
		c.gone = p.Gone
		delete(l.restored, id)
	}
	return c
}

func (l *Leader) loadFailover() {
	l.restored = map[string]persisted{}
	raw, err := os.ReadFile(filepath.Join(l.cfg.StateDir, "failover.json"))
	if err == nil {
		err = json.Unmarshal(raw, &l.restored)
	}
	// Safe to go on without: a member not heard since this start is presumed to hold the longest lease.
	if err != nil && !os.IsNotExist(err) {
		l.restored = map[string]persisted{}
		l.cfg.Logf("deplo-cluster: failover.json cannot be read (%v): what this leader had moved or left waiting is forgotten", err)
	}
}

// saveFailover keeps what a restarted leader must not forget; the caller holds l.mu.
func (l *Leader) saveFailover() {
	if l.cfg.StateDir == "" {
		return
	}
	out := map[string]persisted{}
	for id, p := range l.restored {
		out[id] = p
	}
	for id, c := range l.clusters {
		out[id] = persisted{Waiting: c.waiting, Auth: c.auth, Extended: c.extended, Returning: c.returning, Dropped: c.dropped, Left: c.left, Kept: c.kept, Gone: c.gone}
	}
	l.keep("failover.json", out)
}

// reconcilePlacement is the roster's placement with the leader's own moves laid over it. A move stands until the
// roster repeats it, however long that takes; a workload the roster no longer names is the control plane's to drop.
func (l *Leader) reconcilePlacement(c *cluster, roster []lease.Entry) []lease.Entry {
	out := append([]lease.Entry(nil), roster...)
	at := map[string]int{}
	for i, e := range out {
		at[entryKey(e.Kind, e.ID, e.Replica)] = i
	}
	for k, a := range c.auth {
		i, named := at[k]
		switch {
		case !named:
			delete(c.auth, k)
			delete(c.waiting, k)
		case out[i].Member == a.Entry.Member:
			delete(c.auth, k)
		default:
			out[i].Member = a.Entry.Member
		}
	}
	l.saveFailover()
	return out
}

// adoptWaiting takes over what the roster says is waiting for room, so a leader that starts leading keeps
// trying what the one before it could not place; the caller holds l.mu.
func (l *Leader) adoptWaiting(c *cluster, now time.Time) {
	for _, e := range c.placement {
		key := entryKey(e.Kind, e.ID, e.Replica)
		if e.Member == "" && c.waiting[key] == nil {
			c.waiting[key] = &Waiting{Kind: e.Kind, ID: e.ID, Replica: e.Replica, Reason: StoppedAnswering, Slug: e.Slug, NextTry: now.Add(RetryEvery), Adopted: true}
		}
	}
	l.saveFailover()
}

func (l *Leader) loadRosters() {
	var saved map[string]Roster
	raw, err := os.ReadFile(filepath.Join(l.cfg.StateDir, "rosters.json"))
	if err == nil {
		err = json.Unmarshal(raw, &saved)
	}
	if err != nil {
		if !os.IsNotExist(err) {
			l.cfg.Logf("deplo-cluster: rosters.json cannot be read (%v): serving no cluster until Deplo sends its roster", err)
		}
		return
	}
	for _, r := range saved {
		// Refused: another leader took over while this one was down, so what it had moved is not its to keep.
		if errors.Is(l.SetRoster(r), ErrOlderEpoch) {
			delete(l.restored, r.Cluster)
		}
	}
}

// saveRosters keeps every cluster's last roster; the caller holds l.mu.
func (l *Leader) saveRosters() {
	if l.cfg.StateDir == "" {
		return
	}
	out := map[string]Roster{}
	for id, c := range l.clusters {
		out[id] = c.roster
	}
	l.keep("rosters.json", out)
}
