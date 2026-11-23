package leader

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

// A line of succession: the members that lead after this leader, heir first, then Deplo's machine (ADR-0043).
// https://deplo.build/docs/operations/clusters/leader#the-line-of-succession

const (
	// LineLength is how many members a line names before Deplo's machine.
	LineLength = 2
	// SucceedFor is how long one ask to lead keeps this leader trying without a quorum before it stands by again.
	SucceedFor = 10 * time.Second
)

// line is the heirs Deplo named that answered within NotRespondingAfter and still count, heir first. An heir this
// leader stops hearing leaves the next renewal's line, and comes back at its end. The caller holds l.mu.
func (c *cluster) line(now time.Time) []string {
	alive := func(id string) bool {
		m := c.members[id]
		_, gone := c.gone[id]
		return m != nil && !gone && id != c.roster.Self && now.Sub(latest(m.lastAck, m.answered)) < lease.NotRespondingAfter && slices.Contains(c.roster.Heirs, id)
	}
	order := slices.DeleteFunc(slices.Clone(c.order), func(id string) bool { return !alive(id) })
	for _, id := range c.roster.Heirs {
		if alive(id) && !slices.Contains(order, id) {
			order = append(order, id)
		}
	}
	c.order = order
	return slices.Clone(order[:min(len(order), LineLength)])
}

// lineEnd is Deplo's machine, the end of every line, unless this leader is the one there.
func (c *cluster) lineEnd() string {
	if c.roster.Deplo == c.roster.Self {
		return ""
	}
	return c.roster.Deplo
}

// due says whether a pass goes to this cluster: a pre-vote only while a Succeed epoch is not led yet, and its probe
// only right after a quorum said it would take the epoch.
func (c *cluster) due(p pass, now time.Time) bool {
	switch {
	case p.prevote:
		return c.succeed
	case p.probe && c.succeed:
		return now.Sub(c.prevoted) < 2*lease.RenewEvery
	}
	return true
}

// Standby keeps a roster Deplo gives this leader to take the cluster over from when its member is next in line. It
// changes nothing this leader leads: see Succeed.
func (l *Leader) Standby(r Roster) error {
	if r.Cluster == "" {
		return errors.New("a roster names its cluster")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.standby[r.Cluster] = r
	l.keep("standby.json", l.standby)
	return nil
}

func (l *Leader) loadStandby() {
	raw, err := os.ReadFile(filepath.Join(l.cfg.StateDir, "standby.json"))
	if err == nil {
		err = json.Unmarshal(raw, &l.standby)
	}
	if err != nil && !os.IsNotExist(err) {
		l.cfg.Logf("deplo-cluster: standby.json cannot be read (%v): this leader takes over nothing until Deplo sends it again", err)
	}
	if l.standby == nil {
		l.standby = map[string]Roster{}
	}
}

// SucceedAsk is what the agent beside this leader hands it when its member is next in line and the leader is silent.
type SucceedAsk struct {
	Cluster   string        `json:"cluster"`
	Epoch     uint64        `json:"epoch"`
	Placement []lease.Entry `json:"placement"`
}

// Succeed has this leader try to lead the cluster at the asked epoch, from the standby roster and the placement the
// member last held. It leads only once a quorum pre-votes and then takes the epoch; asked again, it keeps trying.
func (l *Leader) Succeed(a SucceedAsk) error {
	now := l.cfg.Now()
	l.mu.Lock()
	// One that leads keeps its epoch: an ask means its own member went unheard, and a newer epoch would not mend that.
	if c := l.clusters[a.Cluster]; c != nil && !c.superseded && (c.epoch >= a.Epoch || c.leads(now)) {
		if c.succeed {
			c.succeedUntil = now.Add(SucceedFor)
		}
		l.mu.Unlock()
		return nil
	}
	r, ok := l.standby[a.Cluster]
	held := l.epochs[a.Cluster]
	l.mu.Unlock()
	if !ok {
		return errors.New("this leader holds no standby roster for that cluster")
	}
	r.Epoch, r.Succeed = max(a.Epoch, held), true
	r.Placement, r.Standbys = nil, nil
	for _, e := range a.Placement {
		if e.Standby {
			r.Standbys = append(r.Standbys, e)
		} else {
			r.Placement = append(r.Placement, e)
		}
	}
	if err := l.SetRoster(r); err != nil {
		return err
	}
	l.cfg.Logf("deplo-cluster: cluster %s: its leader is silent and this server is next in line: trying to lead at epoch %d", a.Cluster, r.Epoch)
	return nil
}

// standDown drops a cluster this leader tried to take over and that no quorum took by its deadline: it stands by again.
func (l *Leader) standDown(now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for id, c := range l.clusters {
		if c.succeed && c.sure.IsZero() && now.After(c.succeedUntil) {
			l.cfg.Logf("deplo-cluster: cluster %s: no quorum took epoch %d: standing by", id, c.epoch)
			delete(l.clusters, id)
			l.saveRosters()
		}
	}
}

// succeeded tells the control plane, once l.mu is released, that this leader leads a cluster on its own word.
func (l *Leader) succeeded(id string, epoch uint64) func() {
	return func() {
		l.cfg.Logf("deplo-cluster: cluster %s: a quorum took epoch %d: this leader leads it", id, epoch)
		l.log.Append("succeeded", id, map[string]uint64{"epoch": epoch})
	}
}

func latest(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
