package leader_test

import (
	"slices"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/bundles"
	evlog "github.com/DeploCloud/deplo-cluster/internal/events"
	"github.com/DeploCloud/deplo-cluster/internal/leader"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

// withLine is a roster for the leader on self, with srv_b then srv_c as heirs, and agents that pre-vote.
func withLine(r leader.Roster, self string) leader.Roster {
	r.Self, r.Deplo, r.Heirs = self, "srv_deplo", []string{"srv_b", "srv_c"}
	for i := range r.Members {
		r.Members[i].Succession, r.Members[i].ShortFence = true, true
	}
	return r
}

// lined is three members led from srv_a, the leader's own member.
func lined(t *testing.T) *fo {
	t.Helper()
	f := startFO(t, abc())
	if err := f.l.SetRoster(withLine(snapshotRoster(f, 1), "srv_a")); err != nil {
		t.Fatal(err)
	}
	f.pass(3 * time.Second)
	return f
}

// heir is the leader beside srv_b, standing by with Deplo's roster; it shares f's members and clock.
func heir(t *testing.T, f *fo) *fo {
	t.Helper()
	g := &fo{clk: f.clk, ms: f.ms, pathOK: true, dir: t.TempDir(), blind: map[string]bool{}}
	var err error
	if g.store, err = bundles.Open(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	g.l = leader.New(leader.Config{Dial: g.dial, Now: g.clk.Now, StateDir: g.dir, Bundles: g.store, PathOK: func() bool { return true }, StartGrace: -1, Logf: g.logf})
	if err := g.l.Standby(withLine(snapshotRoster(f, 1), "srv_b")); err != nil {
		t.Fatal(err)
	}
	g.away = true
	return g
}

func ask(f *fo, epoch uint64) leader.SucceedAsk {
	return leader.SucceedAsk{Cluster: "clu_1", Epoch: epoch, Placement: f.l.Snapshot().Clusters["clu_1"].Placement}
}

// leadsNow is a leader that would renew and move work right now.
func leadsNow(f *fo) bool { return f.l.Snapshot().Clusters["clu_1"].Leads }

func TestEveryRenewalNamesTheLineAndAnHeirNotHeardLeavesItForTheEnd(t *testing.T) {
	f := lined(t)
	if o := f.lastOrder("fp-srv_c"); !slices.Equal(o.Line, []string{"srv_b", "srv_c"}) || o.Last != "srv_deplo" || o.From != "srv_a" {
		t.Fatalf("a renewal names line %v, last %q, from %q", o.Line, o.Last, o.From)
	}
	f.ms.cut["fp-srv_b"] = true
	f.pass(4 * time.Second)
	if o := f.lastOrder("fp-srv_c"); !slices.Equal(o.Line, []string{"srv_c"}) {
		t.Fatalf("an heir the leader stopped hearing is still named: %v", o.Line)
	}
	f.ms.cut["fp-srv_b"] = false
	f.pass(2 * time.Second)
	if o := f.lastOrder("fp-srv_b"); !slices.Equal(o.Line, []string{"srv_c", "srv_b"}) {
		t.Fatalf("back, srv_b reads line %v: it is no longer heir", o.Line)
	}
}

// The leader's server stops: the heir is refused while the members still heard the leader within the removal time,
// then a quorum takes its epoch and it leads, naming the next line. The old leader, back, steps down.
func TestTheHeirLeadsOnceNoMemberHearsTheLeaderAnyMore(t *testing.T) {
	f := lined(t)
	g := heir(t, f)
	f.ms.cut["fp-srv_a"] = true
	g.blind["srv_a"] = true
	f.clk.Add(5 * time.Second)
	if err := g.l.Succeed(ask(f, 2)); err != nil {
		t.Fatal(err)
	}
	g.step()
	if leadsNow(g) || f.ms.epoch["fp-srv_c"] != 1 {
		t.Fatalf("the heir leads, or a member took its epoch, while the members heard the leader 5 s ago")
	}
	f.clk.Add(lease.PreVoteFor)
	if err := g.l.Succeed(ask(f, 2)); err != nil {
		t.Fatal(err)
	}
	g.step()
	if !leadsNow(g) || f.ms.epoch["fp-srv_c"] != 2 {
		t.Fatalf("the heir does not lead once the members stopped hearing the leader: %+v", g.l.Snapshot().Clusters["clu_1"])
	}
	if o := f.lastOrder("fp-srv_c"); o.From != "srv_b" || !slices.Equal(o.Line, []string{"srv_c"}) || o.Last != "srv_deplo" {
		t.Fatalf("the heir's renewal: from %q, line %v, last %q", o.From, o.Line, o.Last)
	}
	ev, _ := g.l.Events().Since(0)
	if !slices.ContainsFunc(ev, func(e evlog.Event) bool { return e.Type == "succeeded" }) {
		t.Fatal("the control plane is not told the heir leads")
	}
	f.ms.cut["fp-srv_a"] = false
	f.pass(2 * time.Second)
	if v := f.l.Snapshot().Clusters["clu_1"]; !v.Superseded || v.Leads {
		t.Fatalf("the old leader, back, still leads: %+v", v)
	}
}

// A cut between the leader and its heir only: the heir hears a quorum refuse it, and its own member never takes its
// epoch, so the leader keeps leading and names the next member heir.
func TestACutBetweenTheLeaderAndItsHeirChangesNothing(t *testing.T) {
	f := lined(t)
	g := heir(t, f)
	f.blind["srv_b"], g.blind["srv_a"] = true, true
	for i := 0; i < 30; i++ {
		f.clk.Add(lease.RenewEvery)
		f.step()
		if i >= 10 {
			_ = g.l.Succeed(ask(f, 2))
		}
		g.step()
	}
	if !leadsNow(f) || leadsNow(g) || f.ms.epoch["fp-srv_b"] != 1 || f.ms.epoch["fp-srv_c"] != 1 {
		t.Fatalf("the cut changed the leader: f leads %v, g leads %v, epochs %v", leadsNow(f), leadsNow(g), f.ms.epoch)
	}
	if o := f.lastOrder("fp-srv_c"); !slices.Equal(o.Line, []string{"srv_c"}) {
		t.Fatalf("the leader still names the heir it cannot hear: %v", o.Line)
	}
}

// The heir alone cut off for a minute: nothing takes its epoch, it stands by again, and back it is a member.
func TestAnHeirCutOffAloneTakesNothingAndStandsByAgain(t *testing.T) {
	f := lined(t)
	g := heir(t, f)
	f.blind["srv_b"], g.blind["srv_a"], g.blind["srv_c"] = true, true, true
	for i := 0; i < 60; i++ {
		f.clk.Add(lease.RenewEvery)
		f.step()
		if i >= 10 {
			_ = g.l.Succeed(ask(f, 2))
		}
		g.step()
		if leadsNow(g) {
			t.Fatal("an heir cut off alone leads")
		}
	}
	if f.ms.epoch["fp-srv_a"] != 1 || f.ms.epoch["fp-srv_c"] != 1 || f.ms.epoch["fp-srv_b"] != 1 {
		t.Fatalf("a member took the isolated heir's epoch: %v", f.ms.epoch)
	}
	f.blind["srv_b"], g.blind = false, map[string]bool{}
	for i := 0; i < 15; i++ {
		f.clk.Add(lease.RenewEvery)
		f.step()
		g.step()
	}
	if _, tries := g.l.Snapshot().Clusters["clu_1"]; tries || !leadsNow(f) {
		t.Fatal("back on the network, the heir still tries to lead")
	}
	if o := f.lastOrder("fp-srv_b"); !slices.Equal(o.Line, []string{"srv_c", "srv_b"}) {
		t.Fatalf("back, srv_b reads line %v", o.Line)
	}
}

func TestAPreVoteGoesOnlyToAgentsThatAnswerIt(t *testing.T) {
	f := lined(t)
	g := heir(t, f)
	r := withLine(snapshotRoster(f, 1), "srv_b")
	r.Members[2].Succession = false // srv_c's agent is older
	if err := g.l.Standby(r); err != nil {
		t.Fatal(err)
	}
	f.ms.cut["fp-srv_a"] = true
	f.clk.Add(lease.PreVoteFor + time.Second)
	_ = g.l.Succeed(ask(f, 2))
	g.step()
	if leadsNow(g) || f.ms.prevotes["fp-srv_c"] != 0 {
		t.Fatalf("an older agent was asked a pre-vote (%d), or one of three led", f.ms.prevotes["fp-srv_c"])
	}
}

func TestAStandbyRosterOutlivesARestart(t *testing.T) {
	f := lined(t)
	g := heir(t, f)
	again := leader.New(leader.Config{Dial: g.dial, Now: g.clk.Now, StateDir: g.dir, Bundles: g.store})
	if err := again.Succeed(ask(f, 2)); err != nil {
		t.Fatalf("a restarted heir lost its standby roster: %v", err)
	}
	if err := leader.New(leader.Config{Dial: g.dial, Now: g.clk.Now, StateDir: t.TempDir()}).Succeed(ask(f, 2)); err == nil {
		t.Fatal("a leader with no standby roster tried to lead")
	}
}
