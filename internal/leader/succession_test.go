package leader_test

import (
	"context"
	"errors"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/bundles"
	evlog "github.com/DeploCloud/deplo-cluster/internal/events"
	"github.com/DeploCloud/deplo-cluster/internal/leader"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
	"github.com/DeploCloud/deplo-cluster/internal/wire"
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
	return heirOn(t, f, "srv_b")
}

// heirOn is the leader beside self, standing by.
func heirOn(t *testing.T, f *fo, self string) *fo {
	t.Helper()
	g := &fo{t: t, clk: f.clk, ms: f.ms, pathOK: true, dir: t.TempDir(), blind: map[string]bool{}}
	var err error
	if g.store, err = bundles.Open(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	g.l = newLeader(t, leader.Config{Dial: g.dial, Now: g.clk.Now, StateDir: g.dir, Bundles: g.store, PathOK: func() bool { return true }, StartGrace: -1, Logf: g.logf})
	if err := g.l.Standby(withLine(snapshotRoster(f, 1), self)); err != nil {
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
	if at := g.l.Snapshot().Clusters["clu_1"].LedAt; at != f.clk.Now().UTC().Format(time.RFC3339Nano) {
		t.Fatalf("the heir says it led at %q, not when a quorum took its epoch", at)
	}
	if o := f.lastOrder("fp-srv_c"); o.From != "srv_b" || !slices.Equal(o.Line, []string{"srv_c"}) || o.Last != "srv_deplo" {
		t.Fatalf("the heir's renewal: from %q, line %v, last %q", o.From, o.Line, o.Last)
	}
	since := g.l.Snapshot().Clusters["clu_1"].Since
	ev, _ := g.l.Events().Since(since - 1)
	if len(ev) == 0 || ev[0].Type != "epoch" || !slices.ContainsFunc(ev, func(e evlog.Event) bool { return e.Type == "succeeded" }) {
		t.Fatalf("a control plane that adopts the heir cannot replay this epoch from %d: %+v", since, ev)
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
	g.l.Flush()
	again := newLeader(t, leader.Config{Dial: g.dial, Now: g.clk.Now, StateDir: g.dir, Bundles: g.store})
	if err := again.Succeed(ask(f, 2)); err != nil {
		t.Fatalf("a restarted heir lost its standby roster: %v", err)
	}
	if err := newLeader(t, leader.Config{Dial: g.dial, Now: g.clk.Now, StateDir: t.TempDir()}).Succeed(ask(f, 2)); err == nil {
		t.Fatal("a leader with no standby roster tried to lead")
	}
}

// On cluster eu the leader's server was powered off: every pass spent the whole send timeout on it, so a pre-vote
// that a quorum answered was already 2 s old when its probe was due, and no heir, nor Deplo's machine, ever led.
func TestAnHeirLeadsWhileTheDeadLeadersServerTakesTheWholeSendTimeout(t *testing.T) {
	f := lined(t)
	g := heir(t, f)
	dead := func(ctx context.Context, to wire.Party) (net.Conn, error) {
		if to.Fingerprint == "fp-srv_a" {
			f.clk.Add(2 * time.Second)
			return nil, errors.New("i/o timeout")
		}
		return g.ms.dial(ctx, to)
	}
	g.l = newLeader(t, leader.Config{Dial: dead, Now: g.clk.Now, StateDir: t.TempDir(), Bundles: g.store, PathOK: func() bool { return true }, StartGrace: -1})
	if err := g.l.Standby(withLine(snapshotRoster(f, 1), "srv_b")); err != nil {
		t.Fatal(err)
	}
	f.ms.cut["fp-srv_a"] = true
	f.clk.Add(lease.PreVoteFor + time.Second)
	if err := g.l.Succeed(ask(f, 2)); err != nil {
		t.Fatal(err)
	}
	g.step()
	if !leadsNow(g) || f.ms.epoch["fp-srv_c"] != 2 {
		t.Fatalf("the heir does not lead with a dead member in its roster: %+v, epochs %v", g.l.Snapshot().Clusters["clu_1"], f.ms.epoch)
	}
}

// On cluster eu an heir that led was asked by its own server to lead again, at a newer epoch, and gave up the one
// it led at: two heirs passed the cluster back and forth. A leader that leads takes no ask.
func TestALeaderThatLeadsTakesNoAskToLeadAtANewerEpoch(t *testing.T) {
	f := lined(t)
	g := heir(t, f)
	f.ms.cut["fp-srv_a"] = true
	f.clk.Add(lease.PreVoteFor + time.Second)
	_ = g.l.Succeed(ask(f, 2))
	g.step()
	if !leadsNow(g) {
		t.Fatal("the heir does not lead")
	}
	if err := g.l.Succeed(ask(f, 5)); err != nil {
		t.Fatal(err)
	}
	g.step()
	if v := g.l.Snapshot().Clusters["clu_1"]; v.Epoch != 2 || !v.Leads {
		t.Fatalf("a leader that leads took an ask for a newer epoch: %+v", v)
	}
}

// eu, 23 Nov 2026: an heir that stood down after a try at epoch 61 tried again at 61 and led; the snapshot gave no
// epoch start, so Deplo adopted it from its newest event and replayed nothing of what it did.
func TestASecondTryAtTheSameEpochIsReplayedFromWhereItBegan(t *testing.T) {
	f := lined(t)
	g := heir(t, f)
	f.ms.cut["fp-srv_a"] = true
	g.blind["srv_a"], g.blind["srv_c"] = true, true
	f.clk.Add(lease.PreVoteFor + time.Second)
	if err := g.l.Succeed(ask(f, 2)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		f.clk.Add(lease.RenewEvery)
		g.step()
	}
	if _, tries := g.l.Snapshot().Clusters["clu_1"]; tries || leadsNow(g) {
		t.Fatal("alone, the heir still tries, or leads")
	}
	head := g.l.Events().Head()
	g.blind["srv_c"] = false
	if err := g.l.Succeed(ask(f, 2)); err != nil {
		t.Fatal(err)
	}
	g.step()
	if !leadsNow(g) {
		t.Fatalf("the second try does not lead: %+v", g.l.Snapshot().Clusters["clu_1"])
	}
	since := g.l.Snapshot().Clusters["clu_1"].Since
	ev, _ := g.l.Events().Since(since - 1)
	if since <= head || len(ev) == 0 || ev[0].Type != "epoch" || !slices.ContainsFunc(ev, func(e evlog.Event) bool { return e.Type == "succeeded" }) {
		t.Fatalf("since %d (head before the try %d): a control plane cannot replay this epoch: %+v", since, head, ev)
	}
}

// askAs is what member's agent hands the leader beside it: the last renewal's placement, gone set and heard times,
// aged by `since`, the time that renewal is old.
func askAs(f *fo, member string, epoch uint64, since time.Duration) leader.SucceedAsk {
	o := f.lastOrder("fp-" + member)
	heard := map[string]int64{}
	for id, ms := range o.Heard {
		heard[id] = ms + since.Milliseconds()
	}
	return leader.SucceedAsk{Cluster: "clu_1", Epoch: epoch, Placement: o.Placement, Gone: o.Gone, Heard: heard}
}

// eu, 23 Nov 2026, three servers: the leader's server and then the first heir's were powered off past the grace.
// The heir never counted the first as gone, since it never heard it, and the next heir learnt no gone set: one of
// three led nothing. Now the line carries what the leader knew, so the last one leads exactly half on Deplo's word.
func TestTheSecondHeirLeadsExactlyHalfOnDeplosWordAfterTwoLossesOnThreeServers(t *testing.T) {
	f := lined(t)
	g, h := heir(t, f), heirOn(t, f, "srv_c")
	f.ms.cut["fp-srv_a"] = true
	g.blind["srv_a"], h.blind["srv_a"] = true, true
	f.clk.Add(lease.PreVoteFor + time.Second)
	if err := g.l.Succeed(askAs(f, "srv_b", 2, lease.PreVoteFor+time.Second)); err != nil {
		t.Fatal(err)
	}
	g.step()
	if !leadsNow(g) {
		t.Fatal("the first heir does not lead two of three")
	}
	// The grace is counted from when srv_a was last heard, not from the takeover 11 s later.
	g.pass(leader.GoneAfter - lease.PreVoteFor)
	if ev := g.events("member_gone"); len(ev) != 1 || !strings.Contains(ev[0], "srv_a") {
		t.Fatalf("srv_a, last heard %v ago, is not gone for the heir: %v", leader.GoneAfter+time.Second, ev)
	}
	if o := f.lastOrder("fp-srv_c"); !slices.Equal(o.Gone, []string{"fp-srv_a"}) || o.Heard["srv_b"] > 2000 || o.Heard["srv_a"] < leader.GoneAfter.Milliseconds() {
		t.Fatalf("a renewal names gone %v, heard %v", o.Gone, o.Heard)
	}
	f.ms.cut["fp-srv_b"] = true
	h.blind["srv_b"] = true
	h.away = false
	f.clk.Add(lease.PreVoteFor + time.Second)
	if err := h.l.Succeed(askAs(f, "srv_c", 3, lease.PreVoteFor+time.Second)); err != nil {
		t.Fatal(err)
	}
	h.pass(3 * time.Second)
	if !leadsNow(h) || f.ms.epoch["fp-srv_c"] != 3 {
		t.Fatalf("the second heir does not lead the half left on Deplo's word: %+v", h.l.Snapshot().Clusters["clu_1"])
	}
}

// A member the leader has never heard is never said to answer: a leader that just took over says it has not heard it.
func TestALeaderThatTookOverSaysNothingOfAMemberItHasNotHeard(t *testing.T) {
	f := lined(t)
	g := heir(t, f)
	f.ms.cut["fp-srv_a"] = true
	g.blind["srv_a"] = true
	f.clk.Add(lease.PreVoteFor + time.Second)
	if err := g.l.Succeed(ask(f, 2)); err != nil {
		t.Fatal(err)
	}
	g.step()
	for _, m := range g.l.Snapshot().Clusters["clu_1"].Members {
		if want := m.ID != "srv_a"; m.Heard != want {
			t.Fatalf("%s reads heard %v, want %v", m.ID, m.Heard, want)
		}
	}
	for _, e := range g.events("member_state") {
		if strings.Contains(e, "srv_a") && strings.Contains(e, `"ok"`) {
			t.Fatalf("the heir said srv_a answers: %s", e)
		}
	}
}

// eu, 23 Nov 2026: the heir trying to lead (no quorum yet) wrote that every member, its own included, was not
// responding, and Deplo said it once it adopted that heir. One still trying says nothing of its members.
func TestAnHeirStillTryingToLeadSaysNothingOfItsMembers(t *testing.T) {
	f := lined(t)
	g := heir(t, f)
	f.ms.cut["fp-srv_a"] = true
	g.blind["srv_a"] = true
	f.clk.Add(4 * time.Second)
	// 4 s to 9 s after the leader's last renewal: under the removal time, so nobody takes the heir's epoch.
	for i := 0; i < 6; i++ {
		if err := g.l.Succeed(ask(f, 2)); err != nil {
			t.Fatal(err)
		}
		g.step()
		if leadsNow(g) {
			break
		}
		f.clk.Add(time.Second)
	}
	if leadsNow(g) {
		t.Fatal("the heir led while the members still heard the leader")
	}
	if ev := g.events("member_state"); len(ev) != 0 {
		t.Fatalf("an heir still trying said %v", ev)
	}
}

// eu, 24 Nov 2026: an heir's pre-vote waited 2 s for the dead leader's member. A pre-vote takes nothing, so its
// tally is in once a quorum said yes; only the probe, whose answers say what each member runs, hears everyone.
func TestAPreVoteIsOverOnceAQuorumSaidYes(t *testing.T) {
	f := lined(t)
	g := heir(t, f)
	slow := 1500 * time.Millisecond
	f.ms.mu.Lock()
	f.ms.slow = map[string]time.Duration{"fp-srv_a": slow}
	f.ms.mu.Unlock()
	f.clk.Add(lease.PreVoteFor + time.Second)
	if err := g.l.Succeed(ask(f, 2)); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	done := make(chan struct{})
	go func() { g.step(); close(done) }()
	tallied := func() bool {
		g.logMu.Lock()
		defer g.logMu.Unlock()
		return slices.ContainsFunc(g.logged, func(l string) bool { return strings.Contains(l, "would take it") })
	}
	for !tallied() && time.Since(began) < 5*time.Second {
		time.Sleep(10 * time.Millisecond)
	}
	if took := time.Since(began); took >= slow {
		t.Fatalf("the pre-vote's tally came after %s, once the dead member's answer was waited out", took)
	}
	<-done
	if !leadsNow(g) {
		t.Fatal("the heir does not lead after its pre-vote and probe")
	}
}

// The leader's server stops: the heir leads and renews every member left well before the lease the dead leader gave
// it runs out, so no member on a healthy server fences (c41: each fenced at 5 s, the heir led at 10.6 s).
func TestTheHeirRenewsEveryMemberBeforeItsLeaseRunsOut(t *testing.T) {
	f := lined(t)
	for _, fp := range []string{"fp-srv_a", "fp-srv_b", "fp-srv_c"} {
		f.ms.shortFence[fp] = true
	}
	f.pass(2 * time.Second)
	g := heir(t, f)
	f.ms.cut["fp-srv_a"] = true
	g.blind["srv_a"] = true
	ends := map[string]time.Time{"fp-srv_b": f.ms.leaseEnd("fp-srv_b"), "fp-srv_c": f.ms.leaseEnd("fp-srv_c")}
	// The heir's agent asks once its own lease went unrenewed for PreVoteFor; a busy member may need one more try.
	f.clk.Add(lease.PreVoteFor + 2*lease.RenewEvery)
	if err := g.l.Succeed(ask(f, 2)); err != nil {
		t.Fatal(err)
	}
	g.step()
	for fp, end := range ends {
		if o := f.lastOrder(fp); o.From != "srv_b" {
			t.Fatalf("%s was not renewed by the heir: last order from %q", fp, o.From)
		}
		if left := end.Sub(f.clk.Now()); left < 5*time.Second {
			t.Fatalf("%s was renewed %v before the dead leader's lease ran out, want at least 5 s", fp, left)
		}
	}
	// The second in line asks one stagger later, and still renews before the lease runs out.
	if second := lease.PreVoteFor + lease.SucceedStagger + 2*lease.RenewEvery; second >= lease.FenceFor {
		t.Fatalf("the second heir renews %v after the last renewal, past the %v lease", second, lease.FenceFor)
	}
}

// quickStops is a roster whose agents keep to the fence they are given and stop their stacks all at once.
func quickStops(r leader.Roster) leader.Roster {
	for i := range r.Members {
		r.Members[i].QuickStop = true
	}
	return r
}

// The leader on srv_a, cut off from the others past the removal time, gets srv_c back by a probe that lands just
// before its heir's. srv_c then takes the heir's epoch and its refusal never reaches srv_a, so srv_a's leader, sure
// from that probe, renews its own member for SureFor more. That lease ends before the heir counts it ended.
func TestALeaderSureFromAProbeGivesNoLeaseItsHeirDoesNotCount(t *testing.T) {
	f := startFO(t, abc())
	newer(f)
	f.putDB(t, "db_1", "db-main", backup(""))
	if err := f.l.SetRoster(quickStops(withLine(snapshotRoster(f, 1), "srv_a"))); err != nil {
		t.Fatal(err)
	}
	f.away = true
	f.pass(3 * time.Second)
	g := heir(t, f)
	if err := g.l.Standby(quickStops(withLine(snapshotRoster(f, 1), "srv_b"))); err != nil {
		t.Fatal(err)
	}
	g.sealDB(t, "db_1", "db-main", backup(""))
	f.blind["srv_b"], f.blind["srv_c"], g.blind["srv_a"] = true, true, true
	f.pass(lease.PreVoteFor + time.Second)
	if leadsNow(f) {
		t.Fatal("srv_a's leader leads with only its own member")
	}

	a := ask(f, 2)
	var once sync.Once
	f.ms.mu.Lock()
	f.ms.onProbe = func(fp string, o lease.Order) {
		if fp != "fp-srv_c" || o.From != "srv_a" {
			return
		}
		once.Do(func() {
			if err := g.l.Succeed(a); err != nil {
				t.Error(err)
			}
			g.step()
			f.ms.mu.Lock()
			f.ms.fails["fp-srv_c"] = 1
			f.ms.mu.Unlock()
		})
	}
	f.ms.mu.Unlock()
	delete(f.blind, "srv_c")
	f.clk.Add(time.Second)
	f.step()
	f.blind["srv_c"] = true
	took := f.clk.Now()
	if !leadsNow(g) || !leadsNow(f) || f.ms.epoch["fp-srv_c"] != 2 {
		t.Fatalf("no race: the heir leads %v, srv_a's leader leads %v, srv_c holds epoch %d", leadsNow(g), leadsNow(f), f.ms.epoch["fp-srv_c"])
	}
	for i := 0; i < 60 && placed(g, "db_1") == "srv_a"; i++ {
		f.clk.Add(time.Second)
		f.step()
		g.step()
	}
	if placed(g, "db_1") == "srv_a" {
		t.Fatal("the heir never moved the database")
	}
	// An agent that stops its stacks all at once takes up to 11 s once its lease ran out, and the heir 2 s to spare.
	end := f.ms.leaseEnd("fp-srv_a")
	// The heir's count is as it was: one renewal before its takeover, the 20 s lease and those 13 s.
	if waited, after := f.clk.Now().Sub(end), f.clk.Now().Sub(took); waited < 13*time.Second || after != 33*time.Second {
		t.Fatalf("the heir moved the database %v after its takeover, %v after srv_a's lease ran out", after, waited)
	}
}
