package leader_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/leader"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

func abcWithDB(t *testing.T) *fo {
	t.Helper()
	f := startFO(t, abc())
	f.putDB(t, "db_1", "db-main", backup(""))
	f.pass(10 * time.Second)
	return f
}

// split is a network split that keeps f, a leader on another server, and the members in `far` from Deplo and
// from every other member. Deplo's machine replaces f from the next epoch: the leader this returns.
func split(t *testing.T, f *fo, far ...string) *fo {
	t.Helper()
	for _, m := range f.l.Snapshot().Clusters["clu_1"].Members {
		f.blind[m.ID] = !strings.Contains(strings.Join(far, ","), m.ID)
	}
	f.away = true
	g := takeOver(t, f, far...)
	g.putDB(t, "db_1", "db-main", backup(""))
	return g
}

// passAll is pass for leaders that share a clock: each renews and fails over at every 5 s step.
func passAll(d time.Duration, fs ...*fo) {
	for ; d > 0; d -= 5 * time.Second {
		fs[0].clk.Add(5 * time.Second)
		for _, f := range fs {
			f.step()
		}
	}
}

// startedOn is the members that were told to start the database, by any leader: they share the members.
func startedOn(f *fo) string {
	var out []string
	for _, id := range []string{"srv_a", "srv_b", "srv_c"} {
		if strings.Contains(slugs(f, "fp-"+id), "db-main") {
			out = append(out, id)
		}
	}
	return strings.Join(out, ",")
}

// A leader that Deplo's machine replaced never hears of it across a split (spec #176). It is left with srv_a,
// fewer than half of the cluster, so it stops renewing: srv_a has stopped the database before the new leader,
// which cannot hear srv_a, starts it elsewhere. Never two writers.
func TestALeaderLeftWithFewerThanHalfStopsRenewingSoItsMemberFencesBeforeItsDatabaseMoves(t *testing.T) {
	f := abcWithDB(t)
	g := split(t, f, "srv_a")
	for i := 0; i < 100 && startedOn(g) == ""; i++ {
		passAll(5*time.Second, f, g)
	}
	if startedOn(g) == "" {
		t.Fatal("the database never moved off the member that was cut off")
	}
	if end := g.ms.leaseEnd("fp-srv_a"); g.clk.Now().Before(end) {
		t.Fatalf("two writers: the database started on %s while srv_a's lease runs for another %s", startedOn(g), end.Sub(g.clk.Now()))
	}
}

// twoWriters fails the test when the database was started somewhere while `on`, cut off but alive, still holds a lease.
func twoWriters(t *testing.T, f *fo, on string) {
	t.Helper()
	if where := startedOn(f); where != "" && f.clk.Now().Before(f.ms.leaseEnd("fp-"+on)) {
		t.Fatalf("two writers: the database started on %s while %s's lease runs for another %s", where, on, f.ms.leaseEnd("fp-"+on).Sub(f.clk.Now()))
	}
}

// The old leader keeps the larger side, srv_b and srv_c, and goes on leading it. The one on Deplo's machine
// hears only srv_a, fewer than half: it renews nobody, so srv_a has stopped the database by the time the old
// leader, after its usual 81 s, starts it beside itself.
func TestALeaderThatTakesOverWithFewerThanHalfRenewsNobody(t *testing.T) {
	f := abcWithDB(t)
	g := split(t, f, "srv_b", "srv_c")
	for i := 0; i < 30; i++ {
		passAll(5*time.Second, f, g)
		twoWriters(t, f, "srv_a")
	}
	if where := placed(f, "db_1"); where != "srv_b" && where != "srv_c" {
		t.Fatalf("the larger side did not take the database over: it is on %q", where)
	}
	if placed(g, "db_1") != "srv_a" {
		t.Fatalf("a leader that hears fewer than half moved the database to %q", placed(g, "db_1"))
	}
	if _, leading := leads(f.l); !leading {
		t.Fatal("the leader with the larger side stepped down")
	}
}

// Half and half: the old leader with srv_a, Deplo's machine with srv_b. Deplo's word breaks the tie, and it
// only names the leader it handed the cluster to. The old one stops renewing, the new one leads its half.
func TestOfTwoHalvesOnlyTheOneDeploNamesLeads(t *testing.T) {
	f := two(t)
	f.putDB(t, "db_1", "db-main", backup(""))
	f.pass(10 * time.Second)
	g := split(t, f, "srv_a")
	cut := f.clk.Now()
	for i := 0; i < 100 && startedOn(g) == ""; i++ {
		passAll(5*time.Second, f, g)
		twoWriters(t, f, "srv_a")
	}
	if placed(g, "db_1") != "srv_b" || placed(f, "db_1") != "srv_a" {
		t.Fatalf("Deplo's leader has the database on %q, the old one on %q", placed(g, "db_1"), placed(f, "db_1"))
	}
	if last := f.ms.leaseEnd("fp-srv_a").Add(-lease.FenceAfter); last.Sub(cut) > leader.DeploFor {
		t.Fatalf("the old leader renewed srv_a for %s after Deplo last named it", last.Sub(cut))
	}
	if !f.clk.Now().Before(f.ms.leaseEnd("fp-srv_b")) {
		t.Fatal("the half Deplo's leader hears lost its lease")
	}
}

// Deplo is down and every member answers: nothing changes, however long it lasts.
func TestWithDeploDownALeaderThatHearsMostMembersGoesOn(t *testing.T) {
	for name, mem := range map[string]map[string]leader.Member{"one": {"srv_a": roomy("amd64")}, "two": both(), "three": abc()} {
		t.Run(name, func(t *testing.T) {
			f := startFO(t, mem)
			f.away = true
			f.pass(10 * time.Minute)
			for id, s := range states(t, f.l) {
				if s != "ok" || !f.clk.Now().Before(f.ms.leaseEnd("fp-"+id)) {
					t.Fatalf("%s is %s, its lease ends %s", id, s, f.ms.leaseEnd("fp-"+id))
				}
			}
		})
	}
}

// Deplo is down and one of three members dies: two of three is a quorum, so its database still moves.
func TestWithDeploDownFailoverStillWorksWhileMostMembersAnswer(t *testing.T) {
	f := abcWithDB(t)
	f.away = true
	f.ms.cut["fp-srv_a"] = true
	f.pass(85 * time.Second)
	if where := placed(f, "db_1"); where != "srv_b" && where != "srv_c" {
		t.Fatalf("the database is on %q", where)
	}
}

// The price of the tie rule: Deplo is down and one of two members dies. The leader cannot tell this from the
// split above, so it moves nothing and stops renewing the member that is left, which stops its database.
// When Deplo is back the leader leads again and the dead member's work moves.
func TestWithDeploDownAndHalfTheMembersSilentTheLeaderStopsUntilDeploIsBack(t *testing.T) {
	f := two(t)
	f.putDB(t, "db_1", "db-main", backup(""))
	f.pass(10 * time.Second)
	f.away = true
	f.ms.cut["fp-srv_a"] = true
	down := f.clk.Now()
	f.pass(5 * time.Minute)
	if startedOn(f) != "" || placed(f, "prj_1") != "srv_a" {
		t.Fatalf("work moved while the leader could not tell a dead member from a split: %q", startedOn(f))
	}
	if last := f.ms.leaseEnd("fp-srv_b").Add(-lease.FenceAfter); last.Sub(down) > leader.DeploFor {
		t.Fatalf("srv_b was renewed for %s after Deplo went down", last.Sub(down))
	}
	if got := states(t, f.l)["srv_b"]; got != "fenced" {
		t.Fatalf("srv_b reads %s", got)
	}
	f.away = false
	f.pass(10 * time.Second)
	if !f.clk.Now().Before(f.ms.leaseEnd("fp-srv_b")) || states(t, f.l)["srv_b"] != "ok" {
		t.Fatal("srv_b was not renewed again once Deplo was back")
	}
	if placed(f, "db_1") != "srv_b" {
		t.Fatalf("the dead member's database is on %q", placed(f, "db_1"))
	}
}

// A person's move is held to the same rule: a leader that is not sure it leads makes none.
func TestALeaderThatHearsTooFewMakesNoMoveForAPersonEither(t *testing.T) {
	f := held(t, both())
	f.away = true
	f.ms.cut["fp-srv_a"] = true
	f.pass(2 * time.Minute)
	if a := move(f, picked()); a.OK || a.Reason != leader.NotLeading {
		t.Fatalf("answer %+v", a)
	}
}

// The split heals. The old leader hears srv_a's side and the others again: it asks before it renews, hears
// the newer epoch and steps down, without having given srv_a, whose database moved, a fresh lease.
func TestALeaderThatHearsItsMembersAgainAsksBeforeItRenews(t *testing.T) {
	f := abcWithDB(t)
	g := split(t, f, "srv_a")
	for i := 0; i < 100 && startedOn(g) == ""; i++ {
		passAll(5*time.Second, f, g)
	}
	g.blind["srv_a"] = true // the new leader has not reached srv_a yet when the old one does
	ended := f.ms.leaseEnd("fp-srv_a")
	orders := len(f.ms.orders["fp-srv_a"])
	for id := range f.blind {
		f.blind[id] = false
	}
	f.pass(15 * time.Second)
	if _, leading := leads(f.l); leading {
		t.Fatal("the old leader leads again")
	}
	if !f.ms.leaseEnd("fp-srv_a").Equal(ended) || len(f.ms.orders["fp-srv_a"]) != orders {
		t.Fatal("two writers: the old leader gave srv_a a fresh lease on a database that runs elsewhere")
	}
}

// A leader that is told to stop while it hears too few extends nothing: the 5 minutes would outlive the wait
// of the leader that replaced it.
func TestALeaderThatHearsTooFewExtendsNoLeaseWhenItStops(t *testing.T) {
	f := abcWithDB(t)
	g := split(t, f, "srv_a")
	passAll(10*time.Minute, f, g)
	ended := f.ms.leaseEnd("fp-srv_a")
	f.l.Drain(context.Background())
	if !f.ms.leaseEnd("fp-srv_a").Equal(ended) {
		t.Fatalf("a replaced leader extended srv_a's lease by %ds", f.lastOrder("fp-srv_a").ExtendSeconds)
	}
}

// Deplo's machine takes over but hears too few for ten minutes. The old leader may have extended a lease in
// that time, so once the new one is sure, a member it never heard holds the longest lease again.
func TestALeaderThatWasNotSureWaitsTheLongestLeaseOutAgain(t *testing.T) {
	f := abcWithDB(t)
	f.away = true
	g := takeOver(t, f, "srv_a", "srv_b")
	g.putDB(t, "db_1", "db-main", backup(""))
	g.pass(10 * time.Minute)
	if startedOn(g) != "" {
		t.Fatal("a leader that hears one of three moved work")
	}
	g.blind["srv_b"] = false
	g.pass(370 * time.Second)
	if startedOn(g) != "" {
		t.Fatal("the database moved before the longest lease an older leader may have given was over")
	}
	g.pass(15 * time.Second)
	if startedOn(g) == "" {
		t.Fatal("the database never moved")
	}
}

// Deplo hands the cluster to another leader and says so to the one it can still reach: that one stops at
// once, stays stopped across a restart, and leads again only from a roster at least that new.
func TestALeaderToldItWasReplacedStopsAtOnce(t *testing.T) {
	f := two(t)
	srv := httptest.NewServer(f.l.Handler())
	defer srv.Close()
	if r := call(t, srv.URL, "leader.beat", map[string]any{"epochs": map[string]uint64{"clu_1": 2, "clu_other": 7}}); r["ok"] != true {
		t.Fatalf("beat %v", r)
	}
	orders := len(f.ms.orders["fp-srv_a"])
	f.pass(30 * time.Second)
	if _, leading := leads(f.l); leading || len(f.ms.orders["fp-srv_a"]) != orders {
		t.Fatal("a leader that was told it was replaced kept sending orders")
	}
	f.restart()
	if _, leading := leads(f.l); leading {
		t.Fatal("it leads again after a restart")
	}
	if err := f.l.SetRoster(snapshotRoster(f, 1)); !errors.Is(err, leader.ErrOlderEpoch) {
		t.Fatalf("it took the epoch it was replaced at: %v", err)
	}
	r := staleRoster("srv_a", "srv_b")
	r.Epoch = 2
	if err := f.l.SetRoster(r); err != nil {
		t.Fatal(err)
	}
	if e, leading := leads(f.l); !leading || e != 2 {
		t.Fatalf("epoch %d leads %v", e, leading)
	}
}

// A leader that was sure a moment ago is told to stop just after another one took its members over. It asks
// again before it extends, so srv_a, which the new leader cannot hear, is not given five more minutes.
func TestALeaderAsksAgainBeforeItExtendsItsLeases(t *testing.T) {
	f := abcWithDB(t)
	g := takeOver(t, f, "srv_a")
	g.step()
	ended := f.ms.leaseEnd("fp-srv_a")
	f.l.Drain(context.Background())
	if !f.ms.leaseEnd("fp-srv_a").Equal(ended) {
		t.Fatalf("a replaced leader extended srv_a's lease by %ds", f.lastOrder("fp-srv_a").ExtendSeconds)
	}
	if _, leading := leads(f.l); leading {
		t.Fatal("it still leads")
	}
}

// More than half of the members go at once, with Deplo up: its word only breaks a tie. The member that is
// left is renewed no more, until the ones that are gone for good are taken out of the cluster.
func TestWithMoreThanHalfGoneTheRestIsRenewedAgainOnceTheyAreRemoved(t *testing.T) {
	f := abcWithDB(t)
	f.ms.cut["fp-srv_a"], f.ms.cut["fp-srv_b"] = true, true
	f.pass(2 * time.Minute)
	if f.clk.Now().Before(f.ms.leaseEnd("fp-srv_c")) || placed(f, "db_1") != "srv_a" {
		t.Fatal("a leader that hears one of three went on leading")
	}
	r := staleRoster("srv_c")
	r.Placement = nil
	if err := f.l.SetRoster(r); err != nil {
		t.Fatal(err)
	}
	f.pass(10 * time.Second)
	if !f.clk.Now().Before(f.ms.leaseEnd("fp-srv_c")) {
		t.Fatal("the member that is left was not renewed once it was the whole cluster")
	}
}
