package leader_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/bundles"
	"github.com/DeploCloud/deplo-cluster/internal/leader"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

// takeOver is another leader starting to lead f's cluster from the next epoch, with its own state and bundles
// and the control plane's picture. cutOff are members that are alive but that this leader cannot reach.
func takeOver(t *testing.T, f *fo, cutOff ...string) *fo {
	t.Helper()
	return takeOverWith(t, f, snapshotRoster(f, f.l.Snapshot().Clusters["clu_1"].Epoch+1), cutOff...)
}

// takeOverWith is takeOver from the picture Deplo has, which may have missed the old leader's last events.
func takeOverWith(t *testing.T, f *fo, picture leader.Roster, cutOff ...string) *fo {
	t.Helper()
	g := &fo{t: t, clk: f.clk, ms: f.ms, pathOK: true, dir: t.TempDir(), blind: map[string]bool{}}
	var err error
	if g.store, err = bundles.Open(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	for _, id := range cutOff {
		g.blind[id] = true
	}
	// Deplo's machine runs its leader all along: a takeover is a new epoch, not a start, so no start grace.
	g.l = newLeader(t, leader.Config{Dial: g.dial, Now: g.clk.Now, StateDir: g.dir, Bundles: g.store, PathOK: func() bool { return g.pathOK }, StartGrace: -1, Logf: g.logf})
	if err := g.l.SetRoster(picture); err != nil {
		t.Fatal(err)
	}
	// What is live is sealed to the new leader's key.
	g.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", true)
	return g
}

// leaseEnd is when the lease srv_a holds runs out, counted from the order it took last, now.
func leaseEnd(f *fo) time.Time {
	return f.clk.Now().Add(lease.FenceAfter + time.Duration(f.lastOrder("fp-srv_a").ExtendSeconds)*time.Second)
}

// A leader extends its leases for a planned restart and never comes back (spec #176). The one that takes over
// cannot read what it extended, and srv_a is cut off from it yet alive: its database starts nowhere else
// before the lease it holds has run out. Never two writers.
func TestATakeoverStartsNoDatabaseWhileItsCutOffMemberMayStillRunIt(t *testing.T) {
	for name, drainsAfter := range map[string]bool{
		"extended, then replaced":                         false,
		"replaced, then extended before it heard of that": true,
	} {
		t.Run(name, func(t *testing.T) {
			f := two(t)
			f.putDB(t, "db_1", "db-main", backup(""))
			f.pass(10 * time.Second)
			var g *fo
			if drainsAfter {
				g = takeOver(t, f, "srv_a")
				f.clk.Add(5 * time.Second)
				f.l.Drain(context.Background())
			} else {
				f.l.Drain(context.Background())
			}
			if f.lastOrder("fp-srv_a").ExtendSeconds == 0 {
				t.Fatal("srv_a's lease was not extended")
			}
			end := leaseEnd(f)
			if g == nil {
				// Two missed checks later Deplo's machine leads.
				f.clk.Add(15 * time.Second)
				g = takeOver(t, f, "srv_a")
			}
			g.putDB(t, "db_1", "db-main", backup(""))

			for g.clk.Now().Before(end) {
				g.pass(5 * time.Second)
				if strings.Contains(slugs(g, "fp-srv_b"), "db-main") && g.clk.Now().Before(end) {
					t.Fatalf("two writers: the database started on srv_b %s before srv_a's lease ran out", end.Sub(g.clk.Now()))
				}
			}
			// What may run twice did not wait for that.
			if placed(g, "prj_1") != "srv_b" {
				t.Fatalf("the App without a Volume is on %q", placed(g, "prj_1"))
			}
			g.pass(time.Minute)
			if placed(g, "db_1") != "srv_b" {
				t.Fatalf("the database never moved once the lease had run out: %q", placed(g, "db_1"))
			}
		})
	}
}

// The member is really gone. Its Apps move on the usual clock, its database once the longest lease another
// leader may have given it is over: 60 s + 5 min + the margin, counted from the takeover.
func TestATakeoverFailsADeadMembersDatabaseOverOnlyLater(t *testing.T) {
	f := two(t)
	f.putDB(t, "db_1", "db-main", backup(""))
	f.pass(10 * time.Second)
	f.ms.cut["fp-srv_a"] = true
	g := takeOver(t, f)
	g.putDB(t, "db_1", "db-main", backup(""))

	g.pass(85 * time.Second)
	if placed(g, "prj_1") != "srv_b" || placed(g, "db_1") != "srv_a" {
		t.Fatalf("after 85 s: web on %q, the database on %q", placed(g, "prj_1"), placed(g, "db_1"))
	}
	g.pass(290 * time.Second) // 375 s
	if placed(g, "db_1") != "srv_a" {
		t.Fatal("the database moved before the longest lease another leader may grant was over")
	}
	g.pass(10 * time.Second) // 385 s
	if placed(g, "db_1") != "srv_b" || slugs(g, "fp-srv_b") != "web,db-main" {
		t.Fatalf("the database is on %q, starts %q", placed(g, "db_1"), slugs(g, "fp-srv_b"))
	}
	got := g.events("failover")
	if len(got) != 2 || !strings.Contains(got[0], `"moved":["prj_1"]`) || !strings.Contains(got[1], `"moved":["db_1"]`) {
		t.Fatalf("failover events %v", got)
	}
	g.pass(time.Minute)
	if slugs(g, "fp-srv_b") != "web,db-main" {
		t.Fatalf("the same silence moved the work again: %q", slugs(g, "fp-srv_b"))
	}
}

// A member that answered the new leader holds a plain 60 s lease from it: the usual clock applies.
func TestAMemberThatAnsweredTheNewLeaderFailsOverOnTheUsualClock(t *testing.T) {
	f := two(t)
	f.putDB(t, "db_1", "db-main", backup(""))
	f.pass(10 * time.Second)
	g := takeOver(t, f)
	g.putDB(t, "db_1", "db-main", backup(""))
	g.pass(10 * time.Second)
	g.ms.cut["fp-srv_a"] = true
	g.pass(85 * time.Second)
	if placed(g, "db_1") != "srv_b" {
		t.Fatalf("the database is on %q", placed(g, "db_1"))
	}
}

// A person's move is held to the same rule: nothing is restored elsewhere while the member may hold a lease.
func TestAMoveAfterATakeoverWaitsForTheLongestLeaseToo(t *testing.T) {
	f := held(t, both())
	f.pass(10 * time.Second)
	f.ms.cut["fp-srv_a"] = true
	g := takeOver(t, f)
	g.put(t, "prj_3", "files", "2026-01-01T00:00:00Z", false)
	g.pass(375 * time.Second)
	if a := move(g, picked()); a.OK || a.Reason != leader.StillAnswering {
		t.Fatalf("a move off a member that may still run it was taken: %+v", a)
	}
	g.pass(10 * time.Second)
	if a := move(g, picked()); !a.OK {
		t.Fatalf("the move was refused once the lease was surely over: %+v", a)
	}
}

// The same holds for a leader that starts again without its state: no file has to say what it extended.
func TestALeaderThatLostItsStateStillWaitsOutItsOwnExtension(t *testing.T) {
	f := two(t)
	f.putDB(t, "db_1", "db-main", backup(""))
	f.pass(10 * time.Second)
	r := snapshotRoster(f, 1)
	f.l.Drain(context.Background())
	end := leaseEnd(f)
	f.ms.cut["fp-srv_a"] = true
	f.dir = t.TempDir()
	f.restart()
	if err := f.l.SetRoster(r); err != nil {
		t.Fatal(err)
	}
	for f.clk.Now().Before(end) {
		f.pass(5 * time.Second)
		if strings.Contains(slugs(f, "fp-srv_b"), "db-main") && f.clk.Now().Before(end) {
			t.Fatalf("two writers: the database started on srv_b %s before srv_a's lease ran out", end.Sub(f.clk.Now()))
		}
	}
	f.pass(time.Minute)
	if placed(f, "db_1") != "srv_b" {
		t.Fatalf("the database never moved once the lease had run out: %q", placed(f, "db_1"))
	}
}

// newer is f's members as newer agents: they count what leaders say of extensions and keep to a short fence.
func newer(f *fo) {
	f.ms.mu.Lock()
	defer f.ms.mu.Unlock()
	for _, fp := range []string{"fp-srv_a", "fp-srv_b", "fp-srv_c"} {
		f.ms.witness[fp], f.ms.shortFence[fp] = true, true
	}
}

// shortFences is Deplo's picture with every member's agent known to keep to the fence it is given.
func shortFences(r leader.Roster) leader.Roster {
	for i := range r.Members {
		r.Members[i].ShortFence = true
	}
	return r
}

// The member is really gone, and the one that answers says no lease can live on an extension: its database
// moves once the plain 5 s lease and the time to stop are over, not after five minutes.
func TestATakeoverMovesADeadMembersDatabaseOnceAMemberVouchesNoLeaseWasExtended(t *testing.T) {
	f := two(t)
	newer(f)
	f.putDB(t, "db_1", "db-main", backup(""))
	f.pass(10 * time.Second)
	f.ms.cut["fp-srv_a"] = true
	g := takeOverWith(t, f, shortFences(snapshotRoster(f, f.l.Snapshot().Clusters["clu_1"].Epoch+1)))
	g.putDB(t, "db_1", "db-main", backup(""))

	g.pass(20 * time.Second)
	if placed(g, "prj_1") != "srv_b" || placed(g, "db_1") != "srv_a" {
		t.Fatalf("after 20 s: web on %q, the database on %q", placed(g, "prj_1"), placed(g, "db_1"))
	}
	g.pass(10 * time.Second)
	if placed(g, "db_1") != "srv_b" {
		t.Fatalf("after 30 s the database is still on %q", placed(g, "db_1"))
	}
}

// Without Deplo's word on its agent, the dead member may keep the longest fence: the plain lease is that one.
func TestATakeoverWaitsOutTheLongFenceOfAMemberDeploKnowsNothingOf(t *testing.T) {
	f := two(t)
	newer(f)
	f.putDB(t, "db_1", "db-main", backup(""))
	f.pass(10 * time.Second)
	f.ms.cut["fp-srv_a"] = true
	g := takeOver(t, f)
	g.putDB(t, "db_1", "db-main", backup(""))

	g.pass(75 * time.Second)
	if placed(g, "db_1") != "srv_a" {
		t.Fatal("the database moved before a 60 s fence and its stop were over")
	}
	g.pass(15 * time.Second)
	if placed(g, "db_1") != "srv_b" {
		t.Fatalf("after 90 s the database is still on %q", placed(g, "db_1"))
	}
}

// A planned restart extends the leases, and its leader never comes back: the member that answers heard the
// extension announced, so the one that takes over waits it out. Never two writers.
func TestATakeoverWaitsOutAnExtensionTheMembersHeardAnnounced(t *testing.T) {
	f := two(t)
	newer(f)
	f.putDB(t, "db_1", "db-main", backup(""))
	f.pass(10 * time.Second)
	f.l.Drain(context.Background())
	if f.lastOrder("fp-srv_a").ExtendSeconds == 0 {
		t.Fatal("srv_a's lease was not extended")
	}
	end := f.ms.leaseEnd("fp-srv_a")
	f.clk.Add(15 * time.Second)
	g := takeOverWith(t, f, shortFences(snapshotRoster(f, f.l.Snapshot().Clusters["clu_1"].Epoch+1)), "srv_a")
	g.putDB(t, "db_1", "db-main", backup(""))
	for g.clk.Now().Before(end) {
		g.pass(5 * time.Second)
		if strings.Contains(slugs(g, "fp-srv_b"), "db-main") && g.clk.Now().Before(end) {
			t.Fatalf("two writers: the database started on srv_b %s before srv_a's lease ran out", end.Sub(g.clk.Now()))
		}
	}
	// The announcement covers the longest fence an agent may keep, so the move waits a little past the lease.
	g.pass(2 * time.Minute)
	if placed(g, "db_1") != "srv_b" {
		t.Fatalf("the database never moved once the lease had run out: %q", placed(g, "db_1"))
	}
}

// Every member hears an extension may come before any is given: one that does not answer means none is.
func TestADrainExtendsNoLeaseWhileAMemberDoesNotHearTheAnnouncement(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": roomy("amd64"), "srv_c": roomy("amd64")})
	f.pass(10 * time.Second)
	f.ms.cut["fp-srv_c"] = true
	f.l.Drain(context.Background())
	for _, fp := range []string{"fp-srv_a", "fp-srv_b"} {
		if o := f.lastOrder(fp); o.ExtendSeconds != 0 || o.Announce == 0 {
			t.Fatalf("%s was last told %+v, want the announcement and no extension", fp, o)
		}
	}
	delete(f.ms.cut, "fp-srv_c")
	f.l.Drain(context.Background())
	f.ms.mu.Lock()
	defer f.ms.mu.Unlock()
	for _, fp := range []string{"fp-srv_a", "fp-srv_b", "fp-srv_c"} {
		os := f.ms.orders[fp]
		if len(os) < 2 || os[len(os)-1].ExtendSeconds == 0 || os[len(os)-2].Announce == 0 || os[len(os)-2].ExtendSeconds != 0 {
			t.Fatalf("%s: want the announcement, then the extension", fp)
		}
	}
}

// deplo#241: the leader before moved db-main to srv_b, which runs it, and Deplo had not heard yet when the lead moved
// on. The leader that takes over learns it from srv_b's answer to its probe, before it renews anyone: db-main stays on
// srv_b, and no order ever places it back on srv_a, where an old copy would start next to it.
func TestATakeoverKeepsAWriterOnTheMemberThatRunsIt(t *testing.T) {
	f := two(t)
	f.putDB(t, "db_1", "db-main", backup(""))
	f.pass(2 * time.Second)
	epoch := f.l.Snapshot().Clusters["clu_1"].Epoch
	picture := snapshotRoster(f, epoch+1)
	f.ms.mu.Lock()
	f.ms.running["fp-srv_b"] = []lease.Writer{{Slug: "db-main", Epoch: epoch}}
	f.ms.running["fp-srv_a"] = []lease.Writer{{Slug: "db-main", Epoch: epoch - 1}}
	f.ms.mu.Unlock()
	g := takeOverWith(t, f, picture)
	g.putDB(t, "db_1", "db-main", backup(""))
	g.pass(3 * time.Second)
	if got := placed(g, "db_1"); got != "srv_b" {
		t.Fatalf("db-main placed on %s, want srv_b, which runs it at the newest epoch", got)
	}
	g.ms.mu.Lock()
	defer g.ms.mu.Unlock()
	for _, o := range g.ms.orders["fp-srv_a"] {
		for _, e := range o.Placement {
			if o.Epoch > epoch && e.Slug == "db-main" && e.Member == "srv_a" {
				t.Fatalf("an order at epoch %d placed db-main on srv_a: %+v", o.Epoch, o.Placement)
			}
		}
	}
}

// A member whose agent is too old to say what it runs keeps the roster's placement, as before, and the leader says so.
func TestATakeoverKeepsTheRosterForAMemberTooOldToSayWhatItRuns(t *testing.T) {
	f := two(t)
	f.putDB(t, "db_1", "db-main", backup(""))
	f.pass(2 * time.Second)
	epoch := f.l.Snapshot().Clusters["clu_1"].Epoch
	picture := snapshotRoster(f, epoch+1)
	f.ms.mu.Lock()
	f.ms.old["fp-srv_b"], f.ms.old["fp-srv_a"] = true, true
	f.ms.running["fp-srv_b"] = []lease.Writer{{Slug: "db-main", Epoch: epoch}}
	f.ms.mu.Unlock()
	g := takeOverWith(t, f, picture)
	g.putDB(t, "db_1", "db-main", backup(""))
	g.pass(3 * time.Second)
	if got := placed(g, "db_1"); got != "srv_a" {
		t.Fatalf("db-main placed on %s, want srv_a as the roster says: srv_b's agent said nothing", got)
	}
	g.logMu.Lock()
	defer g.logMu.Unlock()
	for _, line := range g.logged {
		if strings.Contains(line, "srv_b's server agent does not say what it runs") {
			return
		}
	}
	t.Fatalf("no line says srv_b keeps the roster: %q", g.logged)
}

// An agent Deplo knows kills its stacks all at once (QuickStop) is surely stopped 13 s after its lease ran out,
// not 21: the dead member's database moves 8 s sooner, never before the 5 s lease and those 13 s are over.
func TestATakeoverMovesAQuickStoppingMembersDatabaseSooner(t *testing.T) {
	f := two(t)
	newer(f)
	f.putDB(t, "db_1", "db-main", backup(""))
	f.pass(10 * time.Second)
	f.ms.cut["fp-srv_a"] = true
	r := shortFences(snapshotRoster(f, f.l.Snapshot().Clusters["clu_1"].Epoch+1))
	for i := range r.Members {
		r.Members[i].QuickStop = true
	}
	g := takeOverWith(t, f, r)
	g.putDB(t, "db_1", "db-main", backup(""))

	g.pass(15 * time.Second)
	if placed(g, "db_1") != "srv_a" {
		t.Fatalf("after 15 s the database moved to %q, before its member's lease and stop were over", placed(g, "db_1"))
	}
	g.pass(5 * time.Second)
	if placed(g, "db_1") != "srv_b" {
		t.Fatalf("after 20 s the database is still on %q", placed(g, "db_1"))
	}
}
