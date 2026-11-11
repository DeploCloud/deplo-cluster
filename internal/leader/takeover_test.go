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
	g := &fo{clk: f.clk, ms: f.ms, pathOK: true, dir: t.TempDir(), blind: map[string]bool{}}
	var err error
	if g.store, err = bundles.Open(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	for _, id := range cutOff {
		g.blind[id] = true
	}
	g.l = leader.New(leader.Config{Dial: g.dial, Now: g.clk.Now, StateDir: g.dir, Bundles: g.store, PathOK: func() bool { return g.pathOK }})
	if err := g.l.SetRoster(snapshotRoster(f, f.l.Snapshot().Clusters["clu_1"].Epoch+1)); err != nil {
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
