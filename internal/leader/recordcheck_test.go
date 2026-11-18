package leader_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

// A move is checked again when it is recorded, not only before its start (deplo-cluster#5): on the lab, web was
// recorded on another member 0.75 s after the one it left answered again.

// startsOnce runs what once, on the first start order a member is handed, and says which member took it.
func (f *fo) startsOnce(what func(fp string, o lease.Order)) *string {
	to := new(string)
	f.ms.onStart = func(fp string, o lease.Order) {
		if *to == "" {
			*to = fp
			what(fp, o)
		}
	}
	return to
}

func (f *fo) answers(fp string) {
	f.ms.mu.Lock()
	f.ms.cut[fp] = false
	f.ms.mu.Unlock()
	f.l.Tick(context.Background())
}

func TestAMoveWhoseMemberAnswersWhileItStartsIsDroppedAndItsCopyRemoved(t *testing.T) {
	f := threeRoomy(t)
	f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", true)
	f.assign(t, "prj_1", "web", "srv_a")
	f.ms.cut["fp-srv_a"] = true
	f.pass(4 * time.Second)
	to := f.startsOnce(func(string, lease.Order) { f.answers("fp-srv_a") })
	f.pass(5 * time.Second)
	if *to == "" {
		t.Fatal("no start was sent")
	}
	if placed(f, "prj_1") != "srv_a" || len(f.events("failed_over")) != 0 {
		t.Fatalf("recorded although srv_a answered while it started: web on %q", placed(f, "prj_1"))
	}
	dropped := f.events("move_dropped")
	if len(dropped) != 1 || !strings.Contains(dropped[0], `"reason":"source_answered"`) || !strings.Contains(dropped[0], `"to":"`+strings.TrimPrefix(*to, "fp-")+`"`) {
		t.Fatalf("move_dropped events %v", dropped)
	}
	// The member that took the start removes its copy, and saying so is no return to the cluster.
	f.ms.mu.Lock()
	f.ms.returned[*to] = &lease.Returned{Removed: []string{"web"}}
	f.ms.mu.Unlock()
	f.pass(5 * time.Second)
	asked := false
	for _, o := range f.ms.orders[*to] {
		asked = asked || o.Return
	}
	if !asked || f.lastOrder(*to).Return {
		t.Fatalf("the member that took the start: asked to remove its copy %v, still asked after it did %v", asked, f.lastOrder(*to).Return)
	}
	if got := f.events("member_returned"); len(got) != 0 {
		t.Fatalf("member_returned %v", got)
	}
	f.pass(time.Minute)
	if placed(f, "prj_1") != "srv_a" || startCount(f) != 1 {
		t.Fatalf("web on %q, %d starts, once srv_a was back", placed(f, "prj_1"), startCount(f))
	}
}

func TestAMoveWhoseWorkloadIsPlacedElsewhereWhileItStartsIsDropped(t *testing.T) {
	f := threeRoomy(t)
	f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", true)
	f.assign(t, "prj_1", "web", "srv_a")
	f.ms.cut["fp-srv_a"] = true
	f.pass(4 * time.Second)
	other := ""
	to := f.startsOnce(func(fp string, _ lease.Order) {
		// Deplo places web meanwhile, on the member that did not take the start.
		other = "srv_b"
		if fp == "fp-srv_b" {
			other = "srv_c"
		}
		r := snapshotRoster(f, 1)
		for i := range r.Placement {
			if r.Placement[i].ID == "prj_1" {
				r.Placement[i].Member = other
			}
		}
		if err := f.l.SetRoster(r); err != nil {
			t.Error(err)
		}
	})
	f.pass(5 * time.Second)
	if *to == "" {
		t.Fatal("no start was sent")
	}
	if placed(f, "prj_1") != other || len(f.events("failed_over")) != 0 {
		t.Fatalf("web on %q, want %q where Deplo placed it", placed(f, "prj_1"), other)
	}
	if d := f.events("move_dropped"); len(d) != 1 || !strings.Contains(d[0], `"reason":"placement_changed"`) {
		t.Fatalf("move_dropped events %v", d)
	}
}

// Never two writers: the database's member is back and answering, so its standby copy is not promoted.
func TestAStandbyIsNotPromotedForAMemberThatAnswersAgain(t *testing.T) {
	f := withStandby(t, "srv_b", backup(""), abc())
	f.ms.cut["fp-srv_a"] = true
	f.pass(75 * time.Second)
	f.ms.cut["fp-srv_a"] = false
	f.pass(2 * time.Minute)
	if placed(f, "db_1") != "srv_a" || len(f.ms.starts["fp-srv_b"])+len(f.ms.starts["fp-srv_c"]) != 0 {
		t.Fatalf("the standby was promoted although its database's member answered again: database on %q", placed(f, "db_1"))
	}
}

// What must never run twice is recorded once its start ran, even if the member it left answers meanwhile: that
// member had stopped it before the promotion, and is told it runs nowhere, then on the promoted member. Undoing the
// promotion would start it again there while the promoted copy still runs.
func TestAPromotionItsMemberAnswersDuringIsRecordedAndTheMemberNeverRunsItAgain(t *testing.T) {
	f := withStandby(t, "srv_b", backup(""), abc())
	f.ms.cut["fp-srv_a"] = true
	told := "?"
	to := f.startsOnce(func(fp string, o lease.Order) {
		f.answers("fp-srv_a")
		for _, e := range f.lastOrder("fp-srv_a").Placement {
			if e.ID == "db_1" && !e.Standby {
				told = "on " + e.Member
			}
		}
	})
	f.pass(90 * time.Second)
	if *to != "fp-srv_b" {
		t.Fatalf("the start went to %q", *to)
	}
	if told != "on " {
		t.Fatalf("during the promotion the member that answered was told its database is %q", told)
	}
	moved := f.events("failed_over")
	if placed(f, "db_1") != "srv_b" || len(moved) != 1 || !strings.Contains(moved[0], `"promoted":true`) || !strings.Contains(moved[0], `"stale":"source_answered"`) {
		t.Fatalf("database on %q, failed_over %v", placed(f, "db_1"), moved)
	}
	if d := f.events("move_dropped"); len(d) != 0 {
		t.Fatalf("a promotion was dropped: %v", d)
	}
	f.pass(10 * time.Second)
	for _, e := range f.lastOrder("fp-srv_a").Placement {
		if e.ID == "db_1" && !e.Standby && e.Member != "srv_b" {
			t.Fatalf("the member it left is told its database is on %q", e.Member)
		}
	}
}
