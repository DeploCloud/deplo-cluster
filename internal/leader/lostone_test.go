package leader_test

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/leader"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

// webOnB is three members with web on srv_b, and srv_c cut off long enough that the others stop counting it.
func webOnB(t *testing.T) *fo {
	t.Helper()
	f := startFO(t, abc())
	f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", true)
	f.assign(t, "prj_1", "web", "srv_b")
	f.ms.cut["fp-srv_c"] = true
	f.pass(leader.GoneAfter + 5*time.Second)
	return f
}

// ADR-0049, members lost one after another: srv_c is out of the majority, so srv_b's loss leaves srv_a as exactly
// half, and Deplo's beat lets its leader move web there.
func TestAMemberGonePastTheGraceStopsCountingSoTheLastOneTakesTheWork(t *testing.T) {
	f := webOnB(t)
	if ev := f.events("member_gone"); len(ev) != 1 || !strings.Contains(ev[0], "srv_c") {
		t.Fatalf("gone events %v", ev)
	}
	if o := f.lastOrder("fp-srv_a"); !slices.Equal(o.Gone, []string{"fp-srv_c"}) {
		t.Fatalf("a renewal names gone %v", o.Gone)
	}
	f.ms.cut["fp-srv_b"] = true
	f.pass(time.Minute)
	if got := placed(f, "prj_1"); got != "srv_a" {
		t.Fatalf("web is on %q", got)
	}
	// The start renews srv_a's lease too, so it names the gone member as every renewal does.
	f.ms.mu.Lock()
	starts := f.ms.starts["fp-srv_a"]
	f.ms.mu.Unlock()
	if len(starts) == 0 || !slices.Equal(starts[len(starts)-1].Gone, []string{"fp-srv_c"}) {
		t.Fatalf("the start order names gone %v", starts)
	}
}

// With Deplo unreachable during the second loss, nothing moves: one of two is no majority without its word.
func TestWithDeploAwayTheSecondLossMovesNothing(t *testing.T) {
	f := webOnB(t)
	f.away = true
	f.pass(leader.DeploFor + time.Second)
	f.ms.cut["fp-srv_b"] = true
	f.pass(time.Minute)
	if got := placed(f, "prj_1"); got != "srv_b" {
		t.Fatalf("web moved to %q", got)
	}
}

// Two lost at the same moment still stop as before: one of three answers, and nothing moves.
func TestTwoLostAtOnceStillMoveNothing(t *testing.T) {
	f := startFO(t, abc())
	f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", true)
	f.assign(t, "prj_1", "web", "srv_b")
	f.ms.cut["fp-srv_b"], f.ms.cut["fp-srv_c"] = true, true
	f.pass(leader.GoneAfter + time.Minute)
	if got := placed(f, "prj_1"); got != "srv_b" {
		t.Fatalf("web moved to %q", got)
	}
	if ev := f.events("member_gone"); len(ev) != 0 {
		t.Fatalf("a leader without a majority took members out: %v", ev)
	}
}

// A gone member that comes back votes and hosts again only once it has answered for GiveCopyAfter.
func TestAGoneMemberCountsAgainOnceItHasCaughtUp(t *testing.T) {
	f := webOnB(t)
	delete(f.ms.cut, "fp-srv_c")
	f.ms.returned["fp-srv_c"] = &lease.Returned{}
	f.pass(time.Minute)
	if len(f.events("member_rejoined")) != 0 || len(f.lastOrder("fp-srv_a").Gone) != 1 {
		t.Fatal("srv_c counts again before it caught up")
	}
	f.pass(leader.GiveCopyAfter)
	if ev := f.events("member_rejoined"); len(ev) != 1 || len(f.lastOrder("fp-srv_a").Gone) != 0 {
		t.Fatalf("rejoined %v, gone %v", ev, f.lastOrder("fp-srv_a").Gone)
	}
}

// Deplo hears a move the moment the leader decides it, before the start is over.
func TestAMoveIsAnnouncedBeforeItsStart(t *testing.T) {
	f := two(t)
	f.ms.cut["fp-srv_a"] = true
	f.pass(10 * time.Second)
	ev := f.events("moving")
	if len(ev) == 0 || !strings.Contains(ev[0], `"to":"srv_b"`) || !strings.Contains(ev[0], `"from":"srv_a"`) {
		t.Fatalf("moving events %v", ev)
	}
}
