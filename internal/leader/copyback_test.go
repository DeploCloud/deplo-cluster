package leader_test

import (
	"strings"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/leader"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

// balancedOnThree is web load-balanced on three members, and srv_c powered off long enough for its copy to start
// beside srv_a's; srv_c then comes back and says it removed its own.
func balancedOnThree(t *testing.T, singleWriter bool) *fo {
	t.Helper()
	f := three(t)
	f.putReplicas(t, "prj_1", "web", true)
	if err := f.l.Assign(leader.Assign{Cluster: "clu_1", Kind: "app", ID: "prj_1", Slug: "web", Balanced: true,
		SingleWriter: singleWriter, Members: []string{"srv_a", "srv_b", "srv_c"}}); err != nil {
		t.Fatal(err)
	}
	f.ms.cut["fp-srv_c"] = true
	f.pass(10 * time.Second)
	if !singleWriter && replicas(f, "prj_1") != "srv_a,srv_b,srv_a" {
		t.Fatalf("after the failover %s", replicas(f, "prj_1"))
	}
	f.ms.returned["fp-srv_c"] = &lease.Returned{Removed: []string{"web"}}
	return f
}

func startsOn(f *fo, fp string) []lease.Order {
	f.ms.mu.Lock()
	defer f.ms.mu.Unlock()
	return append([]lease.Order(nil), f.ms.starts[fp]...)
}

func TestAMemberBackGetsItsCopyOnlyAfterTheWaitAndNoOtherCopyRestarts(t *testing.T) {
	f := balancedOnThree(t, false)
	a, b := len(startsOn(f, "fp-srv_a")), len(startsOn(f, "fp-srv_b"))
	f.ms.cut["fp-srv_c"] = false
	f.pass(leader.GiveCopyAfter - 5*time.Second)
	if replicas(f, "prj_1") != "srv_a,srv_b,srv_a" || len(startsOn(f, "fp-srv_c")) != 0 {
		t.Fatalf("given back before the wait: %s", replicas(f, "prj_1"))
	}
	f.pass(10 * time.Second)
	got := startsOn(f, "fp-srv_c")
	if replicas(f, "prj_1") != "srv_a,srv_b,srv_c" || len(got) != 1 {
		t.Fatalf("after the wait %s, %d starts on srv_c", replicas(f, "prj_1"), len(got))
	}
	// One copy, on srv_c alone: nothing is started again, or stopped, where the others run.
	if got[0].Start.ComposeYAML != "deploy:\n  replicas: 1\n" {
		t.Fatalf("stack %q", got[0].Start.ComposeYAML)
	}
	if len(startsOn(f, "fp-srv_a")) != a || len(startsOn(f, "fp-srv_b")) != b {
		t.Fatal("another member was sent a start")
	}
	ev := f.events("failed_over")
	if last := ev[len(ev)-1]; !strings.Contains(last, `"reason":"copy_back"`) || !strings.Contains(last, `"from":"srv_a"`) || !strings.Contains(last, `"to":"srv_c"`) {
		t.Fatalf("event %s", last)
	}
	p := f.events("placement")
	if !strings.Contains(p[len(p)-1], `"members":["srv_a","srv_b","srv_c"]`) {
		t.Fatalf("placement %s", p[len(p)-1])
	}
	// Given back once: later passes leave it.
	f.pass(time.Minute)
	if len(startsOn(f, "fp-srv_c")) != 1 {
		t.Fatal("started again")
	}
}

func TestAMemberThatDropsAgainWithinTheWaitGetsNothing(t *testing.T) {
	f := balancedOnThree(t, false)
	f.ms.cut["fp-srv_c"] = false
	f.pass(time.Minute)
	f.ms.cut["fp-srv_c"] = true
	f.pass(10 * time.Second)
	f.ms.cut["fp-srv_c"] = false
	f.pass(leader.GiveCopyAfter - 10*time.Second)
	if replicas(f, "prj_1") != "srv_a,srv_b,srv_a" || len(startsOn(f, "fp-srv_c")) != 0 {
		t.Fatalf("a member that flapped got %s", replicas(f, "prj_1"))
	}
	// Its wait starts over from its last return.
	f.pass(20 * time.Second)
	if replicas(f, "prj_1") != "srv_a,srv_b,srv_c" {
		t.Fatalf("once it stayed %s", replicas(f, "prj_1"))
	}
}

func TestOnlyALoadBalancedAppThatKeepsNoDataIsGivenBack(t *testing.T) {
	// One that keeps data waits for its member and never runs twice: nothing of it moves back.
	f := balancedOnThree(t, true)
	before := replicas(f, "prj_1")
	f.ms.cut["fp-srv_c"] = false
	f.pass(leader.GiveCopyAfter + time.Minute)
	if replicas(f, "prj_1") != before || len(startsOn(f, "fp-srv_c")) != 0 {
		t.Fatalf("an App that keeps data went from %s to %s", before, replicas(f, "prj_1"))
	}

	// Replicas the cluster does not load-balance stay where the failover put them.
	g := three(t)
	g.putReplicas(t, "prj_2", "api", true)
	g.assign(t, "prj_2", "api", "srv_a", "srv_b", "srv_c")
	g.ms.cut["fp-srv_c"] = true
	g.pass(10 * time.Second)
	g.ms.returned["fp-srv_c"] = &lease.Returned{Removed: []string{"api"}}
	g.ms.cut["fp-srv_c"] = false
	g.pass(leader.GiveCopyAfter + time.Minute)
	if replicas(g, "prj_2") != "srv_a,srv_b,srv_a" {
		t.Fatalf("a spread App that is not balanced went to %s", replicas(g, "prj_2"))
	}
}

func TestACopyThatFailsToStartStaysWhereTheFailoverPutIt(t *testing.T) {
	f := balancedOnThree(t, false)
	f.ms.refuse["fp-srv_c"] = true
	f.ms.cut["fp-srv_c"] = false
	f.pass(leader.GiveCopyAfter + 10*time.Second)
	if replicas(f, "prj_1") != "srv_a,srv_b,srv_a" {
		t.Fatalf("placed %s", replicas(f, "prj_1"))
	}
	f.ms.refuse["fp-srv_c"] = false
	f.pass(10 * time.Second)
	if replicas(f, "prj_1") != "srv_a,srv_b,srv_c" {
		t.Fatalf("tried again: %s", replicas(f, "prj_1"))
	}
}
