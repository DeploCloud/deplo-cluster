package leader_test

import (
	"strings"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/leader"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

func (f *fo) events(kind string) []string {
	ev, _ := f.l.Events().Since(0)
	var out []string
	for _, e := range ev {
		if e.Type == kind {
			out = append(out, string(e.Data))
		}
	}
	return out
}

func (f *fo) lastOrder(fp string) lease.Order {
	f.ms.mu.Lock()
	defer f.ms.mu.Unlock()
	os := f.ms.orders[fp]
	return os[len(os)-1]
}

// gone is srv_a silent long enough for web to move to srv_b, while files (it holds data) waits for it.
func gone(t *testing.T) *fo {
	f := two(t)
	if err := f.l.Assign(leader.Assign{Cluster: "clu_1", Kind: "app", ID: "prj_3", Slug: "files", SingleWriter: true, Members: []string{"srv_a"}}); err != nil {
		t.Fatal(err)
	}
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	if placed(f, "prj_1") != "srv_b" {
		t.Fatalf("web is on %q", placed(f, "prj_1"))
	}
	return f
}

func TestAReturningMemberIsToldWhatIsStillItsAndOneEventNamesWhatItStopped(t *testing.T) {
	f := gone(t)
	f.ms.cut["fp-srv_a"] = false
	f.pass(5 * time.Second)
	first := f.lastOrder("fp-srv_a")
	where := map[string]string{}
	for _, e := range first.Placement {
		where[e.Slug] = e.Member
	}
	if first.Epoch != 1 || where["web"] != "srv_b" || where["files"] != "srv_a" {
		t.Fatalf("the first order back: epoch %d, placement %v", first.Epoch, where)
	}
	f.pass(5 * time.Second)
	if !f.lastOrder("fp-srv_a").Return {
		t.Fatal("the leader did not ask the member what it removed")
	}
	if f.lastOrder("fp-srv_b").Return {
		t.Fatal("a member that never left was asked")
	}
	if got := f.events("member_returned"); len(got) != 0 {
		t.Fatalf("a return was reported before the member was done: %v", got)
	}

	f.ms.returned["fp-srv_a"] = &lease.Returned{Removed: []string{"web"}}
	f.pass(5 * time.Second)
	got := f.events("member_returned")
	if len(got) != 1 || got[0] != `{"member":"srv_a","stopped":[{"kind":"app","workloadId":"prj_1"}]}` {
		t.Fatalf("return events %v", got)
	}
	f.pass(60 * time.Second)
	if n := len(f.events("member_returned")); n != 1 {
		t.Fatalf("%d return events for one return", n)
	}
	if f.lastOrder("fp-srv_a").Return {
		t.Fatal("the leader keeps asking after it was told")
	}
}

func TestAReturnThatStoppedNothingStillSaysSoOnce(t *testing.T) {
	f := gone(t)
	f.ms.cut["fp-srv_a"] = false
	f.ms.returned["fp-srv_a"] = &lease.Returned{Removed: []string{}}
	f.pass(30 * time.Second)
	if got := f.events("member_returned"); len(got) != 1 || got[0] != `{"member":"srv_a","stopped":[]}` {
		t.Fatalf("return events %v", got)
	}
}

func TestWorkThatFoundNoRoomGoesBackToTheMemberThatReturns(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": {Arch: "amd64", FreeMB: 100}})
	f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", true)
	f.assign(t, "prj_1", "web", "srv_a")
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	if placed(f, "prj_1") != "" {
		t.Fatalf("placed on %q", placed(f, "prj_1"))
	}
	f.ms.cut["fp-srv_a"] = false
	f.pass(10 * time.Second)
	if placed(f, "prj_1") != "srv_a" {
		t.Fatalf("placed on %q, want it back on the member that still has it", placed(f, "prj_1"))
	}
	if got := f.lastOrder("fp-srv_a").Placement[0].Member; got != "srv_a" {
		t.Fatalf("the member was told %q", got)
	}
	ps := f.events("placement")
	if last := ps[len(ps)-1]; !strings.Contains(last, `"members":["srv_a"]`) {
		t.Fatalf("Deplo was told %s", last)
	}
	// A roster that still says "waiting" cannot take it away again, and nothing is started elsewhere.
	r := leader.Roster{Cluster: "clu_1", Epoch: 1, Members: []leader.Member{
		{ID: "srv_a", Fingerprint: "fp-srv_a", Arch: "amd64", FreeMB: 4096}, {ID: "srv_b", Fingerprint: "fp-srv_b", Arch: "amd64", FreeMB: 4096}},
		Placement: []lease.Entry{{Kind: "app", ID: "prj_1", Slug: "web"}}}
	if err := f.l.SetRoster(r); err != nil {
		t.Fatal(err)
	}
	f.pass(60 * time.Second)
	if placed(f, "prj_1") != "srv_a" || len(f.ms.starts["fp-srv_b"]) != 0 {
		t.Fatalf("placed on %q, %d starts on srv_b", placed(f, "prj_1"), len(f.ms.starts["fp-srv_b"]))
	}
}

func TestAMemberQuietForAMomentIsNotAReturn(t *testing.T) {
	f := two(t)
	f.ms.cut["fp-srv_a"] = true
	f.pass(70 * time.Second)
	f.ms.cut["fp-srv_a"] = false
	f.ms.returned["fp-srv_a"] = &lease.Returned{Removed: []string{"web"}}
	f.pass(30 * time.Second)
	if f.lastOrder("fp-srv_a").Return || len(f.events("member_returned")) != 0 {
		t.Fatal("nothing moved, yet the leader treated it as a return")
	}
}

func TestARestartedLeaderStillAsksAMemberThatWasComingBack(t *testing.T) {
	f := gone(t)
	f.ms.cut["fp-srv_a"] = false
	f.pass(5 * time.Second)
	f.l = leader.New(leader.Config{Dial: f.ms.dial, Now: f.clk.Now, StateDir: f.dir, Bundles: f.store, PathOK: func() bool { return true }})
	f.ms.returned["fp-srv_a"] = &lease.Returned{Removed: []string{"web"}}
	f.pass(5 * time.Second)
	if got := f.events("member_returned"); len(got) != 1 {
		t.Fatalf("return events after a restart: %v", got)
	}
}
