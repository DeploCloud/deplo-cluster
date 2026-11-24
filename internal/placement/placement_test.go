package placement

import "testing"

func member(id string, free, workloads int) Member {
	return Member{ID: id, Online: true, Leased: true, Arch: "amd64", Images: []string{"web:1"}, FreeMB: free, Workloads: workloads}
}

func web() Workload {
	return Workload{Kind: "app", ID: "prj_web", Name: "web", Environment: "environ_a", Image: "web:1", Archs: []string{"amd64", "arm64"}, LimitMB: 512}
}

func place(t *testing.T, w Workload, ms ...Member) Answer {
	t.Helper()
	return Place("eu", w, ms)
}

func TestNeededMemory(t *testing.T) {
	cases := []struct{ limit, peak, want int }{{512, 900, 512}, {0, 900, 900}, {0, 100, 256}, {0, 0, 256}, {100, 0, 256}}
	for _, c := range cases {
		if got := Needed(Workload{LimitMB: c.limit, PeakMB: c.peak}); got != c.want {
			t.Errorf("limit %d peak %d: got %d want %d", c.limit, c.peak, got, c.want)
		}
	}
}

func TestEligibility(t *testing.T) {
	bad := func(f func(*Member)) Member { m := member("x", 4096, 0); f(&m); return m }
	for name, m := range map[string]Member{
		"offline":  bad(func(m *Member) { m.Online = false }),
		"unleased": bad(func(m *Member) { m.Leased = false }),
		"emptying": bad(func(m *Member) { m.Emptying = true }),
		"arch":     bad(func(m *Member) { m.Arch = "riscv64" }),
		"image":    bad(func(m *Member) { m.Images = nil }),
	} {
		if a := place(t, web(), m); a.Member != "" || a.Refusal == nil {
			t.Errorf("%s: placed on %q", name, a.Member)
		}
	}
}

func TestNoRoomRefusal(t *testing.T) {
	a := place(t, web(), member("a", 511, 0), member("b", 100, 0))
	if a.Member != "" || a.Refusal == nil {
		t.Fatalf("placed: %+v", a)
	}
	r := a.Refusal
	if r.Reason != NoRoom || r.NeededMB != 512 || r.Cluster != "eu" || r.Workload != "prj_web" {
		t.Fatalf("refusal: %+v", r)
	}
}

func TestRefusalNamesTheFirstFailingStep(t *testing.T) {
	off := member("a", 4096, 0)
	off.Online = false
	if r := place(t, web(), off).Refusal; r.Reason != NoMemberOnline {
		t.Errorf("offline: %v", r.Reason)
	}
	arm := member("a", 4096, 0)
	arm.Arch = "riscv64"
	if r := place(t, web(), arm).Refusal; r.Reason != NoArchitecture {
		t.Errorf("arch: %v", r.Reason)
	}
	bare := member("a", 4096, 0)
	bare.Images = nil
	if r := place(t, web(), bare).Refusal; r.Reason != NoImage {
		t.Errorf("image: %v", r.Reason)
	}
	if r := place(t, web()).Refusal; r.Reason != NoMemberOnline {
		t.Errorf("empty cluster: %v", r.Reason)
	}
}

func TestPrefersDatabaseOfTheEnvironment(t *testing.T) {
	a, b := member("a", 8000, 0), member("b", 1000, 9)
	b.DatabaseEnvs = []string{"environ_a"}
	if got := place(t, web(), a, b).Member; got != "b" {
		t.Fatalf("got %s", got)
	}
}

func TestPrefersAMemberWithoutASibling(t *testing.T) {
	w := web()
	a, b := member("a", 8000, 0), member("b", 1000, 5)
	a.Placed = []string{"prj_web"}
	if got := place(t, w, a, b).Member; got != "b" {
		t.Fatalf("got %s", got)
	}
}

func TestThenMostFreeThenFewestWorkloads(t *testing.T) {
	if got := place(t, web(), member("a", 1000, 0), member("b", 2000, 9)).Member; got != "b" {
		t.Fatalf("free: %s", got)
	}
	if got := place(t, web(), member("a", 2000, 3), member("b", 2000, 1)).Member; got != "b" {
		t.Fatalf("workloads: %s", got)
	}
	if got := place(t, web(), member("b", 2000, 1), member("a", 2000, 1)).Member; got != "a" {
		t.Fatalf("tie: %s", got)
	}
}

func TestAnUnbuiltWorkloadSkipsTheImageAndArchitectureSteps(t *testing.T) {
	w := web()
	w.Image, w.Archs = "", nil
	m := member("a", 1000, 0)
	m.Images, m.Arch = nil, "riscv64"
	if got := place(t, w, m).Member; got != "a" {
		t.Fatalf("got %q", got)
	}
}

func spread(t *testing.T, n int, current []string, ms ...Member) []string {
	t.Helper()
	got, r := Spread("eu", web(), ms, n, current)
	if r != nil {
		t.Fatalf("refused: %+v", r)
	}
	return got
}

func TestSpreadPutsEachReplicaOnAMemberWithNoneOfTheOthers(t *testing.T) {
	got := spread(t, 3, nil, member("a", 4096, 0), member("b", 2048, 0), member("c", 1024, 0))
	if got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("got %v", got)
	}
}

func TestSpreadDoublesUpOnceEveryMemberHasOne(t *testing.T) {
	got := spread(t, 3, nil, member("a", 4096, 0), member("b", 2048, 0))
	if got[0] != "a" || got[1] != "b" || got[2] != "a" {
		t.Fatalf("got %v", got)
	}
}

func TestSpreadKeepsAReplicaWhereItRuns(t *testing.T) {
	// b runs replica 0 and has the least free memory: it still keeps it, and only the new replica is placed.
	got := spread(t, 2, []string{"b"}, member("a", 4096, 0), member("b", 100, 0), member("c", 2048, 0))
	if got[0] != "b" || got[1] != "a" {
		t.Fatalf("got %v", got)
	}
}

func TestSpreadMovesADoubledReplicaOnlyToAMemberWithNone(t *testing.T) {
	cur := []string{"a", "b", "a"}
	if got := spread(t, 3, cur, member("a", 4096, 0), member("b", 4096, 0)); got[2] != "a" {
		t.Fatalf("a doubled replica moved between members that both hold one: %v", got)
	}
	if got := spread(t, 3, cur, member("a", 4096, 0), member("b", 4096, 0), member("c", 512, 0)); got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("got %v", got)
	}
}

func TestSpreadLeavesAMemberThatStoppedAnswering(t *testing.T) {
	dead := member("b", 4096, 0)
	dead.Online = false
	got := spread(t, 2, []string{"a", "b"}, member("a", 4096, 0), dead, member("c", 1024, 0))
	if got[0] != "a" || got[1] != "c" {
		t.Fatalf("got %v", got)
	}
}

func TestSpreadRefusesWhenAReplicaHasNoRoom(t *testing.T) {
	_, r := Spread("eu", web(), []Member{member("a", 600, 0)}, 2, nil)
	if r == nil || r.Reason != NoRoom {
		t.Fatalf("refusal %+v", r)
	}
}

// eu, 24 Nov 2026: two new Apps landed on the member with the least room. A member whose disk would be left with
// less than a tenth free is never chosen, at a deploy or a failover.
func TestAMemberWithoutRoomForTheDataIsNeverChosen(t *testing.T) {
	w := web()
	w.DiskBytes = 2 << 30
	roomy, tight := member("a", 1024, 5), member("b", 8192, 0)
	roomy.DiskFree, roomy.DiskTotal = 5<<30, 10<<30
	tight.DiskFree, tight.DiskTotal = 3<<30-1, 10<<30
	if a := place(t, w, roomy, tight); a.Member != "a" {
		t.Fatalf("placed on %q", a.Member)
	}
	// It holds the data whole already: a copy promoted in place takes nothing more.
	tight.Holds = []string{w.ID}
	if a := place(t, w, tight); a.Member != "b" {
		t.Fatalf("a member that holds the data: %+v", a)
	}
	tight.Holds = nil
	a := place(t, w, tight)
	if a.Refusal == nil || a.Refusal.Reason != NoDisk || a.Refusal.NeededBytes != 2<<30 {
		t.Fatalf("no member with room: %+v", a)
	}
	// A new App holds nothing yet, and still never lands on a disk already under a tenth free.
	w.DiskBytes = 0
	tight.DiskFree = 1<<30 - 1
	if a := place(t, w, tight); a.Refusal == nil || a.Refusal.Reason != NoDisk {
		t.Fatalf("a disk under a tenth free: %+v", a)
	}
	// The member that runs it keeps it, and a disk nobody measured fits.
	tight.Placed = []string{w.ID}
	if a := place(t, w, tight); a.Member != "b" {
		t.Fatalf("the member that runs it: %+v", a)
	}
	if a := place(t, w, member("c", 1024, 0)); a.Member != "c" {
		t.Fatalf("an unknown disk: %+v", a)
	}
}

// eu, 24 Nov 2026: two new lab Apps landed on the member at 80%, the one with the most memory, and grew there to 90%.
func TestANewWorkloadGoesWhereItCanGrow(t *testing.T) {
	w := web()
	w.DataUnknown = true
	full, roomy, roomiest := member("a", 8192, 0), member("b", 2048, 3), member("c", 1024, 5)
	full.DiskFree, full.DiskTotal = 3<<29, 10<<30 // 85% used
	roomy.DiskFree, roomy.DiskTotal = 5<<30, 10<<30
	roomiest.DiskFree, roomiest.DiskTotal = 7<<30, 20<<30
	if a := place(t, w, full, roomy, roomiest); a.Member != "c" {
		t.Fatalf("placed on %q, not the most free disk", a.Member)
	}
	// Past 80% used, never while another member is under it, even with more free bytes.
	big := member("d", 8192, 0)
	big.DiskFree, big.DiskTotal = 9<<30, 50<<30
	if a := place(t, w, big, roomy); a.Member != "b" {
		t.Fatalf("placed on %q, a disk past 80%%", a.Member)
	}
	// Every member past 80%: still the most free disk.
	if a := place(t, w, full, big); a.Member != "d" {
		t.Fatalf("placed on %q", a.Member)
	}
	// Its data known, the usual rule: most free memory.
	w.DataUnknown = false
	if a := place(t, w, full, roomy, roomiest); a.Member != "a" {
		t.Fatalf("placed on %q", a.Member)
	}
}
