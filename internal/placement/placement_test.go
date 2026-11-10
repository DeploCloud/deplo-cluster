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
