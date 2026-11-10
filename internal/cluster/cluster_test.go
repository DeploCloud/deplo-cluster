package cluster

import "testing"

func TestParseRole(t *testing.T) {
	cases := []struct {
		args []string
		role Role
		rest []string
		err  bool
	}{
		{nil, RoleLeader, nil, false},
		{[]string{"--version"}, RoleLeader, []string{"--version"}, false},
		{[]string{"relay"}, RoleRelay, nil, false},
		{[]string{"relay", "--version"}, RoleRelay, []string{"--version"}, false},
		{[]string{"lead"}, "", nil, true},
	}
	for _, c := range cases {
		role, rest, err := ParseRole(c.args)
		if (err != nil) != c.err {
			t.Fatalf("%v: err = %v", c.args, err)
		}
		if err != nil {
			continue
		}
		if role != c.role || len(rest) != len(c.rest) {
			t.Fatalf("%v: got %s %v, want %s %v", c.args, role, rest, c.role, c.rest)
		}
	}
}

func TestStatusReportsVersionRoleAndEpoch(t *testing.T) {
	old := Version
	Version = "1.2.3"
	defer func() { Version = old }()
	got := Status{Role: RoleRelay, Epoch: 7}.String()
	want := "deplo-cluster 1.2.3 relay epoch 7"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestAFreshProcessHoldsNoEpoch(t *testing.T) {
	if InitialEpoch != 0 {
		t.Fatalf("a process that never held a lease reports epoch 0, got %d", InitialEpoch)
	}
}
