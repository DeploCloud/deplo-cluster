package leader_test

import (
	"context"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/leader"
)

// eu, 24 Nov 2026: under a CPU and disk hog on a 1-core member, every member's renewal came 3.3 to 4.6 s after the
// one before: a round waited for its slowest member, twice when it had to ask again first. A slow member now holds
// up only its own renewal.
func TestASlowMemberHoldsUpOnlyItsOwnRenewal(t *testing.T) {
	ms := newMembers()
	ms.now = time.Now
	ms.slow = map[string]time.Duration{"fp-srv_c": 1900 * time.Millisecond}
	l := newLeader(t, leader.Config{Dial: ms.dial, Now: time.Now, StateDir: t.TempDir(), Logf: t.Logf})
	r := leader.Roster{Cluster: "clu_1", Epoch: 1}
	for _, id := range []string{"srv_a", "srv_b", "srv_c"} {
		r.Members = append(r.Members, leader.Member{ID: id, Fingerprint: "fp-" + id})
	}
	if err := l.SetRoster(r); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); l.Run(ctx) }()
	defer func() { cancel(); <-done }()
	// Past the first rounds, which ask every member whether it takes the epoch.
	for end := time.Now().Add(5 * time.Second); ms.renewedAt("fp-srv_a").IsZero(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatal("srv_a was never renewed")
		}
	}
	var worst time.Duration
	last := ms.renewedAt("fp-srv_a")
	for end := time.Now().Add(9 * time.Second); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		if at := ms.renewedAt("fp-srv_a"); at.After(last) {
			worst, last = max(worst, at.Sub(last)), at
		}
	}
	worst = max(worst, time.Since(last))
	if worst > 1500*time.Millisecond {
		t.Fatalf("srv_a waited %s between two renewals behind a member that takes 1.9 s to answer", worst)
	}
}
