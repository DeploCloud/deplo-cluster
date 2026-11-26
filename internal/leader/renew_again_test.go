package leader_test

import (
	"context"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/leader"
)

// deplo-agent#142: a renewal that finds a member's agent restarting waited for the next tick, a 2 s gap in that
// member's lease. It is sent again within its tick, and the other members' renewals keep their second meanwhile.
func TestAMissedRenewalIsSentAgainWithinItsTick(t *testing.T) {
	ms := newMembers()
	ms.now = time.Now
	l := newLeader(t, leader.Config{Dial: ms.dial, Now: time.Now})
	if err := l.SetRoster(roster(1, "srv_a", "srv_b", "srv_c")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); l.Run(ctx) }()
	defer func() { cancel(); <-done }()
	renewedAfter := func(at time.Time) time.Time {
		t.Helper()
		for end := time.Now().Add(3 * time.Second); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
			if got := ms.renewedAt("fp-srv_c"); got.After(at) {
				return got
			}
		}
		t.Fatal("srv_c was not renewed within 3 s")
		return time.Time{}
	}

	// Right after a renewal, past the first rounds: srv_c's agent misses the next one.
	last := renewedAfter(renewedAfter(time.Time{}))
	ms.mu.Lock()
	ms.fails["fp-srv_c"] = 1
	ms.mu.Unlock()
	if gap := renewedAfter(last).Sub(last); gap > 1300*time.Millisecond {
		t.Fatalf("srv_c went %s without a renewal across the one its agent missed", gap)
	}

	// srv_c down for 3 s: each tick reaches it on time and tries again within the tick, never into the next one.
	from := time.Now()
	ms.mu.Lock()
	ms.cut["fp-srv_c"] = true
	ms.mu.Unlock()
	time.Sleep(3200 * time.Millisecond)
	ms.mu.Lock()
	ms.cut["fp-srv_c"] = false
	tried := map[string][]time.Time{}
	for fp, ats := range ms.tried {
		for _, at := range ats {
			if at.After(from) {
				tried[fp] = append(tried[fp], at)
			}
		}
	}
	ms.mu.Unlock()
	ticks := tried["fp-srv_a"]
	if len(ticks) < 3 || len(ticks) > 4 {
		t.Fatalf("srv_a was sent %d orders in 3.2 s, one a tick", len(ticks))
	}
	for _, fp := range []string{"fp-srv_a", "fp-srv_b"} {
		for i := 1; i < len(tried[fp]); i++ {
			if gap := tried[fp][i].Sub(tried[fp][i-1]); gap > 1200*time.Millisecond {
				t.Fatalf("%s waited %s between two renewals while srv_c was down", fp, gap)
			}
		}
	}
	for i := range len(ticks) - 1 {
		var in []time.Time
		for _, at := range tried["fp-srv_c"] {
			if at.After(ticks[i].Add(-50*time.Millisecond)) && at.Before(ticks[i+1].Add(-50*time.Millisecond)) {
				in = append(in, at)
			}
		}
		if len(in) < 2 || in[0].Sub(ticks[i]).Abs() > 50*time.Millisecond || in[len(in)-1].Sub(ticks[i]) > 750*time.Millisecond {
			t.Fatalf("tick %d, at %s: srv_c was tried at %v", i, ticks[i].Format("15:04:05.000"), in)
		}
	}
}
