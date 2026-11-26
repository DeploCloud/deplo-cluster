package leader_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
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

// c48: a renewal sent while a member's agent was down hung for its whole 2 s, the tick at its end skipped that
// member as still taking it, and the agent, back meanwhile, was renewed a tick late: 4 s without a renewal.
func TestAMemberBackAfterAHungRenewalIsRenewedAtOnce(t *testing.T) {
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
	renewedAfter := func(at time.Time, within time.Duration) time.Time {
		t.Helper()
		for end := time.Now().Add(within); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
			if got := ms.renewedAt("fp-srv_c"); got.After(at) {
				return got
			}
		}
		t.Fatalf("srv_c was not renewed within %s", within)
		return time.Time{}
	}

	last := renewedAfter(renewedAfter(time.Time{}, 3*time.Second), 3*time.Second)
	time.Sleep(50 * time.Millisecond)
	ms.mu.Lock()
	ms.hang["fp-srv_c"] = true
	ms.mu.Unlock()
	// Back 0.3 s after the tick at the end of the first hung try: the next tick's renewal is waiting for it.
	time.Sleep(last.Add(3300 * time.Millisecond).Sub(time.Now()))
	ms.mu.Lock()
	ms.hang["fp-srv_c"] = false
	ms.mu.Unlock()
	back := time.Now()
	if late := renewedAfter(last, 3*time.Second).Sub(back); late > 300*time.Millisecond {
		t.Fatalf("srv_c was renewed %s after it was back", late)
	}
}

// c48: every member's renewals came 2 to 3 s apart for a while and the journal said nothing: a round that runs late
// says where its time went, and so does a member that takes long to reach or to answer.
func TestASlowRoundSaysWhereItsTimeWent(t *testing.T) {
	ms := newMembers()
	ms.slow = map[string]time.Duration{"fp-srv_b": time.Second, "fp-srv_c": time.Second}
	var mu sync.Mutex
	var said []string
	logf := func(f string, a ...any) { mu.Lock(); said = append(said, fmt.Sprintf(f, a...)); mu.Unlock() }
	l := newLeader(t, leader.Config{Dial: ms.dial, Now: time.Now, Logf: logf})
	if err := l.SetRoster(roster(1, "srv_a", "srv_b", "srv_c")); err != nil {
		t.Fatal(err)
	}
	l.Tick(context.Background())
	l.Tick(context.Background())
	mu.Lock()
	defer mu.Unlock()
	all := strings.Join(said, "\n")
	if strings.Count(all, "deplo-cluster: a round took") != 1 || !strings.Contains(all, "renewals 1") {
		t.Fatalf("two slow rounds said %q", all)
	}
	if !strings.Contains(all, "member srv_b took 0 ms to reach and 10") {
		t.Fatalf("a slow member said %q", all)
	}
}
