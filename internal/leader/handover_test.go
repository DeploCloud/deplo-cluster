package leader_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/leader"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
	"github.com/DeploCloud/deplo-cluster/internal/wire"
)

// leads is the epoch a leader holds for clu_1 and whether it still sends orders at it.
func leads(l *leader.Leader) (uint64, bool) {
	v, ok := l.Snapshot().Clusters["clu_1"]
	return v.Epoch, ok && !v.Superseded
}

// A leader on another server and the one on Deplo's machine take turns (spec #176): the control plane hands each
// change a new epoch, and a leader that stepped down never comes back at the epoch that replaced it.
func TestTwoLeadersNeverLeadAtTheSameEpoch(t *testing.T) {
	clk := &clock{t: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
	ms := newMembers()
	remote := leader.New(leader.Config{Dial: ms.dial, Now: clk.Now})
	own := leader.New(leader.Config{Dial: ms.dial, Now: clk.Now})
	ctx := context.Background()
	check := func(step string) {
		t.Helper()
		re, rl := leads(remote)
		oe, ol := leads(own)
		if rl && ol && re == oe {
			t.Fatalf("%s: both leaders lead at epoch %d", step, re)
		}
	}
	tick := func() {
		remote.Tick(ctx)
		own.Tick(ctx)
		clk.Add(lease.RenewEvery)
	}

	if err := remote.SetRoster(roster(2, "srv_a")); err != nil {
		t.Fatal(err)
	}
	tick()
	check("the remote leader leads")

	// It goes quiet for Deplo: the leader on Deplo's machine takes over with the next epoch.
	if err := own.SetRoster(roster(3, "srv_a")); err != nil {
		t.Fatal(err)
	}
	own.Tick(ctx)
	// It was only cut off from Deplo: its older orders are refused, so it steps down.
	remote.Tick(ctx)
	if _, l := leads(remote); l {
		t.Fatal("the remote leader kept leading after a member refused its epoch")
	}
	if e, l := leads(own); !l || e != 3 {
		t.Fatalf("Deplo's leader: epoch %d leads %v", e, l)
	}

	// The epoch that replaced it is not one it may lead with, whoever pushes it.
	if err := remote.SetRoster(roster(3, "srv_a")); !errors.Is(err, leader.ErrOlderEpoch) {
		t.Fatalf("a leader that stepped down took the epoch that replaced it: %v", err)
	}
	tick()
	check("after the refused roster")
	if _, l := leads(remote); l {
		t.Fatal("the remote leader leads again at the epoch of the one that replaced it")
	}

	// Hand-back: another epoch. The remote leader leads, and the one on Deplo's machine steps down in turn.
	if err := remote.SetRoster(roster(4, "srv_a")); err != nil {
		t.Fatal(err)
	}
	remote.Tick(ctx)
	own.Tick(ctx)
	check("after the hand-back")
	if e, l := leads(remote); !l || e != 4 {
		t.Fatalf("the remote leader after the hand-back: epoch %d leads %v", e, l)
	}
	if _, l := leads(own); l {
		t.Fatal("Deplo's leader kept leading after the hand-back")
	}
	if err := own.SetRoster(roster(4, "srv_a")); !errors.Is(err, leader.ErrOlderEpoch) {
		t.Fatalf("Deplo's leader took the epoch it handed back: %v", err)
	}
	if got := ms.epoch["fp-srv_a"]; got != 4 {
		t.Fatalf("the member holds epoch %d", got)
	}
}

func TestALeaderThatSteppedDownStaysDownAcrossARestart(t *testing.T) {
	dir := t.TempDir()
	l, _, ms := start(t, dir)
	if err := l.SetRoster(roster(2, "srv_a")); err != nil {
		t.Fatal(err)
	}
	ms.epoch["fp-srv_a"] = 3
	l.Tick(context.Background())

	again := leader.New(leader.Config{Dial: ms.dial, Now: time.Now, StateDir: dir})
	if _, leading := leads(again); leading {
		t.Fatal("a restarted leader leads a cluster it had stepped down from")
	}
	if err := again.SetRoster(roster(3, "srv_a")); !errors.Is(err, leader.ErrOlderEpoch) {
		t.Fatalf("a restarted leader took the epoch that replaced it: %v", err)
	}
	if err := again.SetRoster(roster(4, "srv_a")); err != nil {
		t.Fatal(err)
	}
	if e, leading := leads(again); !leading || e != 4 {
		t.Fatalf("epoch %d leads %v", e, leading)
	}
}

// Another leader led in between: what this one moved before is that leader's to have changed.
func TestALeaderThatLeadsAgainDropsTheMovesItMadeBefore(t *testing.T) {
	f := two(t)
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	if placed(f, "prj_1") != "srv_b" {
		t.Fatalf("placed on %q", placed(f, "prj_1"))
	}
	f.ms.cut["fp-srv_a"] = false
	back := staleRoster("srv_a", "srv_b")
	back.Epoch = 3
	if err := f.l.SetRoster(back); err != nil {
		t.Fatal(err)
	}
	if placed(f, "prj_1") != "srv_a" {
		t.Fatalf("an old move was laid over the roster of a newer epoch: %q", placed(f, "prj_1"))
	}
	if moves := f.l.Snapshot().Clusters["clu_1"].Moves; len(moves) != 0 {
		t.Fatalf("moves %+v", moves)
	}
}

// A leader that did not lead heard nobody. It must hear the silence itself before it moves anything.
func TestALeaderThatLeadsAgainStartsEveryLeaseOver(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": roomy("amd64"), "srv_c": roomy("amd64")})
	f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", true)
	f.assign(t, "prj_1", "web", "srv_c")
	for _, fp := range []string{"fp-srv_a", "fp-srv_b", "fp-srv_c"} {
		f.ms.epoch[fp] = 2
	}
	f.pass(3 * time.Minute)
	if _, leading := leads(f.l); leading {
		t.Fatal("the leader did not step down")
	}

	f.ms.cut["fp-srv_c"] = true
	r := leader.Roster{Cluster: "clu_1", Epoch: 3, Placement: []lease.Entry{{Kind: "app", ID: "prj_1", Member: "srv_c", Slug: "web"}}}
	for _, id := range []string{"srv_a", "srv_b", "srv_c"} {
		r.Members = append(r.Members, leader.Member{ID: id, Fingerprint: "fp-" + id, Arch: "amd64", FreeMB: 4096})
	}
	if err := f.l.SetRoster(r); err != nil {
		t.Fatal(err)
	}
	f.pass(2 * time.Second)
	if placed(f, "prj_1") != "srv_c" || len(f.ms.starts["fp-srv_a"])+len(f.ms.starts["fp-srv_b"]) != 0 {
		t.Fatalf("work moved off a member this leader had not heard silent for 5 s: %q", placed(f, "prj_1"))
	}
	if got := states(t, f.l)["srv_c"]; got != "ok" {
		t.Fatalf("srv_c is %s after 2 s", got)
	}
	f.pass(75 * time.Second)
	if placed(f, "prj_1") == "srv_c" {
		t.Fatal("the work never moved")
	}
}

// What waited for room under the other leader is still waiting: the one that takes over keeps trying it.
func TestALeaderThatTakesOverKeepsRetryingWhatWaitsForRoom(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": roomy("amd64")})
	f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", true)
	r := staleRoster("srv_a", "srv_b")
	r.Epoch = 2
	r.Placement[0].Member = ""
	if err := f.l.SetRoster(r); err != nil {
		t.Fatal(err)
	}
	f.pass(35 * time.Second)
	if placed(f, "prj_1") == "" || len(f.ms.starts["fp-srv_a"])+len(f.ms.starts["fp-srv_b"]) != 1 {
		t.Fatalf("a workload waiting for room was not tried by the leader that took over: %q", placed(f, "prj_1"))
	}
}

// The hand-back lands while an order of the old epoch is still on its way: the refusal that order earns is not
// about the epoch the leader holds by then.
func TestARefusalOfAnOlderOrderDoesNotStopALeaderThatWasHandedANewerEpoch(t *testing.T) {
	var l *leader.Leader
	handed := false
	dial := func(_ context.Context, _ wire.Party) (net.Conn, error) {
		a, b := net.Pipe()
		go func() {
			defer b.Close()
			if _, err := bufio.NewReader(b).ReadBytes('\n'); err != nil {
				return
			}
			r := lease.Reply{OK: true}
			if !handed {
				handed = true
				_ = l.SetRoster(roster(4, "srv_a"))
				r = lease.Reply{Error: lease.StaleEpoch, Epoch: 3}
			}
			raw, _ := json.Marshal(r)
			b.Write(append(raw, '\n'))
		}()
		return a, nil
	}
	l = leader.New(leader.Config{Dial: dial})
	if err := l.SetRoster(roster(2, "srv_a")); err != nil {
		t.Fatal(err)
	}
	l.Tick(context.Background())
	if e, leading := leads(l); !leading || e != 4 {
		t.Fatalf("epoch %d leads %v", e, leading)
	}
}
