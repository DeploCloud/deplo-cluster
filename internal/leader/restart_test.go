package leader_test

import (
	"context"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/leader"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

// restart is the leader's process ending and a new one starting on the same state directory.
func (f *fo) restart() {
	f.l = leader.New(leader.Config{Dial: f.ms.dial, Now: f.clk.Now, StateDir: f.dir, Bundles: f.store, PathOK: func() bool { return f.pathOK }})
}

func snapshotRoster(f *fo, epoch uint64) leader.Roster {
	r := staleRoster("srv_a", "srv_b")
	r.Epoch, r.Placement = epoch, f.l.Snapshot().Clusters["clu_1"].Placement
	return r
}

// An update of the leader (spec #176): it extends every lease, is away for three minutes and comes back. No
// member's lease ran out meanwhile, so none stopped its database, and the leader that comes back moves nothing.
func TestAPlannedRestartFencesNoMemberAndMovesNothing(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": roomy("amd64")})
	f.putDB(t, "db_1", "db-main", backup(""))
	// The control plane's roster names the database: that is what a restarted leader reads back.
	if err := f.l.SetRoster(snapshotRoster(f, 1)); err != nil {
		t.Fatal(err)
	}
	f.pass(10 * time.Second)
	head := f.l.Events().Head()

	f.l.Drain(context.Background())
	// A member's lease lives 60 s past its last renewal, plus what that renewal added.
	ends := map[string]time.Time{}
	for _, fp := range []string{"fp-srv_a", "fp-srv_b"} {
		ends[fp] = f.clk.Now().Add(lease.FenceAfter + time.Duration(f.lastOrder(fp).ExtendSeconds)*time.Second)
	}
	f.clk.Add(3 * time.Minute)
	for fp, end := range ends {
		if !f.clk.Now().Before(end) {
			t.Fatalf("%s fenced itself %s before the leader was back", fp, f.clk.Now().Sub(end))
		}
	}

	f.restart()
	f.pass(2 * time.Minute)
	for id, s := range states(t, f.l) {
		if s != "ok" {
			t.Fatalf("%s is %s after the restart", id, s)
		}
	}
	ev, _ := f.l.Events().Since(head)
	for _, e := range ev {
		if e.Type == "member_state" || e.Type == "failover" || e.Type == "placement" {
			t.Fatalf("the restart cost a %s: %s", e.Type, e.Data)
		}
	}
	if placed(f, "db_1") != "srv_a" || len(f.ms.starts["fp-srv_b"]) != 0 {
		t.Fatalf("the database moved to %q over a planned restart", placed(f, "db_1"))
	}
	// Back to plain renewals: a member that goes silent now is failed over on the usual clock.
	f.ms.cut["fp-srv_a"] = true
	f.pass(85 * time.Second)
	if placed(f, "db_1") != "srv_b" {
		t.Fatalf("the extension outlived the restart: database on %q", placed(f, "db_1"))
	}
}

// A lease this leader extended is alive on a member that heard nobody since, whoever led in between: a newer
// epoch does not shorten it, so nothing of that member moves before it has run out.
func TestAnExtendedLeaseOutlivesAChangeOfHands(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": roomy("amd64")})
	f.putDB(t, "db_1", "db-main", backup(""))
	f.l.Drain(context.Background())
	f.ms.cut["fp-srv_a"] = true
	if err := f.l.SetRoster(snapshotRoster(f, 3)); err != nil {
		t.Fatal(err)
	}
	f.pass(350 * time.Second)
	if placed(f, "db_1") != "srv_a" || len(f.ms.starts["fp-srv_b"]) != 0 {
		t.Fatalf("a newer epoch dropped the extension: the database moved to %q while its member could still run it", placed(f, "db_1"))
	}
	f.pass(40 * time.Second)
	if placed(f, "db_1") != "srv_b" {
		t.Fatalf("the database never moved once the extended lease ran out: %q", placed(f, "db_1"))
	}
}

// A member that came back still owes this leader what it removed. Leading again at a newer epoch does not
// forget the question, so the return is still told, once.
func TestAReturnStillOwedIsAskedForAfterAChangeOfHands(t *testing.T) {
	f := gone(t)
	f.ms.cut["fp-srv_a"] = false
	f.pass(10 * time.Second)
	if !f.lastOrder("fp-srv_a").Return {
		t.Fatal("the leader did not ask the member what it removed")
	}
	if err := f.l.SetRoster(snapshotRoster(f, 3)); err != nil {
		t.Fatal(err)
	}
	f.pass(5 * time.Second)
	if o := f.lastOrder("fp-srv_a"); o.Epoch != 3 || !o.Return {
		t.Fatalf("after the change of hands: epoch %d, asks %v", o.Epoch, o.Return)
	}
	f.ms.returned["fp-srv_a"] = &lease.Returned{Removed: []string{"web"}}
	f.pass(30 * time.Second)
	if got := f.events("member_returned"); len(got) != 1 || got[0] != `{"member":"srv_a","stopped":[{"kind":"app","workloadId":"prj_1"}]}` {
		t.Fatalf("return events %v", got)
	}
	if f.lastOrder("fp-srv_a").Return {
		t.Fatal("the leader keeps asking after it was told")
	}
}

// The restart lands while a plain renewal is still on its way. The extension leaves after it, so the last word
// a member has from this leader is the longer lease, never the 60 s one.
func TestTheExtensionIsTheLastOrderAMemberGets(t *testing.T) {
	f := two(t)
	f.ms.mu.Lock()
	f.ms.slow = map[string]time.Duration{"fp-srv_a": 100 * time.Millisecond}
	f.ms.mu.Unlock()
	ticked := make(chan struct{})
	go func() {
		f.l.Tick(context.Background())
		close(ticked)
	}()
	time.Sleep(20 * time.Millisecond)
	f.l.Drain(context.Background())
	<-ticked
	if o := f.lastOrder("fp-srv_a"); o.ExtendSeconds == 0 {
		t.Fatal("a plain renewal reached the member after its lease was extended")
	}
}

// Once the process is told to stop, the extension is the only order left to send: a renewal that started
// after it would put every lease back to 60 s.
func TestALeaderToldToStopSendsNoMoreRenewals(t *testing.T) {
	f := two(t)
	f.l.Drain(context.Background())
	stopped, cancel := context.WithCancel(context.Background())
	cancel()
	f.l.Tick(stopped)
	f.l.Failover(stopped)
	for _, fp := range []string{"fp-srv_a", "fp-srv_b"} {
		if o := f.lastOrder(fp); o.ExtendSeconds == 0 {
			t.Fatalf("%s got a plain renewal after its lease was extended", fp)
		}
	}
}
