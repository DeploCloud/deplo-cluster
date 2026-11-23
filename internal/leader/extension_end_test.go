package leader_test

import (
	"context"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

// An extension lasts as long as the restart it covers: once every member took a plain renewal again, the leader
// says no lease lives on one, and a leader that takes over moves a lost member's database on the plain fence.
func TestAnExtensionEndsWithTheRestartItCovers(t *testing.T) {
	f := two(t)
	newer(f)
	f.putDB(t, "db_1", "db-main", backup(""))
	f.pass(10 * time.Second)
	f.l.Drain(context.Background())
	if o := f.lastOrder("fp-srv_a"); o.ExtendSeconds == 0 || o.NoExtension {
		t.Fatalf("the drain extends srv_a's lease and says nothing else: %+v", o)
	}
	// The first plain renewal ends each member's extension; the one after says so.
	f.pass(2 * lease.RenewEvery)
	for _, fp := range []string{"fp-srv_a", "fp-srv_b"} {
		if o := f.lastOrder(fp); o.ExtendSeconds != 0 || !o.NoExtension {
			t.Fatalf("%s took a plain renewal again, so no lease lives on the extension: %+v", fp, o)
		}
	}
	lost := f.clk.Now()
	g := takeOverWith(t, f, shortFences(snapshotRoster(f, f.l.Snapshot().Clusters["clu_1"].Epoch+1)), "srv_a")
	g.putDB(t, "db_1", "db-main", backup(""))
	for g.clk.Now().Sub(lost) < 2*time.Minute && placed(g, "db_1") != "srv_b" {
		g.pass(5 * time.Second)
	}
	if placed(g, "db_1") != "srv_b" {
		t.Fatalf("2 minutes after srv_a was lost its database is still on %q: the extension outlived the restart", placed(g, "db_1"))
	}
}

// A member that took the extension and no plain renewal since may still live on it: nobody hears it ended.
func TestAnExtensionAMemberMayStillHoldIsNotEnded(t *testing.T) {
	f := two(t)
	newer(f)
	f.pass(10 * time.Second)
	f.l.Drain(context.Background())
	f.ms.mu.Lock()
	f.ms.cut["fp-srv_b"] = true
	f.ms.mu.Unlock()
	f.pass(30 * time.Second)
	if o := f.lastOrder("fp-srv_a"); o.NoExtension {
		t.Fatalf("srv_b may still live on its extension, so it is not over: %+v", o)
	}
	f.pass(lease.FenceAfter + lease.ExtendFor + time.Minute)
	if o := f.lastOrder("fp-srv_a"); !o.NoExtension {
		t.Fatalf("srv_b's extension ran out long ago: %+v", o)
	}
}
