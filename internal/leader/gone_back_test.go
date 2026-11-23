package leader_test

import (
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/leader"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

// eu, 23 Nov 2026: srv_c was gone past the grace and back, not yet caught up, when the leader's server was lost. The
// heir that is left with exactly half leads on Deplo's word as soon as it is given, not a minute later.
func TestAnHeirLeadsOnDeplosWordWhileAGoneMemberIsBack(t *testing.T) {
	f := lined(t)
	f.ms.cut["fp-srv_c"] = true
	f.pass(leader.GoneAfter + 5*time.Second)
	if o := f.lastOrder("fp-srv_b"); len(o.Gone) != 1 {
		t.Fatalf("srv_c is not gone: %+v", o.Gone)
	}
	delete(f.ms.cut, "fp-srv_c")
	f.pass(2 * time.Second)
	g := heir(t, f)
	f.ms.cut["fp-srv_a"] = true
	g.blind["srv_a"] = true
	f.clk.Add(lease.PreVoteFor + time.Second)
	if err := g.l.Succeed(askAs(f, "srv_b", 2, lease.PreVoteFor+time.Second)); err != nil {
		t.Fatal(err)
	}
	g.away = false
	for waited := time.Duration(0); !leadsNow(g); waited += lease.RenewEvery {
		if waited > 10*time.Second {
			t.Fatalf("on Deplo's word, the heir still does not lead after %v: %+v", waited, g.l.Snapshot().Clusters["clu_1"])
		}
		if err := g.l.Succeed(askAs(f, "srv_b", 2, lease.PreVoteFor+time.Second)); err != nil {
			t.Fatal(err)
		}
		f.clk.Add(lease.RenewEvery)
		g.step()
	}
}
