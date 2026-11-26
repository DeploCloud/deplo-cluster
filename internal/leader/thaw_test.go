package leader_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/leader"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

// kept is what a leader's state file holds for clu_1: its epoch in epochs.json, whether rosters.json names it.
func kept(t *testing.T, f *fo) (uint64, bool) {
	t.Helper()
	f.l.Flush()
	var epochs map[string]uint64
	var rosters map[string]leader.Roster
	for name, v := range map[string]any{"epochs.json": &epochs, "rosters.json": &rosters} {
		raw, err := os.ReadFile(filepath.Join(f.dir, name))
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, v); err != nil {
				t.Fatal(err)
			}
		}
	}
	_, named := rosters["clu_1"]
	return epochs["clu_1"], named
}

// c44: eu-3 hung two minutes, and at the thaw its agent asked its leader to lead on a silence both had slept
// through. The leader refuses until it has run awake as long as the ask waits, and leads only if still unheard.
func TestAThawedHeirAsksToLeadOnlyOnceItHeardTheSilenceAwake(t *testing.T) {
	f := lined(t)
	g := heirOn(t, f, "srv_c")
	g.l.Awake()
	for i := 0; i < 120; i++ {
		f.clk.Add(lease.RenewEvery)
		f.step()
	}
	f.ms.cut["fp-srv_a"], g.blind["srv_a"] = true, true
	thaw := f.clk.Now()
	// The ask may come before the leader's own beat notices the gap.
	if err := g.l.Succeed(ask(f, 3)); !errors.Is(err, leader.ErrAsleep) {
		t.Fatalf("thawed, the leader tries to lead on a silence it slept through: %v", err)
	}
	g.l.Awake()
	for {
		err := g.l.Succeed(ask(f, 3))
		if err == nil {
			break
		}
		if !errors.Is(err, leader.ErrAsleep) || f.clk.Now().Sub(thaw) > time.Minute {
			t.Fatalf("%s after the thaw: %v", f.clk.Now().Sub(thaw), err)
		}
		f.clk.Add(lease.RenewEvery)
		g.l.Awake()
	}
	// Second in line: the heir's wait and one stagger, as its agent counts them.
	if d := f.clk.Now().Sub(thaw); d != lease.PreVoteFor+lease.SucceedStagger {
		t.Fatalf("the leader took the ask %s after the thaw", d)
	}
	g.step()
	if !leadsNow(g) {
		t.Fatalf("awake and its leader silent, the heir does not lead: %+v", g.l.Snapshot().Clusters["clu_1"])
	}
	if epoch, named := kept(t, g); epoch != 3 || !named {
		t.Fatalf("a quorum took epoch 3, and the leader keeps epoch %d (roster kept: %v)", epoch, named)
	}
}

// A leader that ran without a gap is never held up: its member's agent heard the silence awake.
func TestAnHeirThatNeverSleptIsNotHeldUp(t *testing.T) {
	f := lined(t)
	g := heir(t, f)
	g.l.Awake()
	f.ms.cut["fp-srv_a"], g.blind["srv_a"] = true, true
	for d := time.Duration(0); d < lease.PreVoteFor; d += lease.RenewEvery {
		f.clk.Add(lease.RenewEvery)
		g.l.Awake()
	}
	if err := g.l.Succeed(ask(f, 2)); err != nil {
		t.Fatal(err)
	}
	g.step()
	if !leadsNow(g) {
		t.Fatal("an heir that never slept does not lead")
	}
}

// c44: the try at epoch 3 that no quorum took stayed in epochs.json, and the leader refused the move Deplo made to
// it at epoch 2 ("the leader holds a newer epoch"). A try promises nothing: the move is taken, after a restart too.
func TestATryNoQuorumTookPromisesNothing(t *testing.T) {
	for _, restart := range []bool{false, true} {
		f := lined(t)
		g := heirOn(t, f, "srv_c")
		if err := g.l.Succeed(ask(f, 3)); err != nil {
			t.Fatal(err)
		}
		for d := time.Duration(0); d <= leader.SucceedFor+lease.RenewEvery; d += lease.RenewEvery {
			f.clk.Add(lease.RenewEvery)
			f.step()
			g.step()
		}
		if _, tries := g.l.Snapshot().Clusters["clu_1"]; tries || !leadsNow(f) {
			t.Fatal("setup: the heir still tries, or the leader stopped leading")
		}
		if epoch, named := kept(t, g); epoch != 0 || named {
			t.Fatalf("a try no quorum took is kept on disk: epoch %d, roster %v", epoch, named)
		}
		if restart {
			g.restart()
		}
		if err := g.l.SetRoster(withLine(snapshotRoster(f, 2), "srv_c")); err != nil {
			t.Fatalf("restarted %v: Deplo's move at epoch 2 is refused: %v", restart, err)
		}
		g.step()
		f.step()
		if !leadsNow(g) || !f.l.Snapshot().Clusters["clu_1"].Superseded {
			t.Fatalf("restarted %v: moved to at epoch 2, the leader does not lead: %+v", restart, g.l.Snapshot().Clusters["clu_1"])
		}
	}
}

// c44: Deplo named epoch 135 for one leader while the heir took 135 by succession. Each member gives an epoch to one
// leader only, so whichever reaches the members first leads and the other never does at that epoch.
func TestTwoCandidatesAtOneEpochNeverBothLead(t *testing.T) {
	for _, deploFirst := range []bool{false, true} {
		f := lined(t)
		heir := heirOn(t, f, "srv_b")
		named := heirOn(t, f, "srv_c")
		f.ms.cut["fp-srv_a"], heir.blind["srv_a"], named.blind["srv_a"] = true, true, true
		f.clk.Add(lease.PreVoteFor + lease.RenewEvery)
		if err := heir.l.Succeed(ask(f, 2)); err != nil {
			t.Fatal(err)
		}
		if err := named.l.SetRoster(withLine(snapshotRoster(f, 2), "srv_c")); err != nil {
			t.Fatal(err)
		}
		named.away = false
		first, second, self := heir, named, "srv_b"
		if deploFirst {
			first, second, self = named, heir, "srv_c"
		}
		for i := 0; i < 15; i++ {
			first.step()
			second.step()
			if leadsNow(heir) && leadsNow(named) {
				t.Fatalf("Deplo first %v: two leaders at epoch 2", deploFirst)
			}
			f.clk.Add(lease.RenewEvery)
		}
		if !leadsNow(first) || leadsNow(second) {
			t.Fatalf("Deplo first %v: the first to reach the members does not lead alone", deploFirst)
		}
		for _, fp := range []string{"fp-srv_b", "fp-srv_c"} {
			if f.ms.epoch[fp] != 2 || f.ms.from[fp] != self {
				t.Fatalf("Deplo first %v: %s holds epoch %d from %q", deploFirst, fp, f.ms.epoch[fp], f.ms.from[fp])
			}
		}
	}
}
