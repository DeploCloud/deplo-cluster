package leader_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/leader"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

func fiseOn(epoch uint64, member string, decided uint64) leader.Roster {
	r := roster(epoch, "srv_a", "srv_b", "srv_c")
	r.Placement = []lease.Entry{{Kind: "app", ID: "prj_fise", Member: member, Slug: "fise", SingleWriter: true, Epoch: decided}}
	return r
}

func placedIn(o lease.Order) string {
	for _, e := range o.Placement {
		if e.ID == "prj_fise" {
			return e.Member
		}
	}
	return "?"
}

// c43: a watch that read an old leader's buffer again rewrote Deplo's picture with a placement hours old, and the
// roster it pushed stopped fise where it ran. An entry decided before the one the leader holds moves nothing.
func TestAnOlderRosterEntryNeverMovesAWorkloadOffItsMember(t *testing.T) {
	l, _, ms := start(t, "")
	if err := l.SetRoster(fiseOn(130, "srv_b", 114)); err != nil {
		t.Fatal(err)
	}
	l.Tick(context.Background())
	head := l.Events().Head()

	if err := l.SetRoster(fiseOn(130, "srv_c", 112)); err != nil {
		t.Fatal(err)
	}
	l.Tick(context.Background())
	for _, fp := range []string{"fp-srv_a", "fp-srv_b", "fp-srv_c"} {
		got := ms.orders[fp]
		if on := placedIn(got[len(got)-1]); on != "srv_b" {
			t.Fatalf("%s was told fise runs on %s: an entry from epoch 112 moved it", fp, on)
		}
	}
	if told := strings.Join(kinds(l, head), " "); !strings.Contains(told, `placement:{"epoch":114,"kind":"app","members":["srv_b"],"workloadId":"prj_fise"}`) {
		t.Fatalf("the control plane was not told where fise stays: %s", told)
	}
}

// A person's move is decided at the epoch the leader holds, and an older panel names no epoch: both move it.
func TestARosterEntryDecidedNowOrUndatedMovesAWorkload(t *testing.T) {
	for name, decided := range map[string]uint64{"decided now": 130, "undated": 0} {
		t.Run(name, func(t *testing.T) {
			l, _, ms := start(t, "")
			if err := l.SetRoster(fiseOn(130, "srv_b", 114)); err != nil {
				t.Fatal(err)
			}
			l.Tick(context.Background())
			if err := l.SetRoster(fiseOn(130, "srv_c", decided)); err != nil {
				t.Fatal(err)
			}
			l.Tick(context.Background())
			got := ms.orders["fp-srv_c"]
			if on := placedIn(got[len(got)-1]); on != "srv_c" {
				t.Fatalf("fise stayed on %s", on)
			}
		})
	}
}

// A leader handed a newer epoch leads on Deplo's table as it is, whatever it held before (decision 1 of c43).
func TestATakeoverLeadsOnTheRostersPlacement(t *testing.T) {
	l, _, ms := start(t, "")
	if err := l.SetRoster(fiseOn(112, "srv_c", 112)); err != nil {
		t.Fatal(err)
	}
	l.Tick(context.Background())
	if err := l.SetRoster(fiseOn(130, "srv_b", 114)); err != nil {
		t.Fatal(err)
	}
	l.Tick(context.Background())
	got := ms.orders["fp-srv_b"]
	if on := placedIn(got[len(got)-1]); on != "srv_b" {
		t.Fatalf("the new epoch led with fise on %s, the placement it held at 112", on)
	}
}

// watchFrom reads a watch from seq up to the leader's newest event, hello left out.
func watchFrom(t *testing.T, l *leader.Leader, seq uint64) []map[string]any {
	t.Helper()
	srv := httptest.NewServer(l.Handler())
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/watch?since="+strconv.FormatUint(seq, 10), nil)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	var out []map[string]any
	for head := l.Events().Head(); sc.Scan(); {
		var m map[string]any
		_ = json.Unmarshal(sc.Bytes(), &m)
		if m["hello"] == true {
			continue
		}
		out = append(out, m)
		if uint64(m["seq"].(float64)) >= head {
			break
		}
	}
	return out
}

// A watch that starts over from zero reads where an older epoch placed fise only as a place in the sequence, also
// after the leader restarted (c43: eu-3's buffer of its epoch 112 put fise back on eu-3 in Deplo's picture).
func TestAWatchFromZeroReadsNoOlderEpochsPlacement(t *testing.T) {
	dir := t.TempDir()
	l, _, _ := start(t, dir)
	if err := l.SetRoster(fiseOn(112, "srv_b", 100)); err != nil {
		t.Fatal(err)
	}
	if err := l.Assign(leader.Assign{Cluster: "clu_1", Kind: "app", ID: "prj_fise", Slug: "fise", SingleWriter: true, Members: []string{"srv_c"}}); err != nil {
		t.Fatal(err)
	}
	if err := l.SetRoster(fiseOn(130, "srv_b", 114)); err != nil {
		t.Fatal(err)
	}
	l.Events().Append("member_state", "clu_1", map[string]string{"member": "srv_a", "state": "ok"})
	check := func(l *leader.Leader) {
		t.Helper()
		var types []string
		for _, e := range watchFrom(t, l, 0) {
			if e["type"] == "past" && (e["cluster"] != "" || e["data"] != nil) {
				t.Fatalf("an older epoch's event says more than its place: %v", e)
			}
			types = append(types, e["type"].(string))
		}
		if got := strings.Join(types, " "); got != "past past epoch member_state" {
			t.Fatalf("a watch from zero read %s", got)
		}
	}
	check(l)
	l.Flush()
	again, _, _ := start(t, dir)
	check(again)
}

// A leader that stopped leading a cluster (it stood down, or its roster was refused) keeps nothing of it for a
// later roster to inherit: its old moves never lie over Deplo's table.
func TestStateNoSavedRosterTookIsNotInherited(t *testing.T) {
	dir := t.TempDir()
	at := time.Date(2026, 3, 1, 11, 0, 0, 0, time.UTC).Format(time.RFC3339)
	old := `{"clu_1":{"waiting":{},"auth":{"app/prj_fise/0":{"at":"` + at + `","entry":{"kind":"app","id":"prj_fise","replica":0,"member":"srv_c","slug":"fise","singleWriter":true},"from":"srv_b","reason":"stopped_answering","seq":7}}}}`
	if err := os.WriteFile(filepath.Join(dir, "failover.json"), []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	l, _, _ := start(t, dir)
	if err := l.SetRoster(fiseOn(130, "srv_b", 114)); err != nil {
		t.Fatal(err)
	}
	v := l.Snapshot().Clusters["clu_1"]
	if len(v.Moves) != 0 || v.Placement[0].Member != "srv_b" {
		t.Fatalf("epoch 130 leads on an old move: placement %+v, moves %+v", v.Placement, v.Moves)
	}
}

// Deplo tells the leader it was replaced before it hands the next one the cluster: the old one renews every member
// once more on the spot and stops, so the hand-over leaves no member a renewal later than usual (c43: 1.49 s).
func TestAReplacedLeaderRenewsOnceMoreThenStops(t *testing.T) {
	l, _, ms := start(t, "")
	if err := l.SetRoster(fiseOn(129, "srv_b", 114)); err != nil {
		t.Fatal(err)
	}
	l.Tick(context.Background())
	before := len(ms.orders["fp-srv_a"])
	l.Beat(leader.Beat{Epochs: map[string]uint64{"clu_1": 130}})
	got := ms.orders["fp-srv_a"]
	if len(got) != before+1 || got[len(got)-1].Epoch != 129 {
		t.Fatalf("the replaced leader sent %d more orders, the last at epoch %d", len(got)-before, got[len(got)-1].Epoch)
	}
	l.Tick(context.Background())
	if len(ms.orders["fp-srv_a"]) != before+1 || !l.Snapshot().Clusters["clu_1"].Superseded {
		t.Fatal("the replaced leader kept renewing")
	}
}

// Handed a cluster at a newer epoch, a running leader probes and renews at once, not at its next tick.
func TestALeaderHandedANewerEpochRenewsAtOnce(t *testing.T) {
	l, _, ms := start(t, "")
	if err := l.SetRoster(fiseOn(129, "srv_b", 114)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)
	count := func() int {
		ms.mu.Lock()
		defer ms.mu.Unlock()
		return len(ms.orders["fp-srv_a"])
	}
	// Right after a tick, so the next one is a second away.
	for n := count(); count() == n; {
		time.Sleep(time.Millisecond)
	}
	if err := l.SetRoster(fiseOn(130, "srv_b", 114)); err != nil {
		t.Fatal(err)
	}
	handed := time.Now()
	for {
		ms.mu.Lock()
		got := ms.orders["fp-srv_a"]
		renewed := got[len(got)-1].Epoch == 130
		ms.mu.Unlock()
		if renewed {
			break
		}
		if time.Since(handed) > 500*time.Millisecond {
			t.Fatal("no renewal at the newer epoch within half a second of the roster")
		}
		time.Sleep(time.Millisecond)
	}
}
