package leader_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/bundles"
	"github.com/DeploCloud/deplo-cluster/internal/leader"
)

func TestForgetDropsTheClusterItsBundlesAndItsEpochAndSurvivesARestart(t *testing.T) {
	f := two(t)
	other := leader.Roster{Cluster: "clu_2", Epoch: 4, Members: []leader.Member{{ID: "srv_c", Fingerprint: "fp-srv_c"}}}
	if err := f.l.SetRoster(other); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(f.l.Handler())
	defer srv.Close()

	if r := call(t, srv.URL, "leader.forget", map[string]string{"cluster": "clu_1"}); r["ok"] != true {
		t.Fatalf("forget %v", r)
	}
	snap := f.l.Snapshot()
	if _, still := snap.Clusters["clu_1"]; still || snap.Clusters["clu_2"].Epoch != 4 {
		t.Fatalf("clusters after forget: %+v", snap.Clusters)
	}
	if held, _ := f.store.List("clu_1"); len(held) != 0 {
		t.Fatalf("bundles still held: %+v", held)
	}
	if _, err := f.store.Unseal("clu_1", "app", "prj_1"); err != bundles.ErrNone {
		t.Fatalf("the sealed bundle is still on disk: %v", err)
	}
	// Its members are renewed no more, and nothing of it moves.
	f.ms.cut["fp-srv_a"] = true
	before := len(f.ms.starts["fp-srv_b"])
	f.pass(2 * time.Minute)
	if len(f.ms.starts["fp-srv_b"]) != before {
		t.Fatal("a forgotten cluster's work was moved")
	}
	// Forgetting twice, or a cluster never heard of, is not an error.
	if r := call(t, srv.URL, "leader.forget", map[string]string{"cluster": "clu_1"}); r["ok"] != true {
		t.Fatalf("second forget %v", r)
	}
	if r := call(t, srv.URL, "leader.forget", map[string]string{"cluster": ""}); r["status"] != float64(400) {
		t.Fatalf("a forget without a cluster: %v", r)
	}

	again := leader.New(leader.Config{Dial: f.ms.dial, Now: f.clk.Now, StateDir: f.dir, Bundles: f.store})
	snap = again.Snapshot()
	if _, back := snap.Clusters["clu_1"]; back || snap.Clusters["clu_2"].Epoch != 4 {
		t.Fatalf("clusters after a restart: %+v", snap.Clusters)
	}
	// The epoch went with it: an older one is no longer refused for a cluster this leader does not know.
	if err := again.SetRoster(leader.Roster{Cluster: "clu_1", Epoch: 1}); err != nil {
		t.Fatalf("a roster after forget: %v", err)
	}
	again.Tick(context.Background())
}
