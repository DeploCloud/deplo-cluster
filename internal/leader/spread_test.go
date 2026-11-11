package leader_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/DeploCloud/deplo-cluster/internal/bundles"
	"github.com/DeploCloud/deplo-cluster/internal/leader"
)

// putReplicas stores a bundle whose stack says how many replicas it starts; spread marks the count for the leader.
func (f *fo) putReplicas(t *testing.T, id, slug string, spread bool) {
	t.Helper()
	st := bundles.Stack{Slug: slug, ComposeYAML: "deploy:\n  replicas: 2\n"}
	if spread {
		st.ComposeYAML, st.Scale = "deploy:\n  replicas: __N__\n", "__N__"
	}
	raw, _ := json.Marshal(bundles.Bundle{V: 1, Kind: "app", Workload: id, DeploymentID: "dep_" + id, Stack: st})
	rec, _ := age.ParseX25519Recipient(f.store.PublicKey())
	var out bytes.Buffer
	w, _ := age.Encrypt(&out, rec)
	w.Write(raw)
	w.Close()
	if err := f.store.Put("clu_1", "app", id, "dep_"+id, out.Bytes(), bundles.Meta{Name: slug, HasDomain: true}); err != nil {
		t.Fatal(err)
	}
}

func three(t *testing.T) *fo {
	return startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": {Arch: "amd64", FreeMB: 2048}, "srv_c": {Arch: "amd64", FreeMB: 1024}})
}

func replicas(f *fo, id string) string {
	out := map[int]string{}
	for _, e := range f.l.Snapshot().Clusters["clu_1"].Placement {
		if e.ID == id {
			out[e.Replica] = e.Member
		}
	}
	var got []string
	for i := 0; i < len(out); i++ {
		got = append(got, out[i])
	}
	return strings.Join(got, ",")
}

func TestPlaceSpreadsTheReplicasOfAnAppAcrossMembers(t *testing.T) {
	f := three(t)
	ans, err := f.l.Place(leader.PlaceRequest{Cluster: "clu_1", Workload: placementWorkload("prj_1"), Replicas: 3})
	if err != nil || strings.Join(ans.Members, ",") != "srv_a,srv_b,srv_c" {
		t.Fatalf("members %v, %v", ans.Members, err)
	}
	// One that already runs keeps its member: only the new replica is placed.
	ans, _ = f.l.Place(leader.PlaceRequest{Cluster: "clu_1", Workload: placementWorkload("prj_1"), Replicas: 2, Current: []string{"srv_c"}})
	if strings.Join(ans.Members, ",") != "srv_c,srv_a" {
		t.Fatalf("members %v", ans.Members)
	}
}

func TestALostReplicaOfASpreadAppStartsBesideTheOnesOnASurvivingMember(t *testing.T) {
	f := three(t)
	f.putReplicas(t, "prj_1", "web", true)
	f.assign(t, "prj_1", "web", "srv_a", "srv_b", "srv_c")
	f.ms.cut["fp-srv_c"] = true
	f.pass(75 * time.Second)
	if replicas(f, "prj_1") != "srv_a,srv_b,srv_c" {
		t.Fatal("a replica moved before about 81 s of silence")
	}
	f.pass(10 * time.Second)
	got := f.ms.starts["fp-srv_a"]
	if replicas(f, "prj_1") != "srv_a,srv_b,srv_a" || len(got) != 1 || len(f.ms.starts["fp-srv_b"]) != 0 {
		t.Fatalf("placed %s, starts on a %d on b %d", replicas(f, "prj_1"), len(got), len(f.ms.starts["fp-srv_b"]))
	}
	// The member is told how many replicas it runs now: the one it had and the one it took over.
	if got[0].Start.ComposeYAML != "deploy:\n  replicas: 2\n" {
		t.Fatalf("stack %q", got[0].Start.ComposeYAML)
	}
	var last string
	ev, _ := f.l.Events().Since(0)
	for _, e := range ev {
		if e.Type == "placement" {
			last = string(e.Data)
		}
	}
	if !strings.Contains(last, `"members":["srv_a","srv_b","srv_a"]`) {
		t.Fatalf("placement event %s", last)
	}
}

func TestALostReplicaWithNoRoomWaitsWhileTheOthersKeepRunning(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": {Arch: "amd64", FreeMB: 100}, "srv_b": {Arch: "amd64", FreeMB: 100}, "srv_c": roomy("amd64")})
	f.putReplicas(t, "prj_1", "web", true)
	f.assign(t, "prj_1", "web", "srv_a", "srv_b", "srv_c")
	f.ms.cut["fp-srv_c"] = true
	f.pass(85 * time.Second)
	if replicas(f, "prj_1") != "srv_a,srv_b," {
		t.Fatalf("placed %s", replicas(f, "prj_1"))
	}
	// Asked again meanwhile, the leader keeps the waiting replica its own to place.
	ans, _ := f.l.Place(leader.PlaceRequest{Cluster: "clu_1", Workload: placementWorkload("prj_1"), Replicas: 3, Current: []string{"srv_a", "srv_b", ""}})
	if strings.Join(ans.Members, ",") != "srv_a,srv_b," {
		t.Fatalf("members %v", ans.Members)
	}
	// The member comes back: its replica is its own again.
	f.ms.cut["fp-srv_c"] = false
	f.pass(5 * time.Second)
	if replicas(f, "prj_1") != "srv_a,srv_b,srv_c" {
		t.Fatalf("after the return %s", replicas(f, "prj_1"))
	}
}

func TestReplicasKeptTogetherMoveTogetherAndStartOnce(t *testing.T) {
	f := three(t)
	f.putReplicas(t, "prj_1", "web", false)
	f.assign(t, "prj_1", "web", "srv_c", "srv_c")
	f.ms.cut["fp-srv_c"] = true
	f.pass(85 * time.Second)
	got := f.ms.starts["fp-srv_a"]
	if replicas(f, "prj_1") != "srv_a,srv_a" || len(got) != 1 || len(f.ms.starts["fp-srv_b"]) != 0 {
		t.Fatalf("placed %s, starts on a %d on b %d", replicas(f, "prj_1"), len(got), len(f.ms.starts["fp-srv_b"]))
	}
	if got[0].Start.ComposeYAML != "deploy:\n  replicas: 2\n" {
		t.Fatalf("stack %q", got[0].Start.ComposeYAML)
	}
}

func TestReplicasKeptTogetherNeedRoomForAllOfThem(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": {Arch: "amd64", FreeMB: 300}, "srv_c": roomy("amd64")})
	f.putReplicas(t, "prj_1", "web", false)
	f.assign(t, "prj_1", "web", "srv_c", "srv_c")
	f.ms.cut["fp-srv_c"] = true
	f.pass(85 * time.Second)
	if replicas(f, "prj_1") != "," || len(f.ms.starts["fp-srv_a"]) != 0 {
		t.Fatalf("placed %s", replicas(f, "prj_1"))
	}
}
