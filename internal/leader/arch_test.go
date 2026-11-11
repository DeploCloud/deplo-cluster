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
	"github.com/DeploCloud/deplo-cluster/internal/placement"
)

// mixed is one x86 server with little room, one ARM server with the most, and a second x86 server.
func mixed(t *testing.T) *fo {
	return startFO(t, map[string]leader.Member{"srv_a": {Arch: "amd64", FreeMB: 1024}, "srv_b": roomy("arm64"), "srv_c": {Arch: "amd64", FreeMB: 2048}})
}

func (f *fo) putFor(t *testing.T, id, slug string, archs ...string) {
	t.Helper()
	raw, _ := json.Marshal(bundles.Bundle{V: 1, Kind: "app", Workload: id, DeploymentID: "dep_" + id, Stack: bundles.Stack{Slug: slug, ComposeYAML: "services: {}"}})
	rec, _ := age.ParseX25519Recipient(f.store.PublicKey())
	var out bytes.Buffer
	w, _ := age.Encrypt(&out, rec)
	w.Write(raw)
	w.Close()
	if err := f.store.Put("clu_1", "app", id, "dep_"+id, out.Bytes(), bundles.Meta{Name: slug, HasDomain: true, Archs: archs}); err != nil {
		t.Fatal(err)
	}
}

func runsOn(id string, limitMB int, archs ...string) leader.PlaceRequest {
	return leader.PlaceRequest{Cluster: "clu_1", Workload: placement.Workload{Kind: "app", ID: id, Name: id, Archs: archs, LimitMB: limitMB}}
}

func TestAFirstPlacementOnlyOffersMembersOfTheImagesArchitecture(t *testing.T) {
	f := mixed(t)
	for _, c := range []struct {
		archs []string
		want  string
	}{
		{[]string{"amd64"}, "srv_c"},
		{[]string{"arm64"}, "srv_b"},
		// One that runs on both goes where the most memory is left, whatever the processor.
		{[]string{"amd64", "arm64"}, "srv_b"},
	} {
		if ans, err := f.l.Place(runsOn("prj_1", 0, c.archs...)); err != nil || ans.Member != c.want {
			t.Fatalf("%v placed on %+v, %v", c.archs, ans, err)
		}
	}
	// The ARM server has the room, and it is never offered to an x86 image.
	if ans, _ := f.l.Place(runsOn("prj_1", 3000, "amd64")); ans.Refusal == nil || ans.Refusal.Reason != placement.NoRoom || ans.Refusal.NeededMB != 3000 {
		t.Fatalf("answer %+v", ans)
	}
	if ans, _ := f.l.Place(runsOn("prj_1", 0, "riscv64")); ans.Refusal == nil || ans.Refusal.Reason != placement.NoArchitecture {
		t.Fatalf("answer %+v", ans)
	}
}

func TestReplicasSpreadOnlyOverMembersOfTheImagesArchitecture(t *testing.T) {
	f := mixed(t)
	req := runsOn("prj_1", 0, "amd64")
	req.Replicas = 3
	ans, err := f.l.Place(req)
	if err != nil || len(ans.Members) != 3 || strings.Contains(strings.Join(ans.Members, ","), "srv_b") {
		t.Fatalf("members %v, %v", ans.Members, err)
	}
	if got := strings.Join(ans.Members[:2], ","); got != "srv_c,srv_a" {
		t.Fatalf("the first two replicas share a server: %s", got)
	}
}

func TestAFailoverStaysOnTheImagesArchitecture(t *testing.T) {
	f := mixed(t)
	f.putFor(t, "prj_1", "web", "amd64")
	f.assign(t, "prj_1", "web", "srv_a")
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	if placed(f, "prj_1") != "srv_c" || len(f.ms.starts["fp-srv_c"]) != 1 || len(f.ms.starts["fp-srv_b"]) != 0 {
		t.Fatalf("placed %q, starts on the ARM server %d", placed(f, "prj_1"), len(f.ms.starts["fp-srv_b"]))
	}
}

func TestAFailoverWithNoMemberOfTheArchitectureWaits(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": roomy("arm64"), "srv_c": roomy("arm64")})
	f.putFor(t, "prj_1", "web", "amd64")
	f.assign(t, "prj_1", "web", "srv_a")
	f.ms.cut["fp-srv_a"] = true
	f.pass(150 * time.Second)
	if placed(f, "prj_1") != "" || len(f.ms.starts["fp-srv_b"])+len(f.ms.starts["fp-srv_c"]) != 0 {
		t.Fatalf("an x86 image was started on an ARM server: %q", placed(f, "prj_1"))
	}
	var summary string
	ev, _ := f.l.Events().Since(0)
	for _, e := range ev {
		if e.Type == "failover" {
			summary = string(e.Data)
		}
	}
	if !strings.Contains(summary, `"no_architecture"`) {
		t.Fatalf("failover summary %s", summary)
	}
}

func TestAnImageThatRunsOnBothFailsOverToEither(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": roomy("arm64")})
	f.putFor(t, "prj_1", "web", "amd64", "arm64")
	f.assign(t, "prj_1", "web", "srv_a")
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	if placed(f, "prj_1") != "srv_b" || len(f.ms.starts["fp-srv_b"]) != 1 {
		t.Fatalf("placed %q", placed(f, "prj_1"))
	}
}
