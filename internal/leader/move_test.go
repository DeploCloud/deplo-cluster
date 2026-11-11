package leader_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/DeploCloud/deplo-cluster/internal/bundles"
	"github.com/DeploCloud/deplo-cluster/internal/leader"
)

const volumeRestore = `{"kind":"BACKUP_KIND_PROJECT","s3":{"bucket":"backups","secretKey":"s3cr3t","objectKey":"t/prj_3/brun_9.tar.gz"}}`

// sealedMove is the bundle of prj_3 with the backup a person picked, sealed to the leader for one move.
func sealedMove(f *fo, restore *bundles.Restore) string {
	rec, _ := age.ParseX25519Recipient(f.store.PublicKey())
	var out bytes.Buffer
	w, _ := age.Encrypt(&out, rec)
	raw, _ := json.Marshal(bundles.Bundle{V: 1, Kind: "app", Workload: "prj_3", DeploymentID: "dep_prj_3",
		Stack: bundles.Stack{Slug: "files", ComposeYAML: "services: {}"}, Restore: restore})
	w.Write(raw)
	w.Close()
	return base64.StdEncoding.EncodeToString(out.Bytes())
}

func picked() *bundles.Restore {
	return &bundles.Restore{RestoreRef: bundles.RestoreRef{BackupID: "brun_9", TakenAt: "2026-03-01T14:15:00Z"}, Request: json.RawMessage(volumeRestore)}
}

// held is two members with a Volume App on srv_a whose stored bundle names no backup, so it waits for srv_a.
func held(t *testing.T, mem map[string]leader.Member) *fo {
	t.Helper()
	f := startFO(t, mem)
	if err := f.l.Assign(leader.Assign{Cluster: "clu_1", Kind: "app", ID: "prj_3", Slug: "files", SingleWriter: true, Members: []string{"srv_a"}}); err != nil {
		t.Fatal(err)
	}
	f.put(t, "prj_3", "files", "2026-01-01T00:00:00Z", false)
	return f
}

func both() map[string]leader.Member {
	return map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": roomy("amd64")}
}

func events(f *fo, kind string) []string {
	ev, _ := f.l.Events().Since(0)
	var out []string
	for _, e := range ev {
		if e.Type == kind {
			out = append(out, string(e.Data))
		}
	}
	return out
}

func move(f *fo, restore *bundles.Restore) leader.MoveAnswer {
	return f.l.Move(leader.MoveRequest{Cluster: "clu_1", Kind: "app", ID: "prj_3", Sealed: sealedMove(f, restore)})
}

func TestAHeldVolumeAppComesBackOnAnotherMemberFromTheBackupOfOneMove(t *testing.T) {
	f := held(t, both())
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	if placed(f, "prj_3") != "srv_a" || len(f.ms.starts["fp-srv_b"]) != 0 {
		t.Fatal("a Volume App moved on its own")
	}
	if a := move(f, picked()); !a.OK {
		t.Fatalf("the move was refused: %+v", a)
	}
	f.pass(5 * time.Second)
	starts := f.ms.starts["fp-srv_b"]
	if len(starts) != 1 || starts[0].Start.Slug != "files" || starts[0].Start.Restore == nil ||
		starts[0].Start.Restore.BackupID != "brun_9" || string(starts[0].Start.Restore.Request) != volumeRestore {
		t.Fatalf("start orders %+v", starts)
	}
	if placed(f, "prj_3") != "srv_b" {
		t.Fatalf("placed on %q", placed(f, "prj_3"))
	}
	moved := events(f, "failed_over")
	if len(moved) != 1 || !strings.Contains(moved[0], `"backupId":"brun_9"`) || !strings.Contains(moved[0], `"backupAt":"2026-03-01T14:15:00Z"`) ||
		!strings.Contains(moved[0], `"from":"srv_a"`) || !strings.Contains(moved[0], `"to":"srv_b"`) || strings.Contains(moved[0], "s3cr3t") {
		t.Fatalf("failed_over events %v", moved)
	}
	// The bundle the leader keeps still names no backup: the next time its member stops, the App waits again.
	if m, err := f.store.Meta("clu_1", "app", "prj_3"); err != nil || m.Restore != nil {
		t.Fatalf("the kept bundle changed: %+v %v", m.Restore, err)
	}
}

func TestAMoveIsRefusedWhileTheMemberMayStillHoldItsLease(t *testing.T) {
	f := held(t, both())
	f.pass(10 * time.Second)
	if a := move(f, picked()); a.OK || a.Reason != leader.StillAnswering {
		t.Fatalf("a move off a member that answers was taken: %+v", a)
	}
	f.ms.cut["fp-srv_a"] = true
	// Fenced at 60 s, but a member is only known to have stopped its work at 81 s.
	f.pass(70 * time.Second)
	if a := move(f, picked()); a.OK || a.Reason != leader.StillAnswering {
		t.Fatalf("a move before the lease is surely over was taken: %+v", a)
	}
	f.pass(10 * time.Second)
	if len(f.ms.starts["fp-srv_b"]) != 0 || placed(f, "prj_3") != "srv_a" {
		t.Fatal("a refused move started something")
	}
}

func TestAMoveWhoseMemberCameBackBeforeTheStartStartsNothing(t *testing.T) {
	f := held(t, both())
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	if a := move(f, picked()); !a.OK {
		t.Fatalf("%+v", a)
	}
	f.ms.cut["fp-srv_a"] = false
	f.pass(5 * time.Second)
	if len(f.ms.starts["fp-srv_b"]) != 0 || placed(f, "prj_3") != "srv_a" {
		t.Fatal("the App was started elsewhere while its member holds its lease again")
	}
	if failed := events(f, "move_failed"); len(failed) != 1 || !strings.Contains(failed[0], `"reason":"still_answering"`) || !strings.Contains(failed[0], `"workloadId":"prj_3"`) {
		t.Fatalf("move_failed events %v", failed)
	}
}

func TestAMoveThatFindsNoRoomSaysSoAndLeavesTheAppWithItsMember(t *testing.T) {
	f := held(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": {Arch: "amd64", FreeMB: 100}})
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	if a := move(f, picked()); !a.OK {
		t.Fatalf("%+v", a)
	}
	f.pass(5 * time.Second)
	if placed(f, "prj_3") != "srv_a" || len(f.ms.starts["fp-srv_b"]) != 0 {
		t.Fatalf("placed %q", placed(f, "prj_3"))
	}
	if failed := events(f, "move_failed"); len(failed) != 1 || !strings.Contains(failed[0], `"reason":"no_room"`) {
		t.Fatalf("move_failed events %v", failed)
	}
	// One ask is one try: nothing keeps retrying a restore nobody is watching.
	f.pass(60 * time.Second)
	if len(events(f, "move_failed")) != 1 {
		t.Fatal("the move was tried again on its own")
	}
}

func TestAMoveNeedsABackupAndABundleSealedToThisLeader(t *testing.T) {
	f := held(t, both())
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	if a := move(f, nil); a.OK || a.Reason != leader.NoBackup {
		t.Fatalf("a move with no backup was taken: %+v", a)
	}
	srv := httptest.NewServer(f.l.Handler())
	defer srv.Close()
	bad := map[string]string{"cluster": "clu_1", "kind": "app", "id": "prj_3", "sealed": base64.StdEncoding.EncodeToString([]byte("not age"))}
	if r := call(t, srv.URL, "failover.move", bad); r["ok"] != false || r["reason"] != leader.Unreadable {
		t.Fatalf("an unreadable move was taken: %v", r)
	}
	good := map[string]string{"cluster": "clu_1", "kind": "app", "id": "prj_3", "sealed": sealedMove(f, picked())}
	if r := call(t, srv.URL, "failover.move", good); r["ok"] != true {
		t.Fatalf("the call refused a good move: %v", r)
	}
}
