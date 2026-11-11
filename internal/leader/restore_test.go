package leader_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/DeploCloud/deplo-cluster/internal/bundles"
	"github.com/DeploCloud/deplo-cluster/internal/leader"
)

const restoreRequest = `{"kind":"BACKUP_KIND_DATABASE","s3":{"bucket":"backups","secretKey":"s3cr3t","objectKey":"t/db_1/brun_1.dump.gz"}}`

func backup(storeMember string) *bundles.Restore {
	return &bundles.Restore{RestoreRef: bundles.RestoreRef{BackupID: "brun_1", TakenAt: "2026-03-01T11:45:00Z", StoreMember: storeMember}, Request: json.RawMessage(restoreRequest)}
}

// putDB hands the leader a database's bundle and places it on srv_a; restore nil is a database with no backup yet.
func (f *fo) putDB(t *testing.T, id, slug string, restore *bundles.Restore) {
	t.Helper()
	rec, _ := age.ParseX25519Recipient(f.store.PublicKey())
	var out bytes.Buffer
	w, _ := age.Encrypt(&out, rec)
	raw, _ := json.Marshal(bundles.Bundle{V: 1, Kind: "database", Workload: id, DeploymentID: "rev_" + id,
		Stack: bundles.Stack{Slug: slug, ComposeYAML: "services: {}"}, Restore: restore})
	w.Write(raw)
	w.Close()
	if err := f.store.Put("clu_1", "database", id, "rev_"+id, out.Bytes(), bundles.Meta{Name: slug, CreatedAt: "2026-06-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	if err := f.l.Assign(leader.Assign{Cluster: "clu_1", Kind: "database", ID: id, Slug: slug, SingleWriter: true, Members: []string{"srv_a"}}); err != nil {
		t.Fatal(err)
	}
}

func slugs(f *fo, fp string) string {
	var out []string
	for _, o := range f.ms.starts[fp] {
		out = append(out, o.Start.Slug)
	}
	return strings.Join(out, ",")
}

func TestADatabaseIsRestoredFromItsLatestBackupBeforeAnyAppStarts(t *testing.T) {
	f := two(t) // an older App with a domain, on srv_a
	f.putDB(t, "db_1", "db-main", backup(""))
	f.ms.cut["fp-srv_a"] = true
	f.pass(75 * time.Second)
	if len(f.ms.starts["fp-srv_b"]) != 0 || placed(f, "db_1") != "srv_a" {
		t.Fatal("a database was restored before its member's lease ran out")
	}
	f.pass(10 * time.Second)
	if got := slugs(f, "fp-srv_b"); got != "db-main,web" {
		t.Fatalf("start orders %q, want the database first", got)
	}
	db := f.ms.starts["fp-srv_b"][0].Start
	if db.Restore == nil || db.Restore.BackupID != "brun_1" || string(db.Restore.Request) != restoreRequest {
		t.Fatalf("the database's start order carries no restore: %+v", db.Restore)
	}
	if f.ms.starts["fp-srv_b"][1].Start.Restore != nil {
		t.Fatal("a stateless App was given a restore")
	}
	if placed(f, "db_1") != "srv_b" {
		t.Fatalf("the database is placed on %q", placed(f, "db_1"))
	}
	ev, _ := f.l.Events().Since(0)
	var moved []string
	for _, e := range ev {
		if e.Type == "failed_over" {
			moved = append(moved, string(e.Data))
		}
	}
	if len(moved) != 2 || !strings.Contains(moved[0], `"kind":"database"`) || !strings.Contains(moved[0], `"backupId":"brun_1"`) ||
		!strings.Contains(moved[0], `"backupAt":"2026-03-01T11:45:00Z"`) || strings.Contains(moved[1], "backupId") {
		t.Fatalf("failed_over events %v", moved)
	}
	if strings.Contains(strings.Join(moved, ""), "s3cr3t") {
		t.Fatal("an event carries the destination's secret")
	}
	if mv := f.l.Snapshot().Clusters["clu_1"].Moves; len(mv) != 2 || mv[1].ID != "db_1" || mv[1].BackupID != "brun_1" || mv[1].BackupAt != "2026-03-01T11:45:00Z" {
		t.Fatalf("the snapshot's moves do not name the backup: %+v", mv)
	}
}

func TestADatabaseWithNoBackupInItsBundleWaitsForItsMember(t *testing.T) {
	f := two(t)
	f.putDB(t, "db_1", "db-main", nil)
	f.ms.cut["fp-srv_a"] = true
	f.pass(120 * time.Second)
	if placed(f, "db_1") != "srv_a" || slugs(f, "fp-srv_b") != "web" {
		t.Fatalf("an empty database was started elsewhere: placed %q, starts %q", placed(f, "db_1"), slugs(f, "fp-srv_b"))
	}
}

func TestABackupKeptOnAMemberIsRestoredOnThatMemberOnly(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": {Arch: "amd64", FreeMB: 1024}, "srv_c": roomy("amd64")})
	f.putDB(t, "db_1", "db-main", backup("srv_b"))
	f.ms.cut["fp-srv_a"] = true
	f.pass(85 * time.Second)
	if placed(f, "db_1") != "srv_b" || len(f.ms.starts["fp-srv_c"]) != 0 {
		t.Fatalf("placed on %q; the roomier member cannot read the backup", placed(f, "db_1"))
	}
	// A backup kept on the member that died went with it: the database waits for that member.
	g := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": roomy("amd64"), "srv_c": roomy("amd64")})
	g.putDB(t, "db_1", "db-main", backup("srv_a"))
	g.ms.cut["fp-srv_a"] = true
	g.pass(120 * time.Second)
	if placed(g, "db_1") != "srv_a" || len(g.ms.starts["fp-srv_b"])+len(g.ms.starts["fp-srv_c"]) != 0 {
		t.Fatalf("a database whose backups died with its member was started: %q", placed(g, "db_1"))
	}
}

func TestAMemberThatCannotRestoreIsNeverGivenADatabase(t *testing.T) {
	old := roomy("amd64")
	old.NoRestore = true
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": old, "srv_c": {Arch: "amd64", FreeMB: 1024}})
	f.putDB(t, "db_1", "db-main", backup(""))
	f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", false)
	f.assign(t, "prj_1", "web", "srv_a")
	f.ms.cut["fp-srv_a"] = true
	f.pass(85 * time.Second)
	if placed(f, "db_1") != "srv_c" || slugs(f, "fp-srv_b") != "web" {
		t.Fatalf("database on %q, the old agent was asked to start %q", placed(f, "db_1"), slugs(f, "fp-srv_b"))
	}
}

func TestADatabaseAMemberStartedWithoutRestoringIsNotCountedAsMoved(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": roomy("amd64")})
	f.putDB(t, "db_1", "db-main", backup(""))
	f.ms.unrestored["fp-srv_b"] = true
	f.ms.cut["fp-srv_a"] = true
	f.pass(85 * time.Second)
	if placed(f, "db_1") == "srv_b" {
		t.Fatal("a database that came up empty was recorded as moved")
	}
}

// A leader about to restart buys every member five more minutes. A member cut off in that gap still runs its
// database until that time is up, so nothing of its moves before then: never two writers.
func TestNothingMovesWhileAMemberMayStillLiveOnAnExtendedLease(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": roomy("amd64")})
	f.putDB(t, "db_1", "db-main", backup(""))
	state := t.TempDir()
	mk := func() *leader.Leader {
		return leader.New(leader.Config{Dial: f.ms.dial, Now: f.clk.Now, StateDir: state, Bundles: f.store, PathOK: func() bool { return true }})
	}
	first := mk()
	r := leader.Roster{Cluster: "clu_1", Epoch: 1, Members: []leader.Member{{ID: "srv_a", Fingerprint: "fp-srv_a", Arch: "amd64", FreeMB: 4096}, {ID: "srv_b", Fingerprint: "fp-srv_b", Arch: "amd64", FreeMB: 4096}},
		Placement: f.l.Snapshot().Clusters["clu_1"].Placement}
	if err := first.SetRoster(r); err != nil {
		t.Fatal(err)
	}
	first.Drain(context.Background())
	// The member drops off while the leader restarts.
	f.ms.cut["fp-srv_a"] = true
	f.l = mk()
	f.pass(350 * time.Second)
	if placed(f, "db_1") != "srv_a" || len(f.ms.starts["fp-srv_b"]) != 0 {
		t.Fatalf("the database moved %q while its member's extended lease could still be alive", placed(f, "db_1"))
	}
	f.pass(40 * time.Second)
	if placed(f, "db_1") != "srv_b" {
		t.Fatalf("the database never moved once the extended lease ran out: %q", placed(f, "db_1"))
	}
}

func TestAnExtensionEndsWithTheMembersNextRenewal(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": roomy("amd64")})
	f.putDB(t, "db_1", "db-main", backup(""))
	f.l.Drain(context.Background())
	f.pass(10 * time.Second) // both renew normally: the extension is gone on the member, and here
	f.ms.cut["fp-srv_a"] = true
	f.pass(85 * time.Second)
	if placed(f, "db_1") != "srv_b" {
		t.Fatalf("a renewed member kept its extension: database on %q", placed(f, "db_1"))
	}
}
