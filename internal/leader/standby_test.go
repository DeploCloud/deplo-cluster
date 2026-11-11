package leader_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/DeploCloud/deplo-cluster/internal/bundles"
	"github.com/DeploCloud/deplo-cluster/internal/leader"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

var promote = &bundles.Promote{Container: "db-main_standby", User: "postgres", Command: []string{"sh", "-c", "pg_ctl promote"}}

// withStandby is a database on srv_a whose standby copy runs on `on`; restore nil is one with no backup yet.
func withStandby(t *testing.T, on string, restore *bundles.Restore, mem map[string]leader.Member) *fo {
	t.Helper()
	f := startFO(t, mem)
	rec, _ := age.ParseX25519Recipient(f.store.PublicKey())
	var out bytes.Buffer
	w, _ := age.Encrypt(&out, rec)
	raw, _ := json.Marshal(bundles.Bundle{V: 1, Kind: "database", Workload: "db_1", DeploymentID: "rev_1",
		Stack: bundles.Stack{Slug: "db-main", ComposeYAML: "services: {}"}, Restore: restore, Promote: promote})
	w.Write(raw)
	w.Close()
	if err := f.store.Put("clu_1", "database", "db_1", "rev_1", out.Bytes(), bundles.Meta{Name: "db-main"}); err != nil {
		t.Fatal(err)
	}
	f.roster(t, on)
	return f
}

// roster is what the control plane pushes: the database on srv_a, its standby on `on`.
func (f *fo) roster(t *testing.T, on string) {
	t.Helper()
	r := leader.Roster{Cluster: "clu_1", Epoch: 1,
		Placement: []lease.Entry{{Kind: "database", ID: "db_1", Member: "srv_a", Slug: "db-main", SingleWriter: true}},
		Standbys:  []lease.Entry{{Kind: "database", ID: "db_1", Member: on, Slug: "db-main", Standby: true}}}
	for _, m := range f.l.Snapshot().Clusters["clu_1"].Members {
		r.Members = append(r.Members, leader.Member{ID: m.ID, Fingerprint: "fp-" + m.ID, Arch: "amd64", FreeMB: 4096})
	}
	if err := f.l.SetRoster(r); err != nil {
		t.Fatal(err)
	}
}

func abc() map[string]leader.Member {
	return map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": roomy("amd64"), "srv_c": roomy("amd64")}
}

func standbyIn(o lease.Order) string {
	for _, e := range o.Placement {
		if e.Standby {
			return e.Member
		}
	}
	return ""
}

func TestAStandbyIsPromotedOnlyAfterItsDatabasesMemberHasFencedItself(t *testing.T) {
	f := withStandby(t, "srv_b", backup(""), abc())
	f.pass(5 * time.Second)
	if o := f.ms.orders["fp-srv_b"]; len(o) == 0 || standbyIn(o[len(o)-1]) != "srv_b" {
		t.Fatalf("the renewal does not tell the member it holds a standby: %+v", o)
	}
	f.ms.cut["fp-srv_a"] = true
	f.pass(75 * time.Second)
	if len(f.ms.starts["fp-srv_b"])+len(f.ms.starts["fp-srv_c"]) != 0 || placed(f, "db_1") != "srv_a" {
		t.Fatal("the standby was promoted before its database's member had fenced itself")
	}
	f.pass(10 * time.Second)
	got := f.ms.starts["fp-srv_b"]
	if len(got) != 1 || len(f.ms.starts["fp-srv_c"]) != 0 {
		t.Fatalf("start orders: srv_b %d, srv_c %d", len(got), len(f.ms.starts["fp-srv_c"]))
	}
	st := got[0].Start
	if st.Promote == nil || st.Promote.Container != "db-main_standby" || st.Promote.User != "postgres" || strings.Join(st.Promote.Command, " ") != "sh -c pg_ctl promote" {
		t.Fatalf("the order does not promote the standby: %+v", st.Promote)
	}
	if st.Restore != nil {
		t.Fatal("a promotion restored a backup too")
	}
	if standbyIn(got[0]) != "" || got[0].Placement[0].Member != "srv_b" {
		t.Fatalf("the order's placement: %+v", got[0].Placement)
	}
	if placed(f, "db_1") != "srv_b" {
		t.Fatalf("the database is placed on %q", placed(f, "db_1"))
	}
	var moved, placement string
	ev, _ := f.l.Events().Since(0)
	for _, e := range ev {
		switch e.Type {
		case "failed_over":
			moved = string(e.Data)
		case "placement":
			placement = string(e.Data)
		}
	}
	if !strings.Contains(moved, `"promoted":true`) || !strings.Contains(moved, `"to":"srv_b"`) || strings.Contains(moved, "backupId") {
		t.Fatalf("failed_over %s", moved)
	}
	if !strings.Contains(placement, `"members":["srv_b"]`) {
		t.Fatalf("placement %s", placement)
	}
	if mv := f.l.Snapshot().Clusters["clu_1"].Moves; len(mv) != 1 || !mv[0].Promoted || mv[0].BackupID != "" {
		t.Fatalf("the snapshot's move does not say it was a promotion: %+v", mv)
	}
	// A roster that has not seen the promotion cannot put a standby back beside the database it became.
	f.roster(t, "srv_b")
	f.pass(5 * time.Second)
	if o := f.ms.orders["fp-srv_b"]; standbyIn(o[len(o)-1]) != "" || placed(f, "db_1") != "srv_b" {
		t.Fatal("a stale roster brought the promoted standby back")
	}
}

func TestAStandbyIsNotPromotedWhileItsDatabasesMemberMayLiveOnAnExtendedLease(t *testing.T) {
	f := withStandby(t, "srv_b", nil, abc())
	f.l.Drain(context.Background())
	f.ms.cut["fp-srv_a"] = true
	f.pass(350 * time.Second)
	if placed(f, "db_1") != "srv_a" || len(f.ms.starts["fp-srv_b"]) != 0 {
		t.Fatal("the standby was promoted while the old member's extended lease could still be alive")
	}
	f.pass(40 * time.Second)
	if placed(f, "db_1") != "srv_b" {
		t.Fatalf("the standby was never promoted: database on %q", placed(f, "db_1"))
	}
}

func TestTheOldMemberOfAPromotedDatabaseIsToldItRunsElsewhereWhenItReturns(t *testing.T) {
	f := withStandby(t, "srv_b", nil, abc())
	f.ms.cut["fp-srv_a"] = true
	f.pass(85 * time.Second)
	f.ms.cut["fp-srv_a"] = false
	f.pass(10 * time.Second)
	o := f.ms.orders["fp-srv_a"]
	last := o[len(o)-1]
	if !last.Return || len(last.Placement) != 1 || last.Placement[0].Member != "srv_b" || last.Placement[0].Slug != "db-main" {
		t.Fatalf("the returning member was not told its database runs elsewhere: %+v", last)
	}
	if placed(f, "db_1") != "srv_b" {
		t.Fatal("the database went back to its old member")
	}
}

func TestAStandbyThatCannotBePromotedFallsBackToTheBackupOnAnotherMember(t *testing.T) {
	f := withStandby(t, "srv_b", backup(""), abc())
	f.ms.unpromoted["fp-srv_b"] = true
	f.ms.cut["fp-srv_a"] = true
	f.pass(85 * time.Second)
	if placed(f, "db_1") != "srv_c" {
		t.Fatalf("database on %q, want the backup restored on srv_c", placed(f, "db_1"))
	}
	if st := f.ms.starts["fp-srv_c"]; len(st) != 1 || st[0].Start.Restore == nil || st[0].Start.Promote != nil {
		t.Fatalf("srv_c's start orders: %+v", st)
	}
	// With no backup either, the database waits for its member: an empty one never starts.
	g := withStandby(t, "srv_b", nil, abc())
	g.ms.unpromoted["fp-srv_b"] = true
	g.ms.cut["fp-srv_a"] = true
	g.pass(120 * time.Second)
	if placed(g, "db_1") != "srv_a" || len(g.ms.starts["fp-srv_c"]) != 0 {
		t.Fatalf("a database with neither standby nor backup moved to %q", placed(g, "db_1"))
	}
}

func TestAStandbyIsNeverMovedOrStartedWhenItsOwnMemberStops(t *testing.T) {
	f := withStandby(t, "srv_b", backup(""), abc())
	f.ms.cut["fp-srv_b"] = true
	f.pass(120 * time.Second)
	if n := len(f.ms.starts["fp-srv_a"]) + len(f.ms.starts["fp-srv_c"]); n != 0 || placed(f, "db_1") != "srv_a" {
		t.Fatalf("%d start orders after the standby's member stopped; database on %q", n, placed(f, "db_1"))
	}
}

func TestPlacementPlaceNeverPicksAnExcludedMember(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": {Arch: "amd64", FreeMB: 1024}})
	req := leader.PlaceRequest{Cluster: "clu_1", Workload: placementWorkload("db_1"), Exclude: []string{"srv_a"}}
	if ans, err := f.l.Place(req); err != nil || ans.Member != "srv_b" {
		t.Fatalf("answer %+v %v", ans, err)
	}
	req.Exclude = []string{"srv_a", "srv_b"}
	if ans, _ := f.l.Place(req); ans.Refusal == nil || ans.Refusal.Reason != "no_member_online" {
		t.Fatalf("answer %+v", ans)
	}
}

func TestAMoveNeverPromotesAStandbyWhileItsDatabasesMemberHoldsItsLeaseAgain(t *testing.T) {
	f := withStandby(t, "srv_b", nil, abc())
	f.ms.unpromoted["fp-srv_b"] = true
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	f.ms.unpromoted["fp-srv_b"] = false
	rec, _ := age.ParseX25519Recipient(f.store.PublicKey())
	var out bytes.Buffer
	w, _ := age.Encrypt(&out, rec)
	raw, _ := json.Marshal(bundles.Bundle{V: 1, Kind: "database", Workload: "db_1", DeploymentID: "rev_1",
		Stack: bundles.Stack{Slug: "db-main", ComposeYAML: "services: {}"}, Restore: backup(""), Promote: promote})
	w.Write(raw)
	w.Close()
	ask := leader.MoveRequest{Cluster: "clu_1", Kind: "database", ID: "db_1", Sealed: base64.StdEncoding.EncodeToString(out.Bytes())}
	if a := f.l.Move(ask); !a.OK {
		t.Fatalf("the move was refused: %+v", a)
	}
	before := len(f.ms.starts["fp-srv_b"])
	f.ms.cut["fp-srv_a"] = false
	f.pass(5 * time.Second)
	if len(f.ms.starts["fp-srv_b"]) != before || len(f.ms.starts["fp-srv_c"]) != 0 || placed(f, "db_1") != "srv_a" {
		t.Fatalf("a second writer was started while the database's member holds its lease again: database on %q", placed(f, "db_1"))
	}
	if failed := events(f, "move_failed"); len(failed) != 1 || !strings.Contains(failed[0], `"reason":"still_answering"`) {
		t.Fatalf("move_failed events %v", failed)
	}
}
