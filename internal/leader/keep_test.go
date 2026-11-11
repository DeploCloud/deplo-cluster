package leader_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/DeploCloud/deplo-cluster/internal/bundles"
	"github.com/DeploCloud/deplo-cluster/internal/leader"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

const keepRequest = `{"kind":"BACKUP_KIND_PROJECT","s3":{"bucket":"backups","secretKey":"s3cr3t","objectKey":"t/database/db_1/left"},"project":{"slug":"db-main","volumeNames":["deplo-db-main_db-main-data"]}}`

// putKept is putDB for a database whose bundle also says where a returning member saves what it still held.
func (f *fo) putKept(t *testing.T, keep *bundles.Keep) {
	t.Helper()
	rec, _ := age.ParseX25519Recipient(f.store.PublicKey())
	var out bytes.Buffer
	w, _ := age.Encrypt(&out, rec)
	raw, _ := json.Marshal(bundles.Bundle{V: 1, Kind: "database", Workload: "db_1", DeploymentID: "rev_db_1",
		Stack: bundles.Stack{Slug: "db-main", ComposeYAML: "services: {}"}, Restore: backup(""), Keep: keep})
	w.Write(raw)
	w.Close()
	if err := f.store.Put("clu_1", "database", "db_1", "rev_db_1", out.Bytes(), bundles.Meta{Name: "db-main", CreatedAt: "2026-06-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	if err := f.l.Assign(leader.Assign{Cluster: "clu_1", Kind: "database", ID: "db_1", Slug: "db-main", SingleWriter: true, Members: []string{"srv_a"}}); err != nil {
		t.Fatal(err)
	}
}

// movedWithKeep is srv_a silent until its database and its App run on srv_b; the database's bundle holds a keep.
func movedWithKeep(t *testing.T) *fo {
	f := two(t)
	f.putKept(t, &bundles.Keep{Ref: "dst_1", Request: json.RawMessage(keepRequest)})
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	if placed(f, "db_1") != "srv_b" {
		t.Fatalf("the database is on %q", placed(f, "db_1"))
	}
	return f
}

func keepsOf(os []lease.Order) int {
	n := 0
	for _, o := range os {
		n += len(o.Keep)
	}
	return n
}

func TestAReturningMemberIsToldWhereToSaveWhatItStillHoldsWithItsFirstOrder(t *testing.T) {
	f := movedWithKeep(t)
	movedAt := f.clk.Now()
	f.clk.Add(time.Hour)
	f.ms.cut["fp-srv_a"] = false
	f.pass(5 * time.Second)

	f.ms.mu.Lock()
	first := f.ms.orders["fp-srv_a"][len(f.ms.orders["fp-srv_a"])-1]
	toB := keepsOf(f.ms.orders["fp-srv_b"])
	f.ms.mu.Unlock()
	if !first.Return || len(first.Keep) != 1 {
		t.Fatalf("the first order back: return %v, keep %+v", first.Return, first.Keep)
	}
	k := first.Keep[0]
	if k.Slug != "db-main" || k.Ref != "dst_1" || k.StoreMember != "" || string(k.Request) != keepRequest {
		t.Fatalf("keep %+v", k)
	}
	if toB != 0 {
		t.Fatal("a member that never left was handed the destination's credentials")
	}

	f.ms.returned["fp-srv_a"] = &lease.Returned{Removed: []string{"db-main", "web"},
		Saved: []lease.Saved{{Slug: "db-main", Ref: "dst_1", ObjectKey: "t/database/db_1/left-20260301T130135Z-srv_a.tar.gz", SizeBytes: 2048, DecryptedSizeBytes: 4096, Sha256: "feed", At: "2026-03-01T13:01:35Z"}}}
	f.pass(5 * time.Second)
	got := f.events("member_returned")
	if len(got) != 1 {
		t.Fatalf("return events %v", got)
	}
	var ev struct {
		Member  string `json:"member"`
		Stopped []struct {
			Kind string `json:"kind"`
			ID   string `json:"workloadId"`
		} `json:"stopped"`
		Saved []map[string]any `json:"saved"`
		Kept  []map[string]any `json:"kept"`
	}
	if err := json.Unmarshal([]byte(got[0]), &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Member != "srv_a" || len(ev.Stopped) != 2 || len(ev.Saved) != 1 || ev.Kept != nil {
		t.Fatalf("event %s", got[0])
	}
	s := ev.Saved[0]
	if s["kind"] != "database" || s["workloadId"] != "db_1" || s["ref"] != "dst_1" || s["objectKey"] != "t/database/db_1/left-20260301T130135Z-srv_a.tar.gz" ||
		s["sizeBytes"] != float64(2048) || s["decryptedSizeBytes"] != float64(4096) || s["sha256"] != "feed" || s["at"] != "2026-03-01T13:01:35Z" {
		t.Fatalf("saved %v", s)
	}
	at, err := time.Parse(time.RFC3339, s["failedOverAt"].(string))
	if err != nil || at.After(movedAt) || movedAt.Sub(at) > 15*time.Second {
		t.Fatalf("failedOverAt %v, want when the database started on srv_b (by %s)", s["failedOverAt"], movedAt.Format(time.RFC3339))
	}
	if strings.Contains(got[0], "s3cr3t") {
		t.Fatal("the event carries the destination's secret")
	}

	// Told once: the leader stops asking, and stops sending the keep.
	f.ms.mu.Lock()
	before := keepsOf(f.ms.orders["fp-srv_a"])
	f.ms.mu.Unlock()
	f.pass(30 * time.Second)
	f.ms.mu.Lock()
	after := keepsOf(f.ms.orders["fp-srv_a"])
	f.ms.mu.Unlock()
	if f.lastOrder("fp-srv_a").Return || after != before {
		t.Fatalf("the leader keeps sending the keep: %d then %d", before, after)
	}
}

func TestWhatAMemberCouldNotSaveIsReportedWithItsReason(t *testing.T) {
	f := movedWithKeep(t)
	f.ms.cut["fp-srv_a"] = false
	f.ms.returned["fp-srv_a"] = &lease.Returned{Removed: []string{"db-main", "web"},
		Kept: []lease.Kept{{Slug: "db-main", Ref: "dst_1", Reason: lease.KeptBackupFailed, Error: "no space left on device"}}}
	f.pass(10 * time.Second)
	got := f.events("member_returned")
	if len(got) != 1 || !strings.Contains(got[0], `"kept":[{"kind":"database","workloadId":"db_1","ref":"dst_1","reason":"backup_failed","error":"no space left on device"}]`) || strings.Contains(got[0], `"saved"`) {
		t.Fatalf("return events %v", got)
	}
}

func TestAKeepOnAMembersDiskNamesThatMemberAndABundleWithoutOneSendsNothing(t *testing.T) {
	f := two(t)
	f.putKept(t, &bundles.Keep{Ref: "dst_2", StoreMember: "srv_b", Request: json.RawMessage(keepRequest)})
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	f.ms.cut["fp-srv_a"] = false
	f.pass(5 * time.Second)
	if k := f.lastOrder("fp-srv_a").Keep; len(k) != 1 || k[0].StoreMember != "srv_b" {
		t.Fatalf("keep %+v", k)
	}

	f = two(t)
	f.putKept(t, nil)
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	f.ms.cut["fp-srv_a"] = false
	f.pass(5 * time.Second)
	if o := f.lastOrder("fp-srv_a"); !o.Return || len(o.Keep) != 0 {
		t.Fatalf("return %v, keep %+v", o.Return, o.Keep)
	}
}

func TestARestartedLeaderStillKnowsWhatAMemberLeftAndWhen(t *testing.T) {
	f := movedWithKeep(t)
	raw, err := os.ReadFile(filepath.Join(f.dir, "failover.json"))
	if err != nil || strings.Contains(string(raw), "s3cr3t") || strings.Contains(string(raw), "secretKey") {
		t.Fatalf("the leader's state holds the destination's credentials: %s (%v)", raw, err)
	}
	// Deplo's roster has not seen the move yet: it still names srv_a, and the restarted leader lays its own over it.
	if err := f.l.SetRoster(leader.Roster{Cluster: "clu_1", Epoch: 1, Members: []leader.Member{
		{ID: "srv_a", Fingerprint: "fp-srv_a", Arch: "amd64", FreeMB: 4096}, {ID: "srv_b", Fingerprint: "fp-srv_b", Arch: "amd64", FreeMB: 4096}},
		Placement: []lease.Entry{{Kind: "database", ID: "db_1", Member: "srv_a", Slug: "db-main", SingleWriter: true}, {Kind: "app", ID: "prj_1", Member: "srv_a", Slug: "web"}}}); err != nil {
		t.Fatal(err)
	}
	f.l = leader.New(leader.Config{Dial: f.ms.dial, Now: f.clk.Now, StateDir: f.dir, Bundles: f.store, PathOK: func() bool { return true }})
	f.clk.Add(time.Hour)
	f.ms.cut["fp-srv_a"] = false
	f.pass(5 * time.Second)
	if placed(f, "db_1") != "srv_b" {
		t.Fatalf("after a restart the database is on %q", placed(f, "db_1"))
	}
	if o := f.lastOrder("fp-srv_a"); !o.Return || len(o.Keep) != 1 {
		t.Fatalf("after a restart: return %v, keep %+v", o.Return, o.Keep)
	}
	f.ms.returned["fp-srv_a"] = &lease.Returned{Removed: []string{"db-main"},
		Saved: []lease.Saved{{Slug: "db-main", Ref: "dst_1", ObjectKey: "k", At: "2026-03-01T13:01:35Z"}}}
	f.pass(5 * time.Second)
	got := f.events("member_returned")
	if len(got) != 1 || !strings.Contains(got[0], `"failedOverAt":"2026-03-01T12:01:`) {
		t.Fatalf("return events %v", got)
	}
}

func TestAWorkloadThatWentBackToItsMemberIsNotSaved(t *testing.T) {
	f := movedWithKeep(t)
	// Deplo moved the database back onto srv_a before it answered: that volume is the live one again.
	if err := f.l.Assign(leader.Assign{Cluster: "clu_1", Kind: "database", ID: "db_1", Slug: "db-main", SingleWriter: true, Members: []string{"srv_a"}}); err != nil {
		t.Fatal(err)
	}
	f.ms.cut["fp-srv_a"] = false
	f.pass(5 * time.Second)
	if k := f.lastOrder("fp-srv_a").Keep; len(k) != 0 {
		t.Fatalf("keep %+v for a workload placed on the member itself", k)
	}
}
