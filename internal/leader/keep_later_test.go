package leader_test

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/bundles"
	"github.com/DeploCloud/deplo-cluster/internal/leader"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

const storeRequest = `{"kind":"BACKUP_KIND_PROJECT","ageRecipient":"age1example","store":{"objectKey":"deplo/team_a/database/db_1/left"},"project":{"slug":"db-main","volumeNames":["deplo-db-main_db-main-data"]}}`

func onDisk(member string) *bundles.Keep {
	return &bundles.Keep{Ref: "dst_2", StoreMember: member, StoreRoot: "/srv/backups", StoreStem: "deplo/team_a/database/db_1/left", Request: json.RawMessage(storeRequest)}
}

func storesTo(f *fo, fp string) []lease.Store {
	return f.lastOrder(fp).Stores
}

func TestADestinationOnAnotherMembersDiskIsReachedThroughThatMemberAndOnlyIt(t *testing.T) {
	f := two(t)
	f.putKept(t, onDisk("srv_b"))
	if got := storesTo(f, "fp-srv_b"); len(got) != 0 {
		t.Fatalf("srv_b accepts %+v while the database runs where it was placed", got)
	}
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	f.ms.cut["fp-srv_a"] = false
	f.pass(5 * time.Second)

	k := f.lastOrder("fp-srv_a").Keep
	if len(k) != 1 || k[0].StoreMember != "srv_b" || k[0].StoreFingerprint != "fp-srv_b" {
		t.Fatalf("keep %+v, want the certificate of the member that holds the store", k)
	}
	at, err := time.Parse(time.RFC3339, k[0].FailedOverAt)
	if err != nil || at.Before(time.Date(2026, 3, 1, 12, 1, 0, 0, time.UTC)) || at.After(time.Date(2026, 3, 1, 12, 1, 35, 0, time.UTC)) {
		t.Fatalf("failedOverAt %q, want when the database started on srv_b", k[0].FailedOverAt)
	}
	got := storesTo(f, "fp-srv_b")
	if len(got) != 1 || got[0].From != "fp-srv_a" || got[0].Root != "/srv/backups" ||
		!regexp.MustCompile(`^deplo/team_a/database/db_1/left-20260301T1201\d\dZ-srv_a\.tar\.gz\.age$`).MatchString(got[0].ObjectKey) {
		t.Fatalf("srv_b accepts %+v, want the one archive srv_a writes", got)
	}
	if !strings.Contains(got[0].ObjectKey, at.Format("20060102T150405Z")) {
		t.Fatalf("the archive %q is not named after the failover at %s", got[0].ObjectKey, k[0].FailedOverAt)
	}
	if s := storesTo(f, "fp-srv_a"); len(s) != 0 {
		t.Fatalf("the returning member accepts %+v", s)
	}

	// Saved and reported: nothing is accepted any more.
	f.ms.returned["fp-srv_a"] = &lease.Returned{Removed: []string{"db-main", "web"},
		Saved: []lease.Saved{{Slug: "db-main", Ref: "dst_2", ObjectKey: got[0].ObjectKey, At: "2026-03-01T13:01:35Z"}}}
	f.pass(10 * time.Second)
	if s := storesTo(f, "fp-srv_b"); len(s) != 0 {
		t.Fatalf("srv_b still accepts %+v after the return", s)
	}
}

func TestAStoreOnAServerOutsideTheClusterIsNeverHandedAMember(t *testing.T) {
	f := two(t)
	// srv_z is a server of the same Deplo, in another cluster or in none.
	f.putKept(t, onDisk("srv_z"))
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	f.ms.cut["fp-srv_a"] = false
	f.pass(5 * time.Second)
	k := f.lastOrder("fp-srv_a").Keep
	if len(k) != 1 || k[0].StoreMember != "srv_z" || k[0].StoreFingerprint != "" {
		t.Fatalf("keep %+v, want no certificate to dial", k)
	}
	for _, fp := range []string{"fp-srv_a", "fp-srv_b"} {
		if s := storesTo(f, fp); len(s) != 0 {
			t.Fatalf("%s accepts %+v", fp, s)
		}
	}
}

// keptBack is srv_a back with its database on srv_b, having said it could not save what it held.
func keptBack(t *testing.T) *fo {
	f := movedWithKeep(t)
	f.clk.Add(time.Hour)
	f.ms.cut["fp-srv_a"] = false
	f.ms.returned["fp-srv_a"] = &lease.Returned{Removed: []string{"db-main", "web"},
		Kept: []lease.Kept{{Slug: "db-main", Ref: "dst_1", Reason: lease.KeptBackupFailed, Error: "the bucket did not answer"}}}
	f.pass(10 * time.Second)
	if got := f.events("member_returned"); len(got) != 1 {
		t.Fatalf("return events %v", got)
	}
	return f
}

func TestWhatAMemberKeptIsHandedItsDestinationUntilItIsSavedAndThatIsToldOnce(t *testing.T) {
	f := keptBack(t)
	f.pass(30 * time.Second)
	o := f.lastOrder("fp-srv_a")
	if o.Return || len(o.Keep) != 1 || o.Keep[0].Slug != "db-main" || string(o.Keep[0].Request) != keepRequest || o.Keep[0].FailedOverAt == "" {
		t.Fatalf("after the return: return %v, keep %+v", o.Return, o.Keep)
	}
	if got := f.events("left_saved"); len(got) != 0 {
		t.Fatalf("saved events %v before anything was saved", got)
	}

	f.ms.later["fp-srv_a"] = &lease.Later{Gone: []string{"db-main"},
		Saved: []lease.Saved{{Slug: "db-main", Ref: "dst_1", ObjectKey: "t/database/db_1/left-20260301T120130Z-srv_a.tar.gz", SizeBytes: 2048, Sha256: "feed", At: "2026-03-01T15:00:00Z"}}}
	f.pass(5 * time.Second)
	got := f.events("left_saved")
	if len(got) != 1 {
		t.Fatalf("saved events %v", got)
	}
	var ev struct {
		Member string           `json:"member"`
		Saved  []map[string]any `json:"saved"`
	}
	if err := json.Unmarshal([]byte(got[0]), &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Member != "srv_a" || len(ev.Saved) != 1 {
		t.Fatalf("event %s", got[0])
	}
	s := ev.Saved[0]
	if s["kind"] != "database" || s["workloadId"] != "db_1" || s["ref"] != "dst_1" || s["objectKey"] != "t/database/db_1/left-20260301T120130Z-srv_a.tar.gz" ||
		s["sizeBytes"] != float64(2048) || s["sha256"] != "feed" || s["at"] != "2026-03-01T15:00:00Z" || s["failedOverAt"] != o.Keep[0].FailedOverAt {
		t.Fatalf("saved %v", s)
	}
	if strings.Contains(got[0], "s3cr3t") {
		t.Fatal("the event carries the destination's secret")
	}

	// The member keeps answering until it hears no more of it: the leader says it once and stops asking.
	f.pass(30 * time.Second)
	if n := len(f.events("left_saved")); n != 1 {
		t.Fatalf("%d saved events", n)
	}
	if n := len(f.events("member_returned")); n != 1 {
		t.Fatalf("%d return events", n)
	}
	if k := f.lastOrder("fp-srv_a").Keep; len(k) != 0 {
		t.Fatalf("the leader still sends %+v", k)
	}
}

func TestDataThatIsGoneUnsavedEndsTheAskingAndSaysNothing(t *testing.T) {
	f := keptBack(t)
	f.ms.later["fp-srv_a"] = &lease.Later{Gone: []string{"db-main"}}
	f.pass(10 * time.Second)
	if got := f.events("left_saved"); len(got) != 0 {
		t.Fatalf("saved events %v", got)
	}
	if k := f.lastOrder("fp-srv_a").Keep; len(k) != 0 {
		t.Fatalf("the leader still sends %+v", k)
	}
}

func TestARestartedLeaderStillAsksAboutWhatAMemberKept(t *testing.T) {
	f := keptBack(t)
	asked := f.lastOrder("fp-srv_a").Keep
	f.l = leader.New(leader.Config{Dial: f.ms.dial, Now: f.clk.Now, StateDir: f.dir, Bundles: f.store, PathOK: func() bool { return true }})
	if err := f.l.SetRoster(leader.Roster{Cluster: "clu_1", Epoch: 1, Members: []leader.Member{
		{ID: "srv_a", Fingerprint: "fp-srv_a", Arch: "amd64", FreeMB: 4096}, {ID: "srv_b", Fingerprint: "fp-srv_b", Arch: "amd64", FreeMB: 4096}},
		Placement: []lease.Entry{{Kind: "database", ID: "db_1", Member: "srv_b", Slug: "db-main", SingleWriter: true}, {Kind: "app", ID: "prj_1", Member: "srv_b", Slug: "web"}}}); err != nil {
		t.Fatal(err)
	}
	f.pass(5 * time.Second)
	o := f.lastOrder("fp-srv_a")
	if o.Return || len(o.Keep) != 1 || len(asked) != 1 || o.Keep[0].FailedOverAt != asked[0].FailedOverAt {
		t.Fatalf("after a restart: return %v, keep %+v (before %+v)", o.Return, o.Keep, asked)
	}
}

func TestKeptDataOfAWorkloadThatWentBackToItsMemberIsAskedAboutWithoutADestination(t *testing.T) {
	f := keptBack(t)
	if err := f.l.Assign(leader.Assign{Cluster: "clu_1", Kind: "database", ID: "db_1", Slug: "db-main", SingleWriter: true, Members: []string{"srv_a"}}); err != nil {
		t.Fatal(err)
	}
	f.pass(5 * time.Second)
	k := f.lastOrder("fp-srv_a").Keep
	if len(k) != 1 || k[0].Slug != "db-main" || len(k[0].Request) != 0 {
		t.Fatalf("keep %+v, want the stack named and nowhere to save a volume that is live again", k)
	}
}
