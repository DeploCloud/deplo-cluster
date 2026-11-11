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
	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

func threeRoomy(t *testing.T) *fo {
	return startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": roomy("amd64"), "srv_c": roomy("amd64")})
}

// during runs once, while the first start order of the pass is in the member's hands.
func (f *fo) during(what func()) {
	done := false
	f.ms.onStart = func(string, lease.Order) {
		if !done {
			done = true
			what()
		}
	}
}

func startCount(f *fo) int {
	f.ms.mu.Lock()
	defer f.ms.mu.Unlock()
	n := 0
	for _, s := range f.ms.starts {
		n += len(s)
	}
	return n
}

func eventTypes(f *fo) string {
	ev, _ := f.l.Events().Since(0)
	var out []string
	for _, e := range ev {
		out = append(out, e.Type)
	}
	return strings.Join(out, ",")
}

// Never two writers: a member that answers again while the pass is busy with its first workload keeps the rest.
func TestAMemberThatAnswersAgainMidPassKeepsWhatThePassHasNotStarted(t *testing.T) {
	f := threeRoomy(t)
	f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", true)
	f.assign(t, "prj_1", "web", "srv_a")
	f.put(t, "prj_2", "api", "2026-02-01T00:00:00Z", true)
	f.assign(t, "prj_2", "api", "srv_a")
	f.ms.cut["fp-srv_a"] = true
	f.pass(80 * time.Second)
	f.during(func() {
		f.ms.mu.Lock()
		f.ms.cut["fp-srv_a"] = false
		f.ms.mu.Unlock()
		f.l.Tick(context.Background())
	})
	f.pass(5 * time.Second)
	if n := startCount(f); n != 1 || placed(f, "prj_2") != "srv_a" {
		t.Fatalf("%d starts, api on %q: the pass started work of a member that answers again", n, placed(f, "prj_2"))
	}
	f.pass(30 * time.Second)
	if n := startCount(f); n != 1 {
		t.Fatalf("%d starts after the member was back", n)
	}
}

// While a start is under way the workload is shown to the others as placed nowhere: the member it left, back
// in the middle of a long restore, is told neither to run it again nor to remove it.
func TestAStartUnderWayIsShownAsPlacedNowhere(t *testing.T) {
	f := threeRoomy(t)
	f.putDB(t, "db_1", "db-main", backup(""))
	f.ms.cut["fp-srv_a"] = true
	f.pass(80 * time.Second)
	var told string
	f.during(func() {
		f.ms.mu.Lock()
		f.ms.cut["fp-srv_a"] = false
		f.ms.mu.Unlock()
		f.l.Tick(context.Background())
		for _, e := range f.lastOrder("fp-srv_a").Placement {
			if e.ID == "db_1" {
				told = "on " + e.Member
			}
		}
	})
	f.pass(5 * time.Second)
	if told != "on " {
		t.Fatalf("mid-restore the member that came back was told its database is %q", told)
	}
	if placed(f, "db_1") != "srv_b" && placed(f, "db_1") != "srv_c" {
		t.Fatalf("the restored database is on %q", placed(f, "db_1"))
	}
}

func TestAStartThatOutlivesThisLeadersEpochIsNotRecorded(t *testing.T) {
	f := threeRoomy(t)
	f.putDB(t, "db_1", "db-main", backup(""))
	f.ms.cut["fp-srv_a"] = true
	f.pass(80 * time.Second)
	// Deplo names a newer epoch while the restore runs: another leader leads now.
	f.during(func() { f.l.Beat(leader.Beat{Epochs: map[string]uint64{"clu_1": 2}}) })
	f.away = true
	f.pass(5 * time.Second)
	if startCount(f) != 1 {
		t.Fatalf("%d starts", startCount(f))
	}
	if placed(f, "db_1") != "srv_a" || strings.Contains(eventTypes(f), "failed_over") {
		t.Fatalf("a replaced leader recorded its move: database on %q, events %s", placed(f, "db_1"), eventTypes(f))
	}
}

// A second member blips during a long start, so the leader is not sure it leads when it reaches the next
// workload: that one is not stranded, the next pass moves it.
func TestAPassCutShortResumesWithTheRestOfTheDeadMembersWork(t *testing.T) {
	f := threeRoomy(t)
	f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", true)
	f.assign(t, "prj_1", "web", "srv_a")
	f.put(t, "prj_2", "api", "2026-02-01T00:00:00Z", true)
	f.assign(t, "prj_2", "api", "srv_a")
	f.ms.cut["fp-srv_a"] = true
	f.pass(80 * time.Second)
	f.during(func() { f.clk.Add(2 * leader.SureFor) })
	f.pass(5 * time.Second)
	if placed(f, "prj_1") == "srv_a" || placed(f, "prj_2") != "srv_a" {
		t.Fatalf("after the cut-short pass: web on %q, api on %q", placed(f, "prj_1"), placed(f, "prj_2"))
	}
	f.pass(15 * time.Second)
	if placed(f, "prj_2") == "srv_a" {
		t.Fatal("the rest of the dead member's work was never moved")
	}
}

func TestARetryCutShortKeepsWaiting(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": {Arch: "amd64", FreeMB: 100}, "srv_c": {Arch: "amd64", FreeMB: 100}})
	f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", true)
	f.assign(t, "prj_1", "web", "srv_a")
	f.put(t, "prj_2", "api", "2026-02-01T00:00:00Z", true)
	f.assign(t, "prj_2", "api", "srv_a")
	f.ms.cut["fp-srv_a"] = true
	f.pass(85 * time.Second)
	if placed(f, "prj_1") != "" || placed(f, "prj_2") != "" {
		t.Fatalf("placed with no room: %q %q", placed(f, "prj_1"), placed(f, "prj_2"))
	}
	r := snapshotRoster(f, 1)
	for i := range r.Members {
		r.Members[i].FreeMB = 4096
	}
	if err := f.l.SetRoster(r); err != nil {
		t.Fatal(err)
	}
	f.during(func() { f.clk.Add(2 * leader.SureFor) })
	f.pass(35 * time.Second)
	f.pass(45 * time.Second)
	if placed(f, "prj_1") == "" || placed(f, "prj_2") == "" {
		t.Fatalf("a retry cut short was dropped: web on %q, api on %q", placed(f, "prj_1"), placed(f, "prj_2"))
	}
}

// Databases first, also on a retry.
func TestRetriesFollowTheFailoverOrder(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": {Arch: "amd64", FreeMB: 100}, "srv_c": {Arch: "amd64", FreeMB: 100}})
	f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", true)
	f.assign(t, "prj_1", "web", "srv_a")
	f.putDB(t, "db_1", "db-main", backup(""))
	f.ms.cut["fp-srv_a"] = true
	f.pass(85 * time.Second)
	r := snapshotRoster(f, 1)
	for i := range r.Members {
		r.Members[i].FreeMB = 4096
	}
	if err := f.l.SetRoster(r); err != nil {
		t.Fatal(err)
	}
	var order []string
	f.ms.onStart = func(_ string, o lease.Order) { order = append(order, o.Start.Slug) }
	f.pass(35 * time.Second)
	if got := strings.Join(order, ","); got != "db-main,web" {
		t.Fatalf("retried in the order %q, want the database first", got)
	}
}

func (f *fo) putIn(t *testing.T, kind, id, slug, env string, restore *bundles.Restore) {
	t.Helper()
	rec, _ := age.ParseX25519Recipient(f.store.PublicKey())
	var out bytes.Buffer
	w, _ := age.Encrypt(&out, rec)
	raw, _ := json.Marshal(bundles.Bundle{V: 1, Kind: kind, Workload: id, DeploymentID: "dep_" + id,
		Stack: bundles.Stack{Slug: slug, ComposeYAML: "services: {}"}, Restore: restore})
	w.Write(raw)
	w.Close()
	meta := bundles.Meta{Name: slug, Environment: env, HasDomain: kind == "app", CreatedAt: "2026-01-01T00:00:00Z"}
	if err := f.store.Put("clu_1", kind, id, "dep_"+id, out.Bytes(), meta); err != nil {
		t.Fatal(err)
	}
	if err := f.l.Assign(leader.Assign{Cluster: "clu_1", Kind: kind, ID: id, Slug: slug, SingleWriter: kind == "database", Members: []string{"srv_a"}}); err != nil {
		t.Fatal(err)
	}
}

// The database moves first, and its Environment's App follows it to the same member in the same pass.
func TestAnAppFollowsItsEnvironmentsDatabaseWithinOnePass(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": {Arch: "amd64", FreeMB: 1024}, "srv_c": {Arch: "amd64", FreeMB: 8192}})
	// Its backup is on srv_b's disk, so that is where the database is restored.
	f.putIn(t, "database", "db_1", "db-main", "environ_1", backup("srv_b"))
	f.putIn(t, "app", "prj_1", "web", "environ_1", nil)
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	if placed(f, "db_1") != "srv_b" || placed(f, "prj_1") != "srv_b" {
		t.Fatalf("database on %q, its App on %q", placed(f, "db_1"), placed(f, "prj_1"))
	}
}
