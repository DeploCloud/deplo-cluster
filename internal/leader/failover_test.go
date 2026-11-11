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

type fo struct {
	l      *leader.Leader
	clk    *clock
	ms     *members
	store  *bundles.Store
	pathOK bool
	dir    string
}

func startFO(t *testing.T, mem map[string]leader.Member) *fo {
	t.Helper()
	f := &fo{pathOK: true}
	f.clk = &clock{t: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
	f.ms = newMembers()
	var err error
	if f.store, err = bundles.Open(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	f.dir = t.TempDir()
	f.l = leader.New(leader.Config{Dial: f.ms.dial, Now: f.clk.Now, StateDir: f.dir, Bundles: f.store, PathOK: func() bool { return f.pathOK }})
	r := leader.Roster{Cluster: "clu_1", Epoch: 1}
	for _, id := range []string{"srv_a", "srv_b", "srv_c"} {
		if m, ok := mem[id]; ok {
			m.ID, m.Fingerprint = id, "fp-"+id
			r.Members = append(r.Members, m)
		}
	}
	r.Placement = nil
	if err := f.l.SetRoster(r); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *fo) put(t *testing.T, id, slug, created string, domain bool) {
	t.Helper()
	b := bundles.Bundle{V: 1, Kind: "app", Workload: id, DeploymentID: "dep_" + id, Stack: bundles.Stack{Slug: slug, ComposeYAML: "services: {}", Env: map[string]string{"K": "v"}}}
	rec, _ := age.ParseX25519Recipient(f.store.PublicKey())
	var out bytes.Buffer
	w, _ := age.Encrypt(&out, rec)
	raw, _ := json.Marshal(b)
	w.Write(raw)
	w.Close()
	if err := f.store.Put("clu_1", "app", id, "dep_"+id, out.Bytes(), bundles.Meta{Name: slug, HasDomain: domain, CreatedAt: created}); err != nil {
		t.Fatal(err)
	}
}

func (f *fo) assign(t *testing.T, id, slug string, members ...string) {
	t.Helper()
	if err := f.l.Assign(leader.Assign{Cluster: "clu_1", Kind: "app", ID: id, Slug: slug, Members: members}); err != nil {
		t.Fatal(err)
	}
}

// pass advances the clock in 5 s renewal steps, running a tick and a failover pass at each.
func (f *fo) pass(d time.Duration) {
	for ; d > 0; d -= 5 * time.Second {
		f.clk.Add(5 * time.Second)
		f.l.Tick(context.Background())
		f.l.Failover(context.Background())
	}
}

func roomy(arch string) leader.Member { return leader.Member{Arch: arch, FreeMB: 4096} }

func two(t *testing.T) *fo {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": roomy("amd64")})
	f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", true)
	f.assign(t, "prj_1", "web", "srv_a")
	return f
}

func placed(f *fo, id string) string {
	for _, e := range f.l.Snapshot().Clusters["clu_1"].Placement {
		if e.ID == id {
			return e.Member
		}
	}
	return "?"
}

func TestNothingMovesBefore81sAndTheWorkMovesAfter(t *testing.T) {
	f := two(t)
	f.ms.cut["fp-srv_a"] = true
	f.pass(75 * time.Second)
	if len(f.ms.starts["fp-srv_b"]) != 0 || placed(f, "prj_1") != "srv_a" {
		t.Fatal("work moved before about 81 s of silence")
	}
	f.pass(10 * time.Second)
	got := f.ms.starts["fp-srv_b"]
	if len(got) != 1 || got[0].Start.Slug != "web" || got[0].Start.Env["K"] != "v" || placed(f, "prj_1") != "srv_b" {
		t.Fatalf("start orders %+v placed %s", got, placed(f, "prj_1"))
	}
	if got[0].Placement[0].Member != "srv_b" {
		t.Fatalf("the order carried the old placement: %+v", got[0].Placement)
	}
	ev, _ := f.l.Events().Since(0)
	var types []string
	for _, e := range ev {
		types = append(types, e.Type)
	}
	if !strings.Contains(strings.Join(types, ","), "failed_over") || !strings.Contains(strings.Join(types, ","), "failover") {
		t.Fatalf("events %v", types)
	}
	f.pass(60 * time.Second)
	if len(f.ms.starts["fp-srv_b"]) != 1 {
		t.Fatal("the same silence moved the work twice")
	}
}

func TestNothingMovesWhenTheLeaderLostItsOwnPath(t *testing.T) {
	f := two(t)
	f.ms.cut["fp-srv_a"] = true
	f.pathOK = false
	f.pass(120 * time.Second)
	if len(f.ms.starts["fp-srv_b"]) != 0 || placed(f, "prj_1") != "srv_a" {
		t.Fatal("a leader without a path moved work")
	}
	f.pathOK = true
	f.pass(5 * time.Second)
	if placed(f, "prj_1") != "srv_b" {
		t.Fatal("the work did not move once the path was back")
	}
}

func TestNothingMovesWhenMoreThanHalfWentSilentTogether(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": roomy("amd64"), "srv_c": roomy("amd64")})
	f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", true)
	f.assign(t, "prj_1", "web", "srv_a")
	f.ms.cut["fp-srv_a"], f.ms.cut["fp-srv_b"] = true, true
	f.pass(200 * time.Second)
	if placed(f, "prj_1") != "srv_a" || len(f.ms.starts["fp-srv_c"]) != 0 {
		t.Fatal("work moved while most of the cluster was silent")
	}
}

func TestOrderIsDatabasesThenDomainsThenTheRestOldestFirst(t *testing.T) {
	es := []lease.Entry{
		{Kind: "app", ID: "plain_old"}, {Kind: "app", ID: "dom_new"}, {Kind: "database", ID: "db_1"},
		{Kind: "app", ID: "dom_old"}, {Kind: "app", ID: "plain_new"},
	}
	meta := map[string]bundles.Meta{
		"app/plain_old": {CreatedAt: "2026-01-01"}, "app/plain_new": {CreatedAt: "2026-03-01"},
		"app/dom_old": {HasDomain: true, CreatedAt: "2026-01-02"}, "app/dom_new": {HasDomain: true, CreatedAt: "2026-02-01"},
		"database/db_1": {CreatedAt: "2026-06-01"},
	}
	var ids []string
	for _, e := range leader.Order(es, meta) {
		ids = append(ids, e.ID)
	}
	if got := strings.Join(ids, ","); got != "db_1,dom_old,dom_new,plain_old,plain_new" {
		t.Fatalf("order %s", got)
	}
}

func TestWhatDoesNotFitWaitsAndIsRetriedEvery30s(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": {Arch: "amd64", FreeMB: 100}})
	f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", true)
	f.assign(t, "prj_1", "web", "srv_a")
	f.ms.cut["fp-srv_a"] = true
	f.pass(85 * time.Second)
	if placed(f, "prj_1") != "" || len(f.ms.starts["fp-srv_b"]) != 0 {
		t.Fatalf("a workload with no room was placed: %q", placed(f, "prj_1"))
	}
	ev, _ := f.l.Events().Since(0)
	var summary string
	for _, e := range ev {
		if e.Type == "failover" {
			summary = string(e.Data)
		}
	}
	if !strings.Contains(summary, `"no_room"`) || !strings.Contains(summary, `"waiting":[{`) {
		t.Fatalf("failover summary %s", summary)
	}
	// Room appears: the roster says srv_b has memory now. The next retry (within 30 s) places it.
	r := leader.Roster{Cluster: "clu_1", Epoch: 1, Members: []leader.Member{
		{ID: "srv_a", Fingerprint: "fp-srv_a", Arch: "amd64", FreeMB: 4096}, {ID: "srv_b", Fingerprint: "fp-srv_b", Arch: "amd64", FreeMB: 4096}},
		Placement: []lease.Entry{{Kind: "app", ID: "prj_1", Slug: "web"}}}
	if err := f.l.SetRoster(r); err != nil {
		t.Fatal(err)
	}
	f.pass(25 * time.Second)
	if placed(f, "prj_1") != "" {
		t.Fatal("retried before 30 s")
	}
	f.pass(10 * time.Second)
	if placed(f, "prj_1") != "srv_b" || len(f.ms.starts["fp-srv_b"]) != 1 {
		t.Fatalf("not placed after the retry: %q", placed(f, "prj_1"))
	}
}

func TestAMemberThatCannotStartItIsSkippedForTheNext(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": roomy("amd64"), "srv_c": {Arch: "amd64", FreeMB: 2048}})
	f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", false)
	f.assign(t, "prj_1", "web", "srv_a")
	f.ms.refuse["fp-srv_b"] = true
	f.ms.cut["fp-srv_a"] = true
	f.pass(85 * time.Second)
	if placed(f, "prj_1") != "srv_c" || len(f.ms.starts["fp-srv_b"]) != 1 {
		t.Fatalf("placed on %q", placed(f, "prj_1"))
	}
}

func TestAWorkloadWithoutABundleOrWithAVolumeStaysPut(t *testing.T) {
	f := two(t)
	f.assign(t, "prj_2", "worker", "srv_a") // no bundle
	if err := f.l.Assign(leader.Assign{Cluster: "clu_1", Kind: "app", ID: "prj_3", Slug: "files", SingleWriter: true, Members: []string{"srv_a"}}); err != nil {
		t.Fatal(err)
	}
	f.put(t, "prj_3", "files", "2026-01-01T00:00:00Z", false)
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	if placed(f, "prj_2") != "srv_a" || placed(f, "prj_3") != "srv_a" || placed(f, "prj_1") != "srv_b" {
		t.Fatalf("placements %s %s %s", placed(f, "prj_1"), placed(f, "prj_2"), placed(f, "prj_3"))
	}
}

func TestARosterThatHasNotSeenTheMoveCannotUndoIt(t *testing.T) {
	f := two(t)
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	stale := leader.Roster{Cluster: "clu_1", Epoch: 1, Members: []leader.Member{{ID: "srv_a", Fingerprint: "fp-srv_a"}, {ID: "srv_b", Fingerprint: "fp-srv_b"}},
		Placement: []lease.Entry{{Kind: "app", ID: "prj_1", Member: "srv_a", Slug: "web"}}}
	if err := f.l.SetRoster(stale); err != nil {
		t.Fatal(err)
	}
	if placed(f, "prj_1") != "srv_b" {
		t.Fatal("a stale roster moved the work back")
	}
	stale.Placement[0].Member = "srv_b"
	_ = f.l.SetRoster(stale)
	stale.Placement[0].Member = "srv_a"
	_ = f.l.SetRoster(stale)
	if placed(f, "prj_1") != "srv_a" {
		t.Fatal("the leader kept overriding a roster that agreed with it")
	}
}

func TestDrainExtendsEveryLeaseBeforeAPlannedRestart(t *testing.T) {
	f := two(t)
	f.l.Drain(context.Background())
	for _, id := range []string{"fp-srv_a", "fp-srv_b"} {
		os := f.ms.orders[id]
		if len(os) == 0 || os[len(os)-1].ExtendSeconds < 120 {
			t.Fatalf("%s was not given more time: %+v", id, os)
		}
	}
}

func TestPlacementPlaceUsesLeaseStateAndGivenFacts(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": roomy("amd64")})
	f.ms.cut["fp-srv_a"] = true
	f.pass(20 * time.Second)
	ans, err := f.l.Place(leader.PlaceRequest{Cluster: "clu_1", Workload: placementWorkload("prj_9")})
	if err != nil || ans.Member != "srv_b" {
		t.Fatalf("answer %+v %v", ans, err)
	}
	ans, _ = f.l.Place(leader.PlaceRequest{Cluster: "clu_1", Workload: placementWorkload("prj_9"), Members: placementFacts("srv_b", 10)})
	if ans.Refusal == nil || ans.Refusal.Reason != "no_room" {
		t.Fatalf("answer %+v", ans)
	}
	if _, err := f.l.Place(leader.PlaceRequest{Cluster: "other"}); err == nil {
		t.Fatal("placed in a cluster this leader does not serve")
	}
}

func (f *fo) putStack(t *testing.T, id string, s bundles.Stack) {
	t.Helper()
	rec, _ := age.ParseX25519Recipient(f.store.PublicKey())
	var out bytes.Buffer
	w, _ := age.Encrypt(&out, rec)
	raw, _ := json.Marshal(bundles.Bundle{V: 1, Kind: "app", Workload: id, DeploymentID: "dep_" + id, Stack: s})
	w.Write(raw)
	w.Close()
	if err := f.store.Put("clu_1", "app", id, "dep_"+id, out.Bytes(), bundles.Meta{Name: s.Slug}); err != nil {
		t.Fatal(err)
	}
}

func TestAMovedAppKeepsItsReadyCheckRestartRuleFilesAndLateServices(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": roomy("amd64")})
	f.putStack(t, "prj_1", bundles.Stack{Slug: "web", ComposeYAML: "services: {}",
		Ready:            json.RawMessage(`{"port":3000,"portRequired":true}`),
		RestartRule:      json.RawMessage(`{"probe":"PROBE_HTTP","port":3000,"path":"/"}`),
		StartAfterReady:  []string{"connector"},
		Mounts:           []bundles.MountFile{{Path: "config/app.toml", Content: "a = 1"}},
		FilesBindSources: []string{"data"}})
	f.assign(t, "prj_1", "web", "srv_a")
	f.ms.cut["fp-srv_a"] = true
	f.pass(85 * time.Second)
	got := f.ms.starts["fp-srv_b"]
	if len(got) != 1 {
		t.Fatalf("start orders %+v", got)
	}
	s := got[0].Start
	if string(s.Ready) != `{"port":3000,"portRequired":true}` || string(s.RestartRule) != `{"probe":"PROBE_HTTP","port":3000,"path":"/"}` {
		t.Fatalf("ready %s restart rule %s", s.Ready, s.RestartRule)
	}
	if len(s.StartAfterReady) != 1 || s.StartAfterReady[0] != "connector" || len(s.Mounts) != 1 || s.Mounts[0].Content != "a = 1" ||
		len(s.FilesBindSources) != 1 || s.FilesBindSources[0] != "data" {
		t.Fatalf("start order %+v", s)
	}
}

func TestAMemberWhoseAgentCannotStartAMovedAppIsNeverChosen(t *testing.T) {
	old := roomy("amd64")
	old.NoFailover = true
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": old, "srv_c": {Arch: "amd64", FreeMB: 1024}})
	f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", true)
	f.assign(t, "prj_1", "web", "srv_a")
	f.ms.cut["fp-srv_a"] = true
	f.pass(85 * time.Second)
	if placed(f, "prj_1") != "srv_c" || len(f.ms.starts["fp-srv_b"]) != 0 {
		t.Fatalf("placed on %q, start orders to the old agent %d", placed(f, "prj_1"), len(f.ms.starts["fp-srv_b"]))
	}
	// A new App is still placed on it by the control plane: only a move needs the newer agent.
	if ans, _ := f.l.Place(leader.PlaceRequest{Cluster: "clu_1", Workload: placementWorkload("prj_9")}); ans.Member != "srv_b" {
		t.Fatalf("answer %+v", ans)
	}
}

func staleRoster(members ...string) leader.Roster {
	r := leader.Roster{Cluster: "clu_1", Epoch: 1, Placement: []lease.Entry{{Kind: "app", ID: "prj_1", Member: "srv_a", Slug: "web"}}}
	for _, id := range members {
		r.Members = append(r.Members, leader.Member{ID: id, Fingerprint: "fp-" + id, Arch: "amd64", FreeMB: 4096})
	}
	return r
}

func TestAMoveStandsHoweverLongTheControlPlaneWasAwayAndTheSnapshotNamesIt(t *testing.T) {
	f := two(t)
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	f.clk.Add(6 * time.Hour)
	if err := f.l.SetRoster(staleRoster("srv_a", "srv_b")); err != nil {
		t.Fatal(err)
	}
	if placed(f, "prj_1") != "srv_b" {
		t.Fatal("a roster that was away for hours moved the work back")
	}
	moves := f.l.Snapshot().Clusters["clu_1"].Moves
	if len(moves) != 1 || moves[0].ID != "prj_1" || moves[0].Member != "srv_b" || moves[0].From != "srv_a" || moves[0].Reason != leader.StoppedAnswering {
		t.Fatalf("moves %+v", moves)
	}
	ev, _ := f.l.Events().Since(0)
	var seq uint64
	for _, e := range ev {
		if e.Type == "failed_over" {
			seq = e.Seq
		}
	}
	if seq == 0 || moves[0].Seq != seq {
		t.Fatalf("the move names event %d, the failed_over event is %d", moves[0].Seq, seq)
	}
	// Once the roster agrees the move is the control plane's to change again.
	agreed := staleRoster("srv_a", "srv_b")
	agreed.Placement[0].Member = "srv_b"
	_ = f.l.SetRoster(agreed)
	if got := f.l.Snapshot().Clusters["clu_1"].Moves; len(got) != 0 {
		t.Fatalf("moves after the roster agreed: %+v", got)
	}
}

func TestAMoveIsForgottenOnceTheWorkloadLeavesTheRoster(t *testing.T) {
	f := two(t)
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	gone := staleRoster("srv_a", "srv_b")
	gone.Placement = nil
	_ = f.l.SetRoster(gone)
	snap := f.l.Snapshot().Clusters["clu_1"]
	if len(snap.Moves) != 0 || len(snap.Placement) != 0 {
		t.Fatalf("a deleted App is still placed: %+v %+v", snap.Moves, snap.Placement)
	}
}

func TestAWaitingWorkloadIsNamedInTheSnapshotAndStaysWhenTheRosterRepeatsIt(t *testing.T) {
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": {Arch: "amd64", FreeMB: 100}})
	f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", true)
	f.assign(t, "prj_1", "web", "srv_a")
	f.ms.cut["fp-srv_a"] = true
	f.pass(85 * time.Second)
	moves := f.l.Snapshot().Clusters["clu_1"].Moves
	if len(moves) != 1 || moves[0].Member != "" || moves[0].From != "srv_a" {
		t.Fatalf("moves %+v", moves)
	}
	r := staleRoster("srv_a", "srv_b")
	r.Placement[0].Member = ""
	_ = f.l.SetRoster(r)
	f.pass(35 * time.Second)
	if placed(f, "prj_1") != "srv_b" {
		t.Fatalf("a waiting workload the roster repeated was not retried: %q", placed(f, "prj_1"))
	}
}

func TestARestartedLeaderStillFailsOverWithoutTheControlPlane(t *testing.T) {
	f := two(t)
	state := t.TempDir()
	first := leader.New(leader.Config{Dial: f.ms.dial, Now: f.clk.Now, StateDir: state, Bundles: f.store, PathOK: func() bool { return true }})
	r := staleRoster("srv_a", "srv_b")
	if err := first.SetRoster(r); err != nil {
		t.Fatal(err)
	}
	// The process is replaced and nobody pushes a roster again.
	f.l = leader.New(leader.Config{Dial: f.ms.dial, Now: f.clk.Now, StateDir: state, Bundles: f.store, PathOK: func() bool { return true }})
	if placed(f, "prj_1") != "srv_a" {
		t.Fatalf("a restarted leader forgot its cluster: %q", placed(f, "prj_1"))
	}
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	if placed(f, "prj_1") != "srv_b" || len(f.ms.starts["fp-srv_b"]) != 1 {
		t.Fatalf("placed on %q after a restart", placed(f, "prj_1"))
	}
}
