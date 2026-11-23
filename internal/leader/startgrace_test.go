package leader_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/DeploCloud/deplo-cluster/internal/bundles"
	"github.com/DeploCloud/deplo-cluster/internal/leader"
)

// A leader that just started has not heard every member yet: one that is still booting is unknown, not silent
// (deplo-cluster#5). On the lab, a member's agent came up 9 s after its leader, and its web moved.

// coldStart is startFO on three members, but the ones in cut have not answered this leader since it started.
func coldStart(t *testing.T, grace time.Duration, cut ...string) *fo {
	t.Helper()
	f := &fo{t: t, pathOK: true, blind: map[string]bool{}}
	f.clk = &clock{t: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
	f.ms = newMembers()
	f.ms.now = f.clk.Now
	var err error
	if f.store, err = bundles.Open(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	f.dir = t.TempDir()
	f.l = newLeader(t, leader.Config{Dial: f.dial, Now: f.clk.Now, StateDir: f.dir, Bundles: f.store, PathOK: func() bool { return f.pathOK }, StartGrace: grace})
	r := leader.Roster{Cluster: "clu_1", Epoch: 1}
	for _, id := range []string{"srv_a", "srv_b", "srv_c"} {
		r.Members = append(r.Members, leader.Member{ID: id, Fingerprint: "fp-" + id, Arch: "amd64", FreeMB: 4096})
	}
	if err := f.l.SetRoster(r); err != nil {
		t.Fatal(err)
	}
	for _, id := range cut {
		f.ms.cut["fp-"+id] = true
	}
	f.step()
	return f
}

// sealStandby hands the leader withStandby's database bundle; f.roster then places it.
func (f *fo) sealStandby(t *testing.T) {
	t.Helper()
	rec, _ := age.ParseX25519Recipient(f.store.PublicKey())
	var out bytes.Buffer
	w, _ := age.Encrypt(&out, rec)
	raw, _ := json.Marshal(bundles.Bundle{V: 1, Kind: "database", Workload: "db_1", DeploymentID: "rev_1",
		Stack: bundles.Stack{Slug: "db-main", ComposeYAML: "services: {}"}, Restore: backup(""), Promote: promote})
	w.Write(raw)
	w.Close()
	if err := f.store.Put("clu_1", "database", "db_1", "rev_1", out.Bytes(), bundles.Meta{Name: "db-main"}); err != nil {
		t.Fatal(err)
	}
}

func TestALeaderThatJustStartedLeavesTheWorkOfAMemberStillBootingAlone(t *testing.T) {
	f := coldStart(t, 0, "srv_a")
	f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", true)
	f.assign(t, "prj_1", "web", "srv_a")
	f.pass(time.Minute)
	if placed(f, "prj_1") != "srv_a" || startCount(f) != 0 {
		t.Fatalf("a minute after its start the leader moved web, to %q, off a member it never heard", placed(f, "prj_1"))
	}
	// It was only booting: it answers, and nothing of it ever moves.
	f.ms.cut["fp-srv_a"] = false
	f.pass(leader.DefaultStartGrace)
	if placed(f, "prj_1") != "srv_a" || startCount(f) != 0 || len(f.events("failed_over")) != 0 {
		t.Fatalf("web on %q after its member booted, %d starts", placed(f, "prj_1"), startCount(f))
	}
}

func TestAMemberNeverHeardCountsAsSilentOnceTheStartGraceIsOver(t *testing.T) {
	f := coldStart(t, 0, "srv_a")
	f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", true)
	f.assign(t, "prj_1", "web", "srv_a")
	f.pass(leader.DefaultStartGrace - 5*time.Second)
	if placed(f, "prj_1") != "srv_a" {
		t.Fatalf("web moved to %q within the start grace", placed(f, "prj_1"))
	}
	f.pass(15 * time.Second)
	if p := placed(f, "prj_1"); p != "srv_b" && p != "srv_c" {
		t.Fatalf("a member silent since the leader started never had its work moved: web on %q", p)
	}
}

func TestTheStartGraceIsTheLeadersToSet(t *testing.T) {
	for name, c := range map[string]struct {
		grace       time.Duration
		still, gone time.Duration
	}{
		"ten seconds": {10 * time.Second, 8 * time.Second, 10 * time.Second},
		"none":        {-1, 3 * time.Second, 5 * time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			f := coldStart(t, c.grace, "srv_a")
			f.put(t, "prj_1", "web", "2026-01-01T00:00:00Z", true)
			f.assign(t, "prj_1", "web", "srv_a")
			f.pass(c.still)
			if placed(f, "prj_1") != "srv_a" {
				t.Fatalf("web moved to %q after %s", placed(f, "prj_1"), c.still)
			}
			f.pass(c.gone)
			if placed(f, "prj_1") == "srv_a" {
				t.Fatalf("web still on srv_a after %s", c.still+c.gone)
			}
		})
	}
}

// The database of a member this leader has not heard is not taken over either: the member boots, answers, and its
// standby copy is never promoted.
func TestALeaderThatJustStartedPromotesNoStandbyForAMemberStillBooting(t *testing.T) {
	f := coldStart(t, 0, "srv_a")
	f.sealStandby(t)
	f.roster(t, "srv_b")
	f.pass(leader.DefaultStartGrace - 10*time.Second)
	f.ms.cut["fp-srv_a"] = false
	f.pass(5 * time.Minute)
	if placed(f, "db_1") != "srv_a" || len(f.ms.starts["fp-srv_b"]) != 0 {
		t.Fatalf("the standby was promoted for a member that was only booting: database on %q", placed(f, "db_1"))
	}
}
