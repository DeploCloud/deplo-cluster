package leader_test

import (
	"strings"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/leader"
)

// waited is a database that found no room when srv_a went silent, and the picture Deplo has of it: waiting.
// Then srv_a came back and the old leader handed the database back to it, an event Deplo never read.
func waited(t *testing.T) (*fo, leader.Roster) {
	t.Helper()
	f := startFO(t, map[string]leader.Member{"srv_a": roomy("amd64"), "srv_b": {Arch: "amd64", FreeMB: 100}})
	f.putDB(t, "db_1", "db-main", backup(""))
	f.ms.cut["fp-srv_a"] = true
	f.pass(90 * time.Second)
	if placed(f, "db_1") != "" {
		t.Fatalf("the database is on %q, want it waiting for room", placed(f, "db_1"))
	}
	// In Deplo's picture both members have room by now.
	picture := snapshotRoster(f, 2)
	f.ms.cut["fp-srv_a"] = false
	f.pass(10 * time.Second)
	if placed(f, "db_1") != "srv_a" {
		t.Fatalf("the old leader has the database on %q, want it handed back", placed(f, "db_1"))
	}
	f.ms.has["fp-srv_a"] = []string{"db-main"}
	return f, picture
}

func starts(f *fo) string {
	return strings.Trim(slugs(f, "fp-srv_a")+","+slugs(f, "fp-srv_b"), ",")
}

// A leader takes over with a picture that says the database waits for room (spec #176). The member that got
// it back says it has it, so it is that member's again and no second copy is started. Never two writers.
func TestALeaderThatTakesOverHandsWhatWaitsBackToTheMemberThatHasIt(t *testing.T) {
	f, picture := waited(t)
	g := takeOverWith(t, f, picture)
	g.sealDB(t, "db_1", "db-main", backup(""))
	head := g.l.Events().Head()
	g.pass(2 * time.Minute)
	if got := starts(g); got != "" {
		t.Fatalf("two writers: %q was started while srv_a runs the database", got)
	}
	if placed(g, "db_1") != "srv_a" {
		t.Fatalf("the database is on %q", placed(g, "db_1"))
	}
	if got := kinds(g.l, head); len(got) != 1 || !strings.Contains(got[0], `placement:`) || !strings.Contains(got[0], `"members":["srv_a"]`) {
		t.Fatalf("Deplo was told %v", got)
	}
	// Deplo's next roster still says "waiting": it cannot take the database away again.
	if err := g.l.SetRoster(picture); err != nil {
		t.Fatal(err)
	}
	g.pass(time.Minute)
	if placed(g, "db_1") != "srv_a" || starts(g) != "" {
		t.Fatalf("the database is on %q, starts %q", placed(g, "db_1"), starts(g))
	}
}

// The member that has it is cut off from the new leader, on a lease the old one extended. Nothing is started
// until every member has answered or the longest lease is over; then the database comes up from its backup.
func TestALeaderThatTakesOverStartsWhatWaitsOnlyOnceNoMemberCanBeRunningIt(t *testing.T) {
	f, picture := waited(t)
	f.l.Drain(t.Context())
	g := takeOverWith(t, f, picture, "srv_a")
	g.sealDB(t, "db_1", "db-main", backup(""))
	for i := 0; i < 100 && starts(g) == ""; i++ {
		g.pass(5 * time.Second)
	}
	if starts(g) != "db-main" || placed(g, "db_1") != "srv_b" {
		t.Fatalf("starts %q, the database on %q", starts(g), placed(g, "db_1"))
	}
	if end := g.ms.leaseEnd("fp-srv_a"); g.clk.Now().Before(end) {
		t.Fatalf("two writers: the database started on srv_b while srv_a's lease runs for another %s", end.Sub(g.clk.Now()))
	}
}

// An App without a Volume may run twice, so one that waits is tried again on the usual clock.
func TestWhatMayRunTwiceIsStillRetriedAtOnceAfterATakeover(t *testing.T) {
	f := two(t)
	picture := snapshotRoster(f, 2)
	picture.Placement[0].Member = ""
	g := takeOverWith(t, f, picture, "srv_a")
	g.pass(35 * time.Second)
	if placed(g, "prj_1") != "srv_b" {
		t.Fatalf("web is on %q", placed(g, "prj_1"))
	}
}

// A person's move of what the picture says is waiting is held to the same rule.
func TestAMoveOfWhatWaitedBeforeATakeoverWaitsForEveryMemberToo(t *testing.T) {
	f := held(t, both())
	f.pass(10 * time.Second)
	picture := snapshotRoster(f, 2)
	picture.Placement[0].Member = ""
	g := takeOverWith(t, f, picture, "srv_a")
	g.put(t, "prj_3", "files", "2026-01-01T00:00:00Z", false)
	g.pass(375 * time.Second)
	if a := move(g, picked()); a.OK || a.Reason != leader.StillAnswering {
		t.Fatalf("a move of what a cut-off member may still run was taken: %+v", a)
	}
	g.pass(10 * time.Second)
	if a := move(g, picked()); !a.OK {
		t.Fatalf("the move was refused once the lease was surely over: %+v", a)
	}
}
