package events_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/events"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func now() time.Time { return t0 }

func TestEventsKeepTheirOrderAndSinceSkipsWhatWasSeen(t *testing.T) {
	l := events.Open("", 100, now)
	for i := 0; i < 5; i++ {
		l.Append("member_state", "clu_1", map[string]int{"n": i})
	}
	got, gap := l.Since(2)
	if gap || len(got) != 3 || got[0].Seq != 3 || got[2].Seq != 5 {
		t.Fatalf("got %+v gap %v", got, gap)
	}
	if l.Head() != 5 {
		t.Fatalf("head %d", l.Head())
	}
}

func TestEventsSurviveARestartAndKeepCounting(t *testing.T) {
	file := filepath.Join(t.TempDir(), "events.jsonl")
	l := events.Open(file, 100, now)
	l.Append("a", "c", nil)
	l.Append("b", "c", nil)
	again := events.Open(file, 100, now)
	if again.Head() != 2 {
		t.Fatalf("head after restart %d", again.Head())
	}
	if e := again.Append("c", "c", nil); e.Seq != 3 {
		t.Fatalf("next seq %d", e.Seq)
	}
}

func TestPastTheCapTheOldestGoAndTheGapIsSaid(t *testing.T) {
	file := filepath.Join(t.TempDir(), "events.jsonl")
	l := events.Open(file, 3, now)
	for i := 0; i < 10; i++ {
		l.Append("x", "c", nil)
	}
	got, gap := l.Since(0)
	if !gap || got[len(got)-1].Seq != 10 {
		t.Fatalf("got %d events, gap %v", len(got), gap)
	}
	got, gap = l.Since(9)
	if gap || len(got) != 1 {
		t.Fatalf("a recent reader saw gap=%v, %d events", gap, len(got))
	}
	if events.Open(file, 3, now).Head() != 10 {
		t.Fatal("the compacted file lost the head")
	}
}

func TestWaitWakesOnTheNextAppend(t *testing.T) {
	l := events.Open("", 10, now)
	w := l.Wait()
	select {
	case <-w:
		t.Fatal("woke with nothing appended")
	default:
	}
	l.Append("x", "c", nil)
	select {
	case <-w:
	case <-time.After(time.Second):
		t.Fatal("did not wake")
	}
}
