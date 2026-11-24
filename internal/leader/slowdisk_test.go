package leader_test

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// eu, 23 Nov 2026: a new App's data was copied to every member of one host's disk and a state file took seconds to
// sync; the leader held its lock while it did, renewed nobody for 5.8 s and every member fenced. A write that hangs
// (a pipe nobody reads stands in for the disk) now holds no renewal up.
func TestAStateFileThatHangsHoldsNoRenewalUp(t *testing.T) {
	f := startFO(t, abc())
	// The first roster's write is over before the pipe takes the place of its temporary file.
	f.l.Flush()
	hung := filepath.Join(f.dir, "rosters.json.tmp")
	if err := syscall.Mkfifo(hung, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// The writer waits on the pipe: a reader lets it end before the directory goes.
		if r, err := os.OpenFile(hung, os.O_RDONLY|syscall.O_NONBLOCK, 0); err == nil {
			time.Sleep(50 * time.Millisecond)
			r.Close()
		}
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := f.l.SetRoster(snapshotRoster(f, 1)); err != nil {
			t.Error(err)
		}
		before := f.ms.renewedAt("fp-srv_c")
		f.pass(3 * time.Second)
		if !f.ms.renewedAt("fp-srv_c").After(before) {
			t.Error("no renewal went out while a state file was being written")
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the leader waits on a state file it is writing")
	}
}

func (m *members) renewedAt(fp string) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.renewed[fp]
}

// A line a pass every second would write is written once per change: an older agent's answer to every probe of a
// leader that hears too few to lead.
func TestAnOlderAgentsSilenceOnWhatItRunsIsSaidOnceNotEverySecond(t *testing.T) {
	f := startFO(t, abc())
	f.ms.old["fp-srv_a"] = true
	f.ms.cut["fp-srv_b"], f.ms.cut["fp-srv_c"] = true, true
	f.away = true
	f.pass(30 * time.Second)
	n := 0
	f.logMu.Lock()
	defer f.logMu.Unlock()
	for _, line := range f.logged {
		if strings.Contains(line, "does not say what it runs") {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("said %d times that srv_a does not say what it runs", n)
	}
}

// eu, 24 Nov 2026: a member that answered late was said in the events file while the lab's disk was busy, the
// append waited, and the leader renewed nobody for 7.8 s: every lease ran out. An events file that hangs holds no
// renewal up.
func TestAnEventsFileThatHangsHoldsNoRenewalUp(t *testing.T) {
	f := startFO(t, abc())
	f.pass(3 * time.Second)
	f.l.Events().Flush()
	file := filepath.Join(f.dir, "events.jsonl")
	_ = os.Remove(file)
	if err := syscall.Mkfifo(file, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if r, err := os.OpenFile(file, os.O_RDONLY|syscall.O_NONBLOCK, 0); err == nil {
			time.Sleep(50 * time.Millisecond)
			r.Close()
		}
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		// srv_c falls silent: its state changes are events, and srv_b is still renewed.
		f.ms.cut["fp-srv_c"] = true
		before := f.ms.renewedAt("fp-srv_b")
		f.pass(10 * time.Second)
		if !f.ms.renewedAt("fp-srv_b").After(before) {
			t.Error("no renewal went out while an event was being written")
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the leader waits on the events file")
	}
}
