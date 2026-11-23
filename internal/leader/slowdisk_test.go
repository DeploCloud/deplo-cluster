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
