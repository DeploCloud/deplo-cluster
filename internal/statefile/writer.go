package statefile

import (
	"path/filepath"
	"sync"
)

// Writer writes state files in the background, the newest content of each: a caller never waits on the disk, which
// on a busy host can take seconds to sync (eu, 23 Nov 2026: a 6 s fsync held every renewal up).
type Writer struct {
	dir  string
	logf func(string, ...any)
	mu   sync.Mutex
	next map[string][]byte
	busy bool
	// one pass of writes at a time, so a file is never written older after newer
	wmu sync.Mutex
}

// NewWriter writes into dir; logf hears a write that failed.
func NewWriter(dir string, logf func(string, ...any)) *Writer {
	return &Writer{dir: dir, logf: logf, next: map[string][]byte{}}
}

// Put has name written with raw soon, replacing what an earlier Put not yet written would have written.
func (w *Writer) Put(name string, raw []byte) {
	w.mu.Lock()
	w.next[name] = raw
	start := !w.busy
	w.busy = true
	w.mu.Unlock()
	if start {
		go w.drain()
	}
}

func (w *Writer) drain() {
	w.wmu.Lock()
	defer w.wmu.Unlock()
	for {
		w.mu.Lock()
		batch := w.next
		w.next = map[string][]byte{}
		if len(batch) == 0 {
			w.busy = false
			w.mu.Unlock()
			return
		}
		w.mu.Unlock()
		w.write(batch)
	}
}

// Flush returns once everything Put before it is on disk.
func (w *Writer) Flush() {
	w.wmu.Lock()
	defer w.wmu.Unlock()
	w.mu.Lock()
	batch := w.next
	w.next = map[string][]byte{}
	w.mu.Unlock()
	w.write(batch)
}

func (w *Writer) write(batch map[string][]byte) {
	for name, raw := range batch {
		if err := Write(filepath.Join(w.dir, name), raw); err != nil {
			w.logf("deplo-cluster: could not keep %s: %v", name, err)
		}
	}
}
