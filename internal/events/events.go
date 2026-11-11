// Package events is the leader's buffer of what the control plane has not seen yet (spec #176).
package events

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
	"time"
)

// Event is one thing that happened; Seq grows by one and never repeats while the file lives.
type Event struct {
	Seq     uint64          `json:"seq"`
	At      string          `json:"at"`
	Type    string          `json:"type"`
	Cluster string          `json:"cluster"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Log keeps the last `max` events in memory and in a file, so a restart keeps them.
type Log struct {
	mu     sync.Mutex
	file   string
	max    int
	events []Event
	seq    uint64
	wake   chan struct{}
	now    func() time.Time
}

// Open reads the file (an empty path keeps events in memory only).
func Open(file string, max int, now func() time.Time) *Log {
	l := &Log{file: file, max: max, wake: make(chan struct{}), now: now}
	if file == "" {
		return l
	}
	f, err := os.Open(file)
	if err != nil {
		return l
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		var e Event
		if json.Unmarshal(sc.Bytes(), &e) == nil && e.Seq > l.seq {
			l.events = append(l.events, e)
			l.seq = e.Seq
		}
	}
	return l
}

// Append records an event and wakes every watcher.
func (l *Log) Append(typ, cluster string, data any) Event {
	raw, _ := json.Marshal(data)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	e := Event{Seq: l.seq, At: l.now().UTC().Format(time.RFC3339Nano), Type: typ, Cluster: cluster, Data: raw}
	l.events = append(l.events, e)
	// Trimmed at twice the cap, with or without a file to rewrite.
	trimmed := len(l.events) > 2*l.max
	if trimmed {
		l.events = append([]Event(nil), l.events[len(l.events)-l.max:]...)
	}
	if l.file != "" {
		l.persist(e, trimmed)
	}
	close(l.wake)
	l.wake = make(chan struct{})
	return e
}

// persist appends the line, or rewrites the file when the events were just trimmed.
func (l *Log) persist(e Event, trimmed bool) {
	if trimmed {
		tmp := l.file + ".tmp"
		if f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600); err == nil {
			enc := json.NewEncoder(f)
			for _, ev := range l.events {
				_ = enc.Encode(ev)
			}
			f.Close()
			_ = os.Rename(tmp, l.file)
		}
		return
	}
	f, err := os.OpenFile(l.file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	_ = json.NewEncoder(f).Encode(e)
}

// Head is the sequence of the newest event, 0 when there is none.
func (l *Log) Head() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seq
}

// Since returns every event after seq, oldest first. Gap is true when some were dropped to stay under the cap.
func (l *Log) Since(seq uint64) (out []Event, gap bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.events) > 0 && l.events[0].Seq > seq+1 {
		gap = true
	}
	for _, e := range l.events {
		if e.Seq > seq {
			out = append(out, e)
		}
	}
	return out, gap
}

// Wait returns a channel that closes when the next event is appended.
func (l *Log) Wait() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.wake
}
