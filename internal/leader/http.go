package leader

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"
)

// watchPing keeps an idle watch from looking dead to the agent that forwards it.
const watchPing = 20 * time.Second

// Handler is the leader's local socket: POST /call/<method> and GET /watch?since=<seq>.
// Only the agent that supervises the leader can reach it, so it carries no credentials.
func (l *Leader) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /call/leader.roster", func(w http.ResponseWriter, r *http.Request) {
		var req Roster
		if !decode(w, r, &req) {
			return
		}
		if err := l.SetRoster(req); err != nil {
			code := http.StatusBadRequest
			if errors.Is(err, ErrOlderEpoch) {
				code = http.StatusConflict
			}
			http.Error(w, err.Error(), code)
			return
		}
		reply(w, map[string]bool{"ok": true})
	})
	mux.HandleFunc("POST /call/placement.assign", func(w http.ResponseWriter, r *http.Request) {
		var req Assign
		if !decode(w, r, &req) {
			return
		}
		if err := l.Assign(req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply(w, map[string]bool{"ok": true})
	})
	mux.HandleFunc("POST /call/leader.snapshot", func(w http.ResponseWriter, r *http.Request) {
		reply(w, l.Snapshot())
	})
	mux.HandleFunc("POST /call/placement.place", func(w http.ResponseWriter, r *http.Request) {
		var req PlaceRequest
		if !decode(w, r, &req) {
			return
		}
		ans, err := l.Place(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply(w, ans)
	})
	mux.HandleFunc("POST /call/failover.move", func(w http.ResponseWriter, r *http.Request) {
		var req MoveRequest
		if decode(w, r, &req) {
			reply(w, l.Move(req))
		}
	})
	l.bundleRoutes(mux)
	mux.HandleFunc("GET /watch", l.watch)
	return mux
}

func decode(w http.ResponseWriter, r *http.Request, into any) bool {
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(into); err != nil {
		http.Error(w, "unreadable request: "+err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func reply(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// watch streams one JSON line per event after `since`, starting with a hello that names the newest sequence,
// and keeps going until the caller leaves. A buffer that dropped events says so in the hello.
func (l *Leader) watch(w http.ResponseWriter, r *http.Request) {
	since, _ := strconv.ParseUint(r.URL.Query().Get("since"), 10, 64)
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming is not supported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w)
	wake := l.log.Wait()
	head := l.log.Head()
	pending, gap := l.log.Since(since)
	_ = enc.Encode(map[string]any{"hello": true, "head": head, "gap": gap})
	fl.Flush()
	for {
		for _, e := range pending {
			if enc.Encode(e) != nil {
				return
			}
			since = e.Seq
		}
		fl.Flush()
		select {
		case <-r.Context().Done():
			return
		case <-wake:
		case <-time.After(watchPing):
			if enc.Encode(map[string]bool{"ping": true}) != nil {
				return
			}
			fl.Flush()
		}
		wake = l.log.Wait()
		pending, _ = l.log.Since(since)
	}
}
