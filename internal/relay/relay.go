// Package relay carries streams between the parties of one cluster, and reads none of them (ADR-0044).
package relay

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/hashicorp/yamux"

	"github.com/DeploCloud/deplo-cluster/internal/statefile"
	"github.com/DeploCloud/deplo-cluster/internal/wire"
)

// Limit is how many connection attempts one address may make per window, like the agent link door.
type Limit struct {
	Max    int
	Window time.Duration
}

// DefaultLimit is sized for an office whose servers all relink after a restart.
var DefaultLimit = Limit{Max: 120, Window: time.Minute}

// Config is everything a relay needs to run.
type Config struct {
	// Identity is read per connection, so a renewed certificate counts.
	Identity func() (*wire.Identity, error)
	// StateFile keeps the rosters across a restart; empty keeps them in memory only.
	StateFile string
	Limit     Limit
	Logf      func(format string, args ...any)
}

// Relay is the door and the roster behind it.
type Relay struct {
	cfg     Config
	limiter *limiter

	mu       sync.Mutex
	rosters  map[string]map[string]struct{} // cluster id -> party keys
	sessions map[string]*session            // party key -> its one live connection
	pipes    map[*pipe]struct{}
}

type session struct {
	key   string
	mux   *yamux.Session
	close func()
}

type pipe struct {
	a, b   string
	closeA func()
	closeB func()
}

// New returns a relay holding whatever roster its state file kept.
func New(cfg Config) *Relay {
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	if cfg.Limit.Max == 0 {
		cfg.Limit = DefaultLimit
	}
	r := &Relay{
		cfg:      cfg,
		limiter:  newLimiter(cfg.Limit),
		rosters:  map[string]map[string]struct{}{},
		sessions: map[string]*session{},
		pipes:    map[*pipe]struct{}{},
	}
	r.load()
	return r
}

// SetRoster replaces one cluster's parties (none removes it) and cuts off at once whatever it no longer allows.
func (r *Relay) SetRoster(cluster string, parties []wire.Party) {
	r.mu.Lock()
	if len(parties) == 0 {
		delete(r.rosters, cluster)
	} else {
		set := make(map[string]struct{}, len(parties))
		for _, p := range parties {
			set[p.Key()] = struct{}{}
		}
		r.rosters[cluster] = set
	}
	r.save()
	cut := r.revalidate()
	r.mu.Unlock()
	for _, f := range cut {
		f()
	}
}

// revalidate returns what to close now that the roster changed; the caller holds r.mu and runs the closers after releasing it.
func (r *Relay) revalidate() []func() {
	var cut []func()
	for key, s := range r.sessions {
		if !r.knownLocked(key) {
			delete(r.sessions, key)
			cut = append(cut, s.close)
		}
	}
	for p := range r.pipes {
		if !r.sharesLocked(p.a, p.b) {
			delete(r.pipes, p)
			cut = append(cut, p.closeA, p.closeB)
		}
	}
	return cut
}

func (r *Relay) knownLocked(key string) bool {
	for _, set := range r.rosters {
		if _, ok := set[key]; ok {
			return true
		}
	}
	return false
}

func (r *Relay) sharesLocked(a, b string) bool {
	for _, set := range r.rosters {
		_, okA := set[a]
		_, okB := set[b]
		if okA && okB {
			return true
		}
	}
	return false
}

// Connected is how many parties hold a connection.
func (r *Relay) Connected() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sessions)
}

type state struct {
	Rosters map[string][]string `json:"rosters"`
}

func (r *Relay) save() {
	if r.cfg.StateFile == "" {
		return
	}
	st := state{Rosters: map[string][]string{}}
	for c, set := range r.rosters {
		for k := range set {
			st.Rosters[c] = append(st.Rosters[c], k)
		}
	}
	raw, _ := json.Marshal(st)
	err := os.MkdirAll(filepath.Dir(r.cfg.StateFile), 0o700)
	if err == nil {
		err = statefile.Write(r.cfg.StateFile, raw)
	}
	if err != nil {
		r.cfg.Logf("deplo-cluster relay: keep the roster: %v", err)
	}
}

func (r *Relay) load() {
	if r.cfg.StateFile == "" {
		return
	}
	raw, err := os.ReadFile(r.cfg.StateFile)
	if err != nil {
		return
	}
	var st state
	if json.Unmarshal(raw, &st) != nil {
		return
	}
	for c, keys := range st.Rosters {
		set := map[string]struct{}{}
		for _, k := range keys {
			if _, err := wire.ParseParty(k); err == nil {
				set[k] = struct{}{}
			}
		}
		if len(set) > 0 {
			r.rosters[c] = set
		}
	}
}

var yamuxConfig = func() *yamux.Config {
	c := yamux.DefaultConfig()
	c.LogOutput = io.Discard
	return c
}()

// ServeHTTP is the door: it rate-limits per address, upgrades, and holds one connection per party.
func (r *Relay) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != wire.Path {
		http.NotFound(w, req)
		return
	}
	if ok, retry := r.limiter.allow(clientAddress(req)); !ok {
		w.Header().Set("Retry-After", strconv.Itoa(int(retry.Seconds())+1))
		http.Error(w, "too many connections", http.StatusTooManyRequests)
		return
	}
	ws, err := websocket.Accept(w, req, &websocket.AcceptOptions{Subprotocols: []string{wire.Subprotocol}})
	if err != nil {
		return
	}
	if ws.Subprotocol() != wire.Subprotocol {
		ws.Close(websocket.StatusProtocolError, "speak "+wire.Subprotocol)
		return
	}
	ws.SetReadLimit(1 << 20)
	r.hold(req, ws)
}

func (r *Relay) hold(req *http.Request, ws *websocket.Conn) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nc := websocket.NetConn(ctx, ws, websocket.MessageBinary)
	defer nc.Close()
	id, err := r.cfg.Identity()
	if err != nil {
		r.cfg.Logf("deplo-cluster relay: identity: %v", err)
		return
	}
	tc := tls.Server(nc, id.ServerConfig())
	hctx, hcancel := context.WithTimeout(ctx, wire.HeaderTimeout)
	err = tc.HandshakeContext(hctx)
	hcancel()
	if err != nil {
		return
	}
	party := wire.Party{Fingerprint: wire.PeerFingerprint(tc.ConnectionState()), Role: wire.Role(req.Header.Get(wire.RoleHeader))}
	if claim := req.Header.Get(wire.CertHeader); !party.Valid() || (claim != "" && claim != party.Fingerprint) {
		return
	}
	mux, err := yamux.Server(tc, yamuxConfig)
	if err != nil {
		return
	}
	defer mux.Close()
	s := &session{key: party.Key(), mux: mux, close: func() { mux.Close() }}
	if !r.register(s) {
		return
	}
	defer r.unregister(s)
	for {
		st, err := mux.AcceptStream()
		if err != nil {
			return
		}
		go r.handleStream(s, st)
	}
}

// register keeps one connection per party, the newest; a party the roster does not name is refused.
func (r *Relay) register(s *session) bool {
	r.mu.Lock()
	if !r.knownLocked(s.key) {
		r.mu.Unlock()
		return false
	}
	old := r.sessions[s.key]
	r.sessions[s.key] = s
	r.mu.Unlock()
	if old != nil {
		old.close()
	}
	return true
}

func (r *Relay) unregister(s *session) {
	r.mu.Lock()
	if r.sessions[s.key] == s {
		delete(r.sessions, s.key)
	}
	r.mu.Unlock()
}

func (r *Relay) handleStream(from *session, st *yamux.Stream) {
	defer st.Close()
	st.SetReadDeadline(time.Now().Add(wire.HeaderTimeout))
	verb, arg, err := wire.ReadHeader(st)
	st.SetReadDeadline(time.Time{})
	if err != nil || verb != wire.VerbConnect {
		_ = wire.WriteHeader(st, wire.VerbErr, "expected CONNECT")
		return
	}
	to, err := wire.ParseParty(arg)
	if err != nil || to.Key() == from.key {
		_ = wire.WriteHeader(st, wire.VerbErr, "not a party to connect to")
		return
	}
	r.mu.Lock()
	shared := r.sharesLocked(from.key, to.Key())
	target := r.sessions[to.Key()]
	r.mu.Unlock()
	if !shared {
		_ = wire.WriteHeader(st, wire.VerbErr, "not in the same cluster")
		return
	}
	if target == nil {
		_ = wire.WriteHeader(st, wire.VerbErr, "not connected")
		return
	}
	out, err := target.mux.OpenStream()
	if err != nil {
		_ = wire.WriteHeader(st, wire.VerbErr, "not connected")
		return
	}
	defer out.Close()
	if err := wire.WriteHeader(out, wire.VerbFrom, from.key); err != nil {
		_ = wire.WriteHeader(st, wire.VerbErr, "not connected")
		return
	}
	p := &pipe{a: from.key, b: to.Key(), closeA: func() { cut(st) }, closeB: func() { cut(out) }}
	r.mu.Lock()
	// The roster may have moved between the check and here.
	if !r.sharesLocked(p.a, p.b) {
		r.mu.Unlock()
		_ = wire.WriteHeader(st, wire.VerbErr, "not in the same cluster")
		return
	}
	r.pipes[p] = struct{}{}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.pipes, p)
		r.mu.Unlock()
	}()
	if err := wire.WriteHeader(st, wire.VerbOK, ""); err != nil {
		return
	}
	var wg sync.WaitGroup
	copyHalf := func(dst, src *yamux.Stream) {
		defer wg.Done()
		_, _ = io.Copy(dst, src)
		dst.Close()
	}
	wg.Add(2)
	go copyHalf(out, st)
	go copyHalf(st, out)
	wg.Wait()
}

// cut ends a stream now: its pending reads and writes fail, and the peer sees it close.
func cut(s *yamux.Stream) {
	_ = s.SetDeadline(time.Unix(1, 0))
	_ = s.Close()
}

// clientAddress is who a connection attempt is counted against: the peer, and with it the last X-Forwarded-For
// hop when there is one. The hop alone would let anyone who reaches the door directly (a container on this host)
// write a member's address and spend its budget; the peer cannot be forged.
func clientAddress(req *http.Request) string {
	peer, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		peer = req.RemoteAddr
	}
	if xff := req.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		if hop := strings.TrimSpace(parts[len(parts)-1]); hop != "" {
			return peer + "|" + hop
		}
	}
	return peer
}

type limiter struct {
	limit Limit
	mu    sync.Mutex
	hits  map[string]*bucket
	now   func() time.Time
}

type bucket struct {
	n     int
	reset time.Time
}

func newLimiter(l Limit) *limiter {
	return &limiter{limit: l, hits: map[string]*bucket{}, now: time.Now}
}

func (l *limiter) allow(key string) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if len(l.hits) > 4096 {
		for k, b := range l.hits {
			if now.After(b.reset) {
				delete(l.hits, k)
			}
		}
	}
	b := l.hits[key]
	if b == nil || now.After(b.reset) {
		b = &bucket{reset: now.Add(l.limit.Window)}
		l.hits[key] = b
	}
	b.n++
	if b.n > l.limit.Max {
		return false, b.reset.Sub(now)
	}
	return true, 0
}

// Push is what the control plane sends, through the server agent that supervises the relay, for one cluster.
type Push struct {
	Cluster string       `json:"cluster"`
	Parties []wire.Party `json:"parties"`
}

// Admin serves the relay's local socket: the roster push and a status read. It is never on a TCP port.
func (r *Relay) Admin() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /call/relay.roster", func(w http.ResponseWriter, req *http.Request) {
		var p Push
		if err := json.NewDecoder(io.LimitReader(req.Body, 4<<20)).Decode(&p); err != nil {
			http.Error(w, "unreadable roster", http.StatusBadRequest)
			return
		}
		if p.Cluster == "" || len(p.Cluster) > 64 {
			http.Error(w, "a roster names its cluster", http.StatusBadRequest)
			return
		}
		for _, party := range p.Parties {
			if !party.Valid() {
				http.Error(w, fmt.Sprintf("%q is not a party", party.Key()), http.StatusBadRequest)
				return
			}
		}
		r.SetRoster(p.Cluster, p.Parties)
		_ = json.NewEncoder(w).Encode(map[string]int{"parties": len(p.Parties)})
	})
	mux.HandleFunc("POST /call/relay.status", func(w http.ResponseWriter, _ *http.Request) {
		r.mu.Lock()
		st := map[string]any{"connected": len(r.sessions), "clusters": len(r.rosters)}
		r.mu.Unlock()
		_ = json.NewEncoder(w).Encode(st)
	})
	return mux
}
