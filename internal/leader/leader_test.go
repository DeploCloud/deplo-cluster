package leader_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/leader"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
	"github.com/DeploCloud/deplo-cluster/internal/wire"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// members answers a leader's renewals the way an agent does, by fingerprint; absent is a member cut off.
type members struct {
	mu     sync.Mutex
	epoch  map[string]uint64 // fingerprint -> the epoch it holds
	cut    map[string]bool
	orders map[string][]lease.Order
	// starts are the start orders each member was given; refuse makes a member fail them.
	starts map[string][]lease.Order
	refuse map[string]bool
	// unrestored makes a member bring a workload up without restoring its backup, as an older agent would.
	unrestored map[string]bool
	// unpromoted makes a member fail to promote the standby it holds.
	unpromoted map[string]bool
	// startLeft is how long the leader gave each member's last start order.
	startLeft map[string]time.Duration
	// returned is what a member that came back says it removed, once the leader asks.
	returned map[string]*lease.Returned
}

func newMembers() *members {
	return &members{epoch: map[string]uint64{}, cut: map[string]bool{}, orders: map[string][]lease.Order{}, starts: map[string][]lease.Order{}, refuse: map[string]bool{}, unrestored: map[string]bool{}, unpromoted: map[string]bool{}, startLeft: map[string]time.Duration{}, returned: map[string]*lease.Returned{}}
}

func (m *members) dial(ctx context.Context, to wire.Party) (net.Conn, error) {
	var left time.Duration
	if d, ok := ctx.Deadline(); ok {
		left = time.Until(d)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cut[to.Fingerprint] {
		return nil, errors.New("no relay reaches that party")
	}
	a, b := net.Pipe()
	go func() {
		defer b.Close()
		line, err := bufio.NewReader(b).ReadBytes('\n')
		if err != nil {
			return
		}
		var o lease.Order
		_ = json.Unmarshal(line, &o)
		m.mu.Lock()
		held := m.epoch[to.Fingerprint]
		r := lease.Reply{OK: true}
		if o.Epoch < held {
			r = lease.Reply{Error: lease.StaleEpoch, Epoch: held}
		} else {
			m.epoch[to.Fingerprint] = o.Epoch
			m.orders[to.Fingerprint] = append(m.orders[to.Fingerprint], o)
			if o.Return {
				r.Returned = m.returned[to.Fingerprint]
			}
			if o.Start != nil {
				m.starts[to.Fingerprint] = append(m.starts[to.Fingerprint], o)
				m.startLeft[to.Fingerprint] = left
				r.Started = !m.refuse[to.Fingerprint]
				r.Restored = r.Started && o.Start.Restore != nil && !m.unrestored[to.Fingerprint]
				r.Promoted = r.Started && o.Start.Promote != nil
				if !r.Started {
					r = lease.Reply{Error: "no room on this disk"}
				}
				if o.Start.Promote != nil && m.unpromoted[to.Fingerprint] {
					r = lease.Reply{Error: "the standby copy could not be promoted"}
				}
			}
		}
		m.mu.Unlock()
		raw, _ := json.Marshal(r)
		b.Write(append(raw, '\n'))
	}()
	return a, nil
}

func start(t *testing.T, dir string) (*leader.Leader, *clock, *members) {
	t.Helper()
	clk := &clock{t: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
	ms := newMembers()
	l := leader.New(leader.Config{Dial: ms.dial, Now: clk.Now, StateDir: dir})
	return l, clk, ms
}

func roster(epoch uint64, ids ...string) leader.Roster {
	r := leader.Roster{Cluster: "clu_1", Epoch: epoch}
	for _, id := range ids {
		r.Members = append(r.Members, leader.Member{ID: id, Fingerprint: "fp-" + id})
	}
	return r
}

func states(t *testing.T, l *leader.Leader) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, m := range l.Snapshot().Clusters["clu_1"].Members {
		out[m.ID] = m.State
	}
	return out
}

func kinds(l *leader.Leader, since uint64) []string {
	got, _ := l.Events().Since(since)
	var out []string
	for _, e := range got {
		out = append(out, e.Type+":"+string(e.Data))
	}
	return out
}

func TestRenewalCarriesTheEpochAndThePlacement(t *testing.T) {
	l, _, ms := start(t, "")
	r := roster(4, "srv_a")
	r.Placement = []lease.Entry{{Kind: "app", ID: "prj_1", Member: "srv_a", Slug: "web", SingleWriter: true}}
	if err := l.SetRoster(r); err != nil {
		t.Fatal(err)
	}
	l.Tick(context.Background())
	got := ms.orders["fp-srv_a"]
	if len(got) != 1 || got[0].Epoch != 4 || got[0].Member != "srv_a" || got[0].Cluster != "clu_1" || len(got[0].Placement) != 1 || got[0].Placement[0].Slug != "web" {
		t.Fatalf("orders %+v", got)
	}
}

func TestSilentMemberIsNotRespondingAt15sAndFencedAt60sAndBackWhenItAnswers(t *testing.T) {
	l, clk, ms := start(t, "")
	if err := l.SetRoster(roster(1, "srv_a", "srv_b")); err != nil {
		t.Fatal(err)
	}
	ms.cut["fp-srv_b"] = true
	ctx := context.Background()
	for _, step := range []struct {
		advance time.Duration
		want    string
	}{
		{5 * time.Second, "ok"}, {5 * time.Second, "ok"},
		{5 * time.Second, "not_responding"}, // 15 s since the last renewal
		{35 * time.Second, "not_responding"},
		{5 * time.Second, "not_responding"}, // 55 s
		{5 * time.Second, "fenced"},         // 60 s
	} {
		clk.Add(step.advance)
		l.Tick(ctx)
		if got := states(t, l)["srv_b"]; got != step.want {
			t.Fatalf("after %v more: srv_b is %s, want %s", step.advance, got, step.want)
		}
		if got := states(t, l)["srv_a"]; got != "ok" {
			t.Fatalf("the member that answers is %s", got)
		}
	}
	ms.cut["fp-srv_b"] = false
	clk.Add(5 * time.Second)
	l.Tick(ctx)
	if got := states(t, l)["srv_b"]; got != "ok" {
		t.Fatalf("a member that answered again is %s", got)
	}
	want := []string{
		`epoch:{"epoch":1}`,
		`member_state:{"member":"srv_b","state":"not_responding"}`,
		`member_state:{"member":"srv_b","state":"fenced"}`,
		`member_state:{"member":"srv_b","state":"ok"}`,
	}
	if got := kinds(l, 0); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("events %v", got)
	}
}

func TestMemberHoldingANewerEpochMakesTheLeaderStepDown(t *testing.T) {
	l, _, ms := start(t, "")
	ms.epoch["fp-srv_a"] = 9
	if err := l.SetRoster(roster(3, "srv_a")); err != nil {
		t.Fatal(err)
	}
	l.Tick(context.Background())
	if !l.Snapshot().Clusters["clu_1"].Superseded {
		t.Fatal("the leader kept leading with an older epoch")
	}
	if got := kinds(l, 1); len(got) != 1 || got[0] != `superseded:{"epoch":9}` {
		t.Fatalf("events %v", got)
	}
	before := len(ms.orders["fp-srv_a"])
	l.Tick(context.Background())
	if len(ms.orders["fp-srv_a"]) != before {
		t.Fatal("a superseded leader kept sending orders")
	}
}

func TestAnOlderEpochIsRefusedAndTheHeldOneSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	l, _, _ := start(t, dir)
	if err := l.SetRoster(roster(5, "srv_a")); err != nil {
		t.Fatal(err)
	}
	if err := l.SetRoster(roster(4, "srv_a")); !errors.Is(err, leader.ErrOlderEpoch) {
		t.Fatalf("an older epoch was accepted: %v", err)
	}
	again, _, _ := start(t, dir)
	if err := again.SetRoster(roster(2, "srv_a")); !errors.Is(err, leader.ErrOlderEpoch) {
		t.Fatalf("a restarted leader forgot its epoch: %v", err)
	}
	if err := again.SetRoster(roster(6, "srv_a")); err != nil {
		t.Fatal(err)
	}
}

func TestAssignRecordsTheWorkloadAndSaysSo(t *testing.T) {
	l, _, ms := start(t, "")
	if err := l.SetRoster(roster(1, "srv_a")); err != nil {
		t.Fatal(err)
	}
	if err := l.Assign(leader.Assign{Cluster: "clu_1", Kind: "database", ID: "db_1", Slug: "db-main", SingleWriter: true, Members: []string{"srv_a"}}); err != nil {
		t.Fatal(err)
	}
	if err := l.Assign(leader.Assign{Cluster: "other", Kind: "app", ID: "x"}); err == nil {
		t.Fatal("assigned in a cluster this leader does not serve")
	}
	l.Tick(context.Background())
	if p := ms.orders["fp-srv_a"][0].Placement; len(p) != 1 || p[0].ID != "db_1" || !p[0].SingleWriter {
		t.Fatalf("placement %+v", p)
	}
	if got := kinds(l, 1); len(got) != 1 || !strings.HasPrefix(got[0], "placement:") {
		t.Fatalf("events %v", got)
	}
}

func TestWatchReplaysFromASequenceThenStreamsNewEvents(t *testing.T) {
	l, _, _ := start(t, "")
	l.Events().Append("member_state", "clu_1", map[string]string{"member": "a", "state": "ok"})
	l.Events().Append("member_state", "clu_1", map[string]string{"member": "a", "state": "fenced"})
	l.Events().Append("member_state", "clu_1", map[string]string{"member": "a", "state": "ok"})
	srv := httptest.NewServer(l.Handler())
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/watch?since=1", nil)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	read := func() map[string]any {
		if !sc.Scan() {
			t.Fatalf("the stream ended: %v", sc.Err())
		}
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	if h := read(); h["hello"] != true || h["head"].(float64) != 3 || h["gap"] != false {
		t.Fatalf("hello %v", h)
	}
	if e := read(); e["seq"].(float64) != 2 {
		t.Fatalf("first replayed event %v", e)
	}
	if e := read(); e["seq"].(float64) != 3 {
		t.Fatalf("second replayed event %v", e)
	}
	l.Events().Append("epoch", "clu_1", map[string]int{"epoch": 2})
	if e := read(); e["seq"].(float64) != 4 || e["type"] != "epoch" {
		t.Fatalf("live event %v", e)
	}
}
