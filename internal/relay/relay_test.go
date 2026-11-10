package relay_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/party"
	"github.com/DeploCloud/deplo-cluster/internal/relay"
	"github.com/DeploCloud/deplo-cluster/internal/testpki"
	"github.com/DeploCloud/deplo-cluster/internal/wire"
)

type fixture struct {
	t     *testing.T
	ca    *testpki.CA
	relay *relay.Relay
	srv   *httptest.Server
	ctx   context.Context

	stateFile string
}

func newFixture(t *testing.T, limit relay.Limit) *fixture {
	t.Helper()
	ca := testpki.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	self := ca.Leaf(t, "relay")
	state := t.TempDir() + "/rosters.json"
	r := relay.New(relay.Config{
		Identity:  func() (*wire.Identity, error) { return self, nil },
		StateFile: state,
		Limit:     limit,
	})
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return &fixture{t: t, ca: ca, relay: r, srv: srv, ctx: ctx, stateFile: state}
}

type member struct {
	id     *wire.Identity
	party  *party.Party
	got    chan string
	wire   wire.Party
	cancel context.CancelFunc
}

// join starts a party that echoes "<from>:<line>" back on every stream it accepts.
func (f *fixture) join(id *wire.Identity, role wire.Role) *member {
	f.t.Helper()
	m := &member{id: id, got: make(chan string, 8), wire: wire.Party{Fingerprint: id.Fingerprint, Role: role}}
	m.party = party.New(party.Config{
		Role:     role,
		Identity: func() (*wire.Identity, error) { return id, nil },
		Relays:   func() []string { return []string{f.srv.URL} },
		Accept: func(from wire.Party, c net.Conn) {
			defer c.Close()
			buf := make([]byte, 64)
			n, _ := c.Read(buf)
			m.got <- from.Key()
			_, _ = c.Write([]byte("echo:" + string(buf[:n])))
		},
	})
	ctx, cancel := context.WithCancel(f.ctx)
	m.cancel = cancel
	go m.party.Run(ctx)
	return m
}

func (f *fixture) waitConnected(m *member, want int) {
	f.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m.party.Connected() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.t.Fatalf("party connected to %d relays, want %d", m.party.Connected(), want)
}

func exchange(t *testing.T, c net.Conn, line string) string {
	t.Helper()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write([]byte(line)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	return string(buf[:n])
}

func TestStreamBetweenPartiesOfOneClusterIsMutualTLSEndToEnd(t *testing.T) {
	f := newFixture(t, relay.Limit{})
	a, b := f.ca.Leaf(t, "a"), f.ca.Leaf(t, "b")
	ma, mb := f.join(a, wire.RoleMember), f.join(b, wire.RoleLeader)
	f.relay.SetRoster("clu_1", []wire.Party{ma.wire, mb.wire})
	f.waitConnected(ma, 1)
	f.waitConnected(mb, 1)

	c, err := ma.party.Dial(f.ctx, mb.wire)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := exchange(t, c, "hello"); got != "echo:hello" {
		t.Fatalf("got %q", got)
	}
	if from := <-mb.got; from != ma.wire.Key() {
		t.Fatalf("the target saw %q, want %q", from, ma.wire.Key())
	}
}

func TestStreamBetweenClustersIsRefused(t *testing.T) {
	f := newFixture(t, relay.Limit{})
	a, b := f.ca.Leaf(t, "a"), f.ca.Leaf(t, "b")
	ma, mb := f.join(a, wire.RoleMember), f.join(b, wire.RoleMember)
	f.relay.SetRoster("clu_1", []wire.Party{ma.wire})
	f.relay.SetRoster("clu_2", []wire.Party{mb.wire})
	f.waitConnected(ma, 1)
	f.waitConnected(mb, 1)

	c, err := ma.party.Dial(f.ctx, mb.wire)
	if err == nil {
		c.Close()
		t.Fatal("a stream between two clusters was carried")
	}
	if !strings.Contains(err.Error(), "not in the same cluster") {
		t.Fatalf("err = %v", err)
	}
	select {
	case from := <-mb.got:
		t.Fatalf("the other cluster's party was reached by %s", from)
	default:
	}
}

func TestPartyOnTheSameServerKeepsItsRolesApart(t *testing.T) {
	f := newFixture(t, relay.Limit{})
	host, other := f.ca.Leaf(t, "host"), f.ca.Leaf(t, "other")
	leader, memberOnHost := f.join(host, wire.RoleLeader), f.join(host, wire.RoleMember)
	mo := f.join(other, wire.RoleMember)
	// The host leads cluster 1 and belongs to cluster 2; `other` is only in cluster 2.
	f.relay.SetRoster("clu_1", []wire.Party{leader.wire})
	f.relay.SetRoster("clu_2", []wire.Party{memberOnHost.wire, mo.wire})
	f.waitConnected(leader, 1)
	f.waitConnected(memberOnHost, 1)
	f.waitConnected(mo, 1)

	if c, err := mo.party.Dial(f.ctx, leader.wire); err == nil {
		c.Close()
		t.Fatal("a member of cluster 2 reached the leader of cluster 1")
	}
	c, err := mo.party.Dial(f.ctx, memberOnHost.wire)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}

func TestChangingTheRosterCutsTheOldIdentityOffAtOnce(t *testing.T) {
	f := newFixture(t, relay.Limit{})
	a, b := f.ca.Leaf(t, "a"), f.ca.Leaf(t, "b")
	ma, mb := f.join(a, wire.RoleMember), f.join(b, wire.RoleMember)
	f.relay.SetRoster("clu_1", []wire.Party{ma.wire, mb.wire})
	f.waitConnected(ma, 1)
	f.waitConnected(mb, 1)
	c, err := ma.party.Dial(f.ctx, mb.wire)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if exchange(t, c, "one") != "echo:one" {
		t.Fatal("no echo before the change")
	}

	// b rotates its certificate: the roster names the new one and the old identity goes.
	rotated := f.ca.Leaf(t, "b")
	mr := wire.Party{Fingerprint: rotated.Fingerprint, Role: wire.RoleMember}
	f.relay.SetRoster("clu_1", []wire.Party{ma.wire, mr})

	f.waitConnected(mb, 0)
	if f.relay.Connected() > 1 {
		t.Fatalf("the relay still holds %d connections", f.relay.Connected())
	}
	c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Write([]byte("two")); err == nil {
		if _, err := c.Read(make([]byte, 8)); err == nil {
			t.Fatal("the old identity's stream survived the roster change")
		}
	}
	// And it cannot come back on its old certificate.
	time.Sleep(50 * time.Millisecond)
	if f.relay.Connected() != 1 {
		t.Fatalf("connected = %d after the old identity retried", f.relay.Connected())
	}
}

func TestRemovingAPartyFromAClusterEndsItsStreamsButNotItsOtherCluster(t *testing.T) {
	f := newFixture(t, relay.Limit{})
	a, b := f.ca.Leaf(t, "a"), f.ca.Leaf(t, "b")
	ma, mb := f.join(a, wire.RoleMember), f.join(b, wire.RoleMember)
	f.relay.SetRoster("clu_1", []wire.Party{ma.wire, mb.wire})
	f.relay.SetRoster("clu_2", []wire.Party{mb.wire})
	f.waitConnected(ma, 1)
	f.waitConnected(mb, 1)
	c, err := ma.party.Dial(f.ctx, mb.wire)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	f.relay.SetRoster("clu_1", []wire.Party{ma.wire})
	c.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = c.Write([]byte("x"))
	if _, err := c.Read(make([]byte, 8)); err == nil {
		t.Fatal("the stream outlived the pairing")
	}
	f.waitConnected(mb, 1) // still held: it is in cluster 2
}

func TestPartyOutsideEveryRosterIsRefused(t *testing.T) {
	f := newFixture(t, relay.Limit{})
	stranger := f.join(f.ca.Leaf(t, "nobody"), wire.RoleMember)
	time.Sleep(300 * time.Millisecond)
	if stranger.party.Connected() != 0 || f.relay.Connected() != 0 {
		t.Fatal("a party no cluster names was kept")
	}
}

func TestCertificateFromAnotherCAIsRefused(t *testing.T) {
	f := newFixture(t, relay.Limit{})
	id := testpki.Foreign(t, f.ca)
	m := f.join(id, wire.RoleMember)
	f.relay.SetRoster("clu_1", []wire.Party{m.wire})
	time.Sleep(300 * time.Millisecond)
	if f.relay.Connected() != 0 {
		t.Fatal("a certificate from another CA was accepted")
	}
}

func TestExpiredCertificateIsRefused(t *testing.T) {
	f := newFixture(t, relay.Limit{})
	m := f.join(f.ca.Expired(t, "old"), wire.RoleMember)
	f.relay.SetRoster("clu_1", []wire.Party{m.wire})
	time.Sleep(300 * time.Millisecond)
	if f.relay.Connected() != 0 {
		t.Fatal("an expired certificate was accepted")
	}
}

func TestNewestConnectionOfAPartyReplacesTheOlder(t *testing.T) {
	f := newFixture(t, relay.Limit{})
	a := f.ca.Leaf(t, "a")
	first := f.join(a, wire.RoleMember)
	f.relay.SetRoster("clu_1", []wire.Party{first.wire})
	f.waitConnected(first, 1)
	second := f.join(a, wire.RoleMember)
	f.waitConnected(second, 1)
	if f.relay.Connected() != 1 {
		t.Fatalf("the relay holds %d connections for one party", f.relay.Connected())
	}
}

func TestRelayRateLimitsPerAddress(t *testing.T) {
	f := newFixture(t, relay.Limit{Max: 3, Window: time.Minute})
	var last int
	for i := 0; i < 5; i++ {
		resp, err := http.Get(f.srv.URL + wire.Path)
		if err != nil {
			t.Fatal(err)
		}
		last = resp.StatusCode
		resp.Body.Close()
		if i == 4 && resp.Header.Get("Retry-After") == "" {
			t.Fatal("no Retry-After on the refusal")
		}
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("status %d after the limit, want 429", last)
	}
	// Another address behind the proxy has its own budget.
	req, _ := http.NewRequest(http.MethodGet, f.srv.URL+wire.Path, nil)
	req.Header.Set("X-Forwarded-For", "203.0.113.9")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		t.Fatal("one address's attempts counted against another")
	}
}

func TestOnlyTheRelayPathAnswers(t *testing.T) {
	f := newFixture(t, relay.Limit{})
	resp, err := http.Get(f.srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d, want 404", resp.StatusCode)
	}
}

func TestRosterSurvivesARestart(t *testing.T) {
	f := newFixture(t, relay.Limit{})
	a := f.ca.Leaf(t, "a")
	pa := wire.Party{Fingerprint: a.Fingerprint, Role: wire.RoleMember}
	f.relay.SetRoster("clu_1", []wire.Party{pa})

	self := f.ca.Leaf(t, "relay")
	again := relay.New(relay.Config{Identity: func() (*wire.Identity, error) { return self, nil }, StateFile: f.stateFile})
	srv := httptest.NewServer(again)
	defer srv.Close()
	p := party.New(party.Config{
		Role:     wire.RoleMember,
		Identity: func() (*wire.Identity, error) { return a, nil },
		Relays:   func() []string { return []string{srv.URL} },
	})
	go p.Run(f.ctx)
	deadline := time.Now().Add(5 * time.Second)
	for p.Connected() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if p.Connected() != 1 {
		t.Fatal("a restarted relay forgot its roster")
	}
}

func TestAdminRosterPushValidates(t *testing.T) {
	f := newFixture(t, relay.Limit{})
	good := wire.Party{Fingerprint: f.ca.Leaf(t, "a").Fingerprint, Role: wire.RoleMember}
	cases := []struct {
		name, body string
		status     int
	}{
		{"ok", `{"cluster":"clu_1","parties":[{"fingerprint":"` + good.Fingerprint + `","role":"member"}]}`, 200},
		{"empty removes", `{"cluster":"clu_1","parties":[]}`, 200},
		{"no cluster", `{"parties":[]}`, 400},
		{"bad fingerprint", `{"cluster":"c","parties":[{"fingerprint":"abc","role":"member"}]}`, 400},
		{"bad role", `{"cluster":"c","parties":[{"fingerprint":"` + good.Fingerprint + `","role":"root"}]}`, 400},
		{"not json", `nope`, 400},
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		f.relay.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/call/relay.roster", strings.NewReader(c.body)))
		if rec.Code != c.status {
			t.Errorf("%s: status %d, want %d (%s)", c.name, rec.Code, c.status, rec.Body.String())
		}
	}
}

func TestValidateListenRefusesWhatOutsidersCouldReach(t *testing.T) {
	ok := []string{"127.0.0.1:3002", "172.17.0.1:3002", "10.0.0.5:3002", "[::1]:3002", "192.168.1.2:80"}
	bad := []string{"0.0.0.0:3002", ":3002", "[::]:3002", "8.8.8.8:3002", "203.0.113.4:3002", "example.com:3002", "nope"}
	for _, a := range ok {
		if err := relay.ValidateListen(a); err != nil {
			t.Errorf("%s refused: %v", a, err)
		}
	}
	for _, a := range bad {
		if err := relay.ValidateListen(a); err == nil {
			t.Errorf("%s accepted", a)
		}
	}
}
