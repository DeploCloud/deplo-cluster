package leader_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/leader"
	"github.com/DeploCloud/deplo-cluster/internal/lease"
	"github.com/DeploCloud/deplo-cluster/internal/wire"
)

// A member's key rotation rides on the roster and is kept on disk: a leader restarted while Deplo is down still
// reaches a member that rotated (ADR-0050).
func TestRotation_theRosterCarriesItsMembersKeyRotationsAcrossARestart(t *testing.T) {
	old, next := strings.Repeat("a", 64), strings.Repeat("b", 64)
	dir := t.TempDir()
	l := leader.New(leader.Config{StateDir: dir})
	roster := leader.Roster{Cluster: "clu_1", Epoch: 1, Members: []leader.Member{{ID: "srv_1", Fingerprint: old}},
		Rotations: []wire.Rotation{{From: old, To: next}}}
	srv := httptest.NewServer(l.Handler())
	defer srv.Close()
	body, _ := json.Marshal(roster)
	resp, err := http.Post(srv.URL+"/call/leader.roster", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var said struct {
		Rotations *int `json:"rotations"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&said)
	resp.Body.Close()
	if said.Rotations == nil || *said.Rotations != 1 {
		t.Fatalf("the roster's answer does not say it kept the rotation: %+v", said)
	}
	want := []wire.Rotation{{From: old, To: next}}
	if got := l.Rotations(); !slices.Equal(got, want) {
		t.Fatalf("rotations: %+v", got)
	}
	if got := leader.New(leader.Config{StateDir: dir}).Rotations(); !slices.Equal(got, want) {
		t.Fatalf("after a restart: %+v", got)
	}
	if err := l.SetRoster(leader.Roster{Cluster: "clu_1", Epoch: 1, Rotations: []wire.Rotation{{From: old, To: "nope"}}}); err == nil {
		t.Fatal("a rotation to something that is not a certificate was taken")
	}
	if err := l.Forget("clu_1"); err != nil {
		t.Fatal(err)
	}
	if got := l.Rotations(); len(got) != 0 {
		t.Fatalf("a forgotten cluster's rotations stayed: %+v", got)
	}
}

// A member that rotated its key is the member it was: its lease is not started over as a stranger's.
func TestRotation_aMemberThatRotatedKeepsItsLease(t *testing.T) {
	old, next, other := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)
	clk := &clock{t: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
	l := leader.New(leader.Config{Now: clk.Now, Dial: func(context.Context, wire.Party) (net.Conn, error) {
		return nil, errors.New("not answering")
	}})
	roster := func(fp string, rs ...wire.Rotation) {
		t.Helper()
		if err := l.SetRoster(leader.Roster{Cluster: "clu_1", Epoch: 1, Members: []leader.Member{{ID: "srv_1", Fingerprint: fp}}, Rotations: rs}); err != nil {
			t.Fatal(err)
		}
	}
	state := func() string { return l.Snapshot().Clusters["clu_1"].Members[0].State }
	roster(old)
	for range 30 {
		clk.Add(lease.RenewEvery)
		l.Tick(context.Background())
	}
	silent := state()
	if silent == string(lease.OK) {
		t.Fatal("a member that never answers is still ok")
	}
	roster(next, wire.Rotation{From: old, To: next})
	if state() != silent {
		t.Fatalf("the rotation started its lease over: %s, was %s", state(), silent)
	}
	roster(other)
	if state() != string(lease.OK) {
		t.Fatalf("a new certificate with no rotation is a new member, got %s", state())
	}
}
