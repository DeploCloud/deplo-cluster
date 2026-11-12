package lease_test

import (
	"bufio"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

func TestTimingsAreTheSpecs(t *testing.T) {
	if lease.RenewEvery != time.Second || lease.NotRespondingAfter != 3*time.Second || lease.FenceAfter != 60*time.Second {
		t.Fatalf("renew %v, not responding %v, fence %v", lease.RenewEvery, lease.NotRespondingAfter, lease.FenceAfter)
	}
}

func TestStateAfterSilence(t *testing.T) {
	for _, tc := range []struct {
		silent time.Duration
		want   lease.State
	}{
		{0, lease.OK},
		{2999 * time.Millisecond, lease.OK},
		{3 * time.Second, lease.NotResponding},
		{59999 * time.Millisecond, lease.NotResponding},
		{60 * time.Second, lease.Fenced},
		{10 * time.Minute, lease.Fenced},
	} {
		if got := lease.StateAfter(tc.silent); got != tc.want {
			t.Errorf("after %v: %s, want %s", tc.silent, got, tc.want)
		}
	}
}

func TestSendCarriesTheOrderAndReturnsTheReply(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	go func() {
		buf := make([]byte, 4096)
		b.Read(buf)
		b.Write([]byte(`{"ok":false,"error":"stale_epoch","epoch":9}` + "\n"))
	}()
	r, err := lease.Send(a, lease.Order{Cluster: "clu_1", Epoch: 3})
	if err != nil {
		t.Fatal(err)
	}
	if r.OK || r.Error != lease.StaleEpoch || r.Epoch != 9 {
		t.Fatalf("reply %+v", r)
	}
}

func TestLeftNameIsTheFailoverTimeAndTheMember(t *testing.T) {
	at := time.Date(2026, 3, 1, 14, 32, 5, 0, time.FixedZone("cet", 3600))
	if got := lease.LeftName(at, "srv_a", true); got != "-20260301T133205Z-srv_a.tar.gz.age" {
		t.Fatalf("encrypted: %q", got)
	}
	if got := lease.LeftName(at, "srv_a", false); got != "-20260301T133205Z-srv_a.tar.gz" {
		t.Fatalf("plain: %q", got)
	}
}

// deplo-agent reads these exact names (internal/clusterlink/lease): a rename here sends a member nothing.
func TestAKeepAndAStoreAreTheAgentsWireShape(t *testing.T) {
	raw, _ := json.Marshal(lease.Order{Cluster: "clu_1", Member: "srv_a", Placement: []lease.Entry{},
		Keep:   []lease.Keep{{Slug: "db-main", StoreMember: "srv_b", StoreFingerprint: "ab12", FailedOverAt: "2026-03-01T13:32:05Z"}},
		Stores: []lease.Store{{From: "cd34", Root: "/srv/backups", ObjectKey: "k"}}})
	want := `{"cluster":"clu_1","member":"srv_a","epoch":0,"placement":[],` +
		`"keep":[{"slug":"db-main","storeMember":"srv_b","storeFingerprint":"ab12","failedOverAt":"2026-03-01T13:32:05Z"}],` +
		`"stores":[{"from":"cd34","root":"/srv/backups","objectKey":"k"}]}`
	if string(raw) != want {
		t.Fatalf("order %s", raw)
	}
	var r lease.Reply
	if err := json.Unmarshal([]byte(`{"ok":true,"later":{"saved":[{"slug":"db-main","objectKey":"k","sizeBytes":7,"at":"x"}],"gone":["db-main"]}}`), &r); err != nil ||
		r.Later == nil || len(r.Later.Saved) != 1 || r.Later.Saved[0].SizeBytes != 7 || len(r.Later.Gone) != 1 || r.Later.Gone[0] != "db-main" {
		t.Fatalf("reply %+v (%v)", r, err)
	}
}

// deplo-agent reads fenceSeconds and stoppedWithin by these names; a probe renews nothing, so it says no fence.
func TestARenewalSaysHowLongItsLeaseLivesAndAProbeDoesNot(t *testing.T) {
	for _, probe := range []bool{false, true} {
		a, b := net.Pipe()
		got := make(chan string, 1)
		go func() {
			line, _ := bufio.NewReader(b).ReadString('\n')
			got <- line
			b.Write([]byte(`{"ok":true,"stoppedWithin":21}` + "\n"))
		}()
		r, err := lease.Send(a, lease.Order{Cluster: "clu_1", Epoch: 3, Probe: probe})
		a.Close()
		b.Close()
		if err != nil || r.StoppedWithin != 21 {
			t.Fatalf("reply %+v (%v)", r, err)
		}
		if line := <-got; strings.Contains(line, `"fenceSeconds":5`) == probe {
			t.Fatalf("probe %v sent %s", probe, line)
		}
	}
}
