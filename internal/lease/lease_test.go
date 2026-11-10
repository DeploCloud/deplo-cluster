package lease_test

import (
	"net"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

func TestTimingsAreTheSpecs(t *testing.T) {
	if lease.RenewEvery != 5*time.Second || lease.NotRespondingAfter != 15*time.Second || lease.FenceAfter != 60*time.Second {
		t.Fatalf("renew %v, not responding %v, fence %v", lease.RenewEvery, lease.NotRespondingAfter, lease.FenceAfter)
	}
}

func TestStateAfterSilence(t *testing.T) {
	for _, tc := range []struct {
		silent time.Duration
		want   lease.State
	}{
		{0, lease.OK},
		{14999 * time.Millisecond, lease.OK},
		{15 * time.Second, lease.NotResponding},
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
