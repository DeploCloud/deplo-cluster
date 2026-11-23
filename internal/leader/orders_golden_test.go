package leader_test

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/DeploCloud/deplo-cluster/internal/lease"
)

// Every kind of order this package's tests made the leader send, one example each: deplo-agent runs each through
// its own epoch and leader checks (cluster-rollout.md §7). UPDATE_ORDERS=1 rewrites the file.
const ordersGolden = "testdata/orders.golden.json"

var sent = struct {
	sync.Mutex
	kinds map[string]lease.Order
}{kinds: map[string]lease.Order{}}

type sentOrder struct {
	Kind  string      `json:"kind"`
	Order lease.Order `json:"order"`
}

// orderKind names what an order asks; named says it carries its leader.
func orderKind(o lease.Order) string {
	k := "renewal"
	switch {
	case o.PreVote:
		k = "pre-vote"
	case o.Probe:
		k = "probe"
	case o.Start != nil && o.Start.Restore != nil:
		k = "start-restore"
	case o.Start != nil && o.Start.Promote != nil:
		k = "start-promote"
	case o.Start != nil:
		k = "start"
	case o.Return:
		k = "return"
	case len(o.Keep) > 0:
		k = "keep"
	case o.ExtendSeconds > 0:
		k = "extend"
	case o.Announce > 0:
		k = "announce"
	}
	if o.Succeed {
		k += "+succeed"
	}
	if o.From != "" {
		k += "+named"
	}
	return k
}

// recordSent keeps one order of each kind, with what the agent's checks do not read made plain.
func recordSent(o lease.Order) {
	o.Cluster, o.Member, o.Epoch, o.Placement, o.Heard, o.Gone, o.Line, o.Last, o.Keep, o.Stores = "clu_1", "srv_a", 0, nil, nil, nil, nil, "", nil, nil
	if o.From != "" {
		o.From = "leader"
	}
	if s := o.Start; s != nil {
		o.Start = &lease.Start{Slug: "web", Restore: s.Restore, Promote: s.Promote}
		if s.Restore != nil {
			o.Start.Restore = &lease.Restore{}
		}
		if s.Promote != nil {
			o.Start.Promote = &lease.Promote{}
		}
	}
	k := orderKind(o)
	if k == "keep" || k == "keep+named" {
		o.Keep = []lease.Keep{{}}
	}
	sent.Lock()
	defer sent.Unlock()
	// The smallest example of each kind, so the file does not depend on the order tests run in.
	if had, ok := sent.kinds[k]; !ok || orderText(o) < orderText(had) {
		sent.kinds[k] = o
	}
}

func orderText(o lease.Order) string {
	raw, _ := json.Marshal(o)
	return string(raw)
}

func TestMain(m *testing.M) {
	code := m.Run()
	if f := flag.Lookup("test.run"); code == 0 && (f == nil || f.Value.String() == "") {
		if err := checkOrders(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			code = 1
		}
	}
	os.Exit(code)
}

func checkOrders() error {
	var got []sentOrder
	for k, o := range sent.kinds {
		got = append(got, sentOrder{k, o})
	}
	slices.SortFunc(got, func(a, b sentOrder) int { return compare(a.Kind, b.Kind) })
	raw, _ := json.MarshalIndent(got, "", "  ")
	raw = append(raw, '\n')
	if os.Getenv("UPDATE_ORDERS") == "1" {
		_ = os.MkdirAll(filepath.Dir(ordersGolden), 0o755)
		return os.WriteFile(ordersGolden, raw, 0o644)
	}
	want, err := os.ReadFile(ordersGolden)
	if err != nil || string(want) != string(raw) {
		return fmt.Errorf("the leader sends other kinds of orders than %s says (UPDATE_ORDERS=1 to rewrite it, then copy it to deplo-agent):\n%s", ordersGolden, raw)
	}
	return nil
}

func compare(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
