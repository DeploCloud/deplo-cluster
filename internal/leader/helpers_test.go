package leader_test

import (
	"testing"

	"github.com/DeploCloud/deplo-cluster/internal/leader"
	"github.com/DeploCloud/deplo-cluster/internal/placement"
)

func placementWorkload(id string) placement.Workload {
	return placement.Workload{Kind: "app", ID: id, Name: id}
}

func placementFacts(id string, freeMB int) []placement.Member {
	return []placement.Member{{ID: id, Arch: "amd64", FreeMB: freeMB}}
}

// newLeader is leader.New with its state written before the test's directories are removed.
func newLeader(t testing.TB, cfg leader.Config) *leader.Leader {
	l := leader.New(cfg)
	t.Cleanup(l.Flush)
	return l
}
