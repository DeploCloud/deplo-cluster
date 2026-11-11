package leader_test

import "github.com/DeploCloud/deplo-cluster/internal/placement"

func placementWorkload(id string) placement.Workload {
	return placement.Workload{Kind: "app", ID: id, Name: id}
}

func placementFacts(id string, freeMB int) []placement.Member {
	return []placement.Member{{ID: id, Arch: "amd64", FreeMB: freeMB}}
}
