// Package placement is the one rule that picks a cluster member for a workload (spec #176).
package placement

import "sort"

// MinMB is the least memory a workload is assumed to need.
const MinMB = 256

// Reason is why no member was chosen.
type Reason string

const (
	NoMemberOnline Reason = "no_member_online"
	NoArchitecture Reason = "no_architecture"
	NoImage        Reason = "no_image"
	NoRoom         Reason = "no_room"
)

// Workload is what is being placed.
type Workload struct {
	Kind        string `json:"kind"`
	ID          string `json:"id"`
	Name        string `json:"name"`
	Environment string `json:"environment"`
	// Image and Archs left empty mean not built yet, or any: those steps pass.
	Image   string   `json:"image"`
	Archs   []string `json:"archs"`
	LimitMB int      `json:"limitMb"`
	PeakMB  int      `json:"peakMb"`
}

// Member is what the leader knows about one server of the cluster.
type Member struct {
	ID           string   `json:"id"`
	Online       bool     `json:"online"`
	Leased       bool     `json:"leased"`
	Emptying     bool     `json:"emptying"`
	Arch         string   `json:"arch"`
	Images       []string `json:"images"`
	FreeMB       int      `json:"freeMb"`
	Workloads    int      `json:"workloads"`
	DatabaseEnvs []string `json:"databaseEnvs"`
	// Placed lists the ids of the workloads (a replica each) this member runs.
	Placed []string `json:"placed"`
}

// Refusal is the structured answer when nothing fits; the control plane writes the sentence.
type Refusal struct {
	Reason   Reason `json:"reason"`
	NeededMB int    `json:"neededMb"`
	Cluster  string `json:"cluster"`
	Workload string `json:"workload"`
}

// Answer is a member or a refusal.
type Answer struct {
	Member  string   `json:"member,omitempty"`
	Refusal *Refusal `json:"refusal,omitempty"`
	// Members answers a request for several replicas, one per ordinal; empty is one still waiting for room.
	Members []string `json:"members,omitempty"`
}

// Needed is the memory limit, else the 24-hour peak, never below MinMB.
func Needed(w Workload) int {
	n := w.LimitMB
	if n <= 0 {
		n = w.PeakMB
	}
	if n < MinMB {
		n = MinMB
	}
	return n
}

func has(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

// Place picks the member for w: eligible members, then database of its Environment, no sibling, most free after placing, fewest workloads.
func Place(cluster string, w Workload, members []Member) Answer {
	need := Needed(w)
	refuse := func(r Reason) Answer {
		return Answer{Refusal: &Refusal{Reason: r, NeededMB: need, Cluster: cluster, Workload: w.ID}}
	}
	var live, arch, image, fit []Member
	for _, m := range members {
		if m.Online && m.Leased && !m.Emptying {
			live = append(live, m)
		}
	}
	for _, m := range live {
		if len(w.Archs) == 0 || has(w.Archs, m.Arch) {
			arch = append(arch, m)
		}
	}
	for _, m := range arch {
		if w.Image == "" || has(m.Images, w.Image) {
			image = append(image, m)
		}
	}
	for _, m := range image {
		if m.FreeMB >= need {
			fit = append(fit, m)
		}
	}
	switch {
	case len(live) == 0:
		return refuse(NoMemberOnline)
	case len(arch) == 0:
		return refuse(NoArchitecture)
	case len(image) == 0:
		return refuse(NoImage)
	case len(fit) == 0:
		return refuse(NoRoom)
	}
	sort.SliceStable(fit, func(i, j int) bool {
		a, b := fit[i], fit[j]
		if da, db := has(a.DatabaseEnvs, w.Environment), has(b.DatabaseEnvs, w.Environment); da != db {
			return da
		}
		if sa, sb := has(a.Placed, w.ID), has(b.Placed, w.ID); sa != sb {
			return !sa
		}
		if a.FreeMB != b.FreeMB {
			return a.FreeMB > b.FreeMB
		}
		if a.Workloads != b.Workloads {
			return a.Workloads < b.Workloads
		}
		return a.ID < b.ID
	})
	return Answer{Member: fit[0].ID}
}

// Spread places n replicas of w, each on a member running none of the others while one is left. A replica stays
// where current says it runs, and one doubled up on a member moves only to a member that has none.
func Spread(cluster string, w Workload, members []Member, n int, current []string) ([]string, *Refusal) {
	facts := make([]Member, len(members))
	at := map[string]int{}
	for i, m := range members {
		m.Placed = append([]string(nil), m.Placed...)
		for j := 0; j < len(m.Placed); j++ {
			if m.Placed[j] == w.ID {
				m.Placed = append(m.Placed[:j], m.Placed[j+1:]...)
				j--
			}
		}
		facts[i], at[m.ID] = m, i
	}
	runs := func(i int) string {
		if i < len(current) {
			if j, ok := at[current[i]]; ok && facts[j].Online && facts[j].Leased && !facts[j].Emptying {
				return current[i]
			}
		}
		return ""
	}
	out := make([]string, n)
	take := func(i int, id string) {
		out[i] = id
		facts[at[id]].Placed = append(facts[at[id]].Placed, w.ID)
	}
	for i := range out {
		if id := runs(i); id != "" && !has(facts[at[id]].Placed, w.ID) {
			take(i, id)
		}
	}
	for i := range out {
		if out[i] != "" {
			continue
		}
		var none []Member
		for _, m := range facts {
			if !has(m.Placed, w.ID) {
				none = append(none, m)
			}
		}
		ans := Place(cluster, w, none)
		if ans.Member == "" {
			if id := runs(i); id != "" {
				take(i, id)
				continue
			}
			if ans = Place(cluster, w, facts); ans.Refusal != nil {
				return nil, ans.Refusal
			}
		}
		take(i, ans.Member)
		facts[at[ans.Member]].FreeMB -= Needed(w)
	}
	return out, nil
}
