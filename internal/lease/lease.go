// Package lease holds the timings and the order a leader sends each member, a contract with deplo-agent (spec #176).
package lease

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"time"
)

const (
	// RenewEvery is how often the leader renews each member's lease.
	RenewEvery = 5 * time.Second
	// NotRespondingAfter is how long without a renewal before the cluster card says so.
	NotRespondingAfter = 15 * time.Second
	// FenceAfter is how long without a renewal before a member stops its single-writer workloads.
	FenceAfter = 60 * time.Second
	// MaxLine bounds one order or reply; a start order carries a stack and its variables.
	MaxLine = 16 << 20
	// StartTimeout is how long a member may take to bring a moved workload up.
	StartTimeout = 5 * time.Minute
	// ExtendFor is how long a planned restart of the leader buys every member.
	ExtendFor = 5 * time.Minute
)

// State is how a member looks from the leader.
type State string

const (
	OK            State = "ok"
	NotResponding State = "not_responding"
	// Fenced means the member has stopped its single-writer workloads by now, whether it could tell the leader or not.
	Fenced State = "fenced"
)

// StateAfter is a member's state when its last renewal is `silent` old.
func StateAfter(silent time.Duration) State {
	switch {
	case silent >= FenceAfter:
		return Fenced
	case silent >= NotRespondingAfter:
		return NotResponding
	}
	return OK
}

// Entry is one replica of a workload and where the leader placed it.
type Entry struct {
	Kind    string `json:"kind"`
	ID      string `json:"id"`
	Replica int    `json:"replica"`
	// Member is the server id; empty is waiting for room.
	Member string `json:"member"`
	// Slug names the stack on that member.
	Slug string `json:"slug"`
	// SingleWriter workloads (a database, an App with a Volume) stop first when a lease runs out.
	SingleWriter bool `json:"singleWriter"`
}

// Order is what each renewal carries: the leader's epoch and the current placement.
// Member names the receiver, so it knows which entries are its own.
type Order struct {
	Cluster   string  `json:"cluster"`
	Member    string  `json:"member"`
	Epoch     uint64  `json:"epoch"`
	Placement []Entry `json:"placement"`
	// ExtendSeconds adds to the time this lease lives without the next renewal: a leader about to restart sends it.
	ExtendSeconds int `json:"extendSeconds,omitempty"`
	// Start asks the member to bring one moved workload up from its failover bundle.
	Start *Start `json:"start,omitempty"`
}

// RegistryAuth is a registry login a pulled image needs.
type RegistryAuth struct {
	Host     string `json:"host"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// Start is a bundle opened for one restart: what the member's own deploy received, nothing more.
type Start struct {
	DeploymentID  string            `json:"deploymentId"`
	Slug          string            `json:"slug"`
	ProjectID     string            `json:"projectId"`
	ImageRef      string            `json:"imageRef"`
	ComposeYAML   string            `json:"composeYaml"`
	Network       string            `json:"network"`
	Env           map[string]string `json:"env"`
	RegistryAuth  []RegistryAuth    `json:"registryAuth"`
	ComposeUpArgs []string          `json:"composeUpArgs"`
	// Ready and RestartRule are the deploy's own messages as JSON, so the moved App is checked and watched as before.
	Ready       json.RawMessage `json:"ready,omitempty"`
	RestartRule json.RawMessage `json:"restartRule,omitempty"`
	// StartAfterReady names the services the member starts only once the rest is ready.
	StartAfterReady  []string    `json:"startAfterReady,omitempty"`
	Mounts           []MountFile `json:"mounts,omitempty"`
	FilesBindSources []string    `json:"filesBindSources,omitempty"`
}

// MountFile is a file the deploy writes beside the stack.
type MountFile struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

// StaleEpoch is the reply Error of a member that has seen a newer leader.
const StaleEpoch = "stale_epoch"

// Reply is a member's answer; on StaleEpoch, Epoch is the one it holds.
type Reply struct {
	// Started is true only when a Start order ran and the workload came up; an older member never sets it.
	Started bool   `json:"started,omitempty"`
	OK      bool   `json:"ok"`
	Error   string `json:"error,omitempty"`
	Epoch   uint64 `json:"epoch,omitempty"`
}

// ErrStale is a member refusing an older epoch.
var ErrStale = errors.New("a member holds a newer epoch")

// Send writes the order and reads the member's reply on one stream.
func Send(c io.ReadWriter, o Order) (Reply, error) {
	raw, err := json.Marshal(o)
	if err != nil {
		return Reply{}, err
	}
	if _, err := c.Write(append(raw, '\n')); err != nil {
		return Reply{}, err
	}
	line, err := bufio.NewReaderSize(io.LimitReader(c, MaxLine), 4096).ReadBytes('\n')
	if err != nil {
		return Reply{}, err
	}
	var r Reply
	if err := json.Unmarshal(line, &r); err != nil {
		return Reply{}, err
	}
	return r, nil
}
