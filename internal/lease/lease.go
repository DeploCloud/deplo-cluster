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
	RenewEvery = 1 * time.Second
	// NotRespondingAfter is how long without a renewal before the cluster card says so.
	NotRespondingAfter = 3 * time.Second
	// FenceAfter is the longest a member lives without a renewal before it stops its single-writer workloads:
	// an older agent's fence, and what any member is held to until it says StoppedWithin.
	FenceAfter = 60 * time.Second
	// FenceFor is how long this leader's member lives without a renewal: every renewal says so (Order.FenceSeconds).
	FenceFor = 5 * time.Second
	// MaxLine bounds one order or reply; a start order carries a stack and its variables.
	MaxLine = 16 << 20
	// StartTimeout is how long a member may take to bring a moved workload up.
	StartTimeout = 5 * time.Minute
	// RestoreTimeout is how long a start that restores a backup first may take.
	RestoreTimeout = StartTimeout + 30*time.Minute
	// ExtendFor is how long a planned restart of the leader buys every member, and the most a member honours.
	// A leader waits it out for a member it never heard, so raise it here first and in deplo-agent last.
	ExtendFor = 5 * time.Minute
	// AnnounceFor is what an announcement of an extension says a lease may still live: the extension round that
	// follows it, the longest fence and the extension.
	AnnounceFor = 10*time.Second + FenceAfter + ExtendFor
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
	// Standby is a copy that follows the workload from another member: it holds the stack, never the name.
	Standby bool `json:"standby,omitempty"`
	// Balanced is an App the cluster load-balances: a member back from a failover gets its copy again (copyBack).
	Balanced bool `json:"balanced,omitempty"`
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
	// FenceSeconds is how long the lease lives without the next renewal; an older leader never sets it, and its
	// member keeps FenceAfter.
	FenceSeconds int `json:"fenceSeconds,omitempty"`
	// Announces is a leader that extends no lease before every member took its announcement of the extension.
	Announces bool `json:"announces,omitempty"`
	// Announce says a lease of this cluster may live this many seconds more because of an extension that follows.
	Announce int `json:"announce,omitempty"`
	// Probe only asks whether the member takes this epoch. It renews nothing and tells the member nothing else,
	// and a member that answers refuses every older epoch from then on.
	Probe bool `json:"probe,omitempty"`
	// Start asks the member to bring one moved workload up from its failover bundle.
	Start *Start `json:"start,omitempty"`
	// Return asks a member that was presumed dead what it removed; sent until it answers with Returned.
	Return bool `json:"return,omitempty"`
	// Keep says, to a member whose work moved, where to save what it still holds of each before it removes it.
	// It goes on after the return for what the member could not save, until the member says it keeps nothing.
	Keep []Keep `json:"keep,omitempty"`
	// Stores is, to the member whose disk a destination is on, the archives other members may send it.
	Stores []Store `json:"stores,omitempty"`
	// Gone is the certificates of the members gone past the grace that this leader no longer counts for a majority:
	// they vote and host again once back and caught up (ADR-0049).
	Gone []string `json:"gone,omitempty"`
	// From is the server the leader runs on: a member gives each epoch to one leader only.
	From string `json:"from,omitempty"`
	// Line is the members that lead after this leader, heir first, and Last Deplo's machine, which comes after them.
	Line []string `json:"line,omitempty"`
	Last string   `json:"last,omitempty"`
	// PreVote only asks whether the member would take this epoch: it takes nothing and changes nothing.
	PreVote bool `json:"preVote,omitempty"`
	// Succeed is an epoch taken without Deplo's word: refused while the member still hears its current leader.
	Succeed bool `json:"succeed,omitempty"`
}

// Store is one archive another member of the cluster may send this one, to keep in its own backup store.
type Store struct {
	// From is the sender's certificate.
	From      string `json:"from"`
	Root      string `json:"root,omitempty"`
	ObjectKey string `json:"objectKey"`
}

// LeftName ends the name of what a member had left of a workload: the failover's time and the member, so a
// second try writes over the first.
func LeftName(failedOverAt time.Time, member string, encrypted bool) string {
	name := "-" + failedOverAt.UTC().Format("20060102T150405Z") + "-" + member + ".tar.gz"
	if encrypted {
		name += ".age"
	}
	return name
}

// Returned is the stacks a member that came back stopped and removed, because they run elsewhere now.
type Returned struct {
	Removed []string `json:"removed"`
	// Saved is what was written as a backup before its volume went; Kept is what stays on the member, and why.
	Saved []Saved `json:"saved,omitempty"`
	Kept  []Kept  `json:"kept,omitempty"`
	// Aside is the dated copies it kept of the volumes it removed: writes its copy may not have sent yet.
	Aside []Aside `json:"aside,omitempty"`
}

// Aside is one dated copy a returning member kept of a moved stack's volume, before that volume went.
type Aside struct {
	Slug      string `json:"slug"`
	Volume    string `json:"volume"`
	Of        string `json:"of"`
	At        string `json:"at"`
	SizeBytes int64  `json:"sizeBytes"`
}

// Keep is where the data a member still holds of one moved workload goes; credentials travel in Request only.
type Keep struct {
	Slug string `json:"slug"`
	// Ref is the control plane's name for the destination, said back with what was saved.
	Ref string `json:"ref,omitempty"`
	// StoreMember is the member whose disk the destination is on; empty is a bucket every member reaches.
	StoreMember string `json:"storeMember,omitempty"`
	// Request is the member's own backup request as JSON; empty is a workload the leader holds no destination for.
	Request json.RawMessage `json:"request,omitempty"`
	// StoreFingerprint is StoreMember's certificate, set when it is a member of this cluster: the archive is
	// sent to it over the relay.
	StoreFingerprint string `json:"storeFingerprint,omitempty"`
	// FailedOverAt is when the workload moved away, which names the archive.
	FailedOverAt string `json:"failedOverAt,omitempty"`
}

// Saved is one volume archive a returning member wrote, as its backup result reported it.
type Saved struct {
	Slug               string `json:"slug"`
	Ref                string `json:"ref,omitempty"`
	ObjectKey          string `json:"objectKey"`
	SizeBytes          int64  `json:"sizeBytes"`
	DecryptedSizeBytes int64  `json:"decryptedSizeBytes,omitempty"`
	Sha256             string `json:"sha256,omitempty"`
	At                 string `json:"at"`
}

// Why a member kept the data of a moved workload on its own disk.
const (
	KeptStoreElsewhere = "store_elsewhere"
	KeptNoDestination  = "no_destination"
	KeptBackupFailed   = "backup_failed"
)

// Kept is data that was not saved and so was not removed.
type Kept struct {
	Slug   string `json:"slug"`
	Ref    string `json:"ref,omitempty"`
	Reason string `json:"reason"`
	Error  string `json:"error,omitempty"`
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
	// Restore is the backup the member restores before the workload counts as started.
	Restore *Restore `json:"restore,omitempty"`
	// Promote makes the standby copy this member holds the workload, before the stack is started over it.
	Promote *Promote `json:"promote,omitempty"`
	// Certificates are the App's own, for its domains that answer on every server, passed on unread.
	Certificates json.RawMessage `json:"certificates,omitempty"`
}

// Promote is the command the control plane rendered for one engine, run inside the standby's container.
type Promote struct {
	Container string   `json:"container"`
	User      string   `json:"user,omitempty"`
	Command   []string `json:"command"`
}

// Restore names one backup and carries the member's own restore request as JSON, passed on unread.
type Restore struct {
	BackupID string          `json:"backupId"`
	TakenAt  string          `json:"takenAt"`
	Request  json.RawMessage `json:"request"`
}

// MountFile is a file the deploy writes beside the stack.
type MountFile struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

// StaleEpoch is the reply Error of a member that has seen a newer leader.
const StaleEpoch = "stale_epoch"

// LeaderAlive is the reply Error of a member asked for a Succeed epoch while its leader renewed it within PreVoteFor.
const LeaderAlive = "leader_alive"

// PreVoteFor is how long a member keeps refusing a Succeed epoch after its leader's last renewal: the removal time.
const PreVoteFor = 10 * time.Second

// SucceedStagger is how much longer each member further down the line waits before it asks to lead.
const SucceedStagger = 5 * time.Second

// Reply is a member's answer; on StaleEpoch, Epoch is the one it holds.
type Reply struct {
	// Unsure is how many seconds a lease of this cluster may still live because of an extension, as far as the
	// member can tell; nil is an older member.
	Unsure *int `json:"unsure,omitempty"`
	// Started is true only when a Start order ran and the workload came up; an older member never sets it.
	Started bool `json:"started,omitempty"`
	// Restored is true only when the Start order's backup was restored; an older member never sets it.
	Restored bool `json:"restored,omitempty"`
	// Promoted is true only when the Start order's standby was promoted; an older member never sets it.
	Promoted bool   `json:"promoted,omitempty"`
	OK       bool   `json:"ok"`
	Error    string `json:"error,omitempty"`
	Epoch    uint64 `json:"epoch,omitempty"`
	// StoppedWithin is how many seconds after its last renewal this member has surely stopped its single-writer
	// workloads; an older member never sets it.
	StoppedWithin int `json:"stoppedWithin,omitempty"`
	// Returned answers an order's Return once the member is done removing what moved away.
	Returned *Returned `json:"returned,omitempty"`
	// Unplaced is the stacks the order placed on no member that this member still has, running or stopped by
	// itself: what waits for room is handed back to it, never started a second time beside it.
	Unplaced []string `json:"unplaced,omitempty"`
	// Running answers a probe: the single-writer stacks the member runs that its lease placed there, and that
	// lease's epoch. A leader that takes over keeps each where it runs, the newest epoch first (deplo#241).
	Running []Writer `json:"running,omitempty"`
	// Reports says the member answers a probe with Running, also when it runs nothing: a leader keeps the roster
	// for a member that does not, as before.
	Reports bool `json:"reports,omitempty"`
	// Later answers a Keep sent after the return, once the member keeps nothing of a stack any more.
	Later *Later `json:"later,omitempty"`
}

// Writer is a single-writer stack a member runs, and the epoch of the lease that placed it there.
type Writer struct {
	Slug  string `json:"slug"`
	Epoch uint64 `json:"epoch"`
}

// Later is what became of data a member kept past its return.
type Later struct {
	// Saved is what a later try wrote as a backup; Gone is every stack it keeps nothing of now, saved or not.
	Saved []Saved  `json:"saved,omitempty"`
	Gone  []string `json:"gone"`
}

// ErrStale is a member refusing an older epoch.
var ErrStale = errors.New("a member holds a newer epoch")

// Send writes the order and reads the member's reply on one stream.
func Send(c io.ReadWriter, o Order) (Reply, error) {
	if !o.Probe && !o.PreVote {
		o.FenceSeconds, o.Announces = int(FenceFor/time.Second), true
	}
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
