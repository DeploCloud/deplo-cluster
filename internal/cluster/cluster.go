// Package cluster holds what the deplo-cluster binary reports about itself.
package cluster

import "fmt"

// Version is stamped at build time from the release tag.
var Version = "dev"

// InitialEpoch is what a process reports before it has held any lease.
const InitialEpoch uint64 = 0

// Role is what this process does for its cluster.
type Role string

const (
	RoleLeader Role = "leader"
	RoleRelay  Role = "relay"
)

// Status is the one-line identity a process reports.
type Status struct {
	Role  Role
	Epoch uint64
}

func (s Status) String() string {
	return fmt.Sprintf("deplo-cluster %s %s epoch %d", Version, s.Role, s.Epoch)
}

// ParseRole reads the optional leading mode: none means leader, `relay` means relay.
func ParseRole(args []string) (Role, []string, error) {
	if len(args) == 0 || len(args[0]) > 0 && args[0][0] == '-' {
		return RoleLeader, args, nil
	}
	if args[0] == string(RoleRelay) {
		return RoleRelay, args[1:], nil
	}
	return "", nil, fmt.Errorf("unknown mode %q (expected nothing, or relay)", args[0])
}
