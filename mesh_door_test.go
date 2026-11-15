package main

import (
	"context"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/wire"
)

// fakeDoor answers like the agent's way onto the cluster's network: DIAL a known party, and UP when up.
func fakeDoor(t *testing.T, up bool, known string) string {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "mesh.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				verb, arg, err := wire.ReadHeader(c)
				switch {
				case err != nil:
				case verb == doorUp && up, verb == doorDial && arg == known:
					_ = wire.WriteHeader(c, wire.VerbOK, "")
					_, _ = io.Copy(c, c)
				default:
					_ = wire.WriteHeader(c, wire.VerbErr, "no")
				}
			}()
		}
	}()
	return sock
}

func TestMeshDoor_dialsAPartyThroughTheAgent(t *testing.T) {
	to := wire.Party{Fingerprint: strings.Repeat("b", 64), Role: wire.RoleMember}
	d := newMeshDoor(fakeDoor(t, true, to.Key()))
	c, err := d.dial(context.Background(), to)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = c.Write([]byte("ping"))
	buf := make([]byte, 4)
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("read %q %v", buf, err)
	}
	if _, err := d.dial(context.Background(), wire.Party{Fingerprint: strings.Repeat("c", 64), Role: wire.RoleMember}); err == nil {
		t.Fatal("a party the network does not hold was dialed")
	}
}

func TestMeshDoor_saysWhetherTheNetworkIsUp(t *testing.T) {
	if !newMeshDoor(fakeDoor(t, true, "")).up() {
		t.Fatal("an answering network reads as down")
	}
	if newMeshDoor(fakeDoor(t, false, "")).up() {
		t.Fatal("a network with nobody on it reads as up")
	}
	if newMeshDoor("").up() {
		t.Fatal("a leader with no agent network reads as up")
	}
	if _, err := newMeshDoor("").dial(context.Background(), wire.Party{}); err == nil {
		t.Fatal("dialed with no agent network")
	}
}
