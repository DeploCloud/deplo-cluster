package main

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/wire"
)

// meshDoor is the agent's way onto the cluster's network (ADR-0047): the leader asks it for a stream to a party,
// and the agent opens one through the tunnel. A leader without it reaches members through the relays only.
type meshDoor struct {
	socket string
	mu     sync.Mutex
	at     time.Time
	ok     bool
}

const (
	doorDial = "DIAL"
	doorUp   = "UP"
	// doorUpFor is how long the answer to "is the network up" is trusted.
	doorUpFor = 2 * time.Second
)

func newMeshDoor(socket string) *meshDoor { return &meshDoor{socket: socket} }

func (d *meshDoor) open(ctx context.Context, verb, arg string) (net.Conn, error) {
	if d.socket == "" {
		return nil, errors.New("the agent beside this leader is on no cluster network")
	}
	var nd net.Dialer
	c, err := nd.DialContext(ctx, "unix", d.socket)
	if err != nil {
		return nil, err
	}
	dl := time.Now().Add(wire.HeaderTimeout)
	if t, ok := ctx.Deadline(); ok && t.Before(dl) {
		dl = t
	}
	_ = c.SetDeadline(dl)
	if err := wire.WriteHeader(c, verb, arg); err != nil {
		c.Close()
		return nil, err
	}
	got, why, err := wire.ReadHeader(c)
	if err != nil || got != wire.VerbOK {
		c.Close()
		if err == nil {
			err = errors.New("the cluster network refused: " + why)
		}
		return nil, err
	}
	_ = c.SetDeadline(time.Time{})
	return c, nil
}

// dial is the party's Direct: a raw stream to the party through the tunnel, which the party then secures.
func (d *meshDoor) dial(ctx context.Context, to wire.Party) (net.Conn, error) {
	return d.open(ctx, doorDial, to.Key())
}

// up says whether the network reaches at least one server, asked at most every two seconds.
func (d *meshDoor) up() bool {
	if d.socket == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if time.Since(d.at) < doorUpFor {
		return d.ok
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := d.open(ctx, doorUp, "")
	if c != nil {
		c.Close()
	}
	d.at, d.ok = time.Now(), err == nil
	return d.ok
}
