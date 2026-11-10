// deplo-cluster runs as a cluster leader or, with `relay`, as a cluster relay.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/DeploCloud/deplo-cluster/internal/cluster"
	"github.com/DeploCloud/deplo-cluster/internal/party"
	"github.com/DeploCloud/deplo-cluster/internal/relay"
	"github.com/DeploCloud/deplo-cluster/internal/wire"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	role, rest, err := cluster.ParseRole(args)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	fs := flag.NewFlagSet("deplo-cluster", flag.ContinueOnError)
	fs.SetOutput(stderr)
	version := fs.Bool("version", false, "print the version and epoch, then exit")
	cert := fs.String("cert", envOr("DEPLO_CLUSTER_CERT", ""), "this server's certificate (PEM), from the instance's agent CA")
	key := fs.String("key", envOr("DEPLO_CLUSTER_KEY", ""), "its private key (PEM)")
	ca := fs.String("ca", envOr("DEPLO_CLUSTER_CA", ""), "the instance's agent CA (PEM)")
	listen := fs.String("listen", envOr("DEPLO_RELAY_LISTEN", ""), "relay: comma-separated addresses to answer the proxy on (never a public one)")
	socket := fs.String("socket", envOr("DEPLO_RELAY_SOCKET", ""), "relay: unix socket the roster is pushed on")
	stateDir := fs.String("state", envOr("DEPLO_RELAY_STATE", ""), "relay: directory that keeps the roster across restarts")
	relays := fs.String("relays-file", envOr("DEPLO_CLUSTER_RELAYS_FILE", ""), "leader: file listing the relays to dial, one address per line")
	if err := fs.Parse(rest); err != nil {
		return 2
	}
	status := cluster.Status{Role: role, Epoch: cluster.InitialEpoch}
	fmt.Fprintln(stdout, status)
	if *version {
		return 0
	}
	logger := log.New(stderr, "", log.LstdFlags)
	identity := func() (*wire.Identity, error) { return wire.LoadIdentity(*cert, *key, *ca) }
	if role == cluster.RoleRelay {
		if err := runRelay(ctx, identity, *listen, *socket, *stateDir, logger); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}
	if *cert != "" && *relays != "" {
		p := party.New(party.Config{
			Role:     wire.RoleLeader,
			Identity: identity,
			Relays:   func() []string { return readLines(*relays) },
			// Leading arrives in a later ticket: a stream is refused until there is something to say on it.
			Accept: func(_ wire.Party, c net.Conn) { c.Close() },
			Logf:   logger.Printf,
		})
		go p.Run(ctx)
	}
	<-ctx.Done()
	return 0
}

func readLines(file string) []string {
	raw, err := os.ReadFile(file)
	if err != nil {
		return nil
	}
	var out []string
	for _, l := range strings.Split(string(raw), "\n") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}
	return out
}

func runRelay(ctx context.Context, identity func() (*wire.Identity, error), listen, socket, stateDir string, logger *log.Logger) error {
	if listen == "" || socket == "" {
		return errors.New("a relay needs --listen and --socket")
	}
	addrs := strings.Split(listen, ",")
	for _, a := range addrs {
		if err := relay.ValidateListen(strings.TrimSpace(a)); err != nil {
			return err
		}
	}
	state := ""
	if stateDir != "" {
		state = stateDir + "/rosters.json"
	}
	r := relay.New(relay.Config{Identity: identity, StateFile: state, Logf: logger.Printf})
	_ = os.Remove(socket)
	admin, err := net.Listen("unix", socket)
	if err != nil {
		return err
	}
	_ = os.Chmod(socket, 0o600)
	servers := []*http.Server{{Handler: r.Admin(), ReadHeaderTimeout: 10 * time.Second}}
	go servers[0].Serve(admin)
	for _, a := range addrs {
		l, err := net.Listen("tcp", strings.TrimSpace(a))
		if err != nil {
			return err
		}
		s := &http.Server{Handler: r, ReadHeaderTimeout: 10 * time.Second}
		servers = append(servers, s)
		go s.Serve(l)
	}
	<-ctx.Done()
	for _, s := range servers {
		s.Close()
	}
	return nil
}
