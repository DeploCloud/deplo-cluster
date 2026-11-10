// deplo-cluster runs as a cluster leader or, with `relay`, as a cluster relay.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/DeploCloud/deplo-cluster/internal/cluster"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
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
	if err := fs.Parse(rest); err != nil {
		return 2
	}
	status := cluster.Status{Role: role, Epoch: cluster.InitialEpoch}
	fmt.Fprintln(stdout, status)
	if *version {
		return 0
	}
	// Leading and relaying arrive in later tickets; the skeleton only stays up.
	<-ctx.Done()
	return 0
}
