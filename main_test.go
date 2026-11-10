package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestVersionFlagPrintsAndExits(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run(context.Background(), []string{"--version"}, &out, &errb); code != 0 {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	if !strings.HasPrefix(out.String(), "deplo-cluster ") || !strings.Contains(out.String(), "epoch 0") {
		t.Fatalf("unexpected output %q", out.String())
	}
}

func TestStartsAsLeaderAndStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var out, errb bytes.Buffer
	done := make(chan int)
	go func() { done <- run(ctx, nil, &out, &errb) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit %d: %s", code, errb.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not stop on cancel")
	}
	if !strings.Contains(out.String(), " leader epoch 0") {
		t.Fatalf("unexpected output %q", out.String())
	}
}

func TestRelayModeStub(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, errb bytes.Buffer
	if code := run(ctx, []string{"relay"}, &out, &errb); code != 0 {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	if !strings.Contains(out.String(), " relay epoch 0") {
		t.Fatalf("unexpected output %q", out.String())
	}
}

func TestUnknownModeIsRefused(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run(context.Background(), []string{"bogus"}, &out, &errb); code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
	if errb.Len() == 0 {
		t.Fatal("expected a message on stderr")
	}
}
