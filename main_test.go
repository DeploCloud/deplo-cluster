package main

import (
	"bytes"
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net"
	"net/http"
	"os"
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

func TestRelayNeedsWhereToListenAndWhereToTakeItsRoster(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run(context.Background(), []string{"relay"}, &out, &errb); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if !strings.Contains(out.String(), " relay epoch 0") || !strings.Contains(errb.String(), "--listen") {
		t.Fatalf("out %q err %q", out.String(), errb.String())
	}
}

func TestRelayRefusesAPublicAddress(t *testing.T) {
	sock := t.TempDir() + "/r.sock"
	for _, addr := range []string{"0.0.0.0:3002", "203.0.113.4:3002", ":3002"} {
		var out, errb bytes.Buffer
		code := run(context.Background(), []string{"relay", "--listen", addr, "--socket", sock}, &out, &errb)
		if code != 1 || !strings.Contains(errb.String(), "does not listen") && !strings.Contains(errb.String(), "not an address") {
			t.Fatalf("%s: exit %d err %q", addr, code, errb.String())
		}
	}
}

func TestRelayTakesARosterOnItsSocketAndAnswersOnlyItsPath(t *testing.T) {
	dir := t.TempDir()
	sock := dir + "/r.sock"
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int)
	var out, errb bytes.Buffer
	go func() {
		done <- run(ctx, []string{"relay", "--listen", "127.0.0.1:0", "--socket", sock, "--state", dir}, &out, &errb)
	}()
	c := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", sock)
	}}}
	var resp *http.Response
	var err error
	for i := 0; i < 100; i++ {
		resp, err = c.Post("http://relay/call/relay.status", "application/json", nil)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body), `"clusters":0`) {
		t.Fatalf("status %d %s", resp.StatusCode, body)
	}
	fp := strings.Repeat("a", 64)
	push := `{"cluster":"clu_1","parties":[{"fingerprint":"` + fp + `","role":"member"}]}`
	resp, err = c.Post("http://relay/call/relay.roster", "application/json", strings.NewReader(push))
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("push: %v %v", err, resp)
	}
	resp.Body.Close()
	if _, err := os.Stat(dir + "/rosters.json"); err != nil {
		t.Fatalf("the roster was not kept: %v", err)
	}
	cancel()
	if code := <-done; code != 0 {
		t.Fatalf("exit %d: %s", code, errb.String())
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

// Only the relay, behind the proxy, answers anything; the leader and every party only dial out (ADR-0044).
func TestOnlyRunRelayListens(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Name.Name == "runRelay" {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok && (strings.HasPrefix(sel.Sel.Name, "Listen") || strings.HasPrefix(sel.Sel.Name, "Serve")) {
				t.Errorf("%s: %s listens outside runRelay", fset.Position(sel.Pos()), sel.Sel.Name)
			}
			return true
		})
	}
}
