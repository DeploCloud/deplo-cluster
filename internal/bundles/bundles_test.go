package bundles

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"filippo.io/age"
)

func seal(t *testing.T, to string, b Bundle) []byte {
	t.Helper()
	rec, err := age.ParseX25519Recipient(to)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	w, err := age.Encrypt(&out, rec)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(b)
	w.Write(raw)
	w.Close()
	return out.Bytes()
}

func sample(id, dep string) Bundle {
	return Bundle{V: 1, Kind: "app", Workload: id, DeploymentID: dep, Stack: Stack{Slug: "web", ComposeYAML: "services: {}", Env: map[string]string{"TOKEN": "s3cret"}}}
}

func TestTheKeyIsMadeOnceAndOnlyThePublicHalfIsShown(t *testing.T) {
	dir := t.TempDir()
	a, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if a.PublicKey() != b.PublicKey() || !strings.HasPrefix(a.PublicKey(), "age1") {
		t.Fatalf("keys %q %q", a.PublicKey(), b.PublicKey())
	}
	st, _ := os.Stat(filepath.Join(dir, "leader.key"))
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("the key file is %v", st.Mode())
	}
}

func TestABundleIsStoredSealedAndOpensOnlyForARestart(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	meta := Meta{Name: "web", HasDomain: true}
	if err := s.Put("clu_1", "app", "prj_1", "dep_1", seal(t, s.PublicKey(), sample("prj_1", "dep_1")), meta); err != nil {
		t.Fatal(err)
	}
	var disk []byte
	_ = filepath.Walk(dir, func(p string, i os.FileInfo, _ error) error {
		if !i.IsDir() && !strings.HasSuffix(p, "leader.key") {
			raw, _ := os.ReadFile(p)
			disk = append(disk, raw...)
		}
		return nil
	})
	if bytes.Contains(disk, []byte("s3cret")) || bytes.Contains(disk, []byte("services")) {
		t.Fatal("a bundle was stored in the clear")
	}
	got, err := s.Unseal("clu_1", "app", "prj_1")
	if err != nil || got.Stack.Env["TOKEN"] != "s3cret" {
		t.Fatalf("unseal %+v %v", got, err)
	}
	if m, _ := s.Meta("clu_1", "app", "prj_1"); !m.HasDomain || m.Name != "web" {
		t.Fatalf("meta %+v", m)
	}
	list, _ := s.List("clu_1")
	if len(list) != 1 || list[0].ID != "prj_1" || list[0].DeploymentID != "dep_1" {
		t.Fatalf("list %+v", list)
	}
	if other, _ := s.List("clu_2"); len(other) != 0 {
		t.Fatal("another cluster's list shows this bundle")
	}
}

func TestAPutReplacesAndADropForgets(t *testing.T) {
	s, _ := Open(t.TempDir())
	_ = s.Put("clu_1", "app", "prj_1", "dep_1", seal(t, s.PublicKey(), sample("prj_1", "dep_1")), Meta{})
	_ = s.Put("clu_1", "app", "prj_1", "dep_2", seal(t, s.PublicKey(), sample("prj_1", "dep_2")), Meta{})
	if l, _ := s.List("clu_1"); len(l) != 1 || l[0].DeploymentID != "dep_2" {
		t.Fatalf("list %+v", l)
	}
	if err := s.Drop("clu_1", "app", "prj_1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Drop("clu_1", "app", "prj_1"); err != nil {
		t.Fatalf("dropping nothing is not an error: %v", err)
	}
	if _, err := s.Unseal("clu_1", "app", "prj_1"); err != ErrNone {
		t.Fatalf("unseal after drop: %v", err)
	}
}

func TestABundleSealedToAnotherKeyOrNamingAnotherWorkloadIsRefused(t *testing.T) {
	s, _ := Open(t.TempDir())
	other, _ := Open(t.TempDir())
	if err := s.Put("clu_1", "app", "prj_1", "dep_1", seal(t, other.PublicKey(), sample("prj_1", "dep_1")), Meta{}); err == nil {
		t.Fatal("a bundle sealed to another key was kept")
	}
	if err := s.Put("clu_1", "app", "prj_2", "dep_1", seal(t, s.PublicKey(), sample("prj_1", "dep_1")), Meta{}); err == nil {
		t.Fatal("a bundle was kept under another workload's name")
	}
	if err := s.Put("../x", "app", "prj_1", "dep_1", seal(t, s.PublicKey(), sample("prj_1", "dep_1")), Meta{}); err == nil {
		t.Fatal("a path was accepted as a cluster name")
	}
}
