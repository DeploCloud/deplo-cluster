package bundles

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

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

func TestTheIndexNamesTheBackupFromTheBundleItselfAndHoldsNoSecret(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	b := Bundle{V: 1, Kind: "database", Workload: "db_1", DeploymentID: "rev_1", Stack: Stack{Slug: "db-main", ComposeYAML: "services: {}"},
		Restore: &Restore{RestoreRef: RestoreRef{BackupID: "brun_1", TakenAt: "2026-03-01T11:45:00Z", StoreMember: "srv_b"},
			Request: json.RawMessage(`{"s3":{"secretKey":"s3cr3t"},"database":{"password":"pgpass"}}`)}}
	// What the caller says about the restore is not believed: only the sealed bundle is.
	claimed := Meta{Name: "main", Restore: &RestoreRef{BackupID: "brun_other"}}
	if err := s.Put("clu_1", "database", "db_1", "rev_1", seal(t, s.PublicKey(), b), claimed); err != nil {
		t.Fatal(err)
	}
	m, err := s.Meta("clu_1", "database", "db_1")
	if err != nil || m.Restore == nil || m.Restore.BackupID != "brun_1" || m.Restore.StoreMember != "srv_b" || m.Restore.TakenAt != "2026-03-01T11:45:00Z" {
		t.Fatalf("meta %+v %v", m.Restore, err)
	}
	idx, _ := os.ReadFile(filepath.Join(dir, "bundles", "clu_1", "database.db_1.json"))
	if bytes.Contains(idx, []byte("s3cr3t")) || bytes.Contains(idx, []byte("pgpass")) {
		t.Fatalf("the index holds a secret: %s", idx)
	}
	got, _ := s.Unseal("clu_1", "database", "db_1")
	if got.Restore == nil || !bytes.Contains(got.Restore.Request, []byte("s3cr3t")) {
		t.Fatalf("the opened bundle lost its restore request: %+v", got.Restore)
	}
	// A bundle that replaces it without a restore clears the index too.
	b.Restore, b.DeploymentID = nil, "rev_2"
	_ = s.Put("clu_1", "database", "db_1", "rev_2", seal(t, s.PublicKey(), b), claimed)
	if m, _ := s.Meta("clu_1", "database", "db_1"); m.Restore != nil {
		t.Fatalf("the index still names a backup: %+v", m.Restore)
	}
}

// eu, 24 Nov 2026: a failover pass read a bundle's meta under the leader's lock while a Put waited on a busy disk. A
// Put that hangs (a pipe nobody reads stands in for the disk) holds no Meta or Unseal up.
func TestAPutThatHangsHoldsNoReadUp(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	if err := s.Put("clu_1", "app", "prj_1", "dep_1", seal(t, s.PublicKey(), sample("prj_1", "dep_1")), Meta{Environment: "env_1"}); err != nil {
		t.Fatal(err)
	}
	hung, _ := s.path("clu_1", "app", "prj_2", ".age")
	if err := os.MkdirAll(filepath.Dir(hung), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(hung+".tmp", 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if r, err := os.OpenFile(hung+".tmp", os.O_RDONLY|syscall.O_NONBLOCK, 0); err == nil {
			time.Sleep(50 * time.Millisecond)
			r.Close()
		}
	})
	go func() {
		_ = s.Put("clu_1", "app", "prj_2", "dep_1", seal(t, s.PublicKey(), sample("prj_2", "dep_1")), Meta{})
	}()
	time.Sleep(50 * time.Millisecond)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if m, err := s.Meta("clu_1", "app", "prj_1"); err != nil || m.Environment != "env_1" {
			t.Errorf("meta %+v, %v", m, err)
		}
		if _, err := s.Unseal("clu_1", "app", "prj_1"); err != nil {
			t.Errorf("unseal: %v", err)
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a read waited on a Put")
	}
}

// eu, 24 Nov 2026: a renewal read a bundle for its Keep on a saturated disk and one member was not renewed for
// 14.7 s. The Keep a renewal carries comes from memory once read, and follows every Put and Drop.
func TestTheKeepARenewalCarriesNeverWaitsOnTheDisk(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	b := sample("prj_1", "dep_1")
	b.Keep = &Keep{Ref: "dst_1", StoreMember: "srv_b"}
	if err := s.Put("clu_1", "app", "prj_1", "dep_1", seal(t, s.PublicKey(), b), Meta{}); err != nil {
		t.Fatal(err)
	}
	// A store started again reads it once from the disk.
	s, _ = Open(dir)
	if k, err := s.KeepOf("clu_1", "app", "prj_1"); err != nil || k == nil || k.Ref != "dst_1" {
		t.Fatalf("keep %+v, %v", k, err)
	}
	// Then the disk hangs: a pipe nobody writes stands in for the bundle.
	file, _ := s.path("clu_1", "app", "prj_1", ".age")
	if os.Remove(file) != nil || syscall.Mkfifo(file, 0o600) != nil {
		t.Fatal("fifo")
	}
	t.Cleanup(func() {
		if w, err := os.OpenFile(file, os.O_WRONLY|syscall.O_NONBLOCK, 0); err == nil {
			w.Close()
		}
	})
	done := make(chan *Keep, 1)
	go func() { k, _ := s.KeepOf("clu_1", "app", "prj_1"); done <- k }()
	select {
	case k := <-done:
		if k == nil || k.Ref != "dst_1" {
			t.Fatalf("keep %+v", k)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the Keep waited on the disk")
	}
	os.Remove(file)
	b.Keep, b.DeploymentID = &Keep{Ref: "dst_2", StoreMember: "srv_c"}, "dep_2"
	if err := s.Put("clu_1", "app", "prj_1", "dep_2", seal(t, s.PublicKey(), b), Meta{}); err != nil {
		t.Fatal(err)
	}
	if k, _ := s.KeepOf("clu_1", "app", "prj_1"); k == nil || k.Ref != "dst_2" {
		t.Fatalf("after a Put: %+v", k)
	}
	if err := s.Drop("clu_1", "app", "prj_1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.KeepOf("clu_1", "app", "prj_1"); err == nil {
		t.Fatal("a dropped bundle still has a Keep")
	}
}
