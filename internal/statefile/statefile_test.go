package statefile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWrite_replacesTheFileAndLeavesTheOldOneWhenItCannot(t *testing.T) {
	file := filepath.Join(t.TempDir(), "epochs.json")
	if err := Write(file, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := Write(file, []byte("two")); err != nil {
		t.Fatal(err)
	}
	if raw, _ := os.ReadFile(file); string(raw) != "two" {
		t.Fatalf("the file holds %q", raw)
	}
	// A directory in the temporary file's place: the write fails and says so.
	if err := os.Mkdir(file+".tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := Write(file, []byte("three")); err == nil {
		t.Fatal("a write that could not happen reported nothing")
	}
	if raw, _ := os.ReadFile(file); string(raw) != "two" {
		t.Fatalf("after the failed write the file holds %q", raw)
	}
}
