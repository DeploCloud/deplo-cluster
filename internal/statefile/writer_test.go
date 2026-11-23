package statefile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTheNewestPutIsWhatAFlushLeavesOnDisk(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, t.Logf)
	for _, v := range []string{"1", "2", "3"} {
		w.Put("a.json", []byte(v))
	}
	w.Put("b.json", []byte("b"))
	w.Flush()
	for name, want := range map[string]string{"a.json": "3", "b.json": "b"} {
		if got, err := os.ReadFile(filepath.Join(dir, name)); err != nil || string(got) != want {
			t.Fatalf("%s holds %q (%v), want %q", name, got, err, want)
		}
	}
}
