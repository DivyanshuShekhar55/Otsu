package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// mergeOpts uses a fake clock stuck at 150, so tombstones with tiny
// timestamps (like 100) count as "fresh" and are not dropped by Merge.
var mergeOpts = Options{
	MaxFileSize: 100,
	Now:         func() int64 { return 150 },
}

// fillAndMerge writes 6 keys, merges, and closes the store.
func fillAndMerge(t *testing.T, dir string) {
	t.Helper()
	b := openStore(t, dir, mergeOpts)
	for i := 0; i < 6; i++ {
		b.Put(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i)), int64(i+1))
	}
	if err := b.Merge(); err != nil {
		t.Fatal(err)
	}
	b.Close()
}

// hintPath returns the single .hint file in dir.
func hintPath(t *testing.T, dir string) string {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(dir, "*.hint"))
	if len(m) != 1 {
		t.Fatalf("want exactly 1 hint file, got %d", len(m))
	}
	return m[0]
}

// flipByte changes one byte of a file on disk (-1 = the last byte).
func flipByte(t *testing.T, path string, pos int) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if pos < 0 {
		pos = len(data) - 1
	}
	data[pos] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// ---- tombstone grace period ------------------------------------------------

func TestTombstoneGrace(t *testing.T) {
	now := int64(1_000_000)
	b := openStore(t, t.TempDir(), Options{
		TombstoneGrace: 1000 * time.Nanosecond,
		Now:            func() int64 { return now },
	})

	b.Put("keep", []byte("v"), 1_000_000)
	b.Put("gone", []byte("v"), 1_000_000)
	b.Delete("gone", 1_000_000)

	// Inside the grace period: the tombstone survives the merge.
	now = 1_000_500
	if err := b.Merge(); err != nil {
		t.Fatal(err)
	}
	if _, ts, err := b.Get("gone"); !errors.Is(err, ErrNotFound) || ts != 1_000_000 {
		t.Fatalf("tombstone should survive: ts=%d err=%v", ts, err)
	}

	// Past the grace period: the tombstone is forgotten (timestamp back to 0).
	now = 1_002_000
	if err := b.Merge(); err != nil {
		t.Fatal(err)
	}
	if _, ts, err := b.Get("gone"); !errors.Is(err, ErrNotFound) || ts != 0 {
		t.Fatalf("tombstone should be dropped: ts=%d err=%v", ts, err)
	}
	mustGet(t, b, "keep", "v")
}

// ---- hint files ------------------------------------------------------------

// Proof that startup really uses the hint file: damage the merged DATA file.
// A full scan would refuse to open (damage in an older file), but with a good
// hint file the scan is skipped and Open succeeds.
func TestHintFileUsed(t *testing.T) {
	dir := t.TempDir()
	fillAndMerge(t, dir)

	dataPath := strings.TrimSuffix(hintPath(t, dir), ".hint") + ".data"
	flipByte(t, dataPath, -1)

	b := openStore(t, dir, mergeOpts)

	// Exactly one record (the last one in the file) is now damaged, and Get
	// notices through its CRC. Every other key is fine.
	damaged := 0
	for i := 0; i < 6; i++ {
		_, _, err := b.Get(fmt.Sprintf("k%d", i))
		if errors.Is(err, ErrCorrupt) {
			damaged++
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if damaged != 1 {
		t.Fatalf("want exactly 1 damaged key, got %d", damaged)
	}
}

// A bad or missing hint file must never break startup: we fall back to the
// full scan and every key is still there.
func TestHintFileFallback(t *testing.T) {
	for _, mode := range []string{"damaged", "missing"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			fillAndMerge(t, dir)

			hp := hintPath(t, dir)
			if mode == "damaged" {
				data, _ := os.ReadFile(hp)
				flipByte(t, hp, len(data)/2)
			} else {
				os.Remove(hp)
			}

			b := openStore(t, dir, mergeOpts)
			for i := 0; i < 6; i++ {
				mustGet(t, b, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
			}
		})
	}
}