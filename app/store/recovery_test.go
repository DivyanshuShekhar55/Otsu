package store

import (
	"errors"
	"fmt"
	"os"
	"testing"
)

// ---- rotation --------------------------------------------------------------

// With a tiny size limit, many writes must spill into several files,
// and every key must still be readable (also after a restart).
func TestRotation(t *testing.T) {
	dir := t.TempDir()
	b := openStore(t, dir, Options{MaxFileSize: 100}) // ~3 records per file

	for i := 0; i < 10; i++ {
		b.Put(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("value-%d", i)), int64(i+1))
	}
	if n := countFiles(dir, "*.data"); n < 2 {
		t.Fatalf("expected several data files, got %d", n)
	}
	for i := 0; i < 10; i++ {
		mustGet(t, b, fmt.Sprintf("k%d", i), fmt.Sprintf("value-%d", i))
	}

	// Restart: recovery has to stitch all the files back together.
	b.Close()
	b = openStore(t, dir, Options{MaxFileSize: 100})
	for i := 0; i < 10; i++ {
		mustGet(t, b, fmt.Sprintf("k%d", i), fmt.Sprintf("value-%d", i))
	}
}

// ---- startup recovery ------------------------------------------------------

func TestRecoveryAfterReopen(t *testing.T) {
	dir := t.TempDir()
	b := openStore(t, dir, Options{})

	b.Put("a", []byte("1"), 1)
	b.Put("a", []byte("2"), 2) // overwrite
	b.Put("b", []byte("x"), 3)
	b.Delete("b", 4)
	b.Close()

	b = openStore(t, dir, Options{})
	mustGet(t, b, "a", "2")
	mustBeGone(t, b, "b")

	// The tombstone's timestamp must survive too.
	if _, ts, _ := b.Get("b"); ts != 4 {
		t.Fatalf("tombstone ts = %d, want 4", ts)
	}
}

// Last record is cut in half (like a crash during a write).
func TestRecoveryTornTail(t *testing.T) {
	dir := t.TempDir()
	b := openStore(t, dir, Options{})
	b.Put("a", []byte("1"), 1)
	b.Put("b", []byte("2"), 2)
	b.Put("c", []byte("3"), 3)
	b.Close()

	path := dataFileName(dir, 1)
	info, _ := os.Stat(path)
	os.Truncate(path, info.Size()-3) // chop the end of record "c"

	b = openStore(t, dir, Options{})
	mustGet(t, b, "a", "1")
	mustGet(t, b, "b", "2")
	mustBeGone(t, b, "c")

	// New writes must land on clean data and survive another restart.
	b.Put("d", []byte("4"), 4)
	b.Close()
	b = openStore(t, dir, Options{})
	mustGet(t, b, "d", "4")
	mustGet(t, b, "b", "2")
}

// Last record has a flipped byte (CRC no longer matches).
func TestRecoveryBadCRCTail(t *testing.T) {
	dir := t.TempDir()
	b := openStore(t, dir, Options{})
	b.Put("a", []byte("1"), 1)
	b.Put("b", []byte("2"), 2)
	b.Close()

	path := dataFileName(dir, 1)
	info, _ := os.Stat(path)
	f, _ := os.OpenFile(path, os.O_RDWR, 0)
	f.WriteAt([]byte{0xFF}, info.Size()-1)
	f.Close()

	b = openStore(t, dir, Options{})
	mustGet(t, b, "a", "1")
	mustBeGone(t, b, "b")
}

// Damage in an OLDER file is not a torn write, so Open must refuse.
func TestRecoveryCorruptOldFile(t *testing.T) {
	dir := t.TempDir()
	b := openStore(t, dir, Options{MaxFileSize: 60}) // 2 records per file
	b.Put("k0", []byte("value-0"), 1)
	b.Put("k1", []byte("value-1"), 2)
	b.Put("k2", []byte("value-2"), 3) // goes into file 2
	b.Close()

	path := dataFileName(dir, 1)
	info, _ := os.Stat(path)
	f, _ := os.OpenFile(path, os.O_RDWR, 0)
	f.WriteAt([]byte{0xFF}, info.Size()-1)
	f.Close()

	if _, err := Open(dir, Options{MaxFileSize: 60}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

// ---- merge -----------------------------------------------------------------

func TestMerge(t *testing.T) {
	dir := t.TempDir()
	b := openStore(t, dir, mergeOpts)

	// Overwrite the same 4 keys 5 times -> lots of dead records on disk.
	for round := 1; round <= 5; round++ {
		for i := 0; i < 4; i++ {
			b.Put(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", round)), int64(round))
		}
	}
	b.Delete("k3", 100)

	before := countFiles(dir, "*.data")
	if err := b.Merge(); err != nil {
		t.Fatal(err)
	}
	after := countFiles(dir, "*.data")

	// Afterwards: one merged file + one fresh active file.
	if after != 2 || after >= before {
		t.Fatalf("data files before=%d after=%d, want 2", before, after)
	}
	if n := countFiles(dir, "*.hint"); n != 1 {
		t.Fatalf("want 1 hint file, got %d", n)
	}

	check := func(b *Bitcask) {
		t.Helper()
		for i := 0; i < 3; i++ {
			mustGet(t, b, fmt.Sprintf("k%d", i), "v5")
		}
		mustBeGone(t, b, "k3")
		if _, ts, _ := b.Get("k3"); ts != 100 {
			t.Fatalf("tombstone ts = %d, want 100", ts)
		}
	}
	check(b)

	// Writing after a merge works...
	b.Put("new", []byte("after-merge"), 200)

	// ...and everything survives a restart.
	b.Close()
	b = openStore(t, dir, mergeOpts)
	check(b)
	mustGet(t, b, "new", "after-merge")
}