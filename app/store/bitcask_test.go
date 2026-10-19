package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// ---- helpers ---------------------------------------------------------------

// openStore opens a store and closes it automatically when the test ends.
func openStore(t *testing.T, dir string, opts Options) *Bitcask {
	t.Helper()
	b, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

// mustGet fails the test unless key currently holds the value `want`.
func mustGet(t *testing.T, b *Bitcask, key, want string) {
	t.Helper()
	val, _, err := b.Get(key)
	if err != nil || string(val) != want {
		t.Fatalf("Get(%q) = %q, %v; want %q", key, val, err, want)
	}
}

// mustBeGone fails the test unless key is missing or deleted.
func mustBeGone(t *testing.T, b *Bitcask, key string) {
	t.Helper()
	if _, _, err := b.Get(key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(%q): want ErrNotFound, got %v", key, err)
	}
}

// countFiles counts files in dir that match a pattern like "*.data".
func countFiles(dir, pattern string) int {
	m, _ := filepath.Glob(filepath.Join(dir, pattern))
	return len(m)
}

// ---- record format ---------------------------------------------------------

// A record that is encoded and decoded should come back identical.
func TestRecordRoundTrip(t *testing.T) {
	in := record{timestamp: 42, key: []byte("k"), value: []byte("hello")}
	out, err := decodeRecord(in.encode())
	if err != nil {
		t.Fatal(err)
	}
	if out.timestamp != 42 || string(out.key) != "k" || string(out.value) != "hello" || out.tombstone {
		t.Fatalf("round trip mismatch: %+v", out)
	}
}

// Tombstones must survive the round trip and stay distinct from empty values.
func TestTombstoneRoundTrip(t *testing.T) {
	del := record{timestamp: 7, key: []byte("k"), tombstone: true}
	out, err := decodeRecord(del.encode())
	if err != nil || !out.tombstone {
		t.Fatalf("want tombstone, got %+v, err=%v", out, err)
	}

	empty := record{timestamp: 8, key: []byte("k"), value: []byte{}}
	out, err = decodeRecord(empty.encode())
	if err != nil || out.tombstone {
		t.Fatalf("empty value must not be a tombstone: %+v, err=%v", out, err)
	}
}

// ---- basic Get / Put / Delete ----------------------------------------------

func TestPutGet(t *testing.T) {
	b := openStore(t, t.TempDir(), Options{})

	if err := b.Put("a", []byte("1"), 10); err != nil {
		t.Fatal(err)
	}
	val, ts, err := b.Get("a")
	if err != nil || !bytes.Equal(val, []byte("1")) || ts != 10 {
		t.Fatalf("got val=%q ts=%d err=%v", val, ts, err)
	}
}

// Overwriting a key: Get must return the newest value.
func TestOverwrite(t *testing.T) {
	b := openStore(t, t.TempDir(), Options{})

	b.Put("a", []byte("old"), 1)
	b.Put("a", []byte("new"), 2)

	val, ts, _ := b.Get("a")
	if string(val) != "new" || ts != 2 {
		t.Fatalf("got val=%q ts=%d", val, ts)
	}
}

// A write with an OLDER timestamp than what we have must be ignored.
func TestStaleWriteIgnored(t *testing.T) {
	b := openStore(t, t.TempDir(), Options{})

	b.Put("a", []byte("newer"), 10)
	b.Put("a", []byte("older"), 5) // arrives late

	mustGet(t, b, "a", "newer")

	// Same for deletes: an old tombstone must not kill a newer value.
	b.Delete("a", 3)
	mustGet(t, b, "a", "newer")
}

func TestMissingKey(t *testing.T) {
	b := openStore(t, t.TempDir(), Options{})
	mustBeGone(t, b, "nope")
}

// After Delete, Get says "not found" but still reports the tombstone's timestamp.
func TestDelete(t *testing.T) {
	b := openStore(t, t.TempDir(), Options{})

	b.Put("a", []byte("1"), 1)
	b.Delete("a", 2)

	_, ts, err := b.Get("a")
	if !errors.Is(err, ErrNotFound) || ts != 2 {
		t.Fatalf("want ErrNotFound with ts=2, got ts=%d err=%v", ts, err)
	}
}

// Flip one byte on disk: Get must detect it through the CRC.
func TestCorruptionDetected(t *testing.T) {
	dir := t.TempDir()
	b := openStore(t, dir, Options{})

	b.Put("a", []byte("hello world"), 1)

	// Damage the last byte of the file (part of the value).
	f, err := os.OpenFile(dataFileName(dir, 1), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := f.Stat()
	f.WriteAt([]byte{0xFF}, info.Size()-1)
	f.Close()

	if _, _, err := b.Get("a"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

// ---- lock ------------------------------------------------------------------

func TestDirectoryLock(t *testing.T) {
	dir := t.TempDir()
	b1 := openStore(t, dir, Options{})

	// Second open of the same directory must be refused.
	if _, err := Open(dir, Options{}); !errors.Is(err, ErrLocked) {
		t.Fatalf("want ErrLocked, got %v", err)
	}

	// After closing, the directory can be opened again.
	b1.Close()
	b2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	b2.Close()
}