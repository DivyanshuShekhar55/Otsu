package store

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
)

const crashRecords = 5

// referenceLog writes 5 small records into a fresh store and returns the raw
// bytes of the data file, plus the offset where each record ENDS.
// The crash tests damage copies of these bytes and check what survives.
func referenceLog(t *testing.T) (data []byte, ends []int) {
	t.Helper()
	dir := t.TempDir()
	b := openStore(t, dir, Options{})

	pos := 0
	for i := 0; i < crashRecords; i++ {
		key, val := fmt.Sprintf("k%d", i), fmt.Sprintf("value-%d", i)
		b.Put(key, []byte(val), int64(i+1))
		pos += headerSize + len(key) + len(val)
		ends = append(ends, pos)
	}
	b.Close()

	data, err := os.ReadFile(dataFileName(dir, 1))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != pos {
		t.Fatalf("file is %d bytes, expected %d", len(data), pos)
	}
	return data, ends
}

// checkRecovered opens dir and expects that exactly the first `intact`
// records survived, and that the store is still usable afterwards.
func checkRecovered(t *testing.T, dir string, intact int) {
	t.Helper()

	b, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer b.Close()

	for i := 0; i < crashRecords; i++ {
		key := fmt.Sprintf("k%d", i)
		if i < intact {
			mustGet(t, b, key, fmt.Sprintf("value-%d", i))
		} else {
			mustBeGone(t, b, key)
		}
	}

	// New writes must work after recovery.
	if err := b.Put("after", []byte("ok"), 100); err != nil {
		t.Fatal(err)
	}
	mustGet(t, b, "after", "ok")
}

// Simulates a crash at EVERY possible point of the log: the file is cut
// after byte 0, 1, 2 ... up to its full length.
func TestCrashTruncateAtEveryByte(t *testing.T) {
	data, ends := referenceLog(t)
	base := t.TempDir()

	for cut := 0; cut <= len(data); cut++ {
		dir := filepath.Join(base, fmt.Sprint(cut))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dataFileName(dir, 1), data[:cut], 0o644); err != nil {
			t.Fatal(err)
		}

		// A record survives only if the cut is at or after its end.
		intact := 0
		for intact < len(ends) && ends[intact] <= cut {
			intact++
		}
		checkRecovered(t, dir, intact)
	}
}

// Flips every single byte of the log, one at a time. The damaged record and
// everything after it must be dropped; everything before it must survive.
func TestCrashFlipEveryByte(t *testing.T) {
	data, ends := referenceLog(t)
	base := t.TempDir()

	for pos := 0; pos < len(data); pos++ {
		bad := append([]byte(nil), data...) // work on a copy
		bad[pos] ^= 0xFF

		dir := filepath.Join(base, fmt.Sprint(pos))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dataFileName(dir, 1), bad, 0o644); err != nil {
			t.Fatal(err)
		}

		// The damaged record is the first one that ends after byte `pos`.
		damaged := 0
		for ends[damaged] <= pos {
			damaged++
		}
		checkRecovered(t, dir, damaged)
	}
}

// ---------------------------------------------------------------------------
// Kill during write: a REAL process is killed in the middle of writing.
//
// The test binary starts a copy of itself (TestCrashHelper below), reads its
// progress, kills it, and then reopens the directory.
// ---------------------------------------------------------------------------

// TestCrashHelper is NOT a normal test. It only does something when the
// parent test starts it with KV_CRASH_HELPER_DIR set. It writes keys forever
// and prints each number AFTER the Put for it has returned.
func TestCrashHelper(t *testing.T) {
	dir := os.Getenv("KV_CRASH_HELPER_DIR")
	if dir == "" {
		t.Skip("helper process, started by TestKillDuringWrite")
	}

	b, err := Open(dir, Options{MaxFileSize: 4096}) // small: forces rotation too
	if err != nil {
		fmt.Println("open failed:", err)
		os.Exit(1)
	}
	for i := 0; i < 1_000_000; i++ {
		if err := b.Put(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("value-%d", i)), int64(i+1)); err != nil {
			fmt.Println("put failed:", err)
			os.Exit(1)
		}
		fmt.Println(i)
	}
	os.Exit(0)
}

func TestKillDuringWrite(t *testing.T) {
	dir := t.TempDir()

	// Start a copy of this test binary that runs only TestCrashHelper.
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashHelper$")
	cmd.Env = append(os.Environ(), "KV_CRASH_HELPER_DIR="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	// Wait until the helper says it finished ~500 writes, then kill it hard.
	last := -1
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		n, err := strconv.Atoi(scanner.Text())
		if err != nil {
			cmd.Process.Kill()
			cmd.Wait()
			t.Fatalf("helper printed %q", scanner.Text())
		}
		last = n
		if n >= 500 {
			break
		}
	}
	cmd.Process.Kill()
	cmd.Wait() // returns an error because we killed it; that's expected
	if last < 500 {
		t.Fatalf("helper stopped early (last reported write: %d)", last)
	}

	// Reopen. The OS has released the directory lock because the process is gone.
	b := openStore(t, dir, Options{MaxFileSize: 4096})

	// Writes were appended in order, so what survives must be keys 0..m-1
	// with no gaps and correct values.
	m := 0
	for {
		val, _, err := b.Get(fmt.Sprintf("k%d", m))
		if err != nil {
			break
		}
		if string(val) != fmt.Sprintf("value-%d", m) {
			t.Fatalf("k%d has wrong value %q", m, val)
		}
		m++
	}
	for i := m; i < m+50; i++ {
		mustBeGone(t, b, fmt.Sprintf("k%d", i)) // no gaps after the cut
	}

	// Everything the helper reported as written must have survived.
	// (Process kill keeps data the OS already has; only a power cut could
	// lose it, since we don't fsync. That is a documented non-guarantee.)
	if m <= last {
		t.Fatalf("lost acknowledged writes: %d survived, helper reported %d", m, last+1)
	}

	// And the store keeps working.
	if err := b.Put("after", []byte("ok"), 1_000_000_000); err != nil {
		t.Fatal(err)
	}
	mustGet(t, b, "after", "ok")
}