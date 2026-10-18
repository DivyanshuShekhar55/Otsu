//go:build unix

package store

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

// lockDir takes an exclusive lock on <dir>/LOCK and returns the open file.
// The lock lives as long as that file stays open. Closing it releases the
// lock, and the OS also releases it if our process crashes. That's why we
// use flock instead of "create a file and delete it later": a crash can't
// leave a stale lock behind.
func lockDir(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	// LOCK_NB = don't wait; fail right away if someone else holds the lock.
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrLocked
		}
		return nil, err
	}
	return f, nil
}