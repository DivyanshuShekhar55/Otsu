//go:build windows

package store

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

// Windows error code for "another process has this file open".
const errSharingViolation = syscall.Errno(32)

// lockDir takes an exclusive lock on <dir>/LOCK and returns the open file.
//
// Windows trick: open the file with share mode 0, which means "nobody else
// may open this file while I have it". A second Open() then fails with a
// sharing violation. Closing the file (or the process dying) releases it.
func lockDir(dir string) (*os.File, error) {
	path := filepath.Join(dir, "LOCK")

	name, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}

	handle, err := syscall.CreateFile(
		name,
		syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		0,   // share mode 0 = exclusive, this is the lock
		nil, // default security
		syscall.OPEN_ALWAYS, // create the file if it doesn't exist
		syscall.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		if errors.Is(err, errSharingViolation) {
			return nil, ErrLocked
		}
		return nil, err
	}

	// Wrap the raw handle in an *os.File so Close() works like on unix.
	return os.NewFile(uintptr(handle), path), nil
}