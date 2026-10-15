package store 
 
// This is the storage engine based on the Bitcask paper 2010

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ErrNotFound is returned when a key doesn't exist or was deleted.
var ErrNotFound = errors.New("store: key not found")

// keydirEntry says where the LATEST record for a key lives on disk.
// Get() uses it to do exactly one disk read.
type keydirEntry struct {
	fileID    uint32 // which data file
	offset    int64  // where the record starts in that file
	size      uint32 // total record size in bytes (header + key + value)
	timestamp int64  // timestamp of that record
}

// Bitcask is a tiny log-structured key/value store:
//   - all writes are appended to one "active" file
//   - an in-memory map (the keydir) points each key to its latest record
type Bitcask struct {
	mu sync.RWMutex // one lock guards everything below

	dir    string
	keydir map[string]keydirEntry // key to keydir-entry

	files    map[uint32]*os.File // every open data file, by id
	activeID uint32              // id of the file we append to
	active   *os.File            // same file as files[activeID]
	writePos int64               // where the next record will be written
}

// Open opens (or creates) a store in dir.
//
// NOTE: for now the keydir starts empty. Rebuilding it from existing
// files is the "startup recovery" item in the TODO.
func Open(dir string) (*Bitcask, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	b := &Bitcask{
		dir:      dir,
		keydir:   make(map[string]keydirEntry),
		files:    make(map[uint32]*os.File),
		activeID: 1,
	}

	// O_APPEND: the OS always writes at the end of the file, which is
	// exactly what an append-only log wants.
	f, err := os.OpenFile(dataFileName(dir, b.activeID),
		os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	// If the file already has data, continue writing after it.
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	b.active = f
	b.files[b.activeID] = f
	b.writePos = info.Size()
	return b, nil
}

// dataFileName gives names like "000001.data".
// so technically we are using 10^6 files for now
func dataFileName(dir string, id uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.data", id))
}

// Put stores key -> value with the given timestamp.
func (b *Bitcask) Put(key string, value []byte, ts int64) error {
	if key == "" {
		return errors.New("store: empty key")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	return b.write(record{timestamp: ts, key: []byte(key), value: value})
}

// Delete writes a tombstone for key.
//
// We write the tombstone even if we have never seen the key. Later, in the
// quorum layer, a replica that missed the original Put must still remember
// the delete, otherwise old data could come back.
func (b *Bitcask) Delete(key string, ts int64) error {
	if key == "" {
		return errors.New("store: empty key")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	return b.write(record{timestamp: ts, key: []byte(key), tombstone: true})
}

// write appends one record to the active file and points the keydir at it.
// The caller must already hold b.mu.
func (b *Bitcask) write(rec record) error {
	buf := rec.encode()

	n, err := b.active.Write(buf)
	// Move the position by what was ACTUALLY written, even on error, so
	// our offsets never drift from the real file size.
	offset := b.writePos
	b.writePos += int64(n)
	if err != nil {
		return err
	}

	// Only update the keydir after the bytes are safely written.
	b.keydir[string(rec.key)] = keydirEntry{
		fileID:    b.activeID,
		offset:    offset,
		size:      uint32(n),
		timestamp: rec.timestamp,
	}
	return nil
}

// Get returns the value and timestamp for key.
//
// If the key was deleted, it returns ErrNotFound but still gives back the
// tombstone's timestamp (the quorum layer will need that to compare versions).
// If the key was never written, the timestamp is 0.
func (b *Bitcask) Get(key string) ([]byte, int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// 1. Ask the keydir where the latest record is.
	entry, ok := b.keydir[key]
	if !ok {
		return nil, 0, ErrNotFound
	}

	// 2. Read exactly that record from disk (one read).
	buf := make([]byte, entry.size)
	if _, err := b.files[entry.fileID].ReadAt(buf, entry.offset); err != nil {
		return nil, 0, err
	}

	// 3. Check the CRC and parse it.
	rec, err := decodeRecord(buf)
	if err != nil {
		return nil, 0, err
	}

	if rec.tombstone {
		return nil, rec.timestamp, ErrNotFound
	}
	return rec.value, rec.timestamp, nil
}

// Close closes all data files.
func (b *Bitcask) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var firstErr error
	for _, f := range b.files {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}