package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// ErrNotFound is returned when a key doesn't exist or was deleted.
var ErrNotFound = errors.New("store: key not found")

// ErrLocked means another Bitcask already has this directory open.
var ErrLocked = errors.New("store: directory is locked by another process")

// defaultMaxFileSize is used when Options.MaxFileSize is not set.
const defaultMaxFileSize = 64 << 20 // 64 MB

// Options configures a store. The zero value is fine.
type Options struct {
	// MaxFileSize: once the active file would grow past this, we start a
	// new one. Tests use a tiny value so rotation happens quickly.
	MaxFileSize int64
}

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
//   - when that file gets big, we rotate: it becomes read-only, a new one starts
//   - an in-memory map (the keydir) points each key to its latest record
type Bitcask struct {
	mu sync.RWMutex // one lock guards everything below

	dir         string
	maxFileSize int64
	keydir      map[string]keydirEntry

	files    map[uint32]*os.File // every open data file, by id
	maxID    uint32              // highest file id in use (next new file is maxID+1)
	activeID uint32              // id of the file we append to
	active   *os.File            // same file as files[activeID]
	writePos int64               // where the next record will be written

	lock *os.File // holds the directory lock for as long as we are open
}

// Open opens (or creates) a store in dir.
//
// If the directory already has data files, we replay them to rebuild the
// keydir. If another process (or another Open in this process) already has
// the directory open, Open returns ErrLocked.
func Open(dir string, opts Options) (*Bitcask, error) {
	if opts.MaxFileSize <= 0 {
		opts.MaxFileSize = defaultMaxFileSize
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	// Take the lock first, before touching any data file.
	lock, err := lockDir(dir)
	if err != nil {
		return nil, err
	}

	b := &Bitcask{
		dir:         dir,
		maxFileSize: opts.MaxFileSize,
		keydir:      make(map[string]keydirEntry),
		files:       make(map[uint32]*os.File),
		lock:        lock,
	}

	if err := b.load(); err != nil {
		b.closeAll() // don't leak file handles or the lock
		return nil, err
	}
	return b, nil
}

// ---------------------------------------------------------------------------
// File names
// ---------------------------------------------------------------------------

// dataFileName gives names like "000001.data".
func dataFileName(dir string, id uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.data", id))
}

// listDataFileIDs returns the ids of all data files in dir, oldest first.
func listDataFileIDs(dir string) ([]uint32, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.data"))
	if err != nil {
		return nil, err
	}

	var ids []uint32
	for _, p := range paths {
		name := strings.TrimSuffix(filepath.Base(p), ".data")
		n, err := strconv.ParseUint(name, 10, 32)
		if err != nil {
			continue // not one of our files, ignore it
		}
		ids = append(ids, uint32(n))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// ---------------------------------------------------------------------------
// Startup recovery
// ---------------------------------------------------------------------------

// load rebuilds the in-memory state from whatever is already on disk.
func (b *Bitcask) load() error {
	// Temp files left behind by a merge that crashed are useless. Delete them.
	tmps, _ := filepath.Glob(filepath.Join(b.dir, "*.tmp"))
	for _, p := range tmps {
		os.Remove(p)
	}

	ids, err := listDataFileIDs(b.dir)
	if err != nil {
		return err
	}

	// Brand-new directory: start with file 1.
	if len(ids) == 0 {
		return b.newActiveFile(1)
	}

	// Replay files from oldest to newest.
	for i, id := range ids {
		isNewest := i == len(ids)-1
		if err := b.loadFile(id, isNewest); err != nil {
			return err
		}
	}
	return nil
}

// loadFile scans one data file and adds its records to the keydir.
//
// Only the NEWEST file can have a torn (half-written) record at its end,
// because that's the only file we were appending to when a crash happened.
//   - newest file: cut off everything from the first bad record onward
//   - older file: a bad record means real corruption, so we fail loudly
func (b *Bitcask) loadFile(id uint32, isNewest bool) error {
	flags := os.O_RDONLY
	if isNewest {
		flags = os.O_RDWR // we will keep writing at its end
	}
	f, err := os.OpenFile(dataFileName(b.dir, id), flags, 0o644)
	if err != nil {
		return err
	}
	b.files[id] = f // register right away so closeAll() can close it on error
	b.maxID = id

	validEnd, fileSize, err := b.scanFile(id, f)
	if err != nil {
		return err
	}

	if validEnd < fileSize {
		if !isNewest {
			return fmt.Errorf("%w: file %d has a bad record at offset %d", ErrCorrupt, id, validEnd)
		}
		// Torn tail: drop the garbage so new appends start from clean data.
		if err := f.Truncate(validEnd); err != nil {
			return err
		}
	}

	if isNewest {
		b.active = f
		b.activeID = id
		b.writePos = validEnd
	}
	return nil
}

// scanFile reads records from the start of f until the file ends OR it hits
// a record that is torn or fails its CRC. Good records go into the keydir.
//
// It returns validEnd: the offset right after the last good record.
// If validEnd < fileSize, there was junk after it.
func (b *Bitcask) scanFile(id uint32, f *os.File) (validEnd, fileSize int64, err error) {
	info, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	fileSize = info.Size()

	header := make([]byte, headerSize)
	for validEnd < fileSize {
		// Not even a full header left: torn write.
		if fileSize-validEnd < headerSize {
			break
		}
		if _, err := f.ReadAt(header, validEnd); err != nil {
			return 0, 0, err
		}

		// The header tells us how big the record should be.
		// If that runs past the end of the file, it's torn (or garbage).
		total := sizeFromHeader(header)
		if validEnd+total > fileSize {
			break
		}

		buf := make([]byte, total)
		if _, err := f.ReadAt(buf, validEnd); err != nil {
			return 0, 0, err
		}

		// Checksum mismatch: stop here, trust nothing after it.
		rec, decodeErr := decodeRecord(buf)
		if decodeErr != nil {
			break
		}

		// Same "newest timestamp wins" rule as live writes.
		key := string(rec.key)
		if !b.isStale(key, rec.timestamp) {
			b.keydir[key] = keydirEntry{
				fileID:    id,
				offset:    validEnd,
				size:      uint32(total),
				timestamp: rec.timestamp,
			}
		}
		validEnd += total
	}
	return validEnd, fileSize, nil
}

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

// newActiveFile creates file `id` and makes it the one we append to.
// The previous active file (if any) stays open so Get can still read it.
func (b *Bitcask) newActiveFile(id uint32) error {
	// We always write at b.writePos (the end of the file), so we don't use
	// O_APPEND. On Windows, a file opened with O_APPEND can't be truncated,
	// and on every OS Go refuses WriteAt on it.
	f, err := os.OpenFile(dataFileName(b.dir, id), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	b.files[id] = f
	b.active = f
	b.activeID = id
	b.maxID = id
	b.writePos = 0
	return nil
}

// isStale reports whether we already hold a NEWER version of key.
// (Last-write-wins by timestamp. Equal timestamps are not stale: the later
// arrival wins.)
func (b *Bitcask) isStale(key string, ts int64) bool {
	old, ok := b.keydir[key]
	return ok && old.timestamp > ts
}

// Put stores key -> value with the given timestamp.
// A write older than what we already have is silently ignored.
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
	if b.isStale(string(rec.key), rec.timestamp) {
		return nil // we already have something newer
	}

	buf := rec.encode()

	// File would get too big? Start a new one. (The `writePos > 0` check
	// makes sure one huge record still goes into an empty file.)
	if b.writePos > 0 && b.writePos+int64(len(buf)) > b.maxFileSize {
		if err := b.newActiveFile(b.maxID + 1); err != nil {
			return err
		}
	}

	n, err := b.active.WriteAt(buf, b.writePos)
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

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Closing
// ---------------------------------------------------------------------------

// Close closes all data files and releases the directory lock.
func (b *Bitcask) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closeAll()
}

// closeAll closes every file handle plus the lock. Returns the first error.
func (b *Bitcask) closeAll() error {
	var firstErr error
	for _, f := range b.files {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if b.lock != nil {
		if err := b.lock.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
