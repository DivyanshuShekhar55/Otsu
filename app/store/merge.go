package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

// ---------------------------------------------------------------------------
// Hint file format
//
// A hint file sits next to a data file (000007.hint for 000007.data) and
// lists "where is each key" WITHOUT the values. Reading it on startup is much
// faster than scanning the whole data file. Merge writes them, and Open reads
// them back (loadHintFile below), falling back to a full scan if anything is
// wrong with the hint file.
//
//	| crc (4) | ts (8) | key_size (4) | record_size (4) | offset (8) | key |
//
// crc covers everything after itself, same idea as data records.
// ---------------------------------------------------------------------------

const hintHeaderSize = 28

func hintFileName(dir string, id uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.hint", id))
}

func encodeHint(key string, e keydirEntry) []byte {
	buf := make([]byte, hintHeaderSize+len(key))
	binary.BigEndian.PutUint64(buf[4:12], uint64(e.timestamp))
	binary.BigEndian.PutUint32(buf[12:16], uint32(len(key)))
	binary.BigEndian.PutUint32(buf[16:20], e.size)
	binary.BigEndian.PutUint64(buf[20:28], uint64(e.offset))
	copy(buf[hintHeaderSize:], key)
	binary.BigEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(buf[4:]))
	return buf
}

// hintRecord is one parsed hint line: a key and where it lives.
type hintRecord struct {
	key   string
	entry keydirEntry
}

// decodeHints parses a whole hint file. If ANY entry is damaged or cut
// short it returns ErrCorrupt, so the caller never uses half a hint file.
func decodeHints(data []byte, fileID uint32) ([]hintRecord, error) {
	var out []hintRecord

	pos := 0
	for pos < len(data) {
		// Need a full header first.
		if len(data)-pos < hintHeaderSize {
			return nil, ErrCorrupt
		}
		h := data[pos : pos+hintHeaderSize]

		// The key must fit inside what is left of the file.
		keySize := binary.BigEndian.Uint32(h[12:16])
		if uint64(keySize) > uint64(len(data)-pos-hintHeaderSize) {
			return nil, ErrCorrupt
		}
		end := pos + hintHeaderSize + int(keySize)

		// Checksum covers everything after the crc field.
		if crc32.ChecksumIEEE(data[pos+4:end]) != binary.BigEndian.Uint32(h[0:4]) {
			return nil, ErrCorrupt
		}

		out = append(out, hintRecord{
			key: string(data[pos+hintHeaderSize : end]),
			entry: keydirEntry{
				fileID:    fileID,
				timestamp: int64(binary.BigEndian.Uint64(h[4:12])),
				size:      binary.BigEndian.Uint32(h[16:20]),
				offset:    int64(binary.BigEndian.Uint64(h[20:28])),
			},
		})
		pos = end
	}
	return out, nil
}

// loadHintFile fills the keydir from <id>.hint instead of scanning the data
// file. It returns true on success. It returns false (and changes NOTHING)
// if the hint file is missing, damaged, or points outside the data file; the
// caller then falls back to a full scan.
//
// We do not re-check the data records' CRCs here. That is the whole point of
// the speed-up. Get() still verifies the CRC every time it reads a record.
func (b *Bitcask) loadHintFile(id uint32, dataSize int64) bool {
	raw, err := os.ReadFile(hintFileName(b.dir, id))
	if err != nil {
		return false // no hint file: the normal case for non-merged files
	}

	// Parse everything first, so a bad hint file can't leave us with a
	// half-filled keydir.
	hints, err := decodeHints(raw, id)
	if err != nil {
		return false
	}
	for _, h := range hints {
		if h.entry.offset < 0 || h.entry.offset+int64(h.entry.size) > dataSize {
			return false
		}
	}

	// Same "newest timestamp wins" rule as everywhere else.
	for _, h := range hints {
		if !b.isStale(h.key, h.entry.timestamp) {
			b.keydir[h.key] = h.entry
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Merge
// ---------------------------------------------------------------------------

// Merge compacts the store: it copies only the LATEST record of every key
// into one new data file (plus a hint file), then deletes all the old files.
// Overwritten values and old versions disappear, so disk space is reclaimed.
//
// Simple on purpose: Merge holds the lock the whole time, so reads and writes
// wait until it finishes. A real Bitcask merges in the background.
//
// Tombstones (delete markers) are kept for Options.TombstoneGrace. Dropping
// one too early is dangerous in a cluster: a replica that missed the delete
// could later "repair" the old value back into existence. After the grace
// period we assume every replica has seen the delete and forget it.
func (b *Bitcask) Merge() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	mergeID := b.maxID + 1

	// Write to temp names first. A crash in the middle then leaves only
	// *.tmp files, which Open() deletes. The old files stay untouched.
	dataTmp := filepath.Join(b.dir, "merge.data.tmp")
	hintTmp := filepath.Join(b.dir, "merge.hint.tmp")
	finalData := dataFileName(b.dir, mergeID)
	finalHint := hintFileName(b.dir, mergeID)

	newKeydir, err := b.writeMergeFiles(dataTmp, hintTmp, mergeID)
	if err != nil {
		os.Remove(dataTmp)
		os.Remove(hintTmp)
		return err
	}

	// Give the files their real names. writeMergeFiles already closed them:
	// on Windows a file that is still open cannot be renamed.
	if err := os.Rename(dataTmp, finalData); err != nil {
		os.Remove(dataTmp)
		os.Remove(hintTmp)
		return err
	}
	// The hint file is only a speed-up, so we ignore a failure here.
	_ = os.Rename(hintTmp, finalHint)

	// Open the merged file again, this time as the read handle for Get.
	mergeFile, err := os.Open(finalData)
	if err != nil {
		os.Remove(finalData)
		os.Remove(finalHint)
		return err
	}

	// From here on nothing can fail halfway: switch our in-memory state
	// over to the merged file.
	oldFiles := b.files
	b.files = map[uint32]*os.File{mergeID: mergeFile}
	b.keydir = newKeydir
	b.maxID = mergeID

	// Delete the old files. (Close first: Windows can't delete open files.)
	// If a delete fails the leftovers are harmless: they only hold older
	// copies, and recovery keeps the newest timestamp.
	for id, f := range oldFiles {
		f.Close()
		os.Remove(dataFileName(b.dir, id))
		os.Remove(hintFileName(b.dir, id))
	}

	// The merged file is complete and read-only now. New writes need a
	// fresh active file.
	return b.newActiveFile(mergeID + 1)
}

// writeMergeFiles copies every live record into dataPath and writes the
// matching hint entries to hintPath. It returns the NEW keydir. Both files
// are closed when it returns. It does not touch any store state, so on error
// nothing needs to be undone except deleting the temp files.
func (b *Bitcask) writeMergeFiles(dataPath, hintPath string, mergeID uint32) (map[string]keydirEntry, error) {
	dataFile, err := os.OpenFile(dataPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	defer dataFile.Close()

	hintFile, err := os.OpenFile(hintPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	defer hintFile.Close()

	dataW := bufio.NewWriter(dataFile) // buffered: far fewer write calls
	hintW := bufio.NewWriter(hintFile)

	newKeydir := make(map[string]keydirEntry, len(b.keydir))
	var pos int64 // where the next record lands in the merged file
	now := b.now() // read the clock once so every tombstone is judged the same way

	// The keydir only holds the newest record per key, so everything we
	// walk here is "live". Old overwritten versions are simply never copied.
	for key, e := range b.keydir {
		buf := make([]byte, e.size)
		if _, err := b.files[e.fileID].ReadAt(buf, e.offset); err != nil {
			return nil, err
		}

		// Verify the checksum so we never copy a damaged record forward.
		rec, err := decodeRecord(buf)
		if err != nil {
			return nil, fmt.Errorf("merge: key %q: %w", key, err)
		}

		// A delete marker older than the grace period is forgotten: we
		// simply don't copy it. Nothing older survives either, because we
		// only ever copy the newest record per key.
		if rec.tombstone && now-rec.timestamp > int64(b.tombstoneGrace) {
			continue
		}

		// Copy the raw bytes as they are.
		if _, err := dataW.Write(buf); err != nil {
			return nil, err
		}

		newEntry := keydirEntry{
			fileID:    mergeID,
			offset:    pos,
			size:      e.size,
			timestamp: e.timestamp,
		}
		if _, err := hintW.Write(encodeHint(key, newEntry)); err != nil {
			return nil, err
		}

		newKeydir[key] = newEntry
		pos += int64(e.size)
	}

	// Push buffered bytes to the OS, then to the disk.
	if err := dataW.Flush(); err != nil {
		return nil, err
	}
	if err := hintW.Flush(); err != nil {
		return nil, err
	}
	if err := dataFile.Sync(); err != nil {
		return nil, err
	}
	if err := hintFile.Sync(); err != nil {
		return nil, err
	}

	return newKeydir, nil
}