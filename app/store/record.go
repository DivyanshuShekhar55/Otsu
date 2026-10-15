package store

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
)

// ---------------------------------------------------------------------------
// On-disk record layout (all numbers are big-endian):
//
//	+---------+-----------+----------+------------+-----+-------+
//	| crc (4) | ts (8)    | ksz (4)  | vsz (4)    | key | value |
//	+---------+-----------+----------+------------+-----+-------+
//	|<------------ header = 20 bytes ------------>|
//
// - crc covers EVERYTHING after the crc field (ts, sizes, key, value).
//   If any byte changes on disk, the crc will no longer match.
// - A tombstone (delete marker) has vsz = 0xFFFFFFFF and no value bytes.
// ---------------------------------------------------------------------------

const (
	headerSize      = 20
	tombstoneMarker = math.MaxUint32 // stored in the value_size field
)

// ErrCorrupt means a record failed its checksum or has a broken shape.
var ErrCorrupt = errors.New("store: corrupt record")

// record is the in-memory form of one log entry.
type record struct {
	timestamp int64
	key       []byte
	value     []byte
	tombstone bool // true = this record means "key was deleted"
}

// encode turns a record into the exact bytes written to disk.
func (r *record) encode() []byte {
	valueSize := len(r.value)
	valueField := uint32(valueSize) // what we write in the vsz field

	if r.tombstone {
		valueSize = 0                 // no value bytes follow
		valueField = tombstoneMarker // vsz says "I'm a tombstone"
	}

	buf := make([]byte, headerSize+len(r.key)+valueSize)

	// Fill the header (leave bytes 0..3 for the crc, we fill it last).
	binary.BigEndian.PutUint64(buf[4:12], uint64(r.timestamp))
	binary.BigEndian.PutUint32(buf[12:16], uint32(len(r.key)))
	binary.BigEndian.PutUint32(buf[16:20], valueField)

	// Then the key and value bytes.
	copy(buf[headerSize:], r.key)
	copy(buf[headerSize+len(r.key):], r.value)

	// Checksum everything after the crc field, then store it at the front.
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], crc)

	return buf
}

// decodeRecord parses bytes read from disk back into a record.
// It returns ErrCorrupt if the bytes are too short, the sizes don't add up,
// or the checksum doesn't match.
func decodeRecord(buf []byte) (record, error) {
	if len(buf) < headerSize {
		return record{}, ErrCorrupt
	}

	storedCRC := binary.BigEndian.Uint32(buf[0:4])
	timestamp := int64(binary.BigEndian.Uint64(buf[4:12]))
	keySize := int(binary.BigEndian.Uint32(buf[12:16]))
	valueField := binary.BigEndian.Uint32(buf[16:20])

	tombstone := valueField == tombstoneMarker
	valueSize := int(valueField)
	if tombstone {
		valueSize = 0
	}

	// The buffer must be exactly one whole record.
	if len(buf) != headerSize+keySize+valueSize {
		return record{}, ErrCorrupt
	}

	// Recompute the checksum and compare with the one stored on disk.
	if crc32.ChecksumIEEE(buf[4:]) != storedCRC {
		return record{}, ErrCorrupt
	}

	keyEnd := headerSize + keySize
	return record{
		timestamp: timestamp,
		key:       buf[headerSize:keyEnd],
		value:     buf[keyEnd:],
		tombstone: tombstone,
	}, nil
}

// sizeFromHeader reads the first headerSize bytes of a record and says how
// long the WHOLE record is (header + key + value). Recovery uses this to know
// how many bytes to read next.
func sizeFromHeader(header []byte) int64 {
	keySize := int64(binary.BigEndian.Uint32(header[12:16]))
	valueField := binary.BigEndian.Uint32(header[16:20])

	valueSize := int64(valueField)
	if valueField == tombstoneMarker {
		valueSize = 0 // tombstones carry no value bytes
	}
	return headerSize + keySize + valueSize
}