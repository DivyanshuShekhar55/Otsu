package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// ---------------------------------------------------------------------------
// Wire format (all numbers big-endian)
//
// Every message is a FRAME: a 4-byte length, then that many bytes of body.
// The length tells the reader exactly where the message ends on a stream.
//
//	frame    = | length (4) | body |
//
// Request body:
//
//	| op (1) | timestamp (8) | key_size (4) | value_size (4) | key | value |
//
// Response body:
//
//	| status (1) | timestamp (8) | value_size (4) | err_size (4) | value | err |
// ---------------------------------------------------------------------------

const (
	headerSize = 17 // both request and response headers are 17 bytes

	// A sanity limit so a corrupt length can't make us allocate gigabytes.
	maxFrameSize = 64 << 20 // 64 MB
)

var errBadFrame = errors.New("transport: malformed message")

// writeFrame sends one length-prefixed message in a single Write call.
func writeFrame(w io.Writer, body []byte) error {
	buf := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(body)))
	copy(buf[4:], body)
	_, err := w.Write(buf)
	return err
}

// readFrame reads one length-prefixed message.
// It returns io.EOF if the other side closed the connection cleanly
// before sending anything.
func readFrame(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(lenBuf[:])
	if size > maxFrameSize {
		return nil, fmt.Errorf("transport: frame of %d bytes is too large", size)
	}

	body := make([]byte, size)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return body, nil
}

func encodeRequest(r Request) []byte {
	buf := make([]byte, headerSize+len(r.Key)+len(r.Value))
	buf[0] = byte(r.Op)
	binary.BigEndian.PutUint64(buf[1:9], uint64(r.Timestamp))
	binary.BigEndian.PutUint32(buf[9:13], uint32(len(r.Key)))
	binary.BigEndian.PutUint32(buf[13:17], uint32(len(r.Value)))
	copy(buf[headerSize:], r.Key)
	copy(buf[headerSize+len(r.Key):], r.Value)
	return buf
}

func decodeRequest(b []byte) (Request, error) {
	if len(b) < headerSize {
		return Request{}, errBadFrame
	}

	op := Op(b[0])
	if op < OpGet || op > OpDelete {
		return Request{}, fmt.Errorf("transport: unknown op %d", op)
	}

	keySize := uint64(binary.BigEndian.Uint32(b[9:13]))
	valueSize := uint64(binary.BigEndian.Uint32(b[13:17]))

	// The sizes in the header must account for every remaining byte.
	if keySize+valueSize != uint64(len(b)-headerSize) {
		return Request{}, errBadFrame
	}

	keyEnd := headerSize + int(keySize)
	return Request{
		Op:        op,
		Timestamp: int64(binary.BigEndian.Uint64(b[1:9])),
		Key:       string(b[headerSize:keyEnd]),
		Value:     b[keyEnd:],
	}, nil
}

func encodeResponse(r Response) []byte {
	buf := make([]byte, headerSize+len(r.Value)+len(r.Err))
	buf[0] = byte(r.Status)
	binary.BigEndian.PutUint64(buf[1:9], uint64(r.Timestamp))
	binary.BigEndian.PutUint32(buf[9:13], uint32(len(r.Value)))
	binary.BigEndian.PutUint32(buf[13:17], uint32(len(r.Err)))
	copy(buf[headerSize:], r.Value)
	copy(buf[headerSize+len(r.Value):], r.Err)
	return buf
}

func decodeResponse(b []byte) (Response, error) {
	if len(b) < headerSize {
		return Response{}, errBadFrame
	}

	status := Status(b[0])
	if status < StatusOK || status > StatusError {
		return Response{}, fmt.Errorf("transport: unknown status %d", status)
	}

	valueSize := uint64(binary.BigEndian.Uint32(b[9:13]))
	errSize := uint64(binary.BigEndian.Uint32(b[13:17]))
	if valueSize+errSize != uint64(len(b)-headerSize) {
		return Response{}, errBadFrame
	}

	valueEnd := headerSize + int(valueSize)
	return Response{
		Status:    status,
		Timestamp: int64(binary.BigEndian.Uint64(b[1:9])),
		Value:     b[headerSize:valueEnd],
		Err:       string(b[valueEnd:]),
	}, nil
}
