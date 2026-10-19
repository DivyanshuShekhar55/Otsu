package transport

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"
)

// ---- wire format -----------------------------------------------------------

func TestRequestRoundTrip(t *testing.T) {
	in := Request{Op: OpPut, Key: "hello", Value: []byte("world"), Timestamp: 42}
	out, err := decodeRequest(encodeRequest(in))
	if err != nil {
		t.Fatal(err)
	}
	if out.Op != OpPut || out.Key != "hello" || !bytes.Equal(out.Value, []byte("world")) || out.Timestamp != 42 {
		t.Fatalf("round trip mismatch: %+v", out)
	}
}

func TestResponseRoundTrip(t *testing.T) {
	in := Response{Status: StatusError, Value: []byte("v"), Timestamp: 7, Err: "disk full"}
	out, err := decodeResponse(encodeResponse(in))
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != StatusError || !bytes.Equal(out.Value, []byte("v")) || out.Timestamp != 7 || out.Err != "disk full" {
		t.Fatalf("round trip mismatch: %+v", out)
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	good := encodeRequest(Request{Op: OpGet, Key: "k"})

	if _, err := decodeRequest(good[:5]); err == nil {
		t.Fatal("short message should fail")
	}
	if _, err := decodeRequest(append(good, 0xFF)); err == nil {
		t.Fatal("extra trailing byte should fail")
	}

	badOp := append([]byte(nil), good...)
	badOp[0] = 99
	if _, err := decodeRequest(badOp); err == nil {
		t.Fatal("unknown op should fail")
	}

	if _, err := decodeResponse([]byte{1, 2, 3}); err == nil {
		t.Fatal("short response should fail")
	}
}

// ---- TCP -------------------------------------------------------------------

// startServer runs a server around h and closes it when the test ends.
func startServer(t *testing.T, h Handler) *Server {
	t.Helper()
	srv, err := Listen("127.0.0.1:0", h)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	return srv
}

func TestTCPRoundTrip(t *testing.T) {
	// A tiny in-memory "node" that understands Put and Get.
	var mu sync.Mutex
	data := map[string]string{}

	srv := startServer(t, HandlerFunc(func(req Request) Response {
		mu.Lock()
		defer mu.Unlock()
		switch req.Op {
		case OpPut:
			data[req.Key] = string(req.Value)
			return Response{Status: StatusOK, Timestamp: req.Timestamp}
		case OpGet:
			v, ok := data[req.Key]
			if !ok {
				return Response{Status: StatusNotFound}
			}
			return Response{Status: StatusOK, Value: []byte(v)}
		}
		return Response{Status: StatusError, Err: "unsupported"}
	}))

	tr := NewTCP()
	ctx := context.Background()

	// Put then Get.
	resp, err := tr.Send(ctx, srv.Addr(), Request{Op: OpPut, Key: "a", Value: []byte("1"), Timestamp: 5})
	if err != nil || resp.Status != StatusOK || resp.Timestamp != 5 {
		t.Fatalf("put: %+v, %v", resp, err)
	}
	resp, err = tr.Send(ctx, srv.Addr(), Request{Op: OpGet, Key: "a"})
	if err != nil || resp.Status != StatusOK || string(resp.Value) != "1" {
		t.Fatalf("get: %+v, %v", resp, err)
	}

	// Missing key comes back as a normal response, not a network error.
	resp, err = tr.Send(ctx, srv.Addr(), Request{Op: OpGet, Key: "nope"})
	if err != nil || resp.Status != StatusNotFound {
		t.Fatalf("missing key: %+v, %v", resp, err)
	}

	// A node-side error also comes back as a response with err == nil.
	resp, err = tr.Send(ctx, srv.Addr(), Request{Op: OpDelete, Key: "a", Timestamp: 9})
	if err != nil || resp.Status != StatusError || resp.Err != "unsupported" {
		t.Fatalf("delete: %+v, %v", resp, err)
	}
}

// A slow node must not hold the caller past its deadline.
func TestSendTimeout(t *testing.T) {
	srv := startServer(t, HandlerFunc(func(req Request) Response {
		time.Sleep(300 * time.Millisecond)
		return Response{Status: StatusOK}
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := NewTCP().Send(ctx, srv.Addr(), Request{Op: OpGet, Key: "k"})
	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("Send took %v, the deadline was 50ms", elapsed)
	}
}

// Nobody is listening: Send must return an error, not hang.
func TestSendToDeadNode(t *testing.T) {
	srv, err := Listen("127.0.0.1:0", HandlerFunc(func(Request) Response { return Response{Status: StatusOK} }))
	if err != nil {
		t.Fatal(err)
	}
	addr := srv.Addr()
	srv.Close() // now the port is closed

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := NewTCP().Send(ctx, addr, Request{Op: OpGet, Key: "k"}); err == nil {
		t.Fatal("expected an error for a dead node")
	}
}
