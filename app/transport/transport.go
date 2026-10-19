// Package transport moves requests between nodes.
//
// It knows nothing about storage or quorums. It only carries a small
// request to an address and brings back a response. Keeping it this thin
// means the fault simulator (later) can swap in a fake network that drops,
// delays or duplicates messages, without touching the cluster logic.
package transport

import "context"

// Op says what a request wants to do.
type Op uint8

const (
	OpGet    Op = 1
	OpPut    Op = 2
	OpDelete Op = 3
)

// Status is the outcome of a request, as reported by the node that handled it.
type Status uint8

const (
	StatusOK       Status = 1 // worked
	StatusNotFound Status = 2 // key missing or deleted (Timestamp may hold the tombstone's time)
	StatusError    Status = 3 // the node failed; see Response.Err
)

// Request is one message to a node.
type Request struct {
	Op        Op
	Key       string
	Value     []byte // only used by OpPut
	Timestamp int64  // only used by OpPut and OpDelete
}

// Response is the node's answer.
type Response struct {
	Status    Status
	Value     []byte // for a successful Get
	Timestamp int64  // version of the value (or tombstone) that was found or written
	Err       string // human-readable reason when Status is StatusError
}

// Transport is the CLIENT side: "send this request to that address".
//
// Send returns an error only when the NETWORK failed (cannot connect,
// timeout, broken connection). If the node answered, even with
// StatusError, the error is nil. The coordinator needs this difference:
// "node unreachable" and "node said no" are counted differently.
//
// Use a context with a deadline to set the timeout.
type Transport interface {
	Send(ctx context.Context, addr string, req Request) (Response, error)
}

// Handler is the SERVER side: what a node does with an incoming request.
// The node server (next TODO item) will implement it on top of its Store.
type Handler interface {
	Handle(req Request) Response
}

// HandlerFunc lets a plain function act as a Handler (handy in tests).
type HandlerFunc func(req Request) Response

// Handle calls f.
func (f HandlerFunc) Handle(req Request) Response { return f(req) }
