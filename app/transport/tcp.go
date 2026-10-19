package transport

import (
	"context"
	"net"
	"sync"
	"time"
)

// defaultTimeout is used when the caller's context has no deadline, so a
// dead node can never make a request hang forever.
const defaultTimeout = 5 * time.Second

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

// TCPTransport is the default Transport: plain TCP with the frames from
// codec.go. It opens a new connection for every request. That is the
// simplest thing that works; connection reuse can come later if the
// benchmark shows it matters.
type TCPTransport struct{}

// NewTCP returns a ready-to-use TCP transport.
func NewTCP() *TCPTransport { return &TCPTransport{} }

// Compile-time check that we really implement the interface.
var _ Transport = (*TCPTransport)(nil)

// Send connects to addr, sends req, and waits for one response.
func (t *TCPTransport) Send(ctx context.Context, addr string, req Request) (Response, error) {
	// Make sure there is always a deadline.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultTimeout)
		defer cancel()
	}

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return Response{}, err
	}
	defer conn.Close()

	// The deadline covers the write and the wait for the answer too.
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return Response{}, err
	}

	if err := writeFrame(conn, encodeRequest(req)); err != nil {
		return Response{}, err
	}

	body, err := readFrame(conn)
	if err != nil {
		return Response{}, err
	}
	return decodeResponse(body)
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

// Server accepts TCP connections and passes each request to a Handler.
type Server struct {
	listener net.Listener
	handler  Handler
	wg       sync.WaitGroup // waits for the accept loop and every connection

	mu     sync.Mutex
	conns  map[net.Conn]struct{} // open connections, so Close can cut them
	closed bool
}

// Listen starts a server on addr (use "127.0.0.1:0" to let the OS pick a
// free port, then read it back with Addr).
func Listen(addr string, handler Handler) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	s := &Server{
		listener: ln,
		handler:  handler,
		conns:    make(map[net.Conn]struct{}),
	}
	s.wg.Add(1)
	go s.acceptLoop()
	return s, nil
}

// Addr is the address the server is really listening on.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Close stops accepting, drops every open connection, and waits until all
// handlers have returned.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()

	err := s.listener.Close()
	s.wg.Wait()
	return err
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return // listener was closed
		}
		if !s.track(conn) { // we are shutting down
			conn.Close()
			continue
		}
		s.wg.Add(1)
		go s.serveConn(conn)
	}
}

// track remembers an open connection. It returns false if the server is
// already closed.
func (s *Server) track(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.conns[c] = struct{}{}
	return true
}

func (s *Server) untrack(c net.Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

// serveConn answers requests on one connection until the client hangs up.
func (s *Server) serveConn(conn net.Conn) {
	defer s.wg.Done()
	defer s.untrack(conn)
	defer conn.Close()

	for {
		body, err := readFrame(conn)
		if err != nil {
			return // client closed the connection, or it broke
		}

		req, err := decodeRequest(body)
		if err != nil {
			return // garbage on the wire: drop the connection
		}

		resp := s.handler.Handle(req)

		if err := writeFrame(conn, encodeResponse(resp)); err != nil {
			return
		}
	}
}
