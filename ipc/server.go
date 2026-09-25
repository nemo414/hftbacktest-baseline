package ipc

import (
	"context"
	"errors"
	"io"
	"log"
	"net"

	"hftbacktest/engine"
)

type Server struct {
	Engine   *engine.Engine
	Address  string
	Listener net.Listener
}

func NewServer(e *engine.Engine, address string) *Server {
	return &Server{Engine: e, Address: address}
}

// ListenAndServe accepts connections and gives each one one reader and one
// writer goroutine. The reader never writes the socket, and the writer never
// reads it, so net.Conn sees no concurrent Write calls. Commands from all
// readers converge on Engine.inbox; only the engine goroutine touches LOB data.
func (s *Server) ListenAndServe(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.Address)
	if err != nil {
		return err
	}
	s.Listener = listener
	defer listener.Close()
	go func() {
		<-ctx.Done()
		// Closing the listener wakes Accept immediately; this is preferable to
		// polling and keeps shutdown bounded even when no client is connected.
		_ = listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return ctx.Err()
			}
			return err
		}
		go s.serveConn(ctx, conn)
	}
}

func (s *Server) serveConn(parent context.Context, conn net.Conn) {
	connCtx, cancel := context.WithCancel(parent)
	defer cancel()
	defer conn.Close()

	// TCP is a byte stream, not a message bus; ReadCommand uses io.ReadFull to
	// reconstruct exactly one fixed-size frame even when the kernel fragments it.
	// The bounded channel absorbs short bursts while preserving backpressure.
	out := make(chan engine.Event, 128)
	writerDone := make(chan struct{})
	go func() {
		// Context cancellation alone does not interrupt a blocked Read. Closing
		// the connection here wakes the reader during daemon shutdown or after a
		// writer failure, so every connection goroutine can exit promptly.
		<-connCtx.Done()
		_ = conn.Close()
	}()
	go func() {
		defer close(writerDone)
		for {
			select {
			case event := <-out:
				if err := WriteEvent(conn, event); err != nil {
					cancel()
					return
				}
			case <-connCtx.Done():
				return
			}
		}
	}()

	for {
		command, err := ReadCommand(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				log.Printf("client %s: %v", conn.RemoteAddr(), err)
			}
			break
		}
		command.Reply = out
		command.Done = connCtx.Done()
		if err := s.Engine.Submit(connCtx, command); err != nil {
			break
		}
	}
	// Do not close(out): the engine may already have a command in flight. The
	// Done channel tells it to discard late events without a send-on-closed panic.
	cancel()
	<-writerDone
}
