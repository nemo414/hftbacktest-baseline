package engine

import (
	"context"
	"errors"
)

var ErrEngineStopped = errors.New("matching engine stopped")

// Engine is a single-writer actor around OrderBook. Producers may be many
// goroutines (TCP connections, replay workers, or tests), but only Run reads
// inbox and mutates maps/lists. This ownership model avoids race-prone locks in
// the hot path and gives deterministic FIFO ordering: the sequence in which
// commands enter inbox is the sequence the book observes.
type Engine struct {
	book  *OrderBook
	inbox chan Command
}

func NewEngine(book *OrderBook, queueCapacity int) *Engine {
	if queueCapacity < 1 {
		queueCapacity = 1
	}
	return &Engine{book: book, inbox: make(chan Command, queueCapacity)}
}

// Run should execute in exactly one goroutine. A second Run would violate the
// single-writer invariant and create races even though the channel itself is
// safe for multiple senders.
func (e *Engine) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case command := <-e.inbox:
			e.handle(command)
		}
	}
}

func (e *Engine) handle(command Command) {
	events, err := e.book.Process(command)
	if err != nil {
		events = []Event{{Type: EventRejected, OrderID: command.OrderID, Code: RejectUnknown}}
	}
	if command.Reply == nil {
		return
	}
	for _, event := range events {
		// A disconnected client closes Done. Selecting on Done prevents a
		// slow/dead writer from wedging the sole matching goroutine forever.
		select {
		case command.Reply <- event:
		case <-command.Done:
			return
		}
	}
}

// Submit applies backpressure at the bounded inbox instead of allocating an
// unbounded per-connection queue. Callers can map context.DeadlineExceeded to
// a transport-level timeout and retry or drop the order according to policy.
func (e *Engine) Submit(ctx context.Context, command Command) error {
	select {
	case e.inbox <- command:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Engine) Book() *OrderBook { return e.book }
