// Package engine contains the deterministic matching core.  The package does
// not perform network I/O; that boundary is deliberately kept in package ipc.
package engine

// Side is the aggressor/order side. Values are deliberately stable because
// they are part of the tiny binary protocol used by the Python client.
type Side uint8

const (
	Buy  Side = 0
	Sell Side = 1
)

func (s Side) Opposite() Side {
	if s == Buy {
		return Sell
	}
	return Buy
}

// CommandType is the operation requested by a producer.
type CommandType uint8

const (
	CommandNew    CommandType = 1
	CommandCancel CommandType = 2
)

// EventType is the operation result sent back to the producer.
type EventType uint8

const (
	EventAccepted EventType = 1
	EventFill     EventType = 2
	EventCanceled EventType = 3
	EventRejected EventType = 4
)

type RejectCode uint8

const (
	RejectUnknown RejectCode = iota + 1
	RejectInvalidQuantity
	RejectInvalidPrice
	RejectDuplicateOrder
	RejectOrderNotFound
	RejectQueueFull
)

// Event is intentionally a fixed-size, value-only object. It can be copied
// from the matching goroutine to a channel without retaining pooled *Order
// objects. PriceTicks is an integer tick, so no floating point rounding can
// enter matching or PnL accounting.
type Event struct {
	Type         EventType
	OrderID      uint64 // taker, accepted, canceled, or rejected order
	MakerOrderID uint64 // non-zero only for EventFill
	Side         Side   // aggressor side for EventFill
	PriceTicks   int64
	Qty          int64
	Code         RejectCode
}

// Command is passed through the engine inbox. Reply and Done are owned by the
// caller (the TCP connection in package ipc). Done lets the engine stop
// attempting to enqueue a result after a disconnected client; the channel is
// never closed by the engine.
type Command struct {
	Type       CommandType
	OrderID    uint64
	Side       Side
	PriceTicks int64
	Qty        int64
	Reply      chan<- Event
	Done       <-chan struct{}
}
