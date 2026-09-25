package engine

import "sync"

// Order is an intrusive list node. prev/next are maintained by its
// PriceLevel, so enqueue, dequeue-at-head, and arbitrary cancel do not need a
// second allocation or a linear search through the FIFO queue.
type Order struct {
	ID           uint64
	Side         Side
	PriceTicks   int64
	OriginalQty  int64
	RemainingQty int64
	Sequence     uint64
	Prev         *Order
	Next         *Order
	Level        *PriceLevel
}

// orderPool is a process-local freelist. sync.Pool may discard entries at a
// GC cycle, so it is a performance hint rather than a correctness mechanism.
// The book removes an order from every index before putting it back, then
// clears all links in releaseOrder. That prevents a future borrower from
// retaining a large object graph through stale Prev/Next/Level pointers.
var orderPool = sync.Pool{New: func() any { return new(Order) }}

func acquireOrder() *Order {
	o := orderPool.Get().(*Order)
	*o = Order{} // reset fields while the object is exclusively owned
	return o
}

func releaseOrder(o *Order) {
	if o == nil {
		return
	}
	// The caller must already have removed the order from the book's map and
	// level. Clearing again here makes accidental reuse fail closed in tests.
	*o = Order{}
	orderPool.Put(o)
}
