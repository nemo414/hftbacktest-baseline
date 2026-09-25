package engine

import "fmt"

// PriceLevel is a FIFO queue for one side and one integer price. The queue is
// a doubly linked list: appending at tail and removing a known node are both
// O(1), while Head is always the next order entitled to execution.
type PriceLevel struct {
	priceTicks int64
	head       *Order
	tail       *Order
	totalQty   int64
}

func (l *PriceLevel) append(o *Order) {
	o.Prev = l.tail
	o.Next = nil
	o.Level = l
	if l.tail == nil {
		l.head = o
	} else {
		l.tail.Next = o
	}
	l.tail = o
	l.totalQty += o.RemainingQty
}

func (l *PriceLevel) remove(o *Order) {
	if o.Prev != nil {
		o.Prev.Next = o.Next
	} else {
		l.head = o.Next
	}
	if o.Next != nil {
		o.Next.Prev = o.Prev
	} else {
		l.tail = o.Prev
	}
	l.totalQty -= o.RemainingQty
	o.Prev, o.Next, o.Level = nil, nil, nil
}

// OrderBook owns all mutable matching state. It is intentionally not internally
// locked: one Engine goroutine is the sole writer, which removes a mutex
// acquisition from every order. If callers need direct multi-goroutine access,
// wrap Process/TopOfBook with a sync.Mutex. A sync.RWMutex is useful only when
// many long-running readers justify concurrent read snapshots; it still cannot
// protect a reader from observing a partially updated intrusive list unless the
// writer holds the write lock for the whole mutation.
type OrderBook struct {
	minPriceTicks int64
	maxPriceTicks int64
	span          int

	bids []*PriceLevel
	asks []*PriceLevel
	// orders is the O(1) cancel index. It contains only resting orders;
	// fully executed takers are released immediately to orderPool.
	orders map[uint64]*Order

	bidIndex *priceIndex
	askIndex *priceIndex
	sequence uint64
}

func NewOrderBook(minPriceTicks, maxPriceTicks int64) (*OrderBook, error) {
	if maxPriceTicks < minPriceTicks {
		return nil, fmt.Errorf("max price tick %d is below min %d", maxPriceTicks, minPriceTicks)
	}
	diff := maxPriceTicks - minPriceTicks
	// When the subtraction overflows, diff is negative even though max is
	// numerically greater than min. Reject that case before adding one.
	// An explicit bound avoids a silent multi-gigabyte allocation when a
	// malformed configuration uses an unbounded integer price domain.
	if diff < 0 || diff >= 16_000_000 {
		return nil, fmt.Errorf("price range is too large; use a bounded tick ladder")
	}
	span64 := diff + 1
	span := int(span64)
	return &OrderBook{
		minPriceTicks: minPriceTicks,
		maxPriceTicks: maxPriceTicks,
		span:          span,
		bids:          make([]*PriceLevel, span),
		asks:          make([]*PriceLevel, span),
		orders:        make(map[uint64]*Order, 1024),
		bidIndex:      newPriceIndex(span),
		askIndex:      newPriceIndex(span),
	}, nil
}

func (b *OrderBook) indexOf(priceTicks int64) (int, bool) {
	if priceTicks < b.minPriceTicks || priceTicks > b.maxPriceTicks {
		return 0, false
	}
	return int(priceTicks - b.minPriceTicks), true
}

func (b *OrderBook) level(side Side, index int, priceTicks int64, create bool) *PriceLevel {
	levels := b.bids
	indexer := b.bidIndex
	if side == Sell {
		levels = b.asks
		indexer = b.askIndex
	}
	if levels[index] == nil && create {
		levels[index] = &PriceLevel{priceTicks: priceTicks}
		indexer.set(index)
	}
	return levels[index]
}

func (b *OrderBook) dropLevel(side Side, index int) {
	levels := b.bids
	indexer := b.bidIndex
	if side == Sell {
		levels = b.asks
		indexer = b.askIndex
	}
	levels[index] = nil
	indexer.clear(index)
}

func (b *OrderBook) bestLevel(side Side) *PriceLevel {
	var idx int
	var ok bool
	if side == Buy {
		idx, ok = b.bidIndex.max()
	} else {
		idx, ok = b.askIndex.min()
	}
	if !ok {
		return nil
	}
	if side == Buy {
		return b.bids[idx]
	}
	return b.asks[idx]
}

func crosses(takerSide Side, takerPrice int64, makerPrice int64) bool {
	if takerSide == Buy {
		return takerPrice >= makerPrice
	}
	return takerPrice <= makerPrice
}

func (b *OrderBook) detach(o *Order) {
	l := o.Level
	if l == nil {
		return
	}
	l.remove(o)
	if l.head == nil {
		index, _ := b.indexOf(l.priceTicks)
		b.dropLevel(o.Side, index)
	}
}

func (b *OrderBook) processNew(c Command) ([]Event, error) {
	if c.Qty <= 0 {
		return []Event{{Type: EventRejected, OrderID: c.OrderID, Code: RejectInvalidQuantity}}, nil
	}
	if _, ok := b.indexOf(c.PriceTicks); !ok {
		return []Event{{Type: EventRejected, OrderID: c.OrderID, Code: RejectInvalidPrice}}, nil
	}
	if c.Side != Buy && c.Side != Sell {
		return []Event{{Type: EventRejected, OrderID: c.OrderID, Code: RejectUnknown}}, nil
	}
	if _, exists := b.orders[c.OrderID]; exists {
		return []Event{{Type: EventRejected, OrderID: c.OrderID, Code: RejectDuplicateOrder}}, nil
	}

	b.sequence++
	taker := acquireOrder()
	taker.ID = c.OrderID
	taker.Side = c.Side
	taker.PriceTicks = c.PriceTicks
	taker.OriginalQty = c.Qty
	taker.RemainingQty = c.Qty
	taker.Sequence = b.sequence

	events := []Event{{Type: EventAccepted, OrderID: c.OrderID, Side: c.Side, Qty: c.Qty}}
	for taker.RemainingQty > 0 {
		level := b.bestLevel(c.Side.Opposite())
		if level == nil || !crosses(c.Side, taker.PriceTicks, level.priceTicks) {
			break
		}
		maker := level.head // FIFO: the oldest order is always at head.
		fillQty := taker.RemainingQty
		if maker.RemainingQty < fillQty {
			fillQty = maker.RemainingQty
		}
		maker.RemainingQty -= fillQty
		taker.RemainingQty -= fillQty
		level.totalQty -= fillQty
		events = append(events, Event{
			Type: EventFill, OrderID: taker.ID, MakerOrderID: maker.ID,
			Side: taker.Side, PriceTicks: maker.PriceTicks, Qty: fillQty,
		})
		if maker.RemainingQty == 0 {
			// RemainingQty is zero, so remove does not subtract quantity again.
			// Remove from the cancel map before returning to sync.Pool.
			level.remove(maker)
			delete(b.orders, maker.ID)
			if level.head == nil {
				idx, _ := b.indexOf(level.priceTicks)
				b.dropLevel(maker.Side, idx)
			}
			releaseOrder(maker)
		}
	}

	if taker.RemainingQty > 0 {
		idx, _ := b.indexOf(taker.PriceTicks)
		level := b.level(taker.Side, idx, taker.PriceTicks, true)
		level.append(taker)
		b.orders[taker.ID] = taker
	} else {
		releaseOrder(taker)
	}
	return events, nil
}

func (b *OrderBook) processCancel(c Command) ([]Event, error) {
	o, ok := b.orders[c.OrderID]
	if !ok {
		return []Event{{Type: EventRejected, OrderID: c.OrderID, Code: RejectOrderNotFound}}, nil
	}
	remaining := o.RemainingQty
	b.detach(o)
	delete(b.orders, o.ID)
	releaseOrder(o)
	return []Event{{Type: EventCanceled, OrderID: c.OrderID, Qty: remaining}}, nil
}

// Process must be called by the engine's owner goroutine. It returns value
// events, so pooled objects never cross the concurrency boundary.
func (b *OrderBook) Process(c Command) ([]Event, error) {
	switch c.Type {
	case CommandNew:
		return b.processNew(c)
	case CommandCancel:
		return b.processCancel(c)
	default:
		return []Event{{Type: EventRejected, OrderID: c.OrderID, Code: RejectUnknown}}, nil
	}
}

type Quote struct {
	PriceTicks int64
	Qty        int64
}

// TopOfBook is an owner-goroutine read helper. A caller outside that goroutine
// must request it through a channel or protect the whole call with a Mutex;
// copying the two pointers without synchronization would be a data race.
func (b *OrderBook) TopOfBook() (bid *Quote, ask *Quote) {
	if level := b.bestLevel(Buy); level != nil && level.head != nil {
		bid = &Quote{PriceTicks: level.priceTicks, Qty: level.totalQty}
	}
	if level := b.bestLevel(Sell); level != nil && level.head != nil {
		ask = &Quote{PriceTicks: level.priceTicks, Qty: level.totalQty}
	}
	return bid, ask
}

func (b *OrderBook) RestingOrderCount() int { return len(b.orders) }
