package engine

import "testing"

func mustBook(t *testing.T) *OrderBook {
	t.Helper()
	b, err := NewOrderBook(0, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestFIFOAndPricePriority(t *testing.T) {
	b := mustBook(t)
	_, _ = b.Process(Command{Type: CommandNew, OrderID: 1, Side: Buy, PriceTicks: 100, Qty: 3})
	_, _ = b.Process(Command{Type: CommandNew, OrderID: 2, Side: Buy, PriceTicks: 101, Qty: 2})
	_, _ = b.Process(Command{Type: CommandNew, OrderID: 3, Side: Buy, PriceTicks: 101, Qty: 2})
	events, _ := b.Process(Command{Type: CommandNew, OrderID: 9, Side: Sell, PriceTicks: 99, Qty: 5})
	if len(events) != 4 { // accepted plus three fills (2 + 2 + 1)
		t.Fatalf("events=%d, want 4", len(events))
	}
	if events[1].MakerOrderID != 2 || events[1].Qty != 2 {
		t.Fatalf("first fill=%+v", events[1])
	}
	if events[2].MakerOrderID != 3 || events[2].Qty != 2 {
		t.Fatalf("second fill=%+v", events[2])
	}
	if events[3].MakerOrderID != 1 || events[3].Qty != 1 {
		t.Fatalf("third fill=%+v", events[3])
	}
}

func TestCancelIsO1IndexedAndPoolSafe(t *testing.T) {
	b := mustBook(t)
	_, _ = b.Process(Command{Type: CommandNew, OrderID: 42, Side: Sell, PriceTicks: 120, Qty: 7})
	if b.RestingOrderCount() != 1 {
		t.Fatal("order was not resting")
	}
	events, _ := b.Process(Command{Type: CommandCancel, OrderID: 42})
	if len(events) != 1 || events[0].Type != EventCanceled || events[0].Qty != 7 {
		t.Fatalf("cancel events=%+v", events)
	}
	if b.RestingOrderCount() != 0 {
		t.Fatal("cancel index retained order")
	}
}

func TestPriceIndex(t *testing.T) {
	p := newPriceIndex(10_000)
	p.set(9999)
	p.set(3)
	p.set(4097)
	if got, _ := p.min(); got != 3 {
		t.Fatalf("min=%d", got)
	}
	if got, _ := p.max(); got != 9999 {
		t.Fatalf("max=%d", got)
	}
	p.clear(9999)
	if got, _ := p.max(); got != 4097 {
		t.Fatalf("max after clear=%d", got)
	}
}
