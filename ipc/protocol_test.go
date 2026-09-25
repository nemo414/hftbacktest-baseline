package ipc

import (
	"bytes"
	"encoding/binary"
	"testing"

	"hftbacktest/engine"
)

func TestReadCommandUsesFixedBigEndianFrame(t *testing.T) {
	var frame [OrderFrameSize]byte
	frame[0] = byte(engine.CommandNew)
	binary.BigEndian.PutUint64(frame[1:9], 77)
	frame[9] = byte(engine.Sell)
	binary.BigEndian.PutUint64(frame[10:18], uint64(12345))
	binary.BigEndian.PutUint64(frame[18:26], uint64(9))
	command, err := ReadCommand(bytes.NewReader(frame[:]))
	if err != nil {
		t.Fatal(err)
	}
	if command.OrderID != 77 || command.Side != engine.Sell || command.PriceTicks != 12345 || command.Qty != 9 {
		t.Fatalf("decoded command=%+v", command)
	}
}

func TestWriteEventHasStableSizeAndSignedFields(t *testing.T) {
	var wire bytes.Buffer
	err := WriteEvent(&wire, engine.Event{
		Type: engine.EventFill, OrderID: 7, MakerOrderID: 8,
		Side: engine.Buy, PriceTicks: -12, Qty: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if wire.Len() != EventFrameSize {
		t.Fatalf("frame size=%d", wire.Len())
	}
	if got := int64(binary.BigEndian.Uint64(wire.Bytes()[17:25])); got != -12 {
		t.Fatalf("price=%d", got)
	}
}
