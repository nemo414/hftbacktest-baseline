// Package ipc implements a small fixed-size binary protocol. Fixed frames
// avoid JSON parsing, reflection, and per-message allocations on the local
// Python↔Go boundary.
package ipc

import (
	"encoding/binary"
	"fmt"
	"io"

	"hftbacktest/engine"
)

const (
	OrderFrameSize = 26 // type(1) + id(8) + side(1) + price(8) + qty(8)
	EventFrameSize = 34 // type(1) + order id(8) + maker id(8) + price(8) + qty(8) + side/code(1)
)

func ReadCommand(r io.Reader) (engine.Command, error) {
	var frame [OrderFrameSize]byte
	if _, err := io.ReadFull(r, frame[:]); err != nil {
		return engine.Command{}, err
	}
	c := engine.Command{
		Type:       engine.CommandType(frame[0]),
		OrderID:    binary.BigEndian.Uint64(frame[1:9]),
		Side:       engine.Side(frame[9]),
		PriceTicks: int64(binary.BigEndian.Uint64(frame[10:18])),
		Qty:        int64(binary.BigEndian.Uint64(frame[18:26])),
	}
	if c.Type != engine.CommandNew && c.Type != engine.CommandCancel {
		return engine.Command{}, fmt.Errorf("unknown command type %d", c.Type)
	}
	if c.Type == engine.CommandNew && c.Side != engine.Buy && c.Side != engine.Sell {
		return engine.Command{}, fmt.Errorf("unknown side %d", c.Side)
	}
	return c, nil
}

func WriteEvent(w io.Writer, e engine.Event) error {
	var frame [EventFrameSize]byte
	frame[0] = byte(e.Type)
	binary.BigEndian.PutUint64(frame[1:9], e.OrderID)
	binary.BigEndian.PutUint64(frame[9:17], e.MakerOrderID)
	binary.BigEndian.PutUint64(frame[17:25], uint64(e.PriceTicks))
	binary.BigEndian.PutUint64(frame[25:33], uint64(e.Qty))
	if e.Type == engine.EventRejected {
		frame[33] = byte(e.Code)
	} else {
		frame[33] = byte(e.Side)
	}
	_, err := w.Write(frame[:])
	return err
}
