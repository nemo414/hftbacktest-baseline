#!/usr/bin/env python3
"""Tiny market-maker/replay client for the fixed-size Go IPC protocol.

Prices are integer ticks throughout. The client deliberately uses one reader
thread and a queue: TCP responses can arrive while the strategy is sending the
next burst, and a blocked response reader must never stall the producer.
"""

from __future__ import annotations

import argparse
import random
import socket
import struct
import threading
import time
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Iterable

BUY = 0
SELL = 1
NEW = 1
CANCEL = 2
ACCEPTED = 1
FILL = 2
REJECTED = 4

ORDER_FRAME = struct.Struct("!BQbqq")
EVENT_FRAME = struct.Struct("!BQQqqb")


@dataclass(frozen=True)
class Event:
    kind: int
    order_id: int
    maker_order_id: int
    price_ticks: int
    qty: int
    side_or_code: int


@dataclass(frozen=True)
class Tick:
    timestamp_ns: int
    mid_ticks: int


class BinaryOrderClient:
    def __init__(self, host: str, port: int):
        self.sock = socket.create_connection((host, port))
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.events: Queue[Event] = Queue()
        self._stop = threading.Event()
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

    def _read_exact(self, size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            chunk = self.sock.recv(size - len(chunks))
            if not chunk:
                raise ConnectionError("Go engine closed the connection")
            chunks.extend(chunk)
        return bytes(chunks)

    def _read_loop(self) -> None:
        try:
            while not self._stop.is_set():
                raw = self._read_exact(EVENT_FRAME.size)
                self.events.put(Event(*EVENT_FRAME.unpack(raw)))
        except (OSError, ConnectionError):
            if not self._stop.is_set():
                self.events.put(Event(REJECTED, 0, 0, 0, 0, 255))

    def send(self, kind: int, order_id: int, side: int = BUY, price_ticks: int = 0, qty: int = 0) -> None:
        self.sock.sendall(ORDER_FRAME.pack(kind, order_id, side, price_ticks, qty))

    def new(self, order_id: int, side: int, price_ticks: int, qty: int) -> None:
        self.send(NEW, order_id, side, price_ticks, qty)

    def cancel(self, order_id: int) -> None:
        self.send(CANCEL, order_id)

    def drain(self, timeout: float = 0.0) -> list[Event]:
        """Drain responses; a short timeout lets a final burst flush."""
        result: list[Event] = []
        if timeout:
            time.sleep(timeout)
        while True:
            try:
                result.append(self.events.get_nowait())
            except Empty:
                return result

    def drain_until_quiet(self, quiet: float = 0.01, max_wait: float = 1.0) -> list[Event]:
        """Collect all currently queued replies until the stream is quiet.

        A non-blocking ``get_nowait`` can race the Go writer and make a PnL
        report incomplete. This bounded quiet-period drain gives the producer
        time to observe every fixed-size response without waiting forever on a
        broken connection.
        """
        result: list[Event] = []
        deadline = time.monotonic() + max_wait
        while time.monotonic() < deadline:
            remaining = min(quiet, deadline - time.monotonic())
            try:
                result.append(self.events.get(timeout=remaining))
            except Empty:
                break
        return result

    def close(self) -> None:
        self._stop.set()
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()
        self._reader.join(timeout=1.0)


def synthetic_ticks(count: int, start: int = 100_000, seed: int = 7) -> Iterable[Tick]:
    rng = random.Random(seed)
    mid = start
    for _ in range(count):
        mid += rng.choice((-2, -1, 0, 0, 1, 2))
        yield Tick(time.time_ns(), mid)


def calculate_pnl(fills: Iterable[Event], own_orders: dict[int, int], mark_price_ticks: int) -> tuple[int, int, int]:
    """Return (cash_ticks, inventory, marked_to_market_pnl).

    A fill can identify our quote as maker or our order as taker. Maker side is
    opposite the aggressor side carried in the wire event. Values are in
    tick-notional units (price_ticks * quantity), avoiding float drift.
    """
    cash = 0
    inventory = 0
    for fill in fills:
        if fill.kind != FILL:
            continue
        if fill.maker_order_id in own_orders:
            side = 1 - fill.side_or_code
        elif fill.order_id in own_orders:
            side = own_orders[fill.order_id]
        else:
            continue
        notional = fill.price_ticks * fill.qty
        if side == BUY:
            cash -= notional
            inventory += fill.qty
        else:
            cash += notional
            inventory -= fill.qty
    return cash, inventory, cash + inventory * mark_price_ticks


class MarketMaker:
    def __init__(self, client: BinaryOrderClient, spread_ticks: int = 2, qty: int = 1):
        if spread_ticks < 1:
            raise ValueError("spread_ticks must be at least one tick")
        if qty < 1:
            raise ValueError("qty must be positive")
        self.client = client
        self.spread_ticks = spread_ticks
        self.qty = qty
        self.own_orders: dict[int, int] = {}
        self.next_id = 1
        self.collected: list[Event] = []

    def run(self, ticks: Iterable[Tick], inject_taker_flow: bool = True) -> tuple[int, int, int]:
        last_mid = 0
        for tick in ticks:
            last_mid = tick.mid_ticks
            bid = tick.mid_ticks - (self.spread_ticks + 1) // 2
            ask = tick.mid_ticks + self.spread_ticks // 2
            bid_id, ask_id = self.next_id, self.next_id + 1
            self.next_id += 2
            self.own_orders[bid_id] = BUY
            self.own_orders[ask_id] = SELL
            self.client.new(bid_id, BUY, bid, self.qty)
            self.client.new(ask_id, SELL, ask, self.qty)

            # A backtest needs opposing flow to consume quotes. These IDs model
            # an external participant; disable this option when replaying a real
            # historical aggressor stream.
            if inject_taker_flow:
                external = self.next_id
                self.next_id += 1
                self.client.new(external, SELL, bid, self.qty)
                external = self.next_id
                self.next_id += 1
                self.client.new(external, BUY, ask, self.qty)
            self.collected.extend(self.client.drain())
        self.collected.extend(self.client.drain_until_quiet())
        return calculate_pnl(self.collected, self.own_orders, last_mid)


def main() -> None:
    parser = argparse.ArgumentParser(description="market-maker pump for hftd")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--ticks", type=int, default=100)
    parser.add_argument("--spread", type=int, default=2)
    parser.add_argument("--qty", type=int, default=1)
    parser.add_argument("--no-inject-taker", action="store_true")
    args = parser.parse_args()

    client = BinaryOrderClient(args.host, args.port)
    try:
        maker = MarketMaker(client, args.spread, args.qty)
        cash, inventory, pnl = maker.run(
            synthetic_ticks(args.ticks), inject_taker_flow=not args.no_inject_taker
        )
        print(f"cash={cash} inventory={inventory} marked_pnl={pnl} fills={sum(e.kind == FILL for e in maker.collected)}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
