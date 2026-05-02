"""Strategy SDK — the API researchers code against.

The surface here is the contract: the C++ strategy-runner plugin ABI
exposes the same callbacks and submit/cancel verbs. A strategy written in
Python can be backtested deterministically against the live event archive;
the same strategy logic will run in production either as a pybind11 plugin
or be ported to a C++ plugin for hot-path strategies.

Identifiers and price scales mirror core/proto/hot/messages.hpp.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import IntEnum
from typing import Protocol


class Side(IntEnum):
    BUY = 0
    SELL = 1


class OrdType(IntEnum):
    MARKET = 0
    LIMIT = 1
    STOP = 2
    STOP_LIMIT = 3
    PEGGED_MID = 4


class TimeInForce(IntEnum):
    DAY = 0
    IOC = 1
    FOK = 2
    GTC = 3
    OPG = 4
    CLO = 5


@dataclass(frozen=True, slots=True)
class OrderRequest:
    instrument_id: int
    venue_id: int
    side: Side
    qty: int
    price_e8: int
    ord_type: OrdType
    tif: TimeInForce


@dataclass(frozen=True, slots=True)
class BookUpdate:
    instrument_id: int
    bid_price_e8: int
    bid_qty: int
    ask_price_e8: int
    ask_qty: int
    ts_ns: int


@dataclass(frozen=True, slots=True)
class TradeTick:
    instrument_id: int
    price_e8: int
    qty: int
    ts_ns: int


@dataclass(frozen=True, slots=True)
class Fill:
    cl_ord_id: int
    fill_qty: int
    fill_price_e8: int
    ts_ns: int


class Submitter(Protocol):
    def submit(self, req: OrderRequest) -> int: ...
    def cancel(self, cl_ord_id: int) -> None: ...


class Strategy(ABC):
    """Base class for all strategies.

    The runtime constructs the strategy with a Submitter, then drives
    callbacks. Strategies must not block; long-running work belongs in
    the research stack.
    """

    def __init__(self, submitter: Submitter) -> None:
        self._submit = submitter

    @abstractmethod
    def on_book_update(self, book: BookUpdate) -> None: ...

    def on_trade(self, trade: TradeTick) -> None:
        """Default no-op."""

    def on_timer(self, now_ns: int) -> None:
        """Default no-op."""

    def on_fill(self, fill: Fill) -> None:
        """Default no-op; most strategies override to update internal state."""

    def on_session_state(self, state: str) -> None:
        """Default no-op; receives 'pre_open' / 'open' / 'halt' / etc."""

    def submit(self, req: OrderRequest) -> int:
        return self._submit.submit(req)

    def cancel(self, cl_ord_id: int) -> None:
        self._submit.cancel(cl_ord_id)
