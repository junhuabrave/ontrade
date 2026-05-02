from __future__ import annotations

from dataclasses import dataclass, field

from strategies.sdk import (
    BookUpdate,
    Fill,
    OrderRequest,
    OrdType,
    Side,
    Strategy,
    TimeInForce,
    TradeTick,
)


@dataclass
class FakeSubmitter:
    submitted: list[OrderRequest] = field(default_factory=list)
    cancelled: list[int] = field(default_factory=list)
    next_id: int = 1

    def submit(self, req: OrderRequest) -> int:
        self.submitted.append(req)
        cl_ord_id = self.next_id
        self.next_id += 1
        return cl_ord_id

    def cancel(self, cl_ord_id: int) -> None:
        self.cancelled.append(cl_ord_id)


class CrossSpreadOnUpdate(Strategy):
    """Trivial strategy used to exercise the SDK contract."""

    def __init__(self, submitter: FakeSubmitter) -> None:
        super().__init__(submitter)
        self.fills_seen: list[Fill] = []

    def on_book_update(self, book: BookUpdate) -> None:
        self.submit(
            OrderRequest(
                instrument_id=book.instrument_id,
                venue_id=1,
                side=Side.BUY,
                qty=100,
                price_e8=book.ask_price_e8,
                ord_type=OrdType.LIMIT,
                tif=TimeInForce.IOC,
            )
        )

    def on_fill(self, fill: Fill) -> None:
        self.fills_seen.append(fill)


def test_book_update_triggers_submit() -> None:
    fake = FakeSubmitter()
    s = CrossSpreadOnUpdate(fake)
    s.on_book_update(
        BookUpdate(
            instrument_id=42,
            bid_price_e8=99_50_000_000,
            bid_qty=100,
            ask_price_e8=100_00_000_000,
            ask_qty=100,
            ts_ns=1_000_000,
        )
    )
    assert len(fake.submitted) == 1
    req = fake.submitted[0]
    assert req.instrument_id == 42
    assert req.side == Side.BUY
    assert req.qty == 100
    assert req.price_e8 == 100_00_000_000
    assert req.tif == TimeInForce.IOC


def test_fill_callback_updates_internal_state() -> None:
    fake = FakeSubmitter()
    s = CrossSpreadOnUpdate(fake)
    fill = Fill(cl_ord_id=7, fill_qty=50, fill_price_e8=100_00_000_000, ts_ns=2_000_000)
    s.on_fill(fill)
    assert s.fills_seen == [fill]


def test_default_callbacks_are_noops() -> None:
    fake = FakeSubmitter()
    s = CrossSpreadOnUpdate(fake)
    s.on_trade(TradeTick(instrument_id=42, price_e8=100_00_000_000, qty=10, ts_ns=0))
    s.on_timer(now_ns=123)
    s.on_session_state("open")
    assert fake.submitted == []


def test_cancel_routes_through_submitter() -> None:
    fake = FakeSubmitter()
    s = CrossSpreadOnUpdate(fake)
    s.cancel(99)
    assert fake.cancelled == [99]
