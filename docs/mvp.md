# OnTrade MVP — design and flow

Status: shipped 2026-05-03 (commit `f58a764`)

This document describes what the MVP delivers, how the components fit together, and how to walk one synthetic order from market-data ingress through to a position update. Read this after `docs/architecture.md` (which sets the long-term architecture) and ADR-001/002 (which lock the multi-process and wire-format decisions).

## What "MVP" means here

The MVP is **the C++ hot path running end-to-end on synthetic data, in one process, with all components composed via the production-shape rings**. It is *not* a tradeable system — there is no real market data, no FIX gateway, no replay archive, no UI. It exists to prove that the seven primitives the platform commits to (shm rings, POD wire format, single-source-of-time clock, latency histogram, zero-allocation pool, inline pre-trade risk, and the SPSC ring contract) compose without seams, and that adding a strategy plugin against this stack is a straightforward matter of writing a class.

Concretely, the MVP demonstrates:

- A market-data event reaches a strategy in the same process via a real shm ring slot.
- The strategy submits an order through the runner's API; the order traverses three more rings (`to_oms` → `oms_to_venue` → `venue_to_oms`) plus the OMS's own state machine before getting acked and filled.
- The fill flows back to the strategy via the OMS events ring; the strategy's per-order state machine resolves correctly.
- Pre-trade risk runs inline in the OMS process; rejected orders never reach the gateway.
- Each component publishes counters (Stats / `orders_submitted` / `fills_emitted` / etc.) suitable for ops dashboards.
- The whole thing is clean under TSan and ASan+UBSan, so the memory ordering and lifetimes are correct.

## Component map

```
                              ┌──────────────────┐
              md events       │                  │
              (BookUpdate /   │   strategy::     │
              TradeTick) ────▶│   StrategyRunner │
                              │   + Strategy     │
                              │   (e.g.          │
                              │   MomentumStrategy)
                              │                  │
                              └────┬─────────▲───┘
                                   │         │
                          OrderNew/Cancel    │ events
                          (to_oms ring)      │ (events ring)
                                   │         │
                                   ▼         │
                              ┌────────────────┐
                              │   oms::Oms     │
                              │   ─ inline     │
                              │     pre-trade  │
                              │     risk       │
                              │   ─ order-state│
                              │     machine    │
                              │   ─ position   │
                              │   ─ Stats      │
                              └─┬──────────▲───┘
                                │          │
                          OrderNew/Cancel  │ Ack/Fill/CancelAck
                          (to_venue)       │ (venue_in)
                                │          │
                                ▼          │
                              ┌────────────────┐
                              │   gateways::   │
                              │   LoopbackGw   │
                              │   ─ assigns    │
                              │     exch_ord_id│
                              │   ─ auto_ack / │
                              │     auto_fill  │
                              │     knobs      │
                              └────────────────┘

      Storage primitives shared by all components:
      ─ messaging::SpscRing<128, N>   (lock-free, process-shareable)
      ─ memory::ObjectPool<T, N>      (single-threaded, no heap)
      ─ memory::Arena                 (bump-pointer over caller storage)
      ─ runtime::SystemClock / MockClock (one source of time)
      ─ runtime::LatencyHistogram     (power-of-two buckets, lock-free)
      ─ proto::hot::*                 (fixed-layout POD wire format)
```

Every line in that diagram is a real `messaging::SpscRing<128, N>` — including the ones inside a single process. There is no special "in-process bypass" shortcut. That keeps the multi-process deployment a wiring change, not a code change.

## Module-by-module summary

### `core/proto/hot/messages.hpp`

The wire format. POD structs with hand-pinned offsets and `static_assert`'d sizes. The MVP set:

| Message | Size | Used on rings | Producer | Consumer |
|---|---|---|---|---|
| `BookUpdate` | 80 B | md_in | (synthetic / md-normalizer) | StrategyRunner |
| `TradeTick` | 72 B | md_in | (synthetic / md-normalizer) | StrategyRunner |
| `OrderNew` | 120 B | to_oms, to_venue | StrategyRunner, OMS | OMS, Gateway |
| `OrderCancel` | 96 B | to_oms, to_venue | StrategyRunner, OMS | OMS, Gateway |
| `OrderReject` | 104 B | events | OMS | StrategyRunner |
| `OrderAck` | 96 B | venue_in, events | Gateway, OMS | OMS, StrategyRunner |
| `OrderFill` | 128 B | venue_in, events | Gateway, OMS | OMS, StrategyRunner |
| `OrderCancelAck` | 96 B | venue_in, events | Gateway, OMS | OMS, StrategyRunner |

`kHotSlotBytes = 128` (the `OrderFill` ceiling) is the slot size for every hot ring. A single ring can carry any of these without padding overhead beyond the 8-byte rounding the structs already pay for `int64_t` alignment.

### `core/messaging/shm_ring.hpp`

Lock-free SPSC ring, Disruptor-style. Producer + consumer cursors on separate cache lines. Storage is caller-supplied so the same ring works from `alignas(64)` global memory in tests, from a `std::aligned_storage_t` member in production, or from `mmap`'d shared memory in the multi-process build.

Memory ordering: producer writes the slot then store-releases `producer_seq`; consumer load-acquires `producer_seq`, reads the slot, then store-releases `consumer_seq`. Verified clean under TSan.

### `core/memory/{arena,pool}.hpp`

`Arena` is a non-owning bump-pointer allocator over caller storage. `ObjectPool<T, N>` pre-allocates `N` slots of aligned storage and hands them out via `construct()`/`destroy()`. Both single-threaded by design — every component owns its pool from one pinned core. The OMS uses `ObjectPool<TrackedOrder, MaxOpenOrders>` to track in-flight orders without heap traffic.

### `core/runtime/clock.hpp` and `core/runtime/latency.hpp`

`Clock` is a concept (`now_ns()` + `wall_ns()`); `SystemClock` wraps `std::chrono::steady_clock` / `system_clock`; `MockClock` is settable from tests. Every component takes a `Clock` by template parameter so backtests and replays drive time deterministically.

`LatencyHistogram` records ns-resolution durations into 25 power-of-two buckets (64 ns floor, ~1 s ceiling). Lock-free single-writer, relaxed-atomic counters so an external thread can sample the snapshot without a lock. The OMS uses one to record the cost of each `check_pre_trade` call.

`stamp_hop()` / `read_hop()` / `hop_delta()` write to the four `Timestamps` fields (`origin_ns`, `ingress_ns`, `decision_ns`, `submit_ns`) on every order, so end-to-end latency is reconstructible from the message itself — no out-of-band tracing required.

### `core/risk/limits.hpp`

The inline pre-trade check. Single function `check_pre_trade(order, limits, position)` returning `CheckOutcome`. Order-of-checks is intentional (kill switch first; locate; per-order ceilings; position ceilings; fat-finger last because it's the most expensive math). No allocations, no syscalls, no virtual dispatch.

### `core/oms/oms.hpp`

The order-state machine and the integration point where every primitive composes. Four rings:

| Ring | From | To | Carries |
|---|---|---|---|
| `inbound` | strategy | OMS | OrderNew, OrderCancel |
| `venue_in` | gateway | OMS | OrderAck, OrderFill, OrderCancelAck |
| `to_venue` | OMS | gateway | OrderNew, OrderCancel (forwarded) |
| `events` | OMS | strategy / post-trade | OrderReject (synthesized), forwarded OrderAck/Fill/CancelAck |

Per-order state machine:

```
PendingNew ─OrderAck─▶ Working ─OrderFill(remaining > 0)─▶ PartiallyFilled
Working / PartiallyFilled ─OrderFill(remaining = 0)─▶ (release pool slot)
Working / PartiallyFilled ─OrderCancel(in)─▶ PendingCancel
PendingCancel ─OrderCancelAck─▶ (release pool slot)
```

Position is signed and updated from venue fills (not from the strategy). Pool exhaustion at accept time produces a synthesized `OrderReject` rather than a forwarded-but-untracked order. The `Stats` snapshot exposes per-state gauges, lifetime counters, and ring queue depths for ops dashboards.

### `core/gateways/loopback.hpp`

The venue side of the OMS rings, for tests and the smoke binary. Reads `OrderNew`/`OrderCancel`, assigns a monotonic `exch_ord_id`, optionally acks, optionally fills at the order's limit price, optionally acks cancels. Behavior knobs (`auto_ack` / `auto_fill` / `auto_cancel_ack`) let tests script slow-venue and partial-fill scenarios.

`LoopbackGateway` is the *contract* a real `FixGateway` / `OuchGateway` will implement. Replacing it is a swap-out, not a rewrite.

### `core/strategy/runner.hpp` and `core/strategy/momentum.hpp`

`StrategyRunner` is templated on the strategy class (a template-template parameter so the strategy can use the runner as its `Submitter`). The runner owns the strategy by value; the strategy holds a reference to the runner for its `submit` / `cancel` calls. No virtuals on the call path.

`StrategyBase<Submitter>` provides empty-default callbacks (`on_book`, `on_trade`, `on_ack`, `on_fill`, `on_reject`, `on_cancel_ack`); strategies override only what they care about.

`MomentumStrategy` is a one-screen example: enter long when the bid upticks by ≥ N cents, exit at TP / SL in basis points, single-position state machine.

### `app/smoke.cc` and `app/smoke_test.cc`

`smoke.cc` is the runnable demonstrator — five rings allocated as 64-byte-aligned globals, every component constructed for real, a scripted price path pushed through, and stats printed at the end. `smoke_test.cc` is the same scenario as a gtest with assertions.

## End-to-end flow — one OrderNew lifecycle

This is the path of one buy order, from a strategy decision triggered by a `BookUpdate`, all the way back to the strategy's `on_fill`.

```
T+0  ┌─ strategy receives BookUpdate, decides to enter
     │  (bid uptick ≥ entry_threshold_e8)
     │
     │  runner.submit(Side::Buy, instrument_id, qty, price)
     │    ├─ build OrderNew with cl_ord_id, schema, hop-stamped
     │    │  Ingress timestamp
     │    ├─ to_oms.try_claim() → memcpy → commit
     │    └─ return cl_ord_id
     │
T+1  ├─ strategy stores cl_ord_id; state PendingEntry
     │
     │  caller drives the next runner.poll() / oms.poll() / gw.poll()
     │
T+2  ├─ oms.poll() drains its inbound ring, sees the OrderNew:
     │    ├─ stamp_hop(Decision)
     │    ├─ check_pre_trade(order, limits, position)  [risk_lat_ recorded]
     │    │      ├─ kill_switch? no
     │    │      ├─ qty/price valid?  yes
     │    │      ├─ short-locate?  N/A (Buy)
     │    │      ├─ qty/notional ceilings OK
     │    │      ├─ position ceilings OK
     │    │      └─ fat-finger band OK
     │    ├─ orders_pool_.construct() → TrackedOrder slot claimed
     │    │  state=PendingNew, qty_total, qty_filled=0
     │    ├─ active_[active_count_++] = tracked
     │    ├─ stamp_hop(Submit)
     │    └─ to_venue.try_claim() → memcpy → commit
     │
T+3  ├─ gateway.poll() drains to_venue, sees the OrderNew:
     │    ├─ exch_ord_id = ++next_exch_ord_id_
     │    ├─ build OrderAck (ids.cl_ord_id preserved, exch_ord_id set,
     │    │  ts.ingress_ns = clk.wall_ns())
     │    ├─ venue_in.try_claim() → memcpy → commit
     │    └─ build OrderFill (qty = order.qty_raw, price = limit, fee=0)
     │       venue_in.try_claim() → memcpy → commit
     │
T+4  ├─ oms.poll() drains venue_in:
     │    ├─ OrderAck: find_by_cl_ord_id → state PendingNew→Working,
     │    │            store exch_ord_id; emit_event(ack)
     │    └─ OrderFill: find_by_cl_ord_id →
     │                  qty_filled += fill_qty_raw,
     │                  position_.current_qty += signed_fill,
     │                  qty_filled >= qty_total → release(t),
     │                  emit_event(fill)
     │
T+5  └─ runner.poll() drains events:
          ├─ OrderAck: strategy.on_ack(...)        [no-op for momentum]
          └─ OrderFill: strategy.on_fill(...)
                  cl_ord_id == entry_cl_ → state Long, entry_price set

      poll loop reaches quiescence (a == b == c == 0).
```

Every arrow in that diagram is a real shm ring hop. Every `try_claim`/`commit`/`try_read`/`release` pair is the same SPSC contract a separate-process build would use across `mmap`'d memory. The MVP is not a simulation of the production shape; it *is* the production shape, single-process.

## What stays the same in production

- `proto::hot::*` byte layouts — pinned by `static_assert(sizeof)` and `static_assert(offsetof)`. ABI-stable across the FPGA migration.
- `messaging::SpscRing` ring contract — Disruptor-style cursors on separate cache lines, release/acquire ordering, slot index = `seq & (SlotCount - 1)`.
- The four-ring OMS topology (`inbound` / `venue_in` / `to_venue` / `events`).
- The Strategy ↔ Runner ↔ OMS ↔ Gateway boundaries.
- `check_pre_trade` runs in-process with the OMS; it does not become a separate component.

## What changes in production

- The five rings move from `alignas(64) std::byte storage[N]` globals into `mmap`'d shared memory backed by `/dev/shm`.
- Each component runs in its own pinned process (`md-ingress`, `strategy-runner`, `oms`, `venue-gateway`) instead of sharing one `main()`.
- `LoopbackGateway` is replaced by `FixGateway` (libQuickFIX wrap) or a hand-rolled binary protocol gateway (OUCH / iLink-3 / ETI). The gateway's ring contract — read `OrderNew`/`OrderCancel`, write `OrderAck`/`OrderFill`/`OrderCancelAck` — does not change.
- A real market-data path replaces synthetic injection: `md-ingress` reads venue multicast (or a vendor feed), `md-normalizer` produces canonical events, `book-builder` constructs top-of-book.
- An archiver process taps every ring (Aeron archive or bespoke writer) for replay parity and post-trade.
- A control plane process pushes limit updates and reads `Stats` for the ops dashboard.

The MVP's job was to make sure none of those changes require touching the pieces inside the boxes — only the wiring around them.
