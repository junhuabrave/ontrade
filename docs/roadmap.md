# OnTrade roadmap — post-MVP phases

The MVP (`f58a764`) delivered the C++ hot-path stack composed end-to-end on synthetic data in one process. This roadmap details what comes next, in phases ordered by leverage. Each phase has an explicit exit criterion — a concrete check that says "this phase is done" — so progress is measurable rather than narrative.

The 3-year plan in `docs/architecture.md` sets the destination. This roadmap is the order in which we get there.

---

## Phase 1 — Replay parity and event durability

**Why first.** Replay parity is the #1 systematic-trading bug class. Every other phase becomes harder to verify without it: a backtester that diverges from live by 1 tick is a P&L-shaped landmine. We get this right while the system is small enough to instrument fully.

**Scope:**

- An archiver process / thread that taps every shm ring and writes to a single ordered event log on local NVMe (Aeron archive is the natural fit, but a simpler `write()`-per-record file works for the first cut).
- A replay tool that reads the log and re-injects events into the same ring shapes, driven by `MockClock` so timestamps match the originals byte-for-byte.
- A `replay_parity_test` in CI: capture a smoke run; replay it; assert the second run produces the same OrderNew/Ack/Fill sequence.
- A choice on the durability tier: in-memory ring → NVMe (sync flush at shutdown) → S3 cold archive nightly. Document the data-loss window each tier accepts.

**Exit criterion:** `ctest --test-dir build -R replay_parity` is in CI and green for at least three smoke scenarios (entry/exit, partial fills, kill-switch reject).

**Estimate:** 1–2 weeks for one engineer, mostly hands-on-keyboard once the archive format is decided.

---

## Phase 2 — Realistic market data

**Why second.** Synthetic data only proves the wiring works. To exercise corner cases (stale quotes, locked/crossed books, halt sessions, zero-tick bars), the system needs to ingest real recorded data.

**Scope:**

- `core/md/normalizer` — process that consumes a raw feed format (parquet, CSV, or a vendor-specific binary) and produces canonical `BookUpdate`/`TradeTick` messages on the md ring.
- `core/md/book_builder` — consumes raw L2 add/modify/cancel events and produces top-of-book + N-level depth on the md ring. (For MVP scope, we used pre-built `BookUpdate`s; a real venue feed delivers L2 events that need a builder.)
- A historical replay source. Two reasonable starting points:
  1. **Databento Parquet samples** (free tier) — small, real US equities data, covers the corner cases.
  2. **IEX TOPS Pcap** (free, public) — full L1, MBP-1, multiple symbols.
- An updated smoke binary (or a new `app/replay_smoke`) that drives a recorded session through the full stack and prints fill / position summaries.

**Exit criterion:** `app/replay_smoke <symbol> <date>` runs a full trading day's TOPS pcap through the stack with zero ring-full drops at default sizing, and the strategy produces at least one round-trip per day on a momentum-eligible symbol.

**Estimate:** 2–4 weeks. The book-builder is the hard part — corner cases in modify/cancel sequencing burn time.

---

## Phase 3 — First real venue (paper trading)

**Why third.** A FIX or native binary gateway is the largest piece of code that requires venue-specific cooperation (cert harness, conformance testing). Doing it once forces the gateway interface to handle real-world quirks; subsequent venues are mostly translation.

**Scope:**

- `core/gateways/fix` — `FixGateway` implementing the same `from_oms` / `to_oms` ring contract `LoopbackGateway` does, wrapping QuickFIX/n or QuickFIX-cpp.
- Session lifecycle: logon, heartbeat, logout, reset. Session state is per-venue, persists across OMS restarts.
- Sequence-number recovery on reconnect (FIX requires this; a real conformance suite tests for it).
- A target venue for first integration. Pragmatic options:
  1. **IEX TOPS DEEP** — paper-trading tier, free, FIX 4.2.
  2. **Cboe Equities U.S.** — paper-trading harness available via broker.
  3. A broker DMA gateway (Interactive Brokers, Tradeweb) — easier conformance, less direct but ships sooner.
- A paper-trading mode flag on the venue gateway that posts orders to the venue's paper environment, with audit logging.
- `paper_trading_drill` documented monthly run plan, archived results.

**Exit criterion:** 30 consecutive trading days running `MomentumStrategy` (or a more realistic strategy) in paper mode against the chosen venue with zero unexplained order-state breaks at end of day.

**Estimate:** 4–8 weeks. Most of this is conformance testing and venue paperwork, not C++.

---

## Phase 4 — Multi-process deployment

**Why fourth.** The single-process MVP is a tactical compromise. The architecture commits to per-component processes for fault isolation, independent deploy, language flexibility at the edges, and a clean security boundary on the order-emitting components. Phase 4 cashes that commitment in.

**Scope:**

- Move ring storage from `alignas(64) std::byte[]` globals into `shm_open` + `mmap` regions.
- A small `core/runtime/shm_region.hpp` wrapper (POSIX-only is fine; production is Linux).
- One binary per component: `ontrade_md_normalizer`, `ontrade_strategy_runner`, `ontrade_oms`, `ontrade_venue_gateway`. Each takes the shm region paths as command-line args.
- A process supervisor — `systemd` units in production, a `start.sh` for development. CPU pinning via `taskset` / `cgroups`.
- An `infra/` directory with the boot-time `isolcpus=…` documentation and the ops scripts for restart / rollback.
- A multi-process integration test in CI: spawn all four binaries pointed at a tmpfs ring region, drive a smoke scenario, assert the same end state as the single-process smoke test.

**Exit criterion:** the smoke scenario passes when run as a four-process configuration, and end-to-end p99 latency from md ring write to gateway egress is published in CI on every PR (with an alert if it regresses > 10%).

**Estimate:** 2–4 weeks once Phases 1–3 are in.

---

## Phase 5 — Strategy / research stack

**Why fifth.** The Python strategy SDK exists in `strategies/sdk/` but isn't wired to the runtime. Researchers can't run a strategy yet — they'd have to write C++. Phase 5 closes that gap.

**Scope:**

- A pybind11 bridge that exposes the C++ `StrategyRunner` interface to Python: a Python class subclassing `Strategy` runs inside the C++ runner via callbacks.
- Two run modes for the same strategy code:
  1. **Live / paper** — Python strategy loaded via pybind11 plugin host, running on the shared shm rings. Slower than C++ but acceptable for non-HFT mid-freq strategies.
  2. **Backtest** — same Python strategy code, fed events from the Phase-1 archive replay tool, no `pybind11` runtime overhead.
- A backtester (`research/backtester/`) that reads the archive log and drives the Python strategy with deterministic timestamps. Output: per-strategy equity curve, fill quality stats, latency percentile (in backtest these are simulated based on captured book state).
- A `research/feature_store/` skeleton for point-in-time-correct features (Polars + Arrow).
- The "strategies ship with a shared-history backtest" CI rule from the architecture doc: every strategy has a baseline run that CI re-executes; if the equity curve moves, the PR is flagged for review.

**Exit criterion:** at least one strategy is written in Python, backtested against an archived replay, runs in paper-mode against the Phase-3 venue, and the live-vs-backtest P&L delta is < 1 bp / day for 5 consecutive days.

**Estimate:** 4–8 weeks. The pybind11 hot-path concerns (GIL, allocation) need care.

---

## Phase 6 — Operations: control plane, UI, post-trade

**Why sixth, not earlier.** None of the user-facing pieces matter until the underlying system is producing real fills. They become the next bottleneck once Phase 5 lands, because at that point a human needs to monitor, kill, and reconcile.

**Scope:**

- `control/` (Node.js / TypeScript) — gRPC service that receives `Stats` snapshots from each component (via a control ring or an HTTP scrape endpoint), serves the trader UI, persists limit changes, and pushes them back into the OMS via a published-snapshot mechanism.
- `ui/` (React + TypeScript) — positions / P&L / kill switch / per-strategy parameters / latency histogram visualization. Uses the Stats schema from `core/oms/oms.hpp` directly.
- Audit log — every parameter change is signed, timestamped, and archived.
- Auth (Auth0 / Okta integration) for the control plane.
- `post_trade/` (Python) — allocations, fee/commission calc, broker reconciliation, T+1 reporting. Vendor integration for CAT / MiFID II RTS 22/24 (Cappitech or similar). The architecture doc is explicit that this should be bought, not built.

**Exit criterion:** a trader can monitor positions in real time, hit kill switch within 100 ms (drilled monthly), and end-of-day reconciliation against the broker is zero-break before market open the next day.

**Estimate:** 6–12 weeks. Mostly TypeScript/Python, not C++.

---

## What this roadmap does NOT include

These are deliberately out of scope for the next ~12 months. They appear in the architecture doc and are correct destinations, but they require either capital, regulatory effort, or specialized hardware skills that don't pay back until the platform is producing real P&L.

- **Co-location, microwave, FPGA last-hop program** — the architecture's "future last hop" section is explicit that this is a separate program with its own budget, vendor contracts, and 24/7 ops model. Trigger conditions are listed there.
- **Multi-asset (futures / options / crypto)** — the connectivity and risk models diverge enough that cleanly forking the hot-path binary per asset class is the right move when a strategy needs it.
- **Cross-region (eu-west / ap-northeast hosts)** — additive once the us-east host is producing.
- **Direct exchange membership** — broker DMA covers Phase 3 fine; direct membership is a Year-2 conversation.

---

## Order rationale

The phases are ordered so each one's exit criterion unblocks the next:

1. Replay parity → cheap, exhaustive verification of every later phase.
2. Real market data → exposes corner cases that synthetic data hides; verified by replay parity from Phase 1.
3. First venue → forces the gateway interface to handle real-world session lifecycle; uses Phase-2 data for paper-trading drills.
4. Multi-process → cashes in the architectural commitment now that the contracts are stable, and produces real cross-process latency numbers; verified by replay parity.
5. Strategy stack → unblocks the research team; backtests on Phase-1 archive, lives on Phases 3+4.
6. Operations → the human bottleneck once Phase 5 is producing real fills.

A different order is possible (e.g., FIX before market data), but each rearrangement weakens a verification gate.
