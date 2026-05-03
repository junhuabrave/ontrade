# OnTrade

OnTrade is a greenfield systematic trading platform — US/EU equities and ETPs, mid-frequency, cloud-first, with an architecture that admits a future last-hop FPGA / microwave program without rewriting the cloud build. C++23 (with selected C++26 features) for the hot path, Python for strategies and research, TypeScript for the trader UI and ops control plane.

This README is the "where do I start" file. For deeper reads:

- `docs/architecture.md` — the long-form architectural plan (3-year roadmap, build/buy decisions, deployment topology).
- `docs/mvp.md` — what the current MVP delivers, with a component-by-component flow.
- `docs/roadmap.md` — phased plan for getting from MVP to production.
- `docs/adr/` — architectural decisions of record.

## Status

- **MVP:** shipped (`f58a764`, 2026-05-03). The full C++ hot-path stack (md → strategy → OMS → gateway → fill → position update) runs end-to-end on synthetic data, in one process, with all components composed via the same SPSC ring contract a multi-process build will use.
- **Tests:** 125 / 125 green; clean under TSan and ASan + UBSan; gcc-13 + clang-18 on Ubuntu 24.04 in CI.
- **Next:** Phase 1 of `docs/roadmap.md` — replay parity and event durability.

## What runs today

```
                            ┌───────────────────┐
       md events            │                   │     OrderNew /     ┌──────────┐
       (synthetic)  ───────▶│  StrategyRunner   │───  Cancel    ────▶│  OMS     │
                            │  + MomentumStrategy    │                │  (risk + │
                            └─────────▲─────────┘                    │   state) │
                                      │ events                       └────┬─────┘
                                      │ (acks/fills)                      │
                                      │                                   ▼
                                      │                            ┌──────────┐
                                      └────────────────────────────│ Loopback │
                                                                   │ Gateway  │
                                                                   └──────────┘
```

Every arrow is a real `messaging::SpscRing<128, N>`. The MVP runs all of it inside one binary; the multi-process build (Phase 4) will run each component in its own pinned process across the same ring contract.

## Quick start

### Prerequisites

- C++ compiler with full C++23 support: clang ≥ 18, gcc ≥ 13. On macOS, install Homebrew LLVM (`brew install llvm`) — Apple Clang has gaps that bite this codebase.
- CMake ≥ 3.25.
- Python ≥ 3.12 (for the strategy SDK and CI lint pass).

### Build

```bash
# From the project root
cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build --parallel
```

On macOS with Homebrew clang:

```bash
CC=/opt/homebrew/opt/llvm/bin/clang \
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
  cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build --parallel
```

### Run the smoke binary

The smoke binary wires the full hot-path stack and walks a synthetic price path through it, then prints stats from each component.

```bash
./build/app/ontrade_smoke
```

Expected output:

```
ontrade_smoke — running scripted scenario
=== run summary ===
strategy: orders_submitted=3 cancels_submitted=0 md_processed=15 events_processed=6 to_oms_drops=0
oms     : accepted=3 rejected_risk=0 rejected_capacity=0 fills_received=3 open_orders=0 position=100 to_venue_drops=0 event_drops=0
gateway : orders_acked=3 fills_emitted=3 cancels_acked=0 drops=0
strategy state=2 entry_price_e8=10086000000
```

What that says: `MomentumStrategy` entered long on the first big uptick, took profit on the up-drift, and re-entered on the next eligible uptick. The OMS accepted all three orders (no risk rejects, no capacity rejects), the gateway acked + filled all three, and the final position is `+100` (one buy after the entry / exit / re-entry cycle). All ring queues drained — zero drops anywhere.

### Run the tests

```bash
cd build && ctest --output-on-failure --parallel
```

Selected suites:

```bash
ctest --output-on-failure -R OmsFixture          # OMS state machine + Stats
ctest --output-on-failure -R LoopbackFixture     # Gateway in isolation
ctest --output-on-failure -R EndToEndFixture     # OMS ↔ Gateway round-trip
ctest --output-on-failure -R FullStackFixture    # md → strategy → OMS → gateway
ctest --output-on-failure -R MomentumFixture     # MomentumStrategy logic
```

### Run under sanitizers (locally)

```bash
# ASan + UBSan
cmake -B build-asan \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer" \
  -DCMAKE_C_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address,undefined"
cmake --build build-asan --parallel
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
  UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
  ctest --test-dir build-asan --output-on-failure --parallel

# TSan
cmake -B build-tsan \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_CXX_FLAGS="-fsanitize=thread -fno-omit-frame-pointer" \
  -DCMAKE_C_FLAGS="-fsanitize=thread -fno-omit-frame-pointer" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread"
cmake --build build-tsan --parallel
TSAN_OPTIONS=halt_on_error=1 \
  ctest --test-dir build-tsan --output-on-failure --parallel
```

(macOS note: pass `ASAN_OPTIONS=detect_leaks=0:...` locally; macOS dyld leaks system memory at process start that ASan flags as "leaked". CI on Linux runs with `detect_leaks=1`.)

### Python lint + tests

```bash
pip install -e ".[dev]"
ruff check .
ruff format --check .
pytest -v
```

## Repository layout

```
ontrade/
├── core/                  # C++ hot path
│   ├── proto/hot/         # Wire format — POD structs with pinned offsets
│   ├── messaging/         # SpscRing — lock-free shm ring
│   ├── memory/            # Arena, ObjectPool — zero-alloc primitives
│   ├── runtime/           # Clock, LatencyHistogram, hop-stamps
│   ├── risk/              # Inline pre-trade check
│   ├── oms/               # Order-state machine, 4-ring topology, Stats
│   ├── gateways/          # LoopbackGateway (real venues land Phase 3)
│   └── strategy/          # StrategyRunner + MomentumStrategy example
├── app/                   # End-to-end orchestrator binaries + tests
│   ├── smoke.cc           # ontrade_smoke — runnable demonstrator
│   └── smoke_test.cc      # gtest version with assertions
├── strategies/            # Python Strategy SDK (Phase 5 wires it to runtime)
├── tests/                 # Python tests
├── docs/
│   ├── architecture.md    # Long-form 3-year plan
│   ├── mvp.md             # MVP design + end-to-end flow
│   ├── roadmap.md         # Post-MVP phases
│   └── adr/               # Architecture Decisions of Record
├── cmake/                 # Compiler warning helpers
├── CMakeLists.txt
└── pyproject.toml
```

## Contributing

- Read `docs/architecture.md` and the most recent ADR before proposing a structural change.
- Every PR must keep CI green: gcc-13 build, clang-18 build, ASan + UBSan run, TSan run, ruff, pytest.
- Hot-path code targets zero heap allocations on the order path. New code that calls `new`/`malloc` on a poll path will get bounced.
- Wire-format changes (anything in `core/proto/hot/messages.hpp`) are an ABI break — bump `kSchemaMajor` and write an ADR.
- Use `cl_ord_id` and `exch_ord_id` consistently with the layout in `Ids` — these correlate orders across HA failover.
