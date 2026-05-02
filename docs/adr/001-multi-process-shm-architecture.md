# ADR-001 — Multi-process shared-memory architecture for the hot path

Status: accepted
Date: 2026-05-02

## Context

We need a process model for the hot path (md-ingress → normalizer → book → strategy-runner → oms → venue-gateway). Three serious options were considered:

- **A. Monolithic process.** All components are threads in a single binary, communicating via in-process queues.
- **B. Microservices.** Each component is a separate process, possibly on separate hosts, communicating via TCP/gRPC or a message bus.
- **C. Multi-process per host with shared-memory IPC.** Each component is a separate process on the same trading host, pinned to dedicated isolated cores, communicating via lock-free shared-memory ring buffers.

Constraints:

- Latency budget on cloud: < 200 µs tick-to-trade excluding venue network (architecture.md §3.2).
- Failure isolation: a strategy plugin segfault must not kill the OMS or in-flight orders.
- Independent deploy: must be able to ship a new strategy-runner without restarting md or oms during pre-open.
- Future FPGA migration: md-ingress and venue-gateway are likely first FPGA replacements; the rest of the stack must not need to change when they do.

## Decision

**Adopt option C: multi-process per host with shared-memory IPC.**

- Each component is its own process. One trading host per region; one component per pinned isolated core (architecture.md §5.1).
- Inter-component communication: lock-free SPSC/MPSC ring buffers in POSIX shared memory, wrapped by Aeron IPC. The ring layout is fixed in `core/messaging/shm_ring.hpp` and is the contract.
- **Pre-trade risk is the only exception:** it lives inline as a function call inside the OMS process. Even a shared-memory hop would consume too much of the budget on the order-emitting path.
- All inter-process payloads use FlatBuffers (`core/proto/messages.fbs`) for zero-copy reads.
- Cross-host communication (HA replication, archive shipping) uses Aeron UDP unicast.

## Consequences

**Wins:**

- Failure isolation — per-process crash domains.
- Independent deploy — rolling restart of one component without affecting peers.
- Language flexibility at edges — strategy-runner can host pybind11 plugins later without compromising the C++26 core.
- Security — order-emitting processes get a stricter seccomp profile and capability set than research-adjacent processes.
- FPGA-readiness — each process is a swap-out unit. md-ingress and venue-gateway can be replaced by FPGA driver shims feeding the same shm rings without touching the rest of the stack.
- Deterministic replay — every ring is tap-able by the archiver, producing a single ordered event log (architecture.md §13).

**Costs:**

- A shm ring hop (50–200 ns SPSC on modern x86 with cache-line padded slots) is more expensive than a direct function call (a few ns). Acceptable inside the budget in §3.2.
- Operational complexity — process supervision, pinning, isolated-core configuration, shm region sizing, hugepage setup. Mitigated by a single `runtime/` library that all hot-path processes share.
- Cross-process state debugging is harder than thread debugging. Mitigated by deterministic-replay tooling.

**Why A (monolithic) was rejected:**

- Fails failure isolation — a strategy bug crashes the OMS.
- Fails independent deploy — every change is a full restart.
- Conflicts with FPGA-readiness — a monolithic binary has no clean swap-out boundary.

**Why B (microservices) was rejected:**

- Fails latency — even localhost TCP adds tens of microseconds at p99 with much higher jitter than shm; cross-host adds hundreds of microseconds.
- Adds operational complexity (service discovery, retries, partial-failure handling) without a corresponding intra-host benefit.
- Network microservices remain appropriate for cross-host concerns (HA replication, control plane, post-trade) — and we use them there. They are not appropriate intra-host for the order path.

## Alternatives considered (briefly)

- **Thread-per-component with lock-free queues in one binary.** A halfway house. Gets A's downsides (no isolation, single binary deploy) without enough of C's benefits.
- **DPDK / kernel-bypass networking between processes on the same host.** Higher complexity, equivalent latency to shm, no benefit.
- **Disruptor-style single ring with multiple consumers.** Considered for future fan-out stages (one md stream feeding multiple strategies). Not needed for the linear initial pipeline; revisit when fan-out is real.

## Tested by

- `core/messaging/shm_ring.hpp` and its unit tests (`shm_ring_test.cc`) demonstrate the SPSC ring contract — lock-free claim/commit/read/release, FIFO ordering across producer/consumer threads, full-ring back-pressure, and creator/attacher visibility.
- A latency benchmark gating per-hop p99 < 200 ns will land alongside the first integrated pipeline.
