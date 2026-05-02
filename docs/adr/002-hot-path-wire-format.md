# ADR-002 — Fixed-layout POD wire format for the hot path

Status: accepted
Date: 2026-05-02
Supersedes: the FlatBuffers schema previously at `core/proto/messages.fbs`.

## Context

ADR-001 commits the order path to inter-process shared-memory rings. Each ring slot is a fixed-size byte block; producers write a message into the slot and consumers read it. The remaining question — and the one Cline raised in `docs/cline/adr-010-sbe-vs-flatbuffers.md` — is **what wire format goes in the slot**.

The initial scaffold used FlatBuffers (`core/proto/messages.fbs`). Cline argued for SBE (Simple Binary Encoding) on three grounds:

1. **Cache efficiency.** SBE has fixed field offsets; FlatBuffers reads via a per-message vtable, costing one extra dependent load and one extra cache line per field-heavy message.
2. **FPGA-readability.** A future FPGA card (last-hop program, post Year 3) emits or consumes the wire bytes directly. Fixed offsets → trivial Verilog. Vtables → painful.
3. **Determinism.** Message size known at compile time; no allocation, no growth.

The argument is correct on the runtime properties. But SBE's tooling cost — a JVM-based codegen step, generated code in the build graph, an XML schema as the source of truth — is non-trivial for a pre-FPGA, single-language hot path with a small fixed message set.

## Decision

**Adopt SBE's runtime model — fixed-offset, little-endian POD structs — but author the schema as hand-written C++ headers, not via the SBE codegen tool.**

The contract lives in `core/proto/hot/messages.hpp`:

- One header struct (`Header`, 16 bytes) at the front of every message: `schema_major`, `msg_type`, `reserved`, `seq`.
- One POD struct per message type (`OrderNew`, `OrderCancel`, `OrderReplace`, `OrderAck`, `OrderReject`, `OrderFill`, `OrderCancelAck`).
- All multi-byte fields little-endian (x86_64 native; FPGA target is LE Verilog).
- Layout pinned by `static_assert(sizeof(...))` and `static_assert(offsetof(...))` so an unintended layout change fails the build.
- Round-trip via `memcpy` validated by `core/proto/hot/messages_test.cc`.

FlatBuffers stays available as a build dependency for control-plane messages and Aeron-archive snapshots — formats that benefit from schema evolution, reflection, and JSON tooling and that do not run on the order-emitting path. The original `core/proto/messages.fbs` is removed; a new control-plane schema will land alongside the first control-plane caller.

## Consequences

**Wins:**

- Fixed offsets on the hot path (cache, prefetch, branch prediction) without a JVM-based codegen step in the build.
- Trivial-copyable, standard-layout structs — `memcpy` to and from the shm slot is the entire serialization path.
- The contract is one C++ header — no schema-vs-generated-code divergence to police.
- Same byte layout an FPGA card will emit/consume; the layout asserts are the FPGA contract.

**Costs:**

- No automatic schema evolution. A field rename or reorder is a `kSchemaMajor` bump and a hard ABI break — by design, since both processes are deployed together on the same trading host.
- No reflection / JSON pretty-printing for free. We will add a small ad-hoc decoder in the archiver/replay tooling when a caller needs it.
- If a second non-C++ producer or consumer ever needs the format (e.g., a Rust gateway, or the FPGA Verilog team starts shipping), we'll need to either regenerate the schema in that language by hand (cheap for ~10 message types) or introduce SBE codegen at that point. The byte layout is stable across the choice; it's a tooling decision that can be deferred.

**Why hand-written over SBE codegen:**

- We have one consumer language (C++) on the hot path. Python uses the higher-level `strategies/sdk` surface, not the wire format.
- We have ~10 message types and they are stable. SBE's authoring economics turn favorable around hundreds of types or three-plus consumer languages.
- The codegen toolchain (Java tool, generated headers committed or regenerated, CMake custom commands) adds friction to every PR that touches a message.
- Cline's adr-010 names "limited reflection, JSON conversion requires custom code" as costs of SBE — those costs apply to hand-written structs too. We accept them either way.
- If we ever do introduce SBE codegen, the migration is: write an SBE XML matching the existing layout, generate C++ that produces structs identical in byte layout to the current ones, swap the include. The current header *is* the SBE schema, just in C++ instead of XML.

**Why FlatBuffers for control-plane:**

- Control-plane messages (config push, kill switch updates, limit changes) are sent rarely, evolve frequently, and benefit from forward/backward compatibility.
- Archive payloads are read by tools (replay, post-trade analytics, debugging) that genuinely benefit from reflection.
- The vtable cost is irrelevant on these paths.

## Tested by

- `core/proto/hot/messages_test.cc` — `static_assert`s that every struct is trivially copyable and standard-layout, plus runtime tests for `memcpy` round-trip on `OrderNew`, `OrderFill`, and `OrderCancel`.
- The size and offset `static_assert`s inside `messages.hpp` itself act as ABI gates: any layout change fails to compile.

## Related

- `docs/cline/adr-010-sbe-vs-flatbuffers.md` — Cline's original proposal. This ADR is the accepted form of the decision; the runtime properties Cline advocated are adopted, the codegen mechanism is deferred.
- `docs/cline/RESPONSE.md` — point-by-point response to Cline's review including this item.
