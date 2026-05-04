# ADR-003 — Event archive format

Status: accepted
Date: 2026-05-04

## Context

The deployment plan (`/Users/monicay/.claude/plans/i-want-to-have-sorted-mccarthy.md`, Stage 1.1) commits to event-archive-and-replay as the verification gate before paper trading. Without it, divergence between live and replay is invisible until it costs real money.

Two questions need locking before code:

1. **What format do we write?** Aeron archive (industry standard, comes with replay tooling) vs. a hand-rolled binary log (smaller, no dependency, easier to inspect).
2. **What hooks the archive into the rings?** A "tee" wrapper on the ring (intrusive, every component changes) vs. an explicit `archive.append()` call from each producer (intrusive but trivial) vs. a separate archiver process tailing each ring (purest but requires multi-process work we don't have yet).

## Decision

### Format: hand-rolled length-prefixed binary log

```
File layout:
  [Header (32 bytes, fixed)]
  [Record][Record][Record]...

Header (32 bytes):
  [0..7]   magic "ONTRADE\0"
  [8..11]  schema_major (uint32, currently 1)
  [12..19] file_create_wall_ns (int64)
  [20..31] reserved (zero, available for future use)

Record (24-byte header + variable payload):
  [0..3]   total_length (uint32, includes header + payload)
  [4]      ring_tag (uint8 — see RingTag enum)
  [5..7]   reserved (zero, padding to 8-byte align)
  [8..15]  seq (uint64, monotonic per archive)
  [16..23] wall_ns (int64, capture time)
  [24..N]  payload bytes (the message verbatim from the ring slot)
```

`RingTag` is a `uint8_t` enum: Md=1, ToOms=2, ToVenue=3, VenueIn=4, Events=5. Sized at 8 bits so a future proliferation (md_l2, md_imbalance, etc.) doesn't bump record size.

### Hook strategy: explicit `archive.append()` from the producer

The component that commits a message to a ring also calls `archive.append(tag, payload, wall_ns)` immediately after. Two implications:

- The OMS, gateway, and strategy-runner each take an optional `Archive*`. When null, no archive is written; when set, every ring write produces an archive record.
- Order is deterministic: the archive sequence matches the order of commits across all rings, observed from the orchestrator's single-threaded poll loop.

## Why hand-rolled over Aeron archive

Aeron's archive is the "industry standard" answer the architecture doc hedged on (`docs/architecture.md` §11 "Aeron is the default ... consider a hand-rolled ring if profiling shows Aeron's overhead matters").

For the deployment plan in scope:

- **Dependency cost.** Aeron is JVM-rooted; the C++ binding (`aeron-cpp`) drags Aeron-driver as a runtime peer process. Adding a JVM dependency to the trading host *before* anything else is paying complexity up front for capability we don't need yet (replay tooling, distributed log, sub-µs IPC). The MVP runs at scale where 24-byte-overhead-per-record buffered file writes saturate at >10M records/s on NVMe — orders of magnitude past the order-rate budget for mid-freq.
- **Format ownership.** A hand-rolled format is 30 lines of Append code we own end-to-end. Bugs are ours to fix; format changes don't fight a vendor's release cadence. The trade-off — we don't get Aeron's replay tooling for free — is fine because our replay tool is going to be 50 lines of code reading the same format.
- **Migration path.** When (if) we move to Aeron archive in the multi-process / cross-host build, both formats can coexist: each component writes its own log, and an Aeron archiver wraps the cross-host bus. The hand-rolled format stays as the per-process audit trail — replay parity tests still target it.

This decision is reversible. If, by Phase 4 of `docs/roadmap.md`, the operational benefits of Aeron's distributed log become real, we add it then.

## Why explicit `append()` over a "tee" wrapper or a separate process

- **Tee wrapper on `messaging::SpscRing`:** would require every consumer to read the wrapped type, expanding the API surface of the most performance-sensitive primitive in the codebase. Hot-path-allergic.
- **Separate archiver process tailing each ring:** the architecture's eventual answer (each ring is mmap'd shm; an archiver process attaches as a second consumer per ring). Requires (a) SPMC ring support or duplicate publishing, and (b) the multi-process build (Phase 4 of `docs/roadmap.md`). Out of scope for Stage 1.
- **Explicit append from the producer:** trivial to add, trivial to remove, and naturally captures the producer's intent at the moment of commit. The cost is one branch + one buffered write per ring commit; with a 64KB stdio buffer that's roughly one syscall per 400 messages. Acceptable for the order-rate budget.

## Format invariants

- The header magic + schema_major MUST match on read; any mismatch is a hard error.
- Records are written contiguously with no padding between them.
- A torn write (process killed mid-record) is detectable: the reader stops at the first record whose declared `total_length` exceeds remaining file bytes. Earlier records are valid.
- `wall_ns` is the producer's capture time, not the consumer's read time. Replay re-creates this exactly via `MockClock`.
- `seq` is monotonic per file but NOT per ring. To recover per-ring sequence, follow the `seq` field embedded in the payload's `proto::hot::Header`.

## Test plan

- Round-trip: write N records of each ring tag; reader yields the same bytes back in order.
- Torn-tail: truncate a file mid-record; reader stops cleanly at the last well-formed record.
- Mixed-ring interleaving: writes from md, to_oms, to_venue, venue_in, events come back in the order they were appended, regardless of ring tag.
- Schema mismatch: reader rejects a file with wrong magic or wrong schema_major.

## What this ADR does NOT cover

- File rotation (one file per UTC day) — implemented later in Stage 1.1 once the basic format is exercised.
- Compression — not for hot-path archive; cold-tier S3 sync can gzip on upload.
- Encryption at rest — handled by the EBS / S3 encryption layer, not by this format.
- Cross-host log replication — Phase 4 concern.
