# Architecture Decision Records

One markdown file per decision, numbered sequentially: `NNN-short-title.md`.

## Format

```markdown
# ADR-NNN — Title

Status: proposed | accepted | superseded by ADR-MMM
Date: YYYY-MM-DD

## Context
What is the problem and the relevant constraints?

## Decision
What did we decide?

## Consequences
What follows from this — both wins and costs?

## Alternatives considered
What else was on the table, and why not?
```

## When to write one

Required when changing any of:
- Capacity / latency targets in `architecture.md` §3
- Subsystem contracts in `architecture.md` §5–§7
- Critical contract files in `architecture.md` §14

Recommended for any decision a future engineer would reasonably ask "why was this done this way?" about.

## Initial backlog

- ADR-001 — Multi-process shared-memory architecture
- ADR-002 — FlatBuffers as hot-path serialization
- ADR-003 — Cloud-first with FPGA/co-lo as future last-hop program
- ADR-004 — Vendor-normalized market data in Year 1
- ADR-005 — Buy regulatory reporting and surveillance
- ADR-006 — ClickHouse for tick store
