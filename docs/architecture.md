# OnTrade — Platform Architecture

Status: **approved** · Last revised: 2026-05-02 · Owner: TBD

This is the reference architecture for the OnTrade systematic trading platform. It supersedes the initial planning document at `~/.claude/plans/i-want-to-have-sorted-mccarthy.md`. Material changes to this document require an ADR in `docs/adr/`.

---

## 1. Context

Greenfield "all-in-one" systematic trading platform: market data, pre-trade risk, OMS/EMS/SOR, post-trade, and the strategy/research stack, in a single coherent codebase.

| Dimension | Decision |
|---|---|
| Asset classes (initial) | US/global equities, ETPs, EU MTFs (Cboe Europe, Aquis, Turquoise, LSE) |
| Asset classes (later) | Futures, options, FX, crypto — out of scope for initial 3 years |
| Trading style | Mid-frequency systematic. **Sub-millisecond is not a goal.** |
| Latency target (cloud) | Single-digit-ms tick-to-trade, p99 |
| Team / timeline | 5–15 engineers, 2–3 years |
| Deployment | AWS / GCP cloud. Co-lo / microwave / FPGA = future "last-hop" program |
| Languages | C++26 hot path · Python research/strategies · TypeScript UI/control plane |
| Methodology | TDD, deterministic replay, latency-gated CI |

### Non-goals (explicit)

- HFT / market making at top-of-queue. Not the system's purpose; do not design for it.
- FPGA, co-lo, microwave in initial build. See §13 for the future program.
- Node.js for strategies. UI/control plane only.
- kdb+. ClickHouse / QuestDB are sufficient at this scale and cost two orders of magnitude less.

---

## 2. Design principles

1. **Cloud now, last-hop later — but engineer for the last-hop from day one.** Patterns that make co-lo/FPGA viable later (shared memory, zero-copy, kernel bypass, deterministic POD message formats, no order-path allocations) are mandatory now. Their cost on cloud is small; retrofitting them later is a rewrite.
2. **One host, many processes, shared memory.** Not a monolithic process; not microservices over the network. Each critical-path component is its own process pinned to dedicated isolated cores, communicating via lock-free shared-memory ring buffers.
3. **Pre-trade risk is inline, not a service.** A network or even shm hop on the order-emitting path costs the latency budget. Risk *checks* are a function call inside the OMS; risk *aggregation* is a separate process.
4. **Live/sim parity is a contract, not an aspiration.** The C++ hot path emits an Aeron-archived event log; the Python backtester replays the same log. Divergence is a build-failing bug.
5. **Buy commodity infrastructure, build differentiation.** Market data feeds, FIX engines, regulatory reporting, surveillance: buy. OMS/EMS/SOR/risk/research stack: build.
6. **Doc-as-code, ADR for architectural change.** Anything that changes a contract in §16 requires an ADR.

---

## 3. Capacity and latency targets

Concrete numbers so hardware sizing and SLOs are testable.

### 3.1 Throughput

| Feed | Sustained | Peak (1ms) | Notes |
|---|---|---|---|
| US equity SIP (CTA + UTP) | 0.5 M msg/s | 5 M msg/s | At-the-open and 4pm close bursts dominate |
| US equity direct (ITCH/PITCH/ARCA) per venue | 1–3 M msg/s | 10 M msg/s | Per venue, before consolidation |
| EU MTFs aggregate | 0.5 M msg/s | 3 M msg/s | Across Cboe Europe + Aquis + LSE + Turquoise |
| OPRA (options, future) | 80 M msg/s | 250 M msg/s | Out of scope until options added |

Order-path throughput target: **100k orders/sec/host** sustained (well above mid-freq need; gives headroom).

### 3.2 Latency budgets (cloud, p99)

| Hop | Budget |
|---|---|
| Wire ingress → md-normalizer output | < 50 µs |
| md-normalizer → book-builder output | < 20 µs |
| book → strategy decision | < 50 µs (strategy-dependent floor) |
| strategy → OMS submit | < 10 µs |
| OMS pre-trade risk + SOR | < 30 µs |
| OMS → venue gateway egress | < 50 µs |
| Cloud network to venue | 200 µs – 5 ms (depends on venue + region) |
| **Tick-to-trade (excluding venue network)** | **< 200 µs p99** |
| **Tick-to-trade (end-to-end on cloud)** | **< 5 ms p99** |

Latency is measured at every hop with hardware-PTP timestamps (Nitro / GCP C3) and recorded in the Aeron archive. CI fails on >10% regression.

---

## 4. System architecture

```
                      ┌──────────────────────────────────────────────┐
                      │              Trader UI / Ops (TS+React)      │
                      │     positions, P&L, kill switch, params      │
                      └──────────────────────┬───────────────────────┘
                                             │ (gRPC / WebSocket, mTLS)
┌────────────────────┐   ┌──────────────────┴───────────────────┐   ┌────────────────────┐
│  Strategy / Alpha  │   │            Control Plane (Node.js)   │   │  Post-trade (Py)   │
│  (Python)          │◀──┤  param store, run mgmt, audit, auth, ├──▶│ allocations, recon │
│  - backtester      │   │  config service, kill-switch broker  │   │ T+1 reporting,     │
│  - alpha library   │   └──────────────────┬───────────────────┘   │ surveillance feed  │
│  - signal gen      │                      │                       └────────────────────┘
└─────────┬──────────┘                      │ (shadow signals; commands)
          │ signals                         │
          ▼                                 ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│       HOT PATH — one trading host per region/shard, multi-process, shared memory  │
│                                                                                   │
│   md-ingress ─shm─▶ normalizer ─shm─▶ book ─shm─▶ strategy-runner ─shm─▶ oms      │
│       (C++26)         (C++26)       (C++26)         (C++26 / .so)     (C++26)     │
│                                                                          │        │
│                                                            inline pre-trade risk  │
│                                                                          │        │
│                                                                          ▼        │
│                                                                    sor ─shm─▶     │
│                                                                  venue-gateway    │
│                                                                                   │
│   each box = own process, pinned to isolated CPU(s), busy-polling shm ring       │
└──────────────────────────────────────────────────────────────────────────────────┘
                ▲                       │                                  │
                │ ref data              ▼                                  ▼
   ┌─────────────────────┐    ┌──────────────────┐                ┌────────────────┐
   │ Reference data svc  │    │ Tick / event log │                │ Venue gateways │
   │ (symbology, corp    │    │ (Aeron archive,  │                │ (FIX, OUCH,    │
   │  actions, holidays) │    │  WORM S3 archive)│                │  ITCH, native) │
   └─────────────────────┘    └──────────────────┘                └────────────────┘
                                       │
                                       ▼
                              ┌──────────────────┐
                              │ Risk-aggregation │
                              │ Surveillance     │
                              │ Best-ex monitor  │
                              │ (consumers off   │
                              │  the archive)    │
                              └──────────────────┘
```

---

## 5. Hot path topology

### 5.1 Process layout per trading host

```
core 0–1   │ OS, housekeeping, control-plane RPC                    (kernel)
core 2     │ md-ingress      ──shm─▶                                (busy-poll, SCHED_FIFO)
core 3     │ md-normalizer   ──shm─▶                                (busy-poll)
core 4     │ book-builder    ──shm─▶                                (busy-poll)
core 5     │ strategy-runner (loads .so plugins)             ──shm─▶ (busy-poll)
core 6     │ oms + inline pre-trade risk + sor               ──shm─▶ (busy-poll)
core 7     │ venue-gateway (one per venue/site)                     (busy-poll)
core 8     │ aeron-archiver (taps every shm ring → NVMe → WORM S3)  (best effort)
core 9–N   │ telemetry, ops agent, log shipper, config sidecar      (kernel)
```

### 5.2 Why processes, not threads

- **Failure isolation.** Strategy plugin segfault doesn't kill the OMS or in-flight orders.
- **Independent deploy.** Roll a new strategy-runner without touching md or oms.
- **Language flexibility at edges.** strategy-runner can host C++26 plugins now and pybind11-wrapped Python plugins later — without compromising the C++ core.
- **Security boundary.** Order-emitting processes (oms, gateway) run under stricter seccomp + capability profile than research-adjacent processes.
- **Future FPGA migration.** Each process is a swap-out unit. md-ingress and venue-gateway are the most likely first FPGA replacements; the rest of the stack does not notice when they're replaced by an FPGA card driver feeding the same shm ring format.

A single SPSC ring hop is 50–200 ns on modern x86 — well within the latency budget.

### 5.3 Critical-path patterns (mandatory)

- **Zero-copy serialization.** FlatBuffers for inter-process and on-disk; pointer arithmetic into the shm slot, no copy on read. **No JSON, no Protobuf, no ad-hoc structs anywhere on the order path.**
- **Zero allocations on the order path.** Pre-allocated arenas per session; object pools for orders/fills/book-deltas. CI-time allocation tracker (tcmalloc hooks or perf-event-based) fails the build if any path from `md_ingress::on_packet` to `gateway::send` allocates.
- **Kernel bypass at ingress and egress.** Cloud now: AWS ENA Express + AF_XDP for UDP multicast where venues/vendors deliver multicast; busy-polling epoll with `SO_BUSY_POLL` for TCP/FIX. Co-lo later: Solarflare onload / DPDK / Mellanox VMA — same shm-ring contract on the inside, swap the driver.
- **Busy-polling, not event-driven.** Critical-path consumers spin on their shm ring; cores are dedicated, not shared. Power cost is irrelevant at this scale.
- **CPU isolation.** `isolcpus=2-8 nohz_full=2-8 rcu_nocbs=2-8` at boot; tasks pinned via cgroups; IRQs steered off isolated cores; transparent huge pages on.
- **Time.** Hardware PTP where the cloud supports it (AWS Time Sync Service on Nitro c7i/m7i; GCP PTP on C3); software fallback elsewhere. All messages timestamped at ingress and at every hop, with the timestamp source recorded.

### 5.4 C++ standard policy

- **Floor: C++23.** All code must compile under clang-17 / gcc-13 minimum.
- **Ceiling: C++26 features that have landed in clang-20 / gcc-15 with stable ABI.** Modules, `std::expected`, executors-lite, `std::generator`, contracts (opt-in mode). Each C++26 feature requires a `cmake` feature flag and a fallback. No bleeding-edge C++26 in production hot path until at least one compiler ships it stably for one full major release.
- **Pinning:** compiler + stdlib + linker versions locked via Conan profile. Reproducible builds non-negotiable.
- **No exceptions on the order path.** `std::expected<T,E>` only.

### 5.5 Shared-memory bus

Aeron IPC is the default. It provides the SPSC ring, the archiver, and replay tooling for free. For the very tightest links (md-ingress → md-normalizer → book-builder), consider a hand-rolled cache-line-aligned SPSC ring if profiling justifies it. Don't pre-optimize.

The shm ring layout (slot size, sequence counter semantics, claim/commit protocol) is **the contract** — see `core/messaging/shm_ring.hpp`. It is also the contract a future FPGA card writes/reads.

---

## 6. Scalability and high availability

### 6.1 Horizontal sharding

Within a region, scale by adding trading hosts, each owning a disjoint shard. Shard key options:

- `hash(symbol, venue)` — natural for market-data-driven workloads
- `strategy-id` — when a strategy needs many symbols on one host (cross-symbol logic)

### 6.2 Component HA model

| Component | Model | Notes |
|---|---|---|
| md-ingress, normalizer, book, strategy-runner | **Stateless replicate** | Active/active across two hosts; each consumes the same upstream feed |
| oms (+ inline risk, sor) | **Active/standby strongbox** | State replicated via Aeron-archive log shipping over host-to-host network (not shm; shm is intra-host only) |
| venue-gateway | **Active/standby with re-attach** | Standby holds session credentials hot; re-attaches via FIX `Logon ResetSeqNum=N` or venue-specific resume |
| Risk aggregation, surveillance, best-ex monitor | **Stateless re-deriving** | Rebuild from Aeron archive on failover |
| Reference-data service | **Read-replica Postgres + cache** | Standard |

### 6.3 OMS handover — the in-flight orders problem

The classic gotcha. Specified explicitly to avoid hand-waving:

- Every order has a deterministic `cl_ord_id = (host_id, session_id, monotonic_seq)` — same id on primary and standby.
- Standby tails Aeron archive and maintains a shadow OMS state (orders, fills, book-state).
- On failover detection (heartbeat loss + consensus), standby acquires the lease via `etcd` (or a simpler primary-lease scheme) and the venue-gateway re-attaches with a known sequence number.
- For each order the standby believes is in-flight: send `OrderStatusRequest` (FIX MsgType=H) at session resume; reconcile state from venue response before allowing new orders for that account.
- "Stuck" orders (no venue response within N seconds): cancel via venue or escalate to ops. Configurable per-venue policy.
- Failover SLO: < 30 seconds to fully operational with reconciled state. Strategy-runner is paused (can't submit) until OMS confirms reconciliation complete.

Tested via quarterly DR drill (§17).

---

## 7. Subsystems

### 7.1 Market data ingestion

**Decision: vendor-normalized first, raw later.**

| Source | Use | Rationale |
|---|---|---|
| Databento (US, normalized + raw archives) | Year 1 default | Excellent S3 historical archive, low latency live feed, programmatic API, fits cloud |
| Polygon (US) | Backup / cost-comparison | Acceptable quality, cheaper for early stage |
| LSEG (Refinitiv) Real-Time / RDP | EU MTFs | Industry standard for EU coverage |
| ICE Consolidated Feed | Optional alternate | When direct ICE products are added |
| Direct from venue (ITCH, PITCH, MDP3) | Year 2+, where latency justifies | Requires per-venue conformance, cross-connect or wire-image, dedicated normalization codec |

Year 1 ships on normalized vendor feeds. The normalizer abstraction makes raw-from-venue a swap-in: same output shm format, different ingress codec.

### 7.2 Reference data and symbology

A first-class subsystem, **not** an afterthought.

- **Symbology:** OpenFIGI as the canonical mapping; CUSIP/CIK/ticker (US) and ISIN+MIC (EU) cross-references; daily diff against vendor master (Bloomberg BSYM or LSEG) with an exception queue.
- **Corporate actions:** splits, dividends, mergers, ticker changes, IPOs, delistings — applied as point-in-time events to historical tick data and to live position/P&L. Vendor: ICE Corporate Actions or LSEG.
- **Trading calendars:** market holidays, half days, expirations, settlement dates — sourced from MIC-keyed vendor (NYSE, LSE, etc.).
- **Account / book hierarchy:** firm → trading book → account → strategy. Normalized in Postgres; pushed to hot path as a memory-mapped read-only file refreshed at session start (no DB queries on hot path).

Storage: Postgres primary, with a generated immutable snapshot file (FlatBuffers) loaded by hot-path processes at start-of-day.

**Why first-class:** undetected symbology errors cause the worst silent bug class — orders sent for the wrong instrument, P&L attributed to the wrong book, surveillance gaps. Budget a dedicated engineer for this in year 1.

### 7.3 Strategy / research (Python)

- **Backtester:** event-driven (not vectorized-only) so the same strategy code runs in backtest and live. API surface fixed on day one: `on_book_update`, `on_trade`, `on_timer`, `on_session_state`, `submit(order)`, `cancel(id)`.
- **Data substrate:** Polars + Arrow primary; Pandas only at notebook edges.
- **Tick store:** ClickHouse or QuestDB for tick + L2 (last 30 days hot); Parquet on S3 + Arrow for cold; DuckDB / Athena for ad-hoc historical queries.
- **Live ↔ research parity:** the C++ hot path emits Aeron-archived events; the Python backtester reads the **same** events. Divergence is a build-failing bug.
- **Alpha capture:** thin SDK (`from ontrade import Alpha`) — researchers register signals with metadata (universe, refresh cadence, capacity, decay, holding period). Platform handles persistence, point-in-time correctness, feature-store semantics.
- **Shadow / dark-launch mode:** every new strategy runs in production for ≥ 5 trading days emitting "would-have-been" orders to the archive only (not to venue). Live/shadow P&L delta and order distribution are reviewed before flipping the switch.

### 7.4 Backtester realism — required model components

The backtester is only as honest as its market model. Required components, each tested independently:

- **Latency model:** per-venue tick-to-ack distribution, calibrated quarterly from production timestamps.
- **Queue position model:** reconstructs queue position from L2 deltas; conservative default for unknown queue state.
- **Fill probability model:** based on volume traded at price after order placement; configurable adverse-selection adjustment.
- **Slippage / impact model:** square-root impact (Almgren-Chriss style) calibrated per-name from production fills, with explicit handling for sweep / aggressive-cross orders.
- **Session-state effects:** opening/closing auctions, halts, LULD bands — backtester respects market-state machine output (§7.8).
- **Fees/rebates:** maker/taker schedules per venue, including liquidity-rebate tiers.

**Acceptance bar:** for every live-traded strategy, run-day P&L vs. shadow-backtest-on-same-day P&L must be within tolerance (target: 5% RMSE on per-day P&L over 30 trading days). Outside tolerance → backtester model bug or strategy state bug — investigate before adding new strategies.

### 7.5 Risk

Two distinct things often conflated:

**Pre-trade limit checks** (per-order, hot path, inline C++, sub-µs each):
- Notional max (per order, per account, per book, firmwide)
- Position max (per symbol, per account, gross/net)
- Fat-finger price band (max % from last / from NBBO mid)
- Max order size (shares, % of ADV)
- Self-cross prevention (cancel-newest or reject)
- Locate availability for shorts (with point-in-time locate inventory)
- Restricted list / no-trade list
- Venue-specific (ISO, MPID) limits
- Kill switch state (§7.6)

**Risk state aggregation** (per-account, near-real-time, separate process):
- VaR (historical and parametric), beta-adjusted exposure
- Sector / single-name concentration, leverage, liquidity-day-to-flatten
- Gross/net by book, intraday P&L attribution
- Computed off the Aeron event stream; aggregated limits push back to hot-path pre-trade as updates to a shared-memory limits struct.

### 7.6 Kill switch — first-class component

Required by SEC Rule 15c3-5. Architectural surface:

- **Triggers:**
  - Hardware/UI button in trader UI (one-click, MFA-gated)
  - Operations runbook command (CLI with audit trail)
  - Automated risk breach (limits engine)
  - Heartbeat / staleness detector (md feed gap, oms stuck)
  - Venue-side cancel-on-disconnect (configured per-session)
- **Granularity:** strategy / account / book / venue / firmwide. Firmwide is global cancel + halt + alert.
- **Action sequence (firmwide kill, target < 100 ms):**
  1. OMS sets `halt=true` flag — no new orders accepted from any strategy
  2. OMS issues mass-cancel (FIX `OrderMassCancelRequest`, MsgType=q) per venue
  3. Strategy-runner receives `on_session_state(halted)` and stops generating signals
  4. Trader UI displays halted state with red banner
  5. Audit record signed and written to WORM archive
- **Recovery:** explicit two-person ack (trader + risk officer) via UI to clear halt. State persisted across restarts.
- **Tested monthly** (§17).

### 7.7 OMS / EMS / SOR

- **OMS state** lives in the oms process; persisted via Aeron archive + periodic snapshots to S3. Standby on a second host tails the archive (§6.3).
- **SOR** is its own module, testable in isolation. Start simple: one venue per symbol with manual override table. Build dynamic SOR (probability-of-fill, fee/rebate-aware, hidden-liquidity prior) only after real routing data.
- **FIX engine: do not write your own.** Use [QuickFIX/n](https://github.com/connamara/quickfixn) or commercial (Onixs, Fix8) wrapped in a thin C++26 adapter. Native binary protocols (NASDAQ OUCH, ICE iMpact, CME iLink-3) get hand-rolled codecs because they're small and FIX libraries don't cover them.
- **Venue connectivity for EU MTFs** (Cboe Europe, Aquis, Turquoise, LSE): each is a 4–8 engineer-week integration including conformance. Justifies a dedicated connectivity engineer.

### 7.8 Market state machine

A system-wide concern, frequently forgotten until production. Maintained as a single source of truth, consumed by every strategy and risk module.

- **Per-market state:** pre-open / opening-auction / regular / closing-auction / post / halted (market-wide).
- **Per-venue state:** open / closed / outage.
- **Per-symbol state:** trading / halted / LULD-pause / circuit-breaker / SSR-active (short-sale restriction).
- **Source of truth:** SIP messages (LULD, halts) + venue session events + holiday calendar. Aggregated by a stateless `market-state` process and broadcast on its own shm ring.
- **Strategies** receive `on_session_state` and adjust behavior (e.g., no aggressive orders during LULD pause; auction-only orders during opening auction).

### 7.9 Post-trade

Python primary, with Java/Kotlin where regulatory vendor SDKs require.

- **Allocations:** block → account, with audit trail (regulatory).
- **Commission/fee calculation:** per-venue fee schedule, maker/taker, ETF creation/redemption fees.
- **Reconciliation:** daily against broker / prime, against custodian. Zero-break SLO before next open.
- **Regulatory reporting:**
  - **US:** CAT (Consolidated Audit Trail) order events, OATS (where still applicable), Rule 605/606 best-ex.
  - **EU:** MiFID II RTS 22 transaction reports (to ARM), RTS 24 record-keeping, RTS 27/28 best-ex; MiFIR commodity position reports as applicable.
  - **Vendor:** Cappitech / Kaizen / Broadridge — buy, do not build (§9).
- **Record-keeping format:** WORM-compliant, signed, retention per regulation (US 17a-4: 6 years for orders; EU MiFID II: 5–7 years).

### 7.10 Trade surveillance

**Buy.** NICE Actimize, Nasdaq SMARTS, b-next. Feed them order/trade events from the Aeron archive via standard format (FIXML or vendor-specific). Building this in-house at 5–15 engineers is unrealistic and rule packs evolve faster than internal teams.

Internal scope: maintain the data feed, alert routing, case-management hooks. Not the detection logic.

### 7.11 Best-execution monitoring

Capture from day one — reconstructing later is painful.

- Every order/fill carries: NBBO-at-time, venue chosen, fee/rebate, fill rate, queue position estimate, alternate-venue counterfactual.
- Daily report: per-strategy and per-venue best-ex metrics (effective spread, realized spread, fill rate, slippage vs arrival).
- Quarterly RTS 27/28 (EU) and Rule 605/606 (US) reports generated from the same store.
- Drives SOR re-tuning and venue mix decisions.

### 7.12 Configuration management

Operationally critical, often architecturally absent.

- **Source of truth: GitOps repo.** All venue endpoints, FIX session config, risk limits, strategy params, account mappings are versioned YAML in `config/`.
- **Distribution:** central config service (Node.js control plane) loads the repo, validates, signs, and serves to clients via authenticated gRPC.
- **Hot-path consumption:** configuration is materialized as immutable FlatBuffers files at session start; processes mmap them. No runtime config changes on hot path during a trading session (deliberate: config changes mean restart).
- **Limit changes during session:** allowed only through a separate "limits update" channel (signed gRPC → control plane → push to oms via dedicated shm message). Two-person ack required for tightening below current usage; one-person for relaxing within firm-wide cap.
- **Audit:** every config change is a signed git commit + a deployment record + a runtime apply event.

### 7.13 Observability

- **Metrics:** Prometheus + Thanos for long retention. Per-component, per-symbol, per-strategy histograms for: latency at every shm hop, throughput, queue depth, event-loop time, gateway round-trip.
- **Tracing across shm:** custom — every message carries a 64-bit trace id; consumer logs `(trace_id, hop_name, in_ts, out_ts)` to a side-channel ring; aggregator emits OpenTelemetry spans. (OpenTelemetry SDK on the hot path itself is too heavy.)
- **Structured event log:** Loki or ELK; every operationally relevant event (order, fill, halt, deploy, config change) goes here with the trace id.
- **P&L attribution dashboards:** per-strategy realized + unrealized, decomposed into alpha / fees / slippage / risk-adjustment.
- **Alerting:** PagerDuty integration. Key alarms: md-feed staleness > 100 ms, OMS heartbeat loss, any kill-switch trigger, recon break unresolved by 06:00 next-day local, strategy P&L breach.

### 7.14 Security and access

- **Venue credentials:** AWS KMS / GCP KMS for FIX session secrets and exchange-membership API keys. Hot-path processes get derived ephemeral creds at session start; never persisted to disk.
- **MFA on all ops actions** that change limits, deploy, or trigger kill switch.
- **Network segmentation:** trader UI and research VPCs cannot reach trading hosts directly — only via the control plane. Trading hosts have egress only to venues + archive S3 + observability backends, no general internet.
- **Audit log signing:** every order, fill, config change, ops action signed with KMS key + chained hash for tamper evidence. Hourly seal to WORM S3.
- **Deployment:** signed binaries (Sigstore / GCP Binary Authorization). Hot-path processes refuse to start without valid signature.

### 7.15 Storage

| Data | Store | Why |
|---|---|---|
| Tick / L2 (hot, ≤ 30d) | ClickHouse on NVMe | Fast scan, SQL, columnar, free |
| Tick / L2 (cold, > 30d) | Parquet on S3 + Arrow | Cheap, queryable via DuckDB / Athena |
| Reference data | Postgres + materialized FlatBuffers snapshot | Symbology, holidays, corp actions, account/book |
| Trade / order log | Postgres + WORM S3 archive | Compliance source of truth (17a-4 / MiFID retention) |
| In-flight state | Aeron archive + Redis | Hot-path persistence + low-latency cache for control plane |
| Time-series metrics | Prometheus + Thanos | Standard ops |
| Audit log | WORM S3 + signed-chain | Tamper-evident; required for surveillance + 17a-4 |

### 7.16 Messaging

- **Hot path (intra-host):** Aeron IPC over shm.
- **Hot path (cross-host, archive replication, OMS replication):** Aeron UDP unicast.
- **Control plane / non-hot:** NATS JetStream (lighter, easier ops at this team size) or Kafka (if existing ops investment).

---

## 8. Build vs buy

| Component | Decision | Reason |
|---|---|---|
| Market data feeds (live + historical) | **Buy** (Databento, Polygon, LSEG, ICE) | 12+ engineer-years to replicate; not differentiating |
| Normalized vs raw | **Normalized first**, raw later per venue when latency justifies | Year 1 simplification |
| FIX engine | **Buy / OSS** (QuickFIX or Onixs) | Mature, conformance-tested |
| Native binary venue codecs (OUCH/ITCH/iLink/MDP3) | **Build** | Small, perf-critical, FPGA migration candidates |
| OMS/EMS core | **Build** | Differentiating, tight risk integration |
| Pre-trade risk | **Build** | Must be in-process |
| SOR | **Build** (start simple) | Differentiating, evolves with venue mix |
| Backtester / research stack | **Build** (on Polars/Arrow) | Differentiating; OTS doesn't give live/sim parity |
| Tick store | **OSS** (ClickHouse / QuestDB) | kdb+ overkill at scale; license cost brutal |
| FIX certification / conformance | **Outsource** per venue | Each venue requires it; vendors do it cheaper |
| Regulatory reporting (CAT, MiFID) | **Buy** (Cappitech / Kaizen / Broadridge) | Non-differentiating, high regulatory risk |
| Trade surveillance | **Buy** (NICE Actimize / Nasdaq SMARTS / b-next) | Rule packs evolve faster than internal teams |
| Symbology / corp actions | **Buy** (Bloomberg BSYM / LSEG / OpenFIGI for free tier) | Domain-specific, error-prone, vendor maintains |
| Trader UI / ops dashboards | **Build** (TS + React) | Workflow is the IP |
| Auth / SSO | **Buy** (Auth0 / Okta) | Don't reinvent |
| Secrets management | **Buy** (AWS KMS / HashiCorp Vault) | Don't reinvent |

---

## 9. Test-driven development strategy

TDD is the right instinct, but the *kind* of test differs by layer.

- **C++ hot path:** GoogleTest unit; **deterministic replay** as primary integration test (capture an Aeron event log from a real session, replay through the system, assert byte-identical output). Google Benchmark suite gates PRs on p50/p99 latency regression — > 10% fails. libFuzzer on every codec.
- **Risk engine:** property-based (rapidcheck for C++, Hypothesis for Python wrappers). Risk is exactly where adversarial inputs find bugs unit tests miss.
- **Strategy code:** pytest + Hypothesis. Every strategy ships with a "shared history" backtest as integration test — code change moves equity curve → CI flags for review (not auto-fail; backtests *should* move sometimes).
- **End-to-end:** dockerized exchange simulator (`sim/matcher`, contract-locked to live FIX gateway behavior). Full stack pointed at it in CI.
- **No mocking the database** in integration tests. Spin real Postgres / ClickHouse via testcontainers.

### 9.1 Test-fixture management

The hard problem in replay-based testing is fixtures.

- **Golden-master event logs** are versioned in a separate Git LFS repo (size ≫ source).
- Each fixture has a manifest: source date, symbols, venues, market events covered, hash, regenerated-from script.
- Fixtures regenerated annually from a recent trading day; old fixtures retained for regression coverage of historical bug fixes.
- Synthetic fixtures (generated by `sim/`) for edge cases (halts, LULD, fat-finger, partial-fill races).

---

## 10. Phased delivery

### Year 1 — Foundation
- Hot path skeleton (md ingest, book, OMS, inline pre-trade risk, FIX out)
- One US equity venue end-to-end (broker DMA initially → direct later)
- Reference-data subsystem with daily symbology + corp-action pipeline
- Backtester with live/sim parity contract locked
- Kill switch implemented and tested
- Market state machine
- One real strategy in shadow mode → live with strict size limits
- Tick store, basic ops UI, paper-trading mode
- Best-ex capture and audit-log signing operational
- **Exit criterion:** strategy in paper trading 30 days with zero sim/live divergence incidents and zero recon breaks.

### Year 2 — Multi-venue, ETPs, EU MTFs
- 3–5 US venues + SOR v1
- EU MTF connectivity (≥ 3 of: Cboe Europe, Aquis, LSE, Turquoise)
- ETP support (creation/redemption awareness, NAV-relative pricing)
- Real-time risk-aggregation service
- Alpha library / signal store + research notebooks platform
- Post-trade: allocations, recon, vendor-driven regulatory reporting
- Trade-surveillance vendor integration live
- **Exit criterion:** 3+ strategies live across US+EU with full T+1 recon green.

### Year 3 — Scale, sophistication, prepare for last-hop
- SOR v2 (probabilistic, fee-aware)
- Cross-asset risk (if futures/options added)
- Capacity / impact modeling integrated into backtester
- Capacity planning + business case for the last-hop program. **No hardware build yet.**

---

## 11. Future: the "last-hop" program (post Year 3, separate budget and team)

Where co-lo, microwave, and FPGA belong — explicitly not in the initial 3-year build. The architecture above is shaped so this program is additive, not a rewrite.

**Trigger conditions** (any one):
1. A specific strategy's P&L is provably latency-bound on cloud (shadow A/B vs simulated co-lo).
2. Adverse-selection analysis shows orders consistently late at the matching engine.
3. Market-share goals on a venue require it.

**Scope:**
- **Co-lo cages** at the venues that matter for the chosen strategy: NY4/NY5, Carteret, Mahwah, Aurora, LD4, FR2. Cross-connects to each venue.
- **Microwave / mmWave links** between matching-engine sites where it pays: Aurora ↔ Carteret, Carteret ↔ Mahwah, Slough ↔ Frankfurt. Lease (McKay / Anova / Vigilant); do not build.
- **FPGA on the last hop only:**
  - *Ingress FPGA* — parses raw venue multicast (ITCH, MDP3, ETI) directly off-wire into normalized book delta, written to the same shm ring format the cloud `md-normalizer` produces. Rest of stack unchanged.
  - *Egress FPGA* — single-strategy "fast path" bypassing the C++ OMS for pre-validated orders (limits pre-loaded at session start), emitting native binary directly. C++ OMS still runs in parallel as system of record.
- **Microwave is the inter-site last hop**, not intra-site. Inside a cage, fiber + FPGA wins.

**Why a separate program:**
- Different skills (Verilog/HLS, RF, co-lo network engineers).
- Different vendors (NIC mfrs, microwave lessors, hardware spares).
- Different ops model (24/7 on-site, hardware spares, ITAR for some venues).
- Different unit economics ($3–10M/yr line item before staff).

**What the initial build owes the future program:**
- The shm ring message format is the contract. FPGA cards write/read those formats.
- OMS and risk are deterministic and replayable — FPGA fast-path validated against them via shadow trading.
- No code in the cloud build assumes a single binary or a particular kernel/network stack.

---

## 12. Repository layout

```
ontrade/
├── core/              # C++26 hot path
│   ├── md/            # market data normalizer + book builder
│   ├── oms/           # order state machine
│   ├── risk/          # pre-trade limit checks (inline, header-only where possible)
│   ├── sor/           # routing
│   ├── gateways/      # FIX + native binary codecs (per-venue)
│   ├── messaging/     # shm_ring + Aeron wrappers
│   ├── runtime/       # process scaffolding, config, time
│   └── proto/         # FlatBuffers schemas
├── ref_data/          # symbology, corp actions, calendars (Python services)
├── strategies/        # Python strategy code + alpha library
├── research/          # notebooks, backtester, feature store
├── post_trade/        # Python: allocs, recon, reporting, surveillance feed
├── control/           # Node.js / TS control plane + ops API + config service
├── ui/                # React trader UI
├── infra/             # Terraform / Pulumi, k8s manifests, CI
├── sim/               # exchange simulator + replay tooling
├── ops/
│   ├── runbooks/      # incident playbooks (kill-switch, failover, recon-break, etc.)
│   └── dashboards/    # Grafana JSON
├── docs/
│   ├── architecture.md  # ← this file
│   ├── adr/             # architecture decision records
│   └── ...
└── fixtures/          # references into the LFS test-fixture repo
```

**Build system:** Conan + CMake (C++) + uv (Python) + pnpm (TS) initially. Migrate to Bazel at month 3+ when the C++ build alone justifies it.

---

## 13. Verification — how we know it works end-to-end

1. **Replay parity.** Capture production Aeron log; replay through live binary and Python backtester; both produce byte-identical orders.
2. **Latency-budget gates.** CI publishes p50/p99 tick-to-order per build; PR fails on > 10% regression.
3. **Allocation correctness.** Property-based test: random allocations × random partial fills = consistent per-account positions.
4. **Paper trading run.** 30 consecutive trading days in paper before real money. Daily P&L delta paper-vs-parallel-backtest within tolerance.
5. **Recon zero-break.** Every trading day, broker statement reconciles to internal trade log with zero unexplained breaks before next open.
6. **Kill-switch drill.** Monthly, in paper. All open orders cancelled within 100 ms; positions reported correctly.
7. **DR drill.** Quarterly. Kill primary OMS mid-session; standby promotes; in-flight orders reconciled per §6.3; full operation in < 30 s.
8. **Best-ex audit.** Quarterly review of RTS 27/28 / Rule 606 reports; any regression in fill rate or effective spread vs prior quarter triggers SOR re-tuning.
9. **Surveillance alert review.** Weekly review of vendor surveillance alerts; false-positive rate tracked; rule packs tuned with vendor.

---

## 14. Critical contract files (ship first, with tests)

These define the platform. Land them — with tests and ADRs — before any feature code on top.

| File | Purpose |
|---|---|
| `core/proto/hot/messages.hpp` | Fixed-layout POD wire format for the hot-path shm rings. **Single most important file in the system** — also the contract a future FPGA card writes/reads. Layout is pinned by `static_assert`s; changes bump `kSchemaMajor` and require an ADR. See ADR-002. |
| `core/runtime/clock.hpp` | Single source of time, mockable for replay |
| `core/messaging/shm_ring.hpp` | SPSC/MPSC shared-memory ring layout (cache-line-aligned slots, sequence-number semantics, claim/commit) |
| `core/messaging/aeron_bus.hpp` | Aeron IPC + archive wrapper for inter-process messaging needing durability/replay |
| `core/risk/limits.hpp` | Inline pre-trade check interface (called as function inside oms, not over shm) |
| `core/runtime/market_state.hpp` | Per-market / per-venue / per-symbol state machine |
| `strategies/sdk/strategy.py` | Python strategy SDK — identical surface to C++ strategy-runner plugin ABI |
| `sim/matcher/` | Minimal price-time-priority matcher; contract-locked to live FIX-gateway behavior |

Two of these (`core/proto/hot/messages.hpp` and `shm_ring.hpp`) also define the contract for the future FPGA last-hop program. Review them with that future in mind.

---

## 15. Architecture Decision Records

Every architectural change touching §3 capacity targets, §5–§7 subsystem contracts, or §14 critical files requires an ADR.

Format: short markdown in `docs/adr/NNN-title.md` with sections **Context**, **Decision**, **Consequences**, **Alternatives considered**.

Initial ADRs to write:
- ADR-001: Multi-process shared-memory architecture (vs. monolith / vs. microservices)
- ADR-002: FlatBuffers as hot-path serialization (vs. Protobuf / Cap'n Proto)
- ADR-003: Cloud-first with FPGA/co-lo as future last-hop program
- ADR-004: Vendor-normalized market data Year 1 (vs. raw direct)
- ADR-005: Buy regulatory reporting and surveillance (vs. build)
- ADR-006: ClickHouse for tick store (vs. kdb+ / QuestDB)

---

## 16. Open questions (not blockers)

- Direct exchange membership vs broker DMA for US — affects connectivity cost, regulatory burden, latency floor.
- Prime broker / clearing arrangement — drives post-trade integration shape.
- Cloud baremetal (`c7i.metal`) vs virtualized — ~30 µs improvement at ingress; cost vs benefit per shard.
- Crypto in scope eventually? — if yes, connectivity and risk diverge enough that a separate hot-path binary may be cleaner than a unified one.
- ARM (Graviton4) vs x86 for trading hosts — ARM is cheaper and increasingly performant; benchmark on representative workload before committing.

---

## 17. Glossary (selected)

- **ADV** — average daily volume
- **CAT** — Consolidated Audit Trail (US)
- **DMA** — direct market access
- **LULD** — limit up / limit down (US single-stock circuit breakers)
- **MTF** — multilateral trading facility (EU)
- **OUCH / ITCH** — Nasdaq's order-entry / market-data binary protocols
- **MDP3** — CME's market-data protocol
- **RTS 22/24/27/28** — MiFID II regulatory technical standards (transaction reporting, record-keeping, best-ex)
- **SIP** — securities information processor (US consolidated quote/trade feed)
- **SOR** — smart order router
- **SSR** — short-sale restriction (US Rule 201)
- **SPSC / MPSC** — single-producer single-consumer / multi-producer single-consumer queue
- **WORM** — write-once-read-many (regulatory storage)
