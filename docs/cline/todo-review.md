# OnTrade Documentation Review - Todo List

> **Created by Cline** for architecture and documentation review

---

## High Priority

- [ ] **Create ADR template** (`docs/adr/template.md`)
  - Standard format for new ADRs
  - Include status, date, context, decision, consequences, alternatives

- [ ] **Add ADR index** to `docs/adr/README.md`
  - Table listing all ADRs with status and dates
  - Quick reference for decision history

- [ ] **Create onboarding guide** (`docs/onboarding.md`)
  - New engineer setup steps
  - Build system overview (Conan, CMake, uv, pnpm)
  - Key concepts and terminology
  - First code change walkthrough

---

## Medium Priority

- [ ] **Split architecture.md** into focused documents
  - `docs/capacity.md` - §3 throughput and latency targets
  - `docs/subsystems/` - §7 detailed subsystem docs
  - `docs/contracts/` - §14 critical contract files reference
  - Keep architecture.md as high-level overview with links

- [ ] **Create operations documentation** (`docs/operations/`)
  - `runbooks/` - Incident response playbooks (kill-switch, failover, recon-break)
  - `dashboards/` - Grafana dashboard documentation
  - `drills.md` - Monthly kill-switch and quarterly DR drill procedures

- [ ] **Add security documentation** (`docs/security.md`)
  - Threat model for trading system
  - Network segmentation details
  - Audit log signing and tamper-evidence
  - Venue credential management (KMS)

- [ ] **Create API documentation** (`docs/api/`)
  - Control plane gRPC API reference
  - Strategy SDK Python API
  - WebSocket protocol for trader UI

---

## Lower Priority

- [ ] **Expand glossary** (`docs/glossary.md`)
  - Move §17 glossary to standalone file
  - Add more trading industry terms
  - Include acronym quick-reference

- [ ] **Document data retention** (`docs/compliance/retention.md`)
  - WORM storage policies
  - Jurisdiction-specific retention (US 17a-4, EU MiFID II)
  - Data classification (PII, trading data, audit logs)

- [ ] **Add multi-region DR** (`docs/dr-multi-region.md`)
  - Region-level failover strategy
  - RTO/RPO targets per component
  - Cross-region data replication

- [ ] **Create capacity planning guide** (`docs/capacity-planning.md`)
  - When to shard and how
  - Rebalancing procedures
  - Growth projections

- [ ] **Define SLAs/SLOs** (`docs/slo.md`)
  - Service level objectives per component
  - Error budget policies
  - Escalation procedures

---

## Technical Considerations

- [ ] **Document C++26 strategy** (`docs/adr/adr-007-cpp26-policy.md`)
  - Feature flag approach
  - Compiler version pinning
  - Migration path from C++23

- [ ] **Document Python GIL handling** (`docs/adr/adr-008-python-runtime.md`)
  - Multi-strategy isolation
  - Memory management
  - Hot-reloading approach

- [ ] **Document SHM contract versioning** (`docs/adr/adr-009-shm-versioning.md`)
  - Forward compatibility strategy
  - Version negotiation protocol
  - Migration procedures

---

## Process Improvements

- [ ] **Create ADR-000 (Meta-ADR)** (`docs/adr/000-meta-adr.md`)
  - ADR approval authority
  - Conflict resolution process
  - Review cadence

- [ ] **Add decision timelines to open questions** (§16)
  - When each decision must be made
  - Dependencies between decisions

- [ ] **Document RFC process** (`docs/rfc-process.md`)
  - When to use RFC vs ADR
  - Cross-cutting change workflow

- [ ] **Create code review standards** (`docs/code-review.md`)
  - Review checklist
  - Latency regression gates
  - Security review requirements

---

## Nice to Have

- [ ] **Architecture decision log** - Visual timeline of major decisions
- [ ] **Component interaction diagrams** - Sequence diagrams for key flows
- [ ] **Performance benchmarks** - Documented baseline metrics
- [ ] **Troubleshooting guide** - Common issues and resolutions

---

## Review Checklist

Before marking complete:
- [ ] All new documents follow the established markdown format
- [ ] Cross-references between documents are accurate
- [ ] ADRs are numbered sequentially
- [ ] No broken internal links
- [ ] Consistent terminology throughout

---

*This todo list was created by **Cline** based on review of the OnTrade architecture documentation.*
