# ADR-002: Memory Governance

## Title

Canonical memory is execution-backed, governed, and separate from chat context.

## Status

Accepted

## Context

The frozen memory baseline already states that canonical memory is execution-backed truth only, typed, attributable, and promoted explicitly. It also states that full memory governance is the next approved engineering step, which means the project must preserve the baseline limits without pretending the full governance layer is already complete.

## Decision

Digital Foreman will keep canonical memory under governed promotion only.

Canonical memory:

- accepts only explicit promotion candidates
- stores typed owner-scoped records
- rejects transcript memory, raw logs, raw connector payloads, trace, workflow durability state, and idempotency state
- does not auto-inject into execution
- uses deterministic supersession rather than silent overwrite

Architecture and operational status must live in repository files and verification registries, not in chat transcripts.

## Consequences

- execution-backed facts can be reused without collapsing trace/evidence into memory
- chat remains ephemeral interaction context rather than system state
- future memory-governance work must formalize retrieval, validity, and revalidation without breaking the current baseline
- documentation and memory remain distinct governed surfaces
