# Canonical Memory Architecture v1

## What canonical memory is

Canonical memory is a governed store of explicit, typed memory objects that can be reused in future execution. It holds promoted facts, preferences, decisions, document references, entity references, and relationships that passed an explicit write policy.

Canonical memory is execution-grounded. Every object preserves:

- a stable `memory_id`
- an explicit `memory_type`
- a bounded `domain_type`
- `owner_ref` and/or `subject_ref`
- a controlled `content_summary`
- a structured payload
- trust metadata
- source attribution
- lifecycle timestamps and status
- bounded audit metadata for every write or lifecycle transition

## What canonical memory is not

Canonical memory is not:

- raw chat transcript storage
- raw browser or mailbox dumps
- raw external artifacts
- trace storage
- workflow durability state
- idempotency state
- uncontrolled connector sync
- unconstrained semantic search

## Memory object types

v1 defines explicit typed objects only:

- `MemoryFact`
- `MemoryDecision`
- `MemoryPreference`
- `MemoryEntityRef`
- `MemoryDocumentRef`
- `MemoryRelationship`

There is no undifferentiated memory blob type in v1.

## Memory promotion path

Promotion is explicit and bounded:

1. owner input, execution result, structured decision, bounded evidence summary, or controlled document reference produces a `MemoryPromotionCandidate`
2. `memory_policy.py` evaluates the candidate against fail-closed rules
3. `memory_lifecycle.py` computes a typed conflict key and deterministic supersession plan where applicable
4. only allowed candidates are converted into a typed canonical memory object
5. the object is persisted in the canonical memory SQLite store with audit metadata and lifecycle linkage

Source attribution is preserved through `source_type` and `source_ref` on every canonical memory object.

## Lifecycle states

Canonical memory lifecycle is explicit. v1 supports:

- `active`: current canonical truth eligible for default retrieval and context assembly
- `superseded`: historical truth replaced by a newer canonical object under the same typed conflict key
- `deprecated`: historical truth intentionally marked no longer valid for default use without deleting provenance
- `archived`: historical truth retained for audit or reference but excluded from default operational retrieval

Default promotion creates `active` records only. Lifecycle changes are explicit store updates. Canonical records are never silently deleted.

## Supersession rules

Supersession is deterministic and bounded by typed conflict keys:

- preferences supersede prior active preferences for the same owner, subject, and `preference_key`
- decisions supersede prior active decisions for the same owner, subject, and explicit decision scope or reference
- facts supersede prior active facts for the same owner, subject, and `fact_key`
- document references supersede prior active references for the same owner, subject, and canonical document identifier or locator

Supersession preserves history. Prior records remain stored with `status="superseded"` and `superseded_by_memory_id` pointing to the replacing record. The replacing record keeps `audit_metadata.prior_memory_id`.

## Conflict handling

Conflict handling is typed and explicit:

- duplicate active truth with the same typed key and equivalent value is rejected
- incompatible active truth under a supersedable typed key is resolved by deterministic supersession
- unsupported or malformed conflict shapes fail closed

The system does not attempt fuzzy reconciliation, semantic merging, or model-driven conflict resolution.

## Write governance rules

Allowed canonical promotion sources in v1:

- validated owner facts
- validated owner preferences
- approved execution results
- structured decisions
- bounded execution evidence summaries
- document references with controlled summaries

Disallowed canonical promotion sources in v1:

- raw browser dumps
- raw mailbox dumps
- raw transcripts
- speculative model inferences
- unvalidated connector content
- workflow continuity state
- idempotency records
- raw trace payloads

The policy fails closed for unknown candidate kinds or source types.

It also rejects malformed candidates missing attribution and rejects raw printer payloads, raw email bodies, transcript fragments, and raw connector payloads.

## Retrieval and ranking rules

Retrieval is bounded and deterministic. Queries can filter by:

- domain
- memory type
- owner reference
- subject reference
- status
- trust class
- freshness window
- bounded text match

Ranking in v1 is explicit:

1. exact subject and owner match
2. active lifecycle status
3. trust level
4. freshness
5. optional type priority
6. stable `memory_id` tie-break

There is no freeform embedding search in v1.

## Retrieval defaults

Default retrieval is `active` only. Historical states are excluded unless the caller explicitly requests them with lifecycle status filters.

This is intentional:

- default operational reads should not rehydrate stale truth
- historical reads should be deliberate and query-scoped
- ranking should never quietly prefer deprecated or superseded truth over active truth

## Active vs historical semantics

Operational code should treat `active` memory as the canonical present state. Historical statuses exist for provenance, audit, and controlled retrospective reads.

- `active` is eligible for default context assembly
- `superseded`, `deprecated`, and `archived` are historical
- historical objects remain queryable only when explicitly requested
- history does not mutate trace, workflow durability, or idempotency state

## Context assembly rules

`context_assembly.py` turns retrieved canonical memory into a bounded context pack with:

- `memory_refs`
- `fact_summaries`
- `preferences`
- `relevant_decisions`
- `related_document_refs`
- `assembly_metadata`

Default context assembly only includes `active` memory. It enforces bounded per-type caps, stable ordering, and exclusion metadata for records omitted due to lifecycle status or cap limits.

Context assembly is read-only. It does not write memory and it does not replace trace.

## Separation from trace and durable runtime state

Canonical memory uses its own SQLite file and its own schema. It is intentionally separate from:

- `runtime/state/task_state.sqlite3`
- `runtime/state/idempotency.sqlite3`
- `runtime/state/ownerbox_workflow_state.sqlite3`

This separation is structural, not just naming. Canonical memory objects only admit governed memory types. Trace payloads, workflow snapshots, and idempotency records are not canonical memory objects.

Canonical memory is also separate from connector storage. Raw browser, printer, mailbox, transcript, and connector payloads are not stored as canonical memory objects.

## Why canonical memory is not auto-memory

Canonical memory does not self-write from arbitrary interactions. Writes happen only through explicit promotion paths that pass policy checks and lifecycle governance.

This keeps the store deterministic, attributable, and bounded. OwnerBox can read canonical memory only when context assembly is explicitly requested; there is no hidden global memory injection.

## Why this differs from generic chat memory or generic RAG

This design is not generic chat memory because:

- memory writes are explicit, not implicit
- canonical memory objects are typed
- source attribution is mandatory
- domain scoping is mandatory
- write governance is fail-closed
- retrieval is bounded and deterministic

This design is not generic RAG because:

- it does not ingest arbitrary corpora
- it does not default to unbounded semantic retrieval
- it does not treat raw documents as canonical memory
- it distinguishes canonical memory from trace and runtime durability

## Current v1 limitations

- only the `ownerbox` domain is supported
- no BusinessBox or other domain implementations exist yet
- no connector ingestion exists
- no automatic memory promotion exists
- no vector database expansion exists
- no cross-domain reads are allowed by default
- ranking is explicit and rule-based only
