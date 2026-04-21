# Memory Verification v1

Date: 2026-04-15

## Tests Executed

Focused verification was run through `tests/test_canonical_memory_core.py` in the Linux project `venv`.

Coverage in this pack includes:

- active truth uniqueness for preference, fact, decision, and document reference identities
- conflicting active truth handling and explicit supersession
- historical retrieval behavior for superseded, deprecated, and archived records
- raw-input, malformed-input, and speculative-memory rejection
- retrieval default safety and combined filter integrity
- deterministic ranking and deterministic context assembly
- bounded context assembly metadata and per-type caps
- OwnerBox explicit-assembly boundary behavior and no hidden canonical auto-injection
- cross-domain blocking on canonical assembly requests
- malformed store row fail-closed handling
- pre-existing duplicate active legacy row handling
- partial-write rollback on promotion failure
- execution-result promotion path provenance preservation

Final verification run:

- `source venv/bin/activate && pytest -q tests/test_canonical_memory_core.py`
- result: `44 passed`

## Failures Found

Two real issues were reproduced during adversarial verification:

1. malformed persisted rows could escape as low-level decode/validation exceptions instead of a canonical store failure
2. retrieval could return multiple contradictory legacy `active` rows for the same `conflict_key` if the database predated the active-conflict unique index

## Fixes Applied

- `app/memory/memory_store.py`
  - wrapped malformed row decode/validation failures as `CanonicalMemoryStoreError("malformed canonical memory record")`
- `app/memory/memory_retrieval.py`
  - constrained retrieval to a single deterministic `active` winner per `conflict_key`, preventing silent presentation of contradictory legacy active truth
- `app/memory/context_assembly.py`
  - removed wall-clock metadata drift by deriving `assembly_metadata.assembled_at` deterministically from the request/store state
- `app/ownerbox/context_boundary.py`
  - removed wall-clock drift from blocked cross-domain canonical context responses
- `tests/test_canonical_memory_core.py`
  - added adversarial verification cases for duplicate fact/document identity, speculative-memory rejection, combined filters, repeat ranking stability, deterministic context assembly, no hidden auto-injection, malformed store rows, legacy duplicate active rows, and partial-write rollback

## Final Pass Status

PASS

Canonical Memory now passed the adversarial verification pack exercised in the current repository state without scope expansion.

## Critical Observations

- default retrieval and default context assembly remain `active`-only
- historical truth remains queryable only through explicit lifecycle filters
- raw payload promotion paths remain fail-closed
- OwnerBox canonical memory access still requires explicit assembly inputs; no hidden background injection path was observed
- legacy duplicate active rows are now constrained deterministically on read, which protects operational defaults even if old local data is already malformed
- malformed store records now fail closed at the store boundary instead of leaking parser-level exceptions

## Remaining Non-Blocking Caveats

- the verification pack is scoped to the currently implemented `ownerbox` domain only
- pytest emitted pre-existing config warnings for unknown `timeout` and `timeout_method` options; these did not affect canonical memory verification results
