# Canonical Memory Model

## Why storage is not memory

Digital Foreman storage is a backend persistence layer. It is responsible for path safety, domain boundary enforcement, local and secondary backend writes, and artifact retrieval. Storage does not define what an object means inside DF memory.

DF memory is the control-plane object layer used by the registry, resolver, policy gates, trace readers, and conflict handling. Those consumers need a stable structural model even when the persistence backend, file shape, or remote sync strategy changes.

Keeping those responsibilities separate preserves the existing architecture:

- DF remains the control system.
- Storage remains backend-only.
- Memory remains inside DF.
- Nextcloud/OpenCloud remains a secondary backend, not the memory model.
- OpenAI remains outside memory and state.

## Why a canonical object layer is needed

Before this change, artifacts were indexed and persisted with a small artifact-oriented shape, but traces and conflicts carried slightly different semantics on top of it. That made shared reasoning across registry, resolver, and policy code more brittle than necessary.

The canonical `MemoryObject` layer provides one structural envelope for:

- artifacts
- traces
- conflicts
- evidence
- context objects
- future state objects

The canonical schema is:

- `id`
- `domain`
- `memory_class`
- `status`
- `truth_level`
- `execution_role`
- `created_at`
- `updated_at`
- `tags`
- `refs`
- `local_path`
- `remote_path`
- `payload`

Allowed values:

- `domain`: `dev`, `ownerbox`
- `memory_class`: `artifact`, `trace`, `conflict`, `evidence`, `context`, `state`
- `status`: `active`, `resolved`, `archived`, `superseded`
- `truth_level`: `working`, `validated`, `canonical`
- `execution_role`: `input`, `output`, `blocker`, `evidence`, `context_only`, `state_holder`

## How it fits the current DF architecture

The canonical layer is intentionally thin. It does not replace the current storage adapter, registry file, resolver contract, policy gates, or trace/conflict payloads.

Instead it sits between those components:

1. Storage still writes artifacts to the same domain-bound paths.
2. Before persistence, new artifacts are wrapped as canonical memory objects.
3. Registry entries now carry canonical fields while preserving legacy keys such as `type`, `logical_key`, and `timestamp`.
4. Resolver can match on `domain`, `memory_class`, `type`, `tags`, and timestamp ordering without breaking existing `type`-based callers.

This lets DF keep its stable architecture while introducing one common structural model for memory-bearing objects.

## Compatibility and preserved behavior

Compatibility is preserved by keeping legacy fields alongside canonical ones:

- `type` remains the subtype used by existing lookups.
- `logical_key` remains the stable registry key.
- `timestamp` remains available as a legacy alias of `updated_at`.
- Conflict artifacts can retain compatibility `state` values such as `pending_resolution` while their canonical `status` remains enum-clean.

That means existing flows still operate:

- storage adapter writes still work
- memory registry lookups still work
- resolver still resolves by domain and type
- cross-domain conflict escalation and resolution still work
- execution trace persistence and replay checks still work

## Domain isolation

The canonical layer does not collapse domains. Every memory object still carries an explicit `domain`, and storage/root path enforcement remains unchanged.

Isolation is preserved in two places:

- storage enforces domain-local filesystem boundaries
- resolver and registry queries still scope results by `domain`

This keeps `dev` and `ownerbox` memory separated even while they share the same canonical structure.

## Future state layer

The current implementation introduces `memory_class="state"` without changing existing runtime state handling. That is deliberate.

The canonical object model is the stable envelope future state objects can use when DF needs first-class state entries in the registry and resolver. Because the common layer is already in place, future state support can be added by introducing new producers and readers rather than rewriting storage or registry again.
