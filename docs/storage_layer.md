# Storage Layer

## GitHub Versus Storage

GitHub is not storage in Digital Foreman.

- GitHub remains the execution, audit, and development surface.
- The Storage Adapter is the data and memory boundary.
- Artifact persistence must not depend on GitHub paths, commits, or issue state.

## Hot, Warm, Cold Model

- Hot: local-first runtime state, active tasks, active artifacts, and execution outputs.
- Warm: future OpenCloud-backed domain storage.
- Cold: future long-horizon or secondary backends such as Google.

The current implementation remains local-first:

- Local storage is the primary source of truth for active artifacts.
- OpenCloud is a secondary warm or cold layer behind the adapter.
- Runtime execution does not move to OpenCloud in this step.

## Domain Separation Rules

- Allowed storage domains are `ownerbox` and `dev`.
- `ownerbox` resolves to `/home/avoro/ownerbox/artifacts/`.
- `dev` resolves to `/home/avoro/df-dev/artifacts/`.
- Shared or system documentation, when needed, belongs under `/home/avoro/df-system/shared/`.
- A domain may only read, write, or archive artifacts inside its own namespace.
- Cross-domain paths are rejected even when they exist on the same host.

## Why Storage Adapter Comes Before Cloud Integration

- Domain boundaries have to be enforced locally before remote backends are introduced.
- Without a local adapter contract, cloud storage would copy today’s path ambiguity into a harder-to-fix backend.
- The adapter creates a single boundary where future backends can plug in without changing contour rules.

## Current Local Adapter Scope

- Resolve domain-aware artifact paths.
- Save structured JSON artifacts with `id`, `domain`, `timestamp`, `type`, and `payload`.
- Load artifacts only from the caller's own domain namespace.
- Archive artifacts into an `archive/` subfolder inside the same domain namespace.

## OpenCloud Secondary Layer

- Backend selection is config-driven with `local` as the default backend.
- OpenCloud support is explicit and optional through `sync_to_opencloud()` and `fetch_from_opencloud()`.
- `ownerbox` may sync local artifacts to OpenCloud and fetch them back into its own local namespace.
- `dev` may sync local artifacts to OpenCloud only as export. It may not fetch them back through the adapter.
- The adapter expects a full WebDAV base URL and credentials outside the repo, using username plus an App Token.

Cloud remains secondary because the local contour boundary must remain the first enforcement layer. If cloud transport fails, local artifacts still remain valid and available.

## Next Step

The next storage step is a fuller OpenCloud backend adapter lifecycle with directory discovery, retention policy, and later backend expansion without changing the local-first contour rules.
