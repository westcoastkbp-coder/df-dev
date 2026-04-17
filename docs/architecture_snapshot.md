# Architecture Snapshot

Digital Foreman remains one shared DF Core with multiple operational surfaces. On Zephyrus, one machine does not mean one system: the current Linux host now carries two isolated local contours that share DF Core but do not share contour-private memory or artifact domains.

## Shared Core

- DF Core path: `/home/avoro/projects/digital_foreman`
- Responsibility: execution and control foundation, system config, shared adapters, and the future storage adapter layer
- Rule: core code may be shared across contours, but contour-private state may not be implicitly shared

## Local Contours On Zephyrus

### `df-dev`

- Purpose: development, testing, repo work, and dev-only artifact generation
- Local contour root: `/home/avoro/df-dev`
- Current repo baseline: `/home/avoro/projects/digital_foreman`
- Artifact and export roots: `/home/avoro/data/hot/df-dev` and `/home/avoro/data/export/df-dev`

### `ownerbox`

- Purpose: owner-facing tasks, owner memory, owner artifacts, and future personal execution flows
- Local contour root: `/home/avoro/ownerbox`
- Artifact and export roots: `/home/avoro/data/hot/ownerbox` and `/home/avoro/data/export/ownerbox`

## Separation Rules

- One machine does not mean one system.
- DF Core is shared.
- Owner and dev are isolated contours.
- Memory, task namespaces, and artifact paths are contour-separated.
- `df-dev` must not read or write owner-private memory or owner artifact roots directly.
- `ownerbox` must not read or write dev-private memory or dev artifact roots directly.
- Shared behavior must be expressed through DF Core contracts instead of ad hoc cross-directory coupling.

## Inter-System Communication Rule

- No direct file-level coupling is allowed between contour-private directories.
- Future communication between `df-dev` and `ownerbox` must flow through a shared adapter, queue, or explicitly defined contract owned by DF Core.
- The first reserved host boundary for that exchange is `/home/avoro/data/sync-queue`.
- That boundary is structural only in this step; no sync or storage adapter is implemented yet.

## Storage Separation Rule

- Host storage tiers live under `/home/avoro/data`.
- `hot`, `warm`, `cache`, `export`, and `sync-queue` are shared host tiers, not shared contour state.
- Each contour uses its own subpaths inside those tiers.
- Storage abstraction and cloud sync come after contour boundaries are stable and enforced.

## Progression

`df-dev -> ownerbox -> product box`

- `df-dev` improves and validates DF Core.
- `ownerbox` becomes the isolated owner-operational contour on the same machine.
- Product Box still comes later from validated deployment, not from a separate rewrite.

## Migration Path On Current Machine

- The stable repo remains at `/home/avoro/projects/digital_foreman` in this step to avoid breaking the current baseline.
- `/home/avoro/df-dev` is prepared now as the clean dev contour root.
- A later migration may move or bind the repo under `df-dev` only after contour boundaries and storage adapters are ready.
