# Hybrid Compute Layer

## Purpose

Digital Foreman now has a bounded hybrid compute job layer that lets DF Core describe, track, and finalize heavy compute work without running that heavy compute inside the DF execution core itself.

This step is intentionally narrow:

- DF Core is still the control plane and system of record.
- The compute plane is represented only as a job contract and lifecycle.
- No model training runs here yet.
- No cloud provider, container, or orchestration adapter is wired in yet.

## DF Core vs Compute Plane

DF Core remains the authority for:

- creating compute job packets
- deciding the domain (`dev` or `ownerbox`)
- recording canonical state transitions
- writing traceable lifecycle artifacts
- finalizing result artifacts

The compute plane is the future execution surface for expensive work such as:

- long-running evaluation jobs
- embedding batch generation
- reranking batches
- future training jobs

The separation matters because control-plane logic and bounded execution policy should stay responsive, deterministic, and auditable even when GPU jobs are slow or unstable.

## Modes

The job layer supports two execution modes:

- `local_gpu`: the job is intended for a bounded local GPU surface, such as a workstation or dedicated GPU box physically close to DF Core.
- `remote_gpu`: the job is intended for a future remote compute surface managed outside the core DF runtime.

In this version, both modes are contractual only. The dispatcher does not call any provider, queue, or worker runtime. It only simulates lifecycle transitions inside DF’s canonical state and trace model.

## Storage Contract

Each domain keeps its own isolated compute namespace:

- local dev mirror: `<df-dev-root>/compute/jobs/<job_id>.json`
- local ownerbox mirror: `<ownerbox-root>/compute/jobs/<job_id>.json`
- contract path for dev exports: `DF/dev/compute/jobs/<job_id>.json`
- contract path for ownerbox exports: `DF/owner/compute/jobs/<job_id>.json`

Additional lifecycle artifacts are stored alongside the compute namespace:

- `compute/results/<job_id>.json`
- `compute/traces/<job_id>/<transition>.json`

These files are still registered through DF’s canonical memory registry, and state transitions are still written through the canonical state store.

## Lifecycle

The dispatcher currently implements a bounded state machine:

1. `queued`
2. `running`
3. `completed` or `failed`

Every transition does three things:

- updates the canonical job packet
- writes a trace artifact linked to `job_id`
- updates canonical state for entity type `compute_job`

Terminal transitions also write a `compute_result` artifact. Completed results carry `metrics`; failed results carry an `error` payload.

## Why Compute Stays Outside The Control Plane

Heavy GPU or cloud work should not run inside DF Core because it would mix orchestration responsibilities with resource-intensive execution. That makes failure handling, responsiveness, domain isolation, and traceability harder to reason about.

By keeping compute external:

- DF Core can remain deterministic and auditable.
- GPU workers can fail without corrupting control-plane state.
- future adapters can change without rewriting the canonical DF lifecycle contract.

## Future Path

This layer is designed so a later adapter can replace the simulated transition call with a real dispatch implementation.

Expected future adapters:

- GPU box adapter for a dedicated local or LAN-connected machine
- cloud adapter for a managed remote GPU provider

Those adapters should only translate job packets into provider-specific work. They should not replace DF Core’s authority over:

- job creation
- job status transitions
- trace artifacts
- canonical state
- domain isolation
