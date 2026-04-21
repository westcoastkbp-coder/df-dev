# DigitalForemanCore v1

- Version name: `DigitalForemanCore v1`
- Freeze timestamp (UTC): `2026-04-07T02:15:21Z`
- Freeze branch: `codex/lead-followup`
- Current commit SHA at freeze prep: `8587c46f5a4a50483ffb3cf72026d9b9a8e5896d`
- Status: `VALIDATED BASELINE`
- Rollback classification: `SAFE ROLLBACK POINT`

## System Description

First validated Digital Foreman baseline after execution truth hardening, shared context activation, hybrid topology, self-improvement loop, prioritization, core lock, and load stability verification.

## Locked Invariants

- Deterministic execution.
- `FAILED` never upgrades to `COMPLETED`.
- Shared context is the source of truth.
- No policy bypass.
- Self-improvement cannot modify core without approval.
- System remains stable under stress.

## Verification Summary

- Execution truth test:
  `tests/test_adversarial_verification_start.py::test_failed_task_rerun_does_not_upgrade_to_completed` passed on freeze day.
- Idempotency test:
  `tests/test_repeated_signal_idempotency.py::test_repeated_identical_text_signal_creates_single_task_single_execution_and_stable_fallback` passed on freeze day.
- Policy bypass test:
  `tests/test_policy_gate_enforcement.py` passed on freeze day.
- Context consistency test:
  `tests/test_shared_context_activation.py::test_local_write_is_visible_to_remote_and_remote_write_is_visible_to_local` passed on freeze day.
- Self-improvement safety test:
  `tests/test_gap_tasks.py::test_core_targeted_improvement_task_is_created_but_parked_for_approval` passed on freeze day.
- Load stability test:
  `tests/test_gap_tasks.py::test_stress_mix_keeps_non_core_improvements_flowing_without_policy_violations` passed on freeze day, and a 50-gap burst stability rerun completed with `tasks_created=19`, `duplicates_detected=false`, `system_stable=true`, `policy_violations=false`.

## Freeze Notes

- This document records the validated baseline being frozen as `DigitalForemanCore v1`.
- Git freeze artifacts for this baseline are the commit `DigitalForemanCore v1 freeze` and annotated tag `digital-foreman-core-v1`.
