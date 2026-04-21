# Real Lead Intake

## Summary

Simulated a real client lead request through the existing queue, policy, execution, file output, memory, and logging path.

## Input

- `Client wants ADU project, lot 5000 sqft, asking for price`

## Outcome

- created `runtime/out/leads/lead_001.txt`
- extracted `type=ADU`
- extracted `lot_size=5000 sqft`
- stored typed summary in `task_memory`
- updated `tasks.log`, `policy.log`, and `system.log`

## Verification

- ran `pytest tests/test_real_lead_intake.py`
- ran `python scripts/simulate_real_lead_intake.py`
- read back the generated lead file
