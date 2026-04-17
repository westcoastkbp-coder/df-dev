# Lead Follow-Up

## Summary

Extended the lead workflow with a follow-up task that reads the existing `lead_001` artifact, generates a follow-up recommendation, writes a follow-up file, records memory, and updates logs.

## Outcome

- created `runtime/out/leads/lead_001_followup.txt`
- generated summary, next action, and short message
- stored typed follow-up summary in `task_memory`
- updated `tasks.log`, `policy.log`, and `system.log`

## Verification

- ran `pytest tests/test_lead_followup.py`
- ran `python scripts/simulate_lead_followup.py`
- read back the generated follow-up file
