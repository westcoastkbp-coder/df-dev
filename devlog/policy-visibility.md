# Policy Visibility

- Added policy decision logging to `runtime/logs/policy.log`.
- Logging runs on every `evaluate_policy(...)` call.
- Format:
  `[TIME] TASK_ID -> allowed/blocked -> reason`
- No policy rules were changed.
- Verified with tests:
  `python -m pytest -q tests/test_policy_gate.py`
  Result: `5 passed`
- Regression check:
  `python -m pytest -q`
  Result: `14 passed`
- Runtime verification:
  executed one allowed and one blocked policy evaluation
  confirmed lines written to `runtime/logs/policy.log`
