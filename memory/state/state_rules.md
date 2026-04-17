# State Derivation Rules

State must be derived ONLY from events.

Rules:

- no direct state mutation without event
- latest event defines latest state
- state must reflect verified events only

---

## Automatic State Update

- state must be recalculated after each successful event
- state is not optional
- state must reflect latest verified system reality
