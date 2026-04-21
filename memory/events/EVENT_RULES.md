# Event Rules

All system events must:

- follow event_schema.json
- be immutable
- be append-only
- include verification result
- include trace identifiers

No event = no state change.

---

## Event Log

- stored in event_log.jsonl
- append-only
- no edits allowed
- each line = one event

---

## Verification Gate

- no state update without verification
- verification must be explicit
- failed verification blocks state change

---
