# Digital Foreman — Architecture Index

## System Surfaces

1. Human Input Surface
2. Intake Surface
3. Context / Memory Surface
4. Execution Surface
5. Verification Surface
6. Operator Surface

---

## Surface Rules

- Input ≠ Execution
- Context ≠ Verification
- Operator UI ≠ System logic

Each surface must remain isolated.

---

## Execution Principle

- All actions must be deterministic
- All actions must be verifiable
- No hidden side effects
- State must be updated only after verification

---

## Development Rule

- repository = single source of truth
- no logic in UI
- no architecture in chat
