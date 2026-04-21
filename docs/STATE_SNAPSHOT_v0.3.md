# Digital Foreman State Snapshot v0.3

- Milestone: `v0.3-memory-core-stable`
- Status: memory core stable milestone
- Date: `2026-04-15`

## 1. System Definition

Digital Foreman is an execution control system.

It is not:

- a chatbot
- a CRM
- a workflow automation tool

It is:

- a deterministic execution layer
- a control plane over tools and actions

## 2. Core Architecture

Execution flow:

```text
intent
-> domain boundary (OwnerBox)
-> context assembly
-> trust / approval
-> action contract
-> dispatcher
-> adapter
-> external/physical execution
-> result
-> trace
-> evidence
-> canonical memory
-> retrieval
-> context assembly
-> next execution
```

## 3. Implemented Layers

- Execution Core
- Action Contract
- Dispatcher
- OwnerBox Domain
- Trust / Approval Layer
- Hands:
  - Browser Adapter
  - Email Adapter
  - Printer Adapter v1
- Workflow Engine
- Durability Layer
- Idempotency Layer
- Trace System
- Canonical Memory Core:
  - typed memory objects
  - promotion policy
  - lifecycle states
  - conflict-safe truth
  - deterministic retrieval
  - bounded context assembly

## 4. Key Architectural Rules

- no autonomous execution
- no hidden planner logic
- no uncontrolled tool use
- no raw external payload in memory
- no memory auto-injection
- memory is execution-grounded
- memory != trace != state
- all side effects are approval-gated
- deterministic behavior enforced

## 5. Current Capabilities

The system can:

- execute structured workflows
- interact with browser and email adapters
- interact with the physical world through printer adapter v1
- store and reuse canonical knowledge
- maintain consistent truth over time
- resume execution safely
- prevent duplicate side effects

## 6. Proven Properties

- deterministic execution
- approval-gated safety
- idempotent behavior
- restart-safe workflows
- governed memory writes
- no duplicate active truth
- explicit context assembly
- no hidden memory behavior

## 7. Current Limitations

- memory is scoped to OwnerBox only
- no external connectors yet
- no embedding-based retrieval
- no business-domain specialization
- no cross-domain memory

## 8. Next Direction

For the next session:

- connector layer (MCP-based)
- external context integration
- advanced operational scenarios
- physical-world expansion
