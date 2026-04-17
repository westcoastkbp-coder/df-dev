# Digital Foreman

Execution Control System for structured operational workflows.

Digital Foreman is not a CRM, not an assistant, and not a workflow tool.  
It is a system that enforces how work is executed.

---

## Core Concept

The system operates through a deterministic execution model:

- Issues → become structured tasks
- Tasks → pass through routing
- Routing → resolves type and intent
- Policy → defines allowed behavior
- Execution → produces artifacts
- GitHub → acts as the execution surface

## Surface Architecture

Digital Foreman / Execution OS Core is one system, not separate products.

- Dev Box (`Zephyrus`) is the development, testing, and system improvement surface.
- Business Box (`West Coast KBP`) is the real operational and validation surface.
- Product Box is the future standardized sellable surface.

Required progression:

`Dev Box -> Business Box -> Product Box`

Business use is part of the product path, not separate from it. All current work must move cleanly from Dev Box to Business Box without architectural rewrite, and Product Box begins only after Business Box is operationally validated.

---

## System Layers

### 1. Intake Layer
Captures incoming signals (issues, events)

### 2. Routing Layer
Determines task type (SYSTEM_TEST, BUG, TASK, etc.)

### 3. Policy Layer
Defines system responses and allowed actions

### 4. Execution Layer
Performs deterministic actions:
- artifact creation
- Git commits
- PR generation

### 5. GitHub Layer
Acts as:
- state machine
- audit log
- execution surface

---

## Current Capabilities

- Issue → Task transformation
- Type-based routing
- Policy-driven responses
- Artifact generation
- Automatic commits
- Branch creation
- Pull request flow
- GitHub Actions validation (smoke-check)

---

## Example Flow

1. Issue created  
2. System assigns status: PROCESSING  
3. Type is resolved  
4. Policy defines response  
5. Artifact is created  
6. Commit is pushed  
7. PR is created  
8. Status becomes DONE  

---

## Status

Private / In active development

## Dev Workflow

Digital Foreman now treats Linux in WSL as the primary development runtime.

- Open the repo from a WSL terminal first.
- Use the Linux repo path: `/mnt/d/digital_foreman`
- Activate a Linux virtual environment inside the repo: `source .venv/bin/activate` or `source venv/bin/activate`
- Run development commands, git operations, and pytest from WSL

Windows compatibility is still preserved, but Windows-hosted dev tooling now defaults to WSL execution. To temporarily keep a command native on Windows, set `DF_DEV_RUNTIME=native`. To pin a distro, set `DF_WSL_DISTRO=<distro-name>`.

---

## Positioning

Digital Foreman is an:

> Execution Control System

Not:
- CRM  
- chatbot  
- automation tool  

---

## Roadmap

- Full PR automation  
- Status-check enforcement  
- Action-based execution validation  
- Multi-agent orchestration
