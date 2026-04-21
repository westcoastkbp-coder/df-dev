# KBP Website T2 Website Scaffold

Task ID: KBP-WEBSITE-T2-EXECUTION-V1
Parent Task ID: KBP-DEV-WEBSITE-EXECUTION-V1
Depends On: KBP-WEBSITE-T1-EXECUTION-V1
Status: COMPLETED
Owner: Codex
Execution Mode: confirmation
Validated With: .\venv\Scripts\python.exe import check

## Objective

Create the minimum website scaffold under the approved T1 scope.

## Allowed Scope

1. Basic project structure
2. Routing for Home and Contact
3. Shared layout skeleton

## Excluded In This Task

1. Visual polish
2. Lead submission backend handling
3. Additional pages
4. Extra business features

## Delivery Summary

1. Added a minimal public website module under `app/web`.
2. Added two approved routes only: Home and Contact.
3. Added one shared stylesheet for the layout skeleton.
4. Wired static serving and route handlers through the existing FastAPI server.

## Validation

Validation executed:

1. `.\venv\Scripts\python.exe` import check for `app.server`

Validation result:

1. pass

## Next Required Validation

1. Manual route check for `/`
2. Manual route check for `/contact`
3. Visual confirmation that the layout skeleton matches the approved T1 structure
