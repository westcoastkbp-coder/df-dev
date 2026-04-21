# Devlog

- task: DF-FIX-TASK-MAPPING-V1
- scope: centralize product task-id mapping
- changes:
  - added `app/product/task_registry.py`
  - added `app/product/intake.py`
  - added descriptor files in `tasks/active/`
  - updated product task intake to resolve descriptors centrally
- verification:
  - mapped `DF-CREATE-FILE-V1`
  - mapped `DF-READ-FILE-V1`
