# Devlog

- task: DF-ERROR-MODEL-V1
- scope: standard error model
- shape:
  - status: error
  - error_type
  - error_message
  - recoverable
  - task_id
- changes:
  - wrapped product intake errors
  - wrapped product runner failures
  - normalized API response status to completed or error
- verification:
  - invalid task id returns structured error
  - direct health returns structured error
  - normal flow remains completed
