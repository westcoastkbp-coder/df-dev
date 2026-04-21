# Startup Diagnostics

- Added `run_startup_checks()` to verify runtime directories, task memory access, registry load, and a write test to `runtime/out/`.
- Integrated startup diagnostics into `scripts/start_df.py` before ready state.
- Logged startup check results and errors to `runtime/logs/system.log`.
