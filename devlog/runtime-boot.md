# Runtime Boot

- Added `scripts/start_df.py` for synchronous runtime boot and CLI handling.
- Boot initializes runtime directories, loads task memory, runs a basic health check, and logs startup/errors to `runtime/logs/system.log`.
- Verified startup with `python scripts/start_df.py`.
