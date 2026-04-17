# First Boot

- Added `runtime/state/system_state.json` for minimal first-boot state.
- Added deterministic local `generate_system_id()` and `first_boot_init()` to `scripts/start_df.py`.
- Boot now initializes runtime directories, task memory, and system state before ready state, and logs first-boot status to `runtime/logs/system.log`.
