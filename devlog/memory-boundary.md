# Memory Boundary

- Added bounded task memory with `MAX_ENTRIES = 100`.
- Preserved append order with newest entries kept last.
- Verified overflow behavior drops oldest entries and preserves newest 100.
