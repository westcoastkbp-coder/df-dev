# Resource Awareness

- Added `scripts/check_resources.py`.
- Implemented structured resource snapshot:
  `cpu.count`
  `ram.total_gb`
  `ram.available_gb`
  `gpu.available`
  `gpu.name`
  `gpu.vram_gb`
  `mode`
- Added operator/dev command: `resources`
- Integrated command into existing product command flow with action `RESOURCES`.
- Latest snapshot is written to `runtime/logs/resources.log`.
- Verified with tests:
  `python -m pytest -q tests/test_resource_awareness.py`
  Result: `3 passed`
- Regression check:
  `python -m pytest -q`
  Result: `17 passed`
- Runtime verification:
  ran `resources` through `scripts/cli.py`
  command returned structured output
  `runtime/logs/resources.log` updated
  live snapshot: `CPU 22`, `RAM 31.42 GB total / 18.99 GB available`, `GPU NVIDIA GeForce RTX 4080 Laptop GPU`, `VRAM 11.99 GB`, `mode=gpu-enabled`
