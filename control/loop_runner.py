import time
from pathlib import Path

from control.dev_runtime import run_in_dev_env

ROOT = Path(__file__).resolve().parents[1]

print("=== DF LOOP RUNNER STARTED ===")

while True:
    print("\n--- LOOP ITERATION ---")

    # запуск reaction engine
    run_in_dev_env(["python", "control/reaction_engine.py"], cwd=ROOT, check=False)

    print("sleeping 10 seconds...")
    time.sleep(10)
