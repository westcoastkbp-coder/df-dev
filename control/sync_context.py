import json
from pathlib import Path


def load_context() -> dict:
    context_path = Path(__file__).with_name("SYSTEM_CONTEXT.json")
    return json.loads(context_path.read_text(encoding="utf-8"))


def main() -> None:
    payload = load_context()

    print(f"stage: {payload.get('stage')}")
    print(f"broken modules: {payload.get('broken_modules')}")
    print(f"next required: {payload.get('next_required')}")


if __name__ == "__main__":
    main()
