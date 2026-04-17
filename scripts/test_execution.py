from __future__ import annotations

from scripts.execute_action import execute_action


def main() -> None:
    execute_action("test_action", {"value": 123})


if __name__ == "__main__":
    main()
