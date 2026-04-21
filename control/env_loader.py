from pathlib import Path


def load_env():
    env_path = Path(__file__).resolve().parents[1] / ".env"
    data = {}

    if not env_path.is_file():
        return data

    for line in env_path.read_text(encoding="utf-8").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            data[key.strip()] = value.strip()

    return data
