from __future__ import annotations

import os


RUNTIME_PROFILES = {"CONTROL", "FULL"}


def get_runtime_profile() -> str:
    normalized = str(os.getenv("RUNTIME_MODE", "FULL")).strip().upper()
    if normalized in RUNTIME_PROFILES:
        return normalized
    return "FULL"


__all__ = ["get_runtime_profile", "RUNTIME_PROFILES"]
