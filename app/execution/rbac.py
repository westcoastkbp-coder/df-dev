from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from dataclasses import dataclass

from fastapi import HTTPException


@dataclass(frozen=True, slots=True)
class RoleBinding:
    role: str
    permissions: frozenset[str]


ROLE_BINDINGS = {
    "owner": RoleBinding(
        role="owner",
        permissions=frozenset(
            {
                "product.run_task",
                "product.read_task",
                "product.read_task_history",
            }
        ),
    ),
    "admin": RoleBinding(
        role="admin",
        permissions=frozenset(
            {
                "controlled.execute",
                "controlled.read_history",
                "controlled.read_report",
                "controlled.verify_integrity",
                "product.run_task",
                "product.read_task",
                "product.read_task_history",
            }
        ),
    ),
    "operator": RoleBinding(
        role="operator",
        permissions=frozenset(
            {
                "controlled.execute",
                "controlled.read_history",
            }
        ),
    ),
    "auditor": RoleBinding(
        role="auditor",
        permissions=frozenset(
            {
                "controlled.read_history",
                "controlled.read_report",
                "controlled.verify_integrity",
            }
        ),
    ),
    "viewer": RoleBinding(
        role="viewer",
        permissions=frozenset(
            {
                "product.read_task",
                "product.read_task_history",
            }
        ),
    ),
    "installer": RoleBinding(
        role="installer",
        permissions=frozenset(
            {
                "product.read_task",
                "product.read_task_history",
            }
        ),
    ),
    "foreman": RoleBinding(
        role="foreman",
        permissions=frozenset(
            {
                "product.run_task",
                "product.read_task",
                "product.read_task_history",
            }
        ),
    ),
    "engineer": RoleBinding(
        role="engineer",
        permissions=frozenset(
            {
                "product.run_task",
                "product.read_task",
                "product.read_task_history",
            }
        ),
    ),
}

CONTROLLED_ROLE_HEADER_ERROR = "unknown or unauthorized x-df-role"
PRODUCT_ALLOWED_ROLES = frozenset(
    {
        "viewer",
        "installer",
        "engineer",
        "foreman",
        "admin",
        "owner",
    }
)
PRODUCT_ROLE_ERROR = (
    "user_role must be one of: viewer, installer, engineer, foreman, admin, owner"
)
PRINCIPAL_TOKEN_SECRET_ENV = "DIGITAL_FOREMAN_RBAC_SECRET"
PRINCIPAL_TOKEN_ERROR = "invalid or expired principal token"


def _normalize_role(role: object) -> str:
    return str(role or "").strip().lower()


def _normalize_actor_id(actor_id: object) -> str:
    return str(actor_id or "").strip()


def _base64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _base64url_decode(value: str) -> bytes:
    normalized = str(value or "").strip()
    padding = "=" * (-len(normalized) % 4)
    return base64.urlsafe_b64decode(normalized + padding)


def _principal_token_secret() -> str:
    return str(os.getenv(PRINCIPAL_TOKEN_SECRET_ENV, "")).strip()


def _sign_principal_payload(serialized_payload: str, secret: str) -> str:
    digest = hmac.new(
        secret.encode("utf-8"),
        serialized_payload.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return _base64url_encode(digest)


def build_principal(*, actor_id: object, role: object) -> dict[str, str]:
    normalized_actor_id = _normalize_actor_id(actor_id)
    normalized_role = _normalize_role(role)
    return {
        "actor_id": normalized_actor_id,
        "role": normalized_role,
    }


def issue_principal_token(
    *,
    actor_id: object,
    role: object,
    expires_at: int | None = None,
    secret: str | None = None,
) -> str:
    token_secret = str(secret or _principal_token_secret()).strip()
    if not token_secret:
        raise ValueError(f"{PRINCIPAL_TOKEN_SECRET_ENV} is required to issue tokens")

    principal = build_principal(actor_id=actor_id, role=role)
    payload = {
        "actor_id": principal["actor_id"],
        "role": principal["role"],
        "exp": int(expires_at) if expires_at is not None else int(time.time()) + 3600,
    }
    serialized_payload = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    encoded_payload = _base64url_encode(serialized_payload.encode("utf-8"))
    signature = _sign_principal_payload(serialized_payload, token_secret)
    return f"{encoded_payload}.{signature}"


def authenticate_principal_token(
    token: object, *, secret: str | None = None
) -> dict[str, str]:
    raw_token = str(token or "").strip()
    token_secret = str(secret or _principal_token_secret()).strip()
    if not raw_token or not token_secret:
        raise HTTPException(status_code=401, detail=PRINCIPAL_TOKEN_ERROR)

    try:
        encoded_payload, signature = raw_token.split(".", 1)
        serialized_payload = _base64url_decode(encoded_payload).decode("utf-8")
        expected_signature = _sign_principal_payload(serialized_payload, token_secret)
        if not hmac.compare_digest(signature, expected_signature):
            raise ValueError("signature mismatch")
        payload = json.loads(serialized_payload)
    except (
        ValueError,
        json.JSONDecodeError,
        UnicodeDecodeError,
        base64.binascii.Error,
    ):
        raise HTTPException(status_code=401, detail=PRINCIPAL_TOKEN_ERROR) from None

    principal = build_principal(
        actor_id=payload.get("actor_id", ""),
        role=payload.get("role", ""),
    )
    expires_at = int(payload.get("exp", 0) or 0)
    if (
        not principal["actor_id"]
        or not principal["role"]
        or expires_at <= int(time.time())
    ):
        raise HTTPException(status_code=401, detail=PRINCIPAL_TOKEN_ERROR)
    return principal


def permissions_for_role(role: object) -> frozenset[str]:
    normalized_role = _normalize_role(role)
    binding = ROLE_BINDINGS.get(normalized_role)
    if binding is None:
        return frozenset()
    return binding.permissions


def require_controlled_principal(*, actor_id: object, role: object) -> dict[str, str]:
    principal = build_principal(actor_id=actor_id, role=role)
    if not principal["actor_id"]:
        raise HTTPException(status_code=401, detail="missing x-df-actor-id header")
    if principal["role"] not in {"admin", "operator", "auditor"}:
        raise HTTPException(status_code=403, detail=CONTROLLED_ROLE_HEADER_ERROR)
    return principal


def require_permission(principal: dict[str, str], permission: str) -> dict[str, str]:
    role = _normalize_role(principal.get("role", ""))
    normalized_permission = str(permission or "").strip()
    if normalized_permission not in permissions_for_role(role):
        raise HTTPException(
            status_code=403,
            detail=f"role `{role}` is not allowed to perform `{normalized_permission}`",
        )
    return {
        "actor_id": _normalize_actor_id(principal.get("actor_id", "")),
        "role": role,
    }


def authorize_controlled_action(
    *, actor_id: object, role: object, permission: str
) -> dict[str, str]:
    return require_permission(
        require_controlled_principal(actor_id=actor_id, role=role),
        permission,
    )


def authorize_product_action(
    *, user_id: object, user_role: object, action: str
) -> dict[str, str]:
    principal = build_principal(actor_id=user_id, role=user_role)
    normalized_action = str(action or "").strip()
    if not principal["actor_id"]:
        raise HTTPException(status_code=400, detail="user_id must not be empty")
    if principal["role"] not in PRODUCT_ALLOWED_ROLES:
        raise HTTPException(status_code=400, detail=PRODUCT_ROLE_ERROR)
    permission = f"product.{normalized_action}"
    if permission not in permissions_for_role(principal["role"]):
        raise HTTPException(
            status_code=403,
            detail=f"role `{principal['role']}` is not allowed to perform `{normalized_action}`",
        )
    return principal
