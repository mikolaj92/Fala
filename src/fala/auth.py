from __future__ import annotations

import json
import os
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Mapping

from fastapi import Request


class RuntimeRole(StrEnum):
    viewer = "viewer"
    worker = "worker"
    operator = "operator"
    admin = "admin"


class RuntimePermission(StrEnum):
    read = "read"
    process_write = "process_write"
    operate = "operate"
    admin = "admin"


ROLE_PERMISSIONS: dict[RuntimeRole, frozenset[RuntimePermission]] = {
    RuntimeRole.viewer: frozenset({RuntimePermission.read}),
    RuntimeRole.worker: frozenset(
        {
            RuntimePermission.read,
            RuntimePermission.process_write,
        }
    ),
    RuntimeRole.operator: frozenset(
        {
            RuntimePermission.read,
            RuntimePermission.process_write,
            RuntimePermission.operate,
        }
    ),
    RuntimeRole.admin: frozenset(RuntimePermission),
}


@dataclass(frozen=True)
class RuntimePrincipal:
    actor: str
    role: RuntimeRole
    source: str = "api"
    tenant_id: str | None = None
    key_id: str | None = None


@dataclass(frozen=True)
class RuntimeApiKey:
    secret: str
    role: RuntimeRole = RuntimeRole.admin
    actor: str | None = None
    source: str = "api-key"
    tenant_id: str | None = None
    key_id: str | None = None

    def principal(self) -> RuntimePrincipal:
        return RuntimePrincipal(
            actor=self.actor or self.key_id or "api-key",
            role=self.role,
            source=self.source,
            tenant_id=self.tenant_id,
            key_id=self.key_id,
        )


class RuntimeAuthError(Exception):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class RuntimeAccessPolicy:
    def __init__(
        self,
        *,
        api_keys: Mapping[str, RuntimeApiKey] | None = None,
        auth_required: bool | None = None,
    ) -> None:
        self.api_keys = dict(api_keys or {})
        self.auth_required = bool(self.api_keys) if auth_required is None else auth_required

    @classmethod
    def disabled(cls) -> "RuntimeAccessPolicy":
        return cls(api_keys={}, auth_required=False)

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> "RuntimeAccessPolicy":
        env = environ or os.environ
        api_keys = parse_api_keys(env.get("FALA_API_KEYS"))
        auth_required = _env_truthy(env.get("FALA_AUTH_REQUIRED"))
        if api_keys:
            auth_required = True
        return cls(api_keys=api_keys, auth_required=auth_required)

    @classmethod
    def from_key_specs(
        cls,
        specs: Mapping[str, str | Mapping[str, Any] | RuntimeApiKey],
        *,
        auth_required: bool = True,
    ) -> "RuntimeAccessPolicy":
        keys: dict[str, RuntimeApiKey] = {}
        for secret, spec in specs.items():
            if isinstance(spec, RuntimeApiKey):
                keys[secret] = spec
            elif isinstance(spec, str):
                keys[secret] = RuntimeApiKey(
                    secret=secret,
                    role=RuntimeRole(spec),
                    key_id=secret,
                )
            else:
                keys[secret] = _api_key_from_mapping(secret, spec)
        return cls(api_keys=keys, auth_required=auth_required)

    def authenticate(self, request: Request) -> RuntimePrincipal:
        if not self.auth_required:
            principal = RuntimePrincipal(
                actor=(
                    request.headers.get("x-fala-actor")
                    or request.headers.get("x-operator-id")
                    or request.headers.get("x-user-email")
                    or "anonymous"
                ),
                role=RuntimeRole.admin,
                source=request.headers.get("x-fala-source") or "dev",
            )
            request.state.fala_principal = principal
            return principal

        token = _request_token(request)
        if not token:
            raise RuntimeAuthError(401, "Missing Fala API key")
        api_key = self.api_keys.get(token)
        if api_key is None:
            raise RuntimeAuthError(401, "Invalid Fala API key")

        principal = api_key.principal()
        if request.headers.get("x-fala-source"):
            principal = RuntimePrincipal(
                actor=principal.actor,
                role=principal.role,
                source=request.headers["x-fala-source"],
                tenant_id=principal.tenant_id,
                key_id=principal.key_id,
            )
        request.state.fala_principal = principal
        return principal

    def require(
        self,
        request: Request,
        permission: RuntimePermission,
    ) -> RuntimePrincipal:
        principal = principal_from_request(request) or self.authenticate(request)
        if permission not in ROLE_PERMISSIONS[principal.role]:
            raise RuntimeAuthError(
                403,
                (
                    f"Role {principal.role.value!r} cannot perform "
                    f"{permission.value!r}"
                ),
            )
        return principal

    def stamp_run_metadata(
        self,
        principal: RuntimePrincipal | None,
        metadata: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        stamped = dict(metadata or {})
        if principal is not None and principal.tenant_id and _metadata_tenant_id(stamped) is None:
            stamped["tenant_id"] = principal.tenant_id
        return stamped

    def require_run_metadata(
        self,
        principal: RuntimePrincipal | None,
        metadata: Mapping[str, Any] | None,
    ) -> None:
        if principal is None or principal.tenant_id is None:
            return
        tenant_id = _metadata_tenant_id(metadata or {})
        if tenant_id is not None and tenant_id != principal.tenant_id:
            raise RuntimeAuthError(404, "Run not found")


def principal_from_request(request: Request) -> RuntimePrincipal | None:
    value = getattr(request.state, "fala_principal", None)
    return value if isinstance(value, RuntimePrincipal) else None


def api_permission_for_request(request: Request) -> RuntimePermission:
    method = request.method.upper()
    if method in {"GET", "HEAD", "OPTIONS"}:
        return RuntimePermission.read

    route_path = _route_path(request)
    if route_path in {
        "/process-runtime/runs/validate",
        "/process-runtime/runs/plan",
    }:
        return RuntimePermission.read
    if _is_process_write_path(route_path):
        return RuntimePermission.process_write
    return RuntimePermission.operate


def web_permission_for_request(request: Request) -> RuntimePermission:
    if request.method.upper() in {"GET", "HEAD", "OPTIONS"}:
        return RuntimePermission.read
    return RuntimePermission.operate


def parse_api_keys(value: str | None) -> dict[str, RuntimeApiKey]:
    if not value:
        return {}
    stripped = value.strip()
    if not stripped:
        return {}
    if stripped[0] in "[{":
        return _parse_json_api_keys(stripped)
    keys: dict[str, RuntimeApiKey] = {}
    for index, item in enumerate(part.strip() for part in stripped.split(",") if part.strip()):
        secret, role, tenant_id = _split_key_spec(item)
        key_id = f"key_{index + 1}"
        keys[secret] = RuntimeApiKey(
            secret=secret,
            role=RuntimeRole(role),
            actor=key_id,
            tenant_id=tenant_id,
            key_id=key_id,
        )
    return keys


def _request_token(request: Request) -> str | None:
    authorization = request.headers.get("authorization")
    if authorization:
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() == "bearer" and token.strip():
            return token.strip()
    token = request.headers.get("x-fala-api-key")
    return token.strip() if token and token.strip() else None


def _parse_json_api_keys(value: str) -> dict[str, RuntimeApiKey]:
    loaded = json.loads(value)
    keys: dict[str, RuntimeApiKey] = {}
    if isinstance(loaded, list):
        for index, item in enumerate(loaded):
            if not isinstance(item, dict) or not isinstance(item.get("key"), str):
                raise ValueError("FALA_API_KEYS list entries require a string key")
            secret = item["key"]
            key_id = str(item.get("id") or f"key_{index + 1}")
            keys[secret] = _api_key_from_mapping(secret, {**item, "id": key_id})
        return keys
    if isinstance(loaded, dict):
        for index, (secret, spec) in enumerate(loaded.items()):
            if isinstance(spec, str):
                spec = {"role": spec, "id": f"key_{index + 1}"}
            if not isinstance(spec, dict):
                raise ValueError("FALA_API_KEYS object values require role or object")
            keys[secret] = _api_key_from_mapping(secret, spec)
        return keys
    raise ValueError("FALA_API_KEYS must be JSON object, JSON list, or comma list")


def _api_key_from_mapping(secret: str, spec: Mapping[str, Any]) -> RuntimeApiKey:
    key_id = _optional_str(spec.get("id")) or _optional_str(spec.get("key_id")) or secret
    return RuntimeApiKey(
        secret=secret,
        role=RuntimeRole(str(spec.get("role") or RuntimeRole.admin.value)),
        actor=_optional_str(spec.get("actor")) or key_id,
        source=_optional_str(spec.get("source")) or "api-key",
        tenant_id=_optional_str(spec.get("tenant_id")) or _optional_str(spec.get("tenant")),
        key_id=key_id,
    )


def _split_key_spec(item: str) -> tuple[str, str, str | None]:
    parts = item.split(":")
    if not parts[0]:
        raise ValueError("FALA_API_KEYS contains an empty secret")
    role = parts[1] if len(parts) > 1 and parts[1] else RuntimeRole.admin.value
    tenant_id = parts[2] if len(parts) > 2 and parts[2] else None
    if len(parts) > 3:
        raise ValueError("FALA_API_KEYS comma entries use key[:role[:tenant_id]]")
    return parts[0], role, tenant_id


def _route_path(request: Request) -> str:
    route = request.scope.get("route")
    path = getattr(route, "path", None)
    route_path = str(path or request.url.path)
    if route_path.startswith("/api/"):
        return route_path[4:]
    if route_path == "/api":
        return "/"
    return route_path


def _is_process_write_path(route_path: str) -> bool:
    return (
        route_path.endswith("/claim")
        or route_path.endswith("/renew")
        or route_path.endswith("/events")
        or route_path.endswith("/status")
        or route_path.endswith("/output")
        or "/workers/" in route_path
        or "/streams/" in route_path
    )


def _metadata_tenant_id(metadata: Mapping[str, Any]) -> str | None:
    value = metadata.get("tenant_id")
    if isinstance(value, str) and value:
        return value
    process_runtime = metadata.get("process_runtime")
    if isinstance(process_runtime, Mapping):
        nested = process_runtime.get("tenant_id")
        if isinstance(nested, str) and nested:
            return nested
    return None


def _env_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
