from __future__ import annotations

import asyncio
import inspect
import os
import threading
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, TypeVar

from fala.errors import FalaConfigurationError, FalaRuntimeError

T = TypeVar("T")


class EmbeddedRuntimeConfigError(FalaConfigurationError, ValueError):
    """Raised when embedded-runtime path configuration is unsafe."""


class RuntimeServiceConcurrencyError(FalaRuntimeError):
    """Raised when one sync runtime driver is used concurrently."""


@dataclass(frozen=True)
class EmbeddedRuntimeConfig:
    db_path: Path
    work_root: Path
    artifact_store_root: Path
    process_artifact_root: Path


@dataclass(frozen=True)
class EmbeddedWorkerRunResult:
    run_id: str
    pipeline_id: str
    worker_id: str
    results: list[Any]
    run: Any | None

    @property
    def completed_count(self) -> int:
        return sum(1 for result in self.results if getattr(result, "completed", False))

    @property
    def error_count(self) -> int:
        return sum(1 for result in self.results if getattr(result, "error", None))


def resolve_embedded_runtime_config(
    *,
    prefix: str,
    default_root: str | Path,
    default_db_filename: str = "fala.sqlite",
    env: Mapping[str, str] | None = None,
    aliases: Mapping[str, str | Iterable[str]] | None = None,
) -> EmbeddedRuntimeConfig:
    normalized_prefix = prefix.strip().strip("_").upper()
    if not normalized_prefix:
        raise EmbeddedRuntimeConfigError("Embedded runtime env prefix cannot be blank")
    values = env if env is not None else os.environ
    alias_map = _normalize_aliases(aliases or {})
    work_root_key = f"{normalized_prefix}_WORK_ROOT"
    db_path_key = f"{normalized_prefix}_DB_PATH"
    artifact_store_key = f"{normalized_prefix}_ARTIFACT_STORE_ROOT"
    process_artifact_key = f"{normalized_prefix}_PROCESS_ARTIFACT_ROOT"

    work_root = _resolve_config_path(
        key=work_root_key,
        aliases=alias_map["work_root"],
        default=Path(default_root).expanduser().resolve(),
        env=values,
    )
    return EmbeddedRuntimeConfig(
        work_root=work_root,
        db_path=_resolve_config_path(
            key=db_path_key,
            aliases=alias_map["db_path"],
            default=work_root / default_db_filename,
            env=values,
        ),
        artifact_store_root=_resolve_config_path(
            key=artifact_store_key,
            aliases=alias_map["artifact_store_root"],
            default=work_root / "artifact-store",
            env=values,
        ),
        process_artifact_root=_resolve_config_path(
            key=process_artifact_key,
            aliases=alias_map["process_artifact_root"],
            default=work_root / "process-artifacts",
            env=values,
        ),
    )


class SyncRuntimeDriver:
    """Synchronous host facade for event-loop-affine runtime objects.

    The same driver may be reused sequentially. Concurrent calls fail closed with
    RuntimeServiceConcurrencyError so hosts do not see low-level asyncio
    cross-loop errors.
    """

    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime
        self._lock = threading.Lock()

    def run(self, operation: Callable[[Any], Awaitable[T] | T]) -> T:
        if _running_loop_exists():
            raise RuntimeServiceConcurrencyError(
                "SyncRuntimeDriver.run cannot be called from an active event loop; "
                "use Fala async APIs directly in async hosts."
            )
        if not self._lock.acquire(blocking=False):
            raise RuntimeServiceConcurrencyError(
                "This Fala runtime is already being driven synchronously. Use one "
                "runtime/driver per concurrent worker or serialize access."
            )
        try:
            result = operation(self.runtime)
            if inspect.isawaitable(result):
                return asyncio.run(result)
            return result
        finally:
            self._lock.release()


async def run_embedded_adapter_until_idle(
    service: Any,
    *,
    run_id: str,
    pipeline_id: str,
    worker_id: str,
    adapter_kind: str,
    process_id: str | None = None,
    capabilities: list[str] | None = None,
    resources: Any | None = None,
    adapters: Any | None = None,
    lease_seconds: float = 300.0,
    max_steps: int = 1000,
    access_policy: Any | None = None,
    base_url: str = "http://fala-embedded-runtime.test",
) -> EmbeddedWorkerRunResult:
    from fastapi import FastAPI
    import httpx

    from fala.auth import RuntimeAccessPolicy
    from fala.client import ProcessRuntimeClient
    from fala.routes import create_runtime_router
    from fala.worker import AdapterProcessRuntimeWorker

    policy = access_policy or RuntimeAccessPolicy.disabled()
    app = FastAPI(title="Fala embedded runtime")
    app.include_router(
        create_runtime_router(service, access_policy=policy),
        prefix="/api",
    )
    async with ProcessRuntimeClient(
        base_url,
        transport=httpx.ASGITransport(app=app),
    ) as client:
        worker = AdapterProcessRuntimeWorker(
            client=client,
            pipeline_id=pipeline_id,
            worker_id=worker_id,
            adapter_kind=adapter_kind,  # type: ignore[arg-type]
            capabilities=capabilities,
            resources=resources,
            adapters=adapters,
            lease_seconds=lease_seconds,
        )
        results = await worker.run_until_idle(
            run_id=run_id,
            process_id=process_id,
            max_steps=max_steps,
        )
    run = await service.sync_run_lifecycle(run_id)
    return EmbeddedWorkerRunResult(
        run_id=run_id,
        pipeline_id=pipeline_id,
        worker_id=worker_id,
        results=results,
        run=run,
    )


def _resolve_config_path(
    *,
    key: str,
    aliases: Iterable[str],
    default: Path,
    env: Mapping[str, str],
) -> Path:
    values: list[tuple[str, Path]] = []
    for candidate_key in _unique_keys([key, *aliases]):
        raw = env.get(candidate_key)
        if raw is None:
            continue
        values.append((candidate_key, _coerce_config_path(candidate_key, raw)))
    if not values:
        return default.expanduser().resolve()
    first_key, first_path = values[0]
    conflicts = [
        (candidate_key, candidate_path)
        for candidate_key, candidate_path in values[1:]
        if candidate_path != first_path
    ]
    if conflicts:
        conflict_keys = ", ".join(candidate_key for candidate_key, _ in conflicts)
        raise EmbeddedRuntimeConfigError(
            f"{first_key} conflicts with alias value(s): {conflict_keys}"
        )
    return first_path


def _coerce_config_path(key: str, raw: str) -> Path:
    value = raw.strip()
    if not value:
        raise EmbeddedRuntimeConfigError(f"{key} cannot be blank")
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise EmbeddedRuntimeConfigError(f"{key} must be an absolute path: {value!r}")
    return path.resolve()


_SUPPORTED_ALIAS_FIELDS = {
    "work_root",
    "db_path",
    "artifact_store_root",
    "process_artifact_root",
}


def _normalize_aliases(
    aliases: Mapping[str, str | Iterable[str]],
) -> dict[str, list[str]]:
    normalized = {field: [] for field in _SUPPORTED_ALIAS_FIELDS}
    for field, value in aliases.items():
        if field not in normalized:
            supported = ", ".join(sorted(_SUPPORTED_ALIAS_FIELDS))
            raise EmbeddedRuntimeConfigError(
                f"Unsupported embedded runtime alias field {field!r}; supported: {supported}"
            )
        raw_aliases = [value] if isinstance(value, str) else list(value)
        for alias in raw_aliases:
            alias_name = str(alias).strip()
            if not alias_name:
                raise EmbeddedRuntimeConfigError(
                    f"Alias for embedded runtime field {field!r} cannot be blank"
                )
            normalized[field].append(alias_name)
    return normalized


def _unique_keys(keys: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for key in keys:
        if key in seen:
            continue
        seen.add(key)
        unique.append(key)
    return unique


def _running_loop_exists() -> bool:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


__all__ = [
    "EmbeddedRuntimeConfig",
    "EmbeddedRuntimeConfigError",
    "EmbeddedWorkerRunResult",
    "RuntimeServiceConcurrencyError",
    "SyncRuntimeDriver",
    "resolve_embedded_runtime_config",
    "run_embedded_adapter_until_idle",
]
