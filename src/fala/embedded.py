from __future__ import annotations

import asyncio
import inspect
import os
import threading
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

T = TypeVar("T")


class EmbeddedRuntimeConfigError(ValueError):
    """Raised when embedded-runtime path configuration is unsafe."""


class RuntimeServiceConcurrencyError(RuntimeError):
    """Raised when one sync runtime driver is used concurrently."""


@dataclass(frozen=True)
class EmbeddedRuntimeConfig:
    db_path: Path
    work_root: Path
    artifact_store_root: Path
    process_artifact_root: Path


def resolve_embedded_runtime_config(
    *,
    prefix: str,
    default_root: str | Path,
    env: Mapping[str, str] | None = None,
) -> EmbeddedRuntimeConfig:
    normalized_prefix = prefix.strip().strip("_").upper()
    if not normalized_prefix:
        raise EmbeddedRuntimeConfigError("Embedded runtime env prefix cannot be blank")
    values = env if env is not None else os.environ
    work_root_key = f"{normalized_prefix}_WORK_ROOT"
    db_path_key = f"{normalized_prefix}_DB_PATH"
    artifact_store_key = f"{normalized_prefix}_ARTIFACT_STORE_ROOT"
    process_artifact_key = f"{normalized_prefix}_PROCESS_ARTIFACT_ROOT"

    work_root = _resolve_config_path(
        key=work_root_key,
        default=Path(default_root).expanduser().resolve(),
        env=values,
    )
    return EmbeddedRuntimeConfig(
        work_root=work_root,
        db_path=_resolve_config_path(
            key=db_path_key,
            default=work_root / "fala.sqlite",
            env=values,
        ),
        artifact_store_root=_resolve_config_path(
            key=artifact_store_key,
            default=work_root / "artifact-store",
            env=values,
        ),
        process_artifact_root=_resolve_config_path(
            key=process_artifact_key,
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


def _resolve_config_path(
    *,
    key: str,
    default: Path,
    env: Mapping[str, str],
) -> Path:
    raw = env.get(key)
    if raw is None:
        return default.expanduser().resolve()
    value = raw.strip()
    if not value:
        raise EmbeddedRuntimeConfigError(f"{key} cannot be blank")
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise EmbeddedRuntimeConfigError(f"{key} must be an absolute path: {value!r}")
    return path.resolve()


def _running_loop_exists() -> bool:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


__all__ = [
    "EmbeddedRuntimeConfig",
    "EmbeddedRuntimeConfigError",
    "RuntimeServiceConcurrencyError",
    "SyncRuntimeDriver",
    "resolve_embedded_runtime_config",
]
