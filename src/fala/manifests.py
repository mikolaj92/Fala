"""Domain manifests riding run records' opaque ``metadata`` channel.

A caller that owns a domain shape (a result manifest, a job record, ...) can
persist it onto the runtime backend's :class:`~fala.runtime_backend.Run`
records instead of maintaining a sidecar store: the manifest rides
``Run.metadata`` under a caller-chosen key. The caller owns the manifest's
shape; Fala owns its durable persistence.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from fala.runtime_backend import RunStatus, Run, RuntimeBackendService


async def get_run_manifest(
    service: RuntimeBackendService, *, run_id: str, key: str
) -> dict[str, Any] | None:
    """Read one run's manifest stored under ``key``, or ``None``.

    A missing run and a run without a mapping under ``key`` both read as
    ``None``; distinguishing them is a caller concern.
    """
    run = await service.backend.get_run(run_id=run_id)
    if run is None:
        return None
    manifest = run.metadata.get(key)
    return manifest if isinstance(manifest, dict) else None


async def fetch_run_manifests(service: RuntimeBackendService, key: str) -> list[dict[str, Any]]:
    """Collect every manifest stored on the run records under ``key``.

    Iterates the backend's run records and reads each run's opaque
    ``metadata[key]``. Runs without a manifest under ``key`` (e.g. other
    pipelines sharing the store) are skipped.
    """
    manifests: list[dict[str, Any]] = []
    for run in await service.list_runs():
        manifest = run.metadata.get(key)
        if isinstance(manifest, dict):
            manifests.append(manifest)
    return manifests


def load_run_manifests(db_path: str | Path, key: str) -> list[dict[str, Any]]:
    """Synchronously load every manifest under ``key`` from the backend db.

    Constructs a fresh SQLite-backed :class:`RuntimeBackendService` over
    ``db_path`` and drives one coroutine to completion; the backend opens a
    plain per-operation connection, so building it outside an event loop and
    using it here is safe.
    """
    service = RuntimeBackendService.sqlite(Path(db_path))
    return asyncio.run(fetch_run_manifests(service, key))


async def attach_run_manifest(
    service: RuntimeBackendService,
    *,
    run_id: str,
    key: str,
    manifest: dict[str, Any],
) -> None:
    """Attach a manifest to an existing run record, fail-closed if missing.

    Reads the run, merges the manifest into a copy of its ``metadata`` channel
    under ``key``, and puts it back. Call this once the run is idle (no
    concurrent writer) so the get/put pair is race-free.
    """
    existing = await service.backend.get_run(run_id=run_id)
    if existing is None:
        raise ValueError(f"Run {run_id!r} not found in the state store.")
    metadata = dict(existing.metadata)
    metadata[key] = manifest
    await service.backend.put_run(existing.model_copy(update={"metadata": metadata}))


async def put_run_manifest(
    service: RuntimeBackendService,
    *,
    run_id: str,
    key: str,
    manifest: dict[str, Any],
    title: str | None = None,
    status: RunStatus = RunStatus.completed,
) -> None:
    """Persist a manifest as a synthetic run record.

    For events with no executed run to attach to (e.g. a cache hit that
    short-circuited a pipeline): records a synthetic run carrying the manifest
    so the event stays visible in the same backend-persisted history as
    executed runs. An existing run under ``run_id`` is replaced outright; use
    :func:`upsert_run_manifest` to preserve other metadata keys instead.
    """
    run = Run(id=run_id, title=title, status=status, metadata={key: manifest})
    await service.backend.put_run(run)


async def upsert_run_manifest(
    service: RuntimeBackendService,
    *,
    run_id: str,
    key: str,
    manifest: dict[str, Any],
    title: str | None = None,
    create_status: RunStatus = RunStatus.created,
) -> None:
    """Write a manifest onto a run record, creating the record if missing.

    An existing run keeps its status, title, and other metadata keys; only
    the manifest under ``key`` is replaced.
    """
    existing = await service.backend.get_run(run_id=run_id)
    if existing is None:
        run = Run(id=run_id, title=title, status=create_status, metadata={key: manifest})
    else:
        metadata = dict(existing.metadata)
        metadata[key] = manifest
        run = existing.model_copy(update={"metadata": metadata})
    await service.backend.put_run(run)
