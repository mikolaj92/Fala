"""Durable jobs projected onto run records.

A job is caller-owned domain state riding a :class:`~fala.runtime_backend.Run`
record's opaque ``metadata`` channel under a caller-chosen key -- the same
offload pattern as :mod:`fala.manifests`: the caller owns the job's domain
shape (a :class:`BaseJob` subclass), Fala's runtime backend owns its durable
persistence. The lifecycle is deliberately small: enqueue (optionally
scheduled), run one attempt through a caller-supplied ``execute`` callable,
and re-queue for a retry within the retry budget.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from fala.manifests import get_run_manifest, upsert_run_manifest
from fala.runtime_backend import RuntimeBackendService

JobStatus = Literal["queued", "running", "completed", "partial_failure", "failed"]

_RETRYABLE_STATUSES: tuple[JobStatus, ...] = ("partial_failure", "failed")


class JobEvent(BaseModel):
    timestamp: datetime
    status: JobStatus
    message: str


class BaseJob(BaseModel):
    """Minimal durable job state; subclass to add caller domain fields.

    ``extra="allow"`` keeps unknown fields intact across a load/save
    round-trip, so a manifest written by a richer subclass survives being
    handled through a narrower type.
    """

    model_config = ConfigDict(extra="allow")

    job_id: str
    scheduled_for: datetime | None = None
    max_retries: int = Field(default=1, ge=0)
    attempts: int = Field(default=0, ge=0)
    status: JobStatus = "queued"
    events: list[JobEvent] = Field(default_factory=list)
    last_error: str | None = None


J = TypeVar("J", bound=BaseJob)


def enqueue_job(
    job: J,
    *,
    db_path: str | Path,
    key: str,
    run_id_prefix: str,
    title: str | None = None,
) -> J:
    """Persist a freshly-created job and record its initial queued event."""
    job.events.append(
        _event(
            "queued",
            (
                f"Job scheduled for {job.scheduled_for.isoformat()} and waiting in the queue."
                if job.scheduled_for is not None
                else "Job created and waiting in the queue."
            ),
        )
    )
    _save_job(job, db_path=db_path, key=key, run_id_prefix=run_id_prefix, title=title)
    return job


def run_job(
    db_path: str | Path,
    job_id: str,
    *,
    key: str,
    run_id_prefix: str,
    job_type: type[J],
    execute: Callable[[J], tuple[JobStatus, str]],
    title: str | None = None,
) -> J:
    """Run one attempt of a queued job through ``execute``.

    A job scheduled for the future stays queued. Otherwise the attempt is
    recorded, ``execute`` runs with the job (and may set domain fields on it
    in place), and its returned ``(status, message)`` becomes the terminal
    event. An ``execute`` exception is recorded explicitly as a terminal
    ``"failed"`` job carrying ``last_error``, never swallowed.
    """
    job = load_job(db_path, job_id, key=key, run_id_prefix=run_id_prefix, job_type=job_type)
    if job.scheduled_for is not None and job.scheduled_for > datetime.now(UTC):
        job.status = "queued"
        job.events.append(
            _event("queued", f"Job remains queued until {job.scheduled_for.isoformat()}.")
        )
        _save_job(job, db_path=db_path, key=key, run_id_prefix=run_id_prefix, title=title)
        return job
    job.status = "running"
    job.attempts += 1
    job.events.append(_event("running", f"Job attempt {job.attempts} started."))
    _save_job(job, db_path=db_path, key=key, run_id_prefix=run_id_prefix, title=title)

    try:
        status, message = execute(job)
    except Exception as exc:
        # justified: job boundary. An execute failure is recorded explicitly
        # as a terminal "failed" job carrying last_error, never swallowed.
        job.status = "failed"
        job.last_error = str(exc)
        job.events.append(_event("failed", f"Job execution failed: {exc}"))
        _save_job(job, db_path=db_path, key=key, run_id_prefix=run_id_prefix, title=title)
        return job

    job.status = status
    job.events.append(_event(status, message))
    job.last_error = None
    _save_job(job, db_path=db_path, key=key, run_id_prefix=run_id_prefix, title=title)
    return job


def retry_job(
    db_path: str | Path,
    job_id: str,
    *,
    key: str,
    run_id_prefix: str,
    job_type: type[J],
    execute: Callable[[J], tuple[JobStatus, str]],
    title: str | None = None,
) -> J:
    """Re-queue a failed/partially-failed job and run the next attempt."""
    job = load_job(db_path, job_id, key=key, run_id_prefix=run_id_prefix, job_type=job_type)
    if job.attempts > job.max_retries:
        raise ValueError(f"Job {job_id} exhausted retry budget.")
    if job.status not in _RETRYABLE_STATUSES:
        raise ValueError(f"Job {job_id} is not retryable from status {job.status}.")
    job.status = "queued"
    job.events.append(_event("queued", "Job re-queued for retry."))
    _save_job(job, db_path=db_path, key=key, run_id_prefix=run_id_prefix, title=title)
    return run_job(
        db_path,
        job_id,
        key=key,
        run_id_prefix=run_id_prefix,
        job_type=job_type,
        execute=execute,
        title=title,
    )


def load_job(
    db_path: str | Path,
    job_id: str,
    *,
    key: str,
    run_id_prefix: str,
    job_type: type[J],
) -> J:
    """Load a job manifest from the backend db, fail-closed if absent."""
    manifest = asyncio.run(
        get_run_manifest(
            RuntimeBackendService.sqlite(Path(db_path)),
            run_id=f"{run_id_prefix}{job_id}",
            key=key,
        )
    )
    if manifest is None:
        raise ValueError(f"Job not found: {job_id}")
    return job_type.model_validate(manifest)


def _save_job(
    job: BaseJob,
    *,
    db_path: str | Path,
    key: str,
    run_id_prefix: str,
    title: str | None,
) -> None:
    asyncio.run(
        upsert_run_manifest(
            RuntimeBackendService.sqlite(Path(db_path)),
            run_id=f"{run_id_prefix}{job.job_id}",
            key=key,
            manifest=job.model_dump(mode="json"),
            title=title,
        )
    )


def _event(status: JobStatus, message: str) -> JobEvent:
    return JobEvent(timestamp=datetime.now(UTC), status=status, message=message)
