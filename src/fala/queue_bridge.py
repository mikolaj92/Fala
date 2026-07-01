from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import RLock
from typing import TYPE_CHECKING, Any, ClassVar, Iterable, Iterator, Protocol, TextIO
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from pydantic import BaseModel, ConfigDict, Field

from fala.adapters import AdapterRegistry
from fala.models import (
    ProcessEvent,
    ProcessExecutionContext,
    ProcessOutput,
    ProcessSpec,
    ProcessStatus,
    ResourceSpec,
    RuntimeId,
    new_id,
)
from fala.scheduler import ClaimedProcess, ScheduledProcess

if TYPE_CHECKING:
    import httpx

    from fala.client import ProcessRuntimeClient


class QueueWorkEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId = Field(default_factory=lambda: new_id("work"))
    queue: str | None = None
    pipeline_id: str
    run_id: str
    document_id: str
    process_id: str
    worker_id: str | None = None
    attempt: int
    claim_expires_at: str
    process: ScheduledProcess
    context: ProcessExecutionContext
    metadata: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_claim(
        cls,
        claim: ClaimedProcess,
        *,
        queue: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "QueueWorkEnvelope":
        adapter_queue = claim.process.adapter.get("queue")
        return cls(
            queue=queue or (adapter_queue if isinstance(adapter_queue, str) else None),
            pipeline_id=claim.pipeline_id,
            run_id=claim.run_id,
            document_id=claim.document_id,
            process_id=claim.process.id,
            worker_id=claim.worker_id,
            attempt=claim.attempt,
            claim_expires_at=claim.claim_expires_at.isoformat(),
            process=claim.process,
            context=claim.context,
            metadata=metadata or {},
        )


class QueueResultEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId = Field(default_factory=lambda: new_id("result"))
    work_id: str
    queue: str | None = None
    pipeline_id: str
    run_id: str
    document_id: str
    process_id: str
    worker_id: str | None = None
    attempt: int
    status: ProcessStatus = ProcessStatus.completed
    output: ProcessOutput | None = None
    events: list[ProcessEvent] = Field(default_factory=list)
    error: str | None = None
    error_kind: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def completed(
        cls,
        work: QueueWorkEnvelope,
        output: ProcessOutput | dict[str, Any],
        *,
        events: list[ProcessEvent] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "QueueResultEnvelope":
        return cls(
            work_id=work.id,
            queue=work.queue,
            pipeline_id=work.pipeline_id,
            run_id=work.run_id,
            document_id=work.document_id,
            process_id=work.process_id,
            worker_id=work.worker_id,
            attempt=work.attempt,
            status=ProcessStatus.completed,
            output=(
                output
                if isinstance(output, ProcessOutput)
                else ProcessOutput.model_validate(output)
            ),
            events=events or [],
            metadata=metadata or {},
        )

    @classmethod
    def failed(
        cls,
        work: QueueWorkEnvelope,
        *,
        error: str,
        error_kind: str | None = "worker_error",
        events: list[ProcessEvent] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "QueueResultEnvelope":
        return cls(
            work_id=work.id,
            queue=work.queue,
            pipeline_id=work.pipeline_id,
            run_id=work.run_id,
            document_id=work.document_id,
            process_id=work.process_id,
            worker_id=work.worker_id,
            attempt=work.attempt,
            status=ProcessStatus.failed,
            events=events or [],
            error=error,
            error_kind=error_kind,
            metadata=metadata or {},
        )


class SQLiteQueueWorkRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    queue: str | None = None
    state: str
    lease_owner: str | None = None
    lease_expires_at: str | None = None
    delivery_count: int = 0
    last_error: str | None = None
    created_at: str
    updated_at: str
    work: QueueWorkEnvelope | None = None


class QueueBridgeTransport(Protocol):
    async def publish_work(self, envelope: QueueWorkEnvelope) -> None:
        ...

    async def publish_result(self, envelope: QueueResultEnvelope) -> None:
        ...


class QueueBrokerTransport(QueueBridgeTransport, Protocol):
    async def claim_work(
        self,
        *,
        queue: str | None = None,
        worker_id: str | None = None,
        lease_seconds: float = 300.0,
        max_deliveries: int | None = None,
    ) -> QueueWorkEnvelope | None:
        ...

    async def complete_work(self, work_id: str) -> None:
        ...

    async def release_work(self, work_id: str, *, error: str | None = None) -> None:
        ...

    async def fail_work(self, work_id: str, *, error: str | None = None) -> None:
        ...

    async def load_work(
        self,
        *,
        queue: str | None = None,
        limit: int | None = None,
        state: str | None = None,
    ) -> list[QueueWorkEnvelope]:
        ...

    async def list_work_records(
        self,
        *,
        queue: str | None = None,
        limit: int | None = None,
        state: str | None = None,
        include_payload: bool = False,
    ) -> list[SQLiteQueueWorkRecord]:
        ...

    async def requeue_work(
        self,
        work_id: str,
        *,
        reset_delivery_count: bool = True,
    ) -> QueueWorkEnvelope | None:
        ...

    async def load_results(
        self,
        *,
        queue: str | None = None,
        limit: int | None = None,
        state: str = "ready",
    ) -> list[QueueResultEnvelope]:
        ...

    async def mark_result_applied(self, result_id: str) -> None:
        ...

    async def stats(self) -> dict[str, Any]:
        ...


class RedisClient(Protocol):
    def hset(
        self,
        name: str,
        key: str | None = None,
        value: str | None = None,
        **kwargs: Any,
    ) -> Any:
        ...

    def hgetall(self, name: str) -> dict[Any, Any]:
        ...

    def rpush(self, name: str, *values: str) -> Any:
        ...

    def lpop(self, name: str) -> Any:
        ...

    def sadd(self, name: str, *values: str) -> Any:
        ...

    def smembers(self, name: str) -> set[Any]:
        ...


class JsonlQueueTransport:
    def __init__(self, *, work_file: str | Path, result_file: str | Path) -> None:
        self.work_file = Path(work_file)
        self.result_file = Path(result_file)

    async def publish_work(self, envelope: QueueWorkEnvelope) -> None:
        await asyncio.to_thread(_append_jsonl, self.work_file, envelope)

    async def publish_result(self, envelope: QueueResultEnvelope) -> None:
        await asyncio.to_thread(_append_jsonl, self.result_file, envelope)

    def load_work(self) -> list[QueueWorkEnvelope]:
        return list(read_work_jsonl(self.work_file))

    def load_results(self) -> list[QueueResultEnvelope]:
        return list(read_result_jsonl(self.result_file))


class _MemoryQueueBackend:
    def __init__(self) -> None:
        self.lock = RLock()
        self.work: dict[str, dict[str, Any]] = {}
        self.results: dict[str, dict[str, Any]] = {}


class MemoryQueueTransport:
    """In-process broker transport for embedded tests and previews.

    Named `memory://...` transports share state inside the current Python
    process. They are not durable and do not coordinate separate processes.
    """

    _backends: ClassVar[dict[str, _MemoryQueueBackend]] = {}

    def __init__(self, name: str = "default") -> None:
        self.name = name or "default"
        self._backend = self._backends.setdefault(self.name, _MemoryQueueBackend())

    async def publish_work(self, envelope: QueueWorkEnvelope) -> None:
        now = _now_iso()
        payload = envelope.model_dump_json()
        with self._backend.lock:
            row = self._backend.work.get(envelope.id)
            if row is None:
                self._backend.work[envelope.id] = {
                    "id": envelope.id,
                    "queue": envelope.queue,
                    "state": "ready",
                    "payload": payload,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "delivery_count": 0,
                    "last_error": None,
                    "created_at": now,
                    "updated_at": now,
                }
            else:
                row["queue"] = envelope.queue
                row["payload"] = payload
                row["updated_at"] = now

    async def publish_result(self, envelope: QueueResultEnvelope) -> None:
        now = _now_iso()
        payload = envelope.model_dump_json()
        with self._backend.lock:
            row = self._backend.results.get(envelope.id)
            if row is None:
                self._backend.results[envelope.id] = {
                    "id": envelope.id,
                    "work_id": envelope.work_id,
                    "queue": envelope.queue,
                    "state": "ready",
                    "payload": payload,
                    "created_at": now,
                    "updated_at": now,
                    "applied_at": None,
                }
            else:
                row["work_id"] = envelope.work_id
                row["queue"] = envelope.queue
                row["payload"] = payload
                row["state"] = "ready"
                row["applied_at"] = None
                row["updated_at"] = now

    async def claim_work(
        self,
        *,
        queue: str | None = None,
        worker_id: str | None = None,
        lease_seconds: float = 300.0,
        max_deliveries: int | None = None,
    ) -> QueueWorkEnvelope | None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than zero")
        if max_deliveries is not None and max_deliveries < 1:
            raise ValueError("max_deliveries must be greater than zero")
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        lease_expires_at = (now + timedelta(seconds=lease_seconds)).isoformat()
        with self._backend.lock:
            rows = sorted(
                self._backend.work.values(),
                key=lambda row: (row["created_at"], row["id"]),
            )
            for row in rows:
                if queue is not None and row["queue"] != queue:
                    continue
                if not _memory_work_is_claimable(row, now_iso):
                    continue
                if (
                    max_deliveries is not None
                    and int(row["delivery_count"]) >= max_deliveries
                ):
                    row["state"] = "dead_letter"
                    row["lease_owner"] = None
                    row["lease_expires_at"] = None
                    row["last_error"] = (
                        "max_deliveries exceeded "
                        f"({row['delivery_count']}/{max_deliveries})"
                    )
                    row["updated_at"] = now_iso
                    continue
                row["state"] = "leased"
                row["lease_owner"] = worker_id
                row["lease_expires_at"] = lease_expires_at
                row["delivery_count"] = int(row["delivery_count"]) + 1
                row["updated_at"] = now_iso
                return QueueWorkEnvelope.model_validate_json(row["payload"])
        return None

    async def complete_work(self, work_id: str) -> None:
        await self._update_work_state(work_id, "completed", None)

    async def release_work(self, work_id: str, *, error: str | None = None) -> None:
        await self._update_work_state(work_id, "ready", error)

    async def fail_work(self, work_id: str, *, error: str | None = None) -> None:
        await self._update_work_state(work_id, "failed", error)

    async def load_work(
        self,
        *,
        queue: str | None = None,
        limit: int | None = None,
        state: str | None = None,
    ) -> list[QueueWorkEnvelope]:
        rows = self._filtered_work_rows(queue=queue, state=state, limit=limit)
        return [QueueWorkEnvelope.model_validate_json(row["payload"]) for row in rows]

    async def list_work_records(
        self,
        *,
        queue: str | None = None,
        limit: int | None = None,
        state: str | None = None,
        include_payload: bool = False,
    ) -> list[SQLiteQueueWorkRecord]:
        rows = self._filtered_work_rows(queue=queue, state=state, limit=limit)
        return [
            SQLiteQueueWorkRecord(
                id=row["id"],
                queue=row["queue"],
                state=row["state"],
                lease_owner=row["lease_owner"],
                lease_expires_at=row["lease_expires_at"],
                delivery_count=int(row["delivery_count"]),
                last_error=row["last_error"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                work=(
                    QueueWorkEnvelope.model_validate_json(row["payload"])
                    if include_payload
                    else None
                ),
            )
            for row in rows
        ]

    async def requeue_work(
        self,
        work_id: str,
        *,
        reset_delivery_count: bool = True,
    ) -> QueueWorkEnvelope | None:
        now = _now_iso()
        with self._backend.lock:
            row = self._backend.work.get(work_id)
            if row is None:
                return None
            row["state"] = "ready"
            row["lease_owner"] = None
            row["lease_expires_at"] = None
            if reset_delivery_count:
                row["delivery_count"] = 0
            row["last_error"] = None
            row["updated_at"] = now
            return QueueWorkEnvelope.model_validate_json(row["payload"])

    async def load_results(
        self,
        *,
        queue: str | None = None,
        limit: int | None = None,
        state: str = "ready",
    ) -> list[QueueResultEnvelope]:
        _validate_limit(limit)
        with self._backend.lock:
            rows = [
                row
                for row in sorted(
                    self._backend.results.values(),
                    key=lambda item: (item["created_at"], item["id"]),
                )
                if row["state"] == state
                and (queue is None or row["queue"] == queue)
            ]
            if limit is not None:
                rows = rows[:limit]
            return [
                QueueResultEnvelope.model_validate_json(row["payload"])
                for row in rows
            ]

    async def mark_result_applied(self, result_id: str) -> None:
        now = _now_iso()
        with self._backend.lock:
            row = self._backend.results.get(result_id)
            if row is None:
                return
            row["state"] = "applied"
            row["applied_at"] = now
            row["updated_at"] = now

    async def stats(self) -> dict[str, Any]:
        with self._backend.lock:
            return {
                "work": _memory_stats_rows(self._backend.work.values()),
                "results": _memory_stats_rows(self._backend.results.values()),
            }

    async def _update_work_state(
        self,
        work_id: str,
        state: str,
        error: str | None,
    ) -> None:
        now = _now_iso()
        with self._backend.lock:
            row = self._backend.work.get(work_id)
            if row is None:
                return
            row["state"] = state
            row["lease_owner"] = None
            row["lease_expires_at"] = None
            row["last_error"] = error
            row["updated_at"] = now

    def _filtered_work_rows(
        self,
        *,
        queue: str | None,
        state: str | None,
        limit: int | None,
    ) -> list[dict[str, Any]]:
        _validate_limit(limit)
        with self._backend.lock:
            rows = [
                row
                for row in sorted(
                    self._backend.work.values(),
                    key=lambda item: (item["created_at"], item["id"]),
                )
                if (queue is None or row["queue"] == queue)
                and (state is None or row["state"] == state)
            ]
            return rows[:limit] if limit is not None else rows


class RedisQueueTransport:
    """Redis-backed broker transport for shared worker deployments.

    Redis stores queue row state in hashes and uses ready lists as lightweight
    indexes. The runtime contract remains the same as SQLite/memory: publish,
    lease, complete/release/fail, dead-letter, requeue, result apply, and stats.
    """

    def __init__(
        self,
        target: str,
        *,
        client: RedisClient | None = None,
        prefix: str | None = None,
    ) -> None:
        parsed = urlparse(target)
        if parsed.scheme not in {"redis", "rediss"}:
            raise ValueError("Redis queue broker target must use redis:// or rediss://")
        query = parse_qs(parsed.query)
        self.prefix = prefix or (query.get("prefix") or ["fala"])[0]
        self.target = _redis_target_without_fala_query(target)
        self._client = client or _default_redis_client(self.target)

    async def publish_work(self, envelope: QueueWorkEnvelope) -> None:
        await asyncio.to_thread(self._publish_work_sync, envelope)

    async def publish_result(self, envelope: QueueResultEnvelope) -> None:
        await asyncio.to_thread(self._publish_result_sync, envelope)

    async def claim_work(
        self,
        *,
        queue: str | None = None,
        worker_id: str | None = None,
        lease_seconds: float = 300.0,
        max_deliveries: int | None = None,
    ) -> QueueWorkEnvelope | None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than zero")
        if max_deliveries is not None and max_deliveries < 1:
            raise ValueError("max_deliveries must be greater than zero")
        return await asyncio.to_thread(
            self._claim_work_sync,
            queue,
            worker_id,
            lease_seconds,
            max_deliveries,
        )

    async def complete_work(self, work_id: str) -> None:
        await asyncio.to_thread(self._update_work_state, work_id, "completed", None)

    async def release_work(self, work_id: str, *, error: str | None = None) -> None:
        await asyncio.to_thread(self._update_work_state, work_id, "ready", error)

    async def fail_work(self, work_id: str, *, error: str | None = None) -> None:
        await asyncio.to_thread(self._update_work_state, work_id, "failed", error)

    async def load_work(
        self,
        *,
        queue: str | None = None,
        limit: int | None = None,
        state: str | None = None,
    ) -> list[QueueWorkEnvelope]:
        rows = await asyncio.to_thread(
            self._filtered_work_rows,
            queue,
            state,
            limit,
        )
        return [QueueWorkEnvelope.model_validate_json(row["payload"]) for row in rows]

    async def list_work_records(
        self,
        *,
        queue: str | None = None,
        limit: int | None = None,
        state: str | None = None,
        include_payload: bool = False,
    ) -> list[SQLiteQueueWorkRecord]:
        rows = await asyncio.to_thread(
            self._filtered_work_rows,
            queue,
            state,
            limit,
        )
        return [
            SQLiteQueueWorkRecord(
                id=row["id"],
                queue=row["queue"],
                state=row["state"],
                lease_owner=row["lease_owner"],
                lease_expires_at=row["lease_expires_at"],
                delivery_count=int(row["delivery_count"]),
                last_error=row["last_error"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                work=(
                    QueueWorkEnvelope.model_validate_json(row["payload"])
                    if include_payload
                    else None
                ),
            )
            for row in rows
        ]

    async def requeue_work(
        self,
        work_id: str,
        *,
        reset_delivery_count: bool = True,
    ) -> QueueWorkEnvelope | None:
        return await asyncio.to_thread(
            self._requeue_work_sync,
            work_id,
            reset_delivery_count,
        )

    async def load_results(
        self,
        *,
        queue: str | None = None,
        limit: int | None = None,
        state: str = "ready",
    ) -> list[QueueResultEnvelope]:
        rows = await asyncio.to_thread(
            self._filtered_result_rows,
            queue,
            state,
            limit,
        )
        return [QueueResultEnvelope.model_validate_json(row["payload"]) for row in rows]

    async def mark_result_applied(self, result_id: str) -> None:
        await asyncio.to_thread(self._mark_result_applied_sync, result_id)

    async def stats(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._stats_sync)

    def _publish_work_sync(self, envelope: QueueWorkEnvelope) -> None:
        now = _now_iso()
        existing = self._work_row(envelope.id)
        if existing is None:
            row = {
                "id": envelope.id,
                "queue": envelope.queue,
                "state": "ready",
                "payload": envelope.model_dump_json(),
                "lease_owner": None,
                "lease_expires_at": None,
                "delivery_count": 0,
                "last_error": None,
                "created_at": now,
                "updated_at": now,
            }
            self._save_work_row(row)
            self._client.rpush(self._ready_work_key(envelope.queue), envelope.id)
        else:
            existing["queue"] = envelope.queue
            existing["payload"] = envelope.model_dump_json()
            existing["updated_at"] = now
            self._save_work_row(existing)
        self._remember_queue(envelope.queue)

    def _publish_result_sync(self, envelope: QueueResultEnvelope) -> None:
        now = _now_iso()
        row = {
            "id": envelope.id,
            "work_id": envelope.work_id,
            "queue": envelope.queue,
            "state": "ready",
            "payload": envelope.model_dump_json(),
            "created_at": now,
            "updated_at": now,
            "applied_at": None,
        }
        existing = self._result_row(envelope.id)
        if existing is not None:
            row["created_at"] = existing["created_at"]
        self._save_result_row(row)
        self._client.rpush(self._ready_result_key(envelope.queue), envelope.id)
        self._remember_queue(envelope.queue)

    def _claim_work_sync(
        self,
        queue: str | None,
        worker_id: str | None,
        lease_seconds: float,
        max_deliveries: int | None,
    ) -> QueueWorkEnvelope | None:
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        lease_expires_at = (now + timedelta(seconds=lease_seconds)).isoformat()
        self._requeue_expired_leases(queue=queue, now_iso=now_iso)
        for ready_key in self._ready_work_keys_for_claim(queue):
            while True:
                work_id = _redis_str(self._client.lpop(ready_key))
                if work_id is None:
                    break
                row = self._work_row(work_id)
                if row is None:
                    continue
                if queue is not None and row["queue"] != queue:
                    continue
                if row["state"] != "ready":
                    continue
                if (
                    max_deliveries is not None
                    and int(row["delivery_count"]) >= max_deliveries
                ):
                    row["state"] = "dead_letter"
                    row["lease_owner"] = None
                    row["lease_expires_at"] = None
                    row["last_error"] = (
                        "max_deliveries exceeded "
                        f"({row['delivery_count']}/{max_deliveries})"
                    )
                    row["updated_at"] = now_iso
                    self._save_work_row(row)
                    continue
                row["state"] = "leased"
                row["lease_owner"] = worker_id
                row["lease_expires_at"] = lease_expires_at
                row["delivery_count"] = int(row["delivery_count"]) + 1
                row["updated_at"] = now_iso
                self._save_work_row(row)
                return QueueWorkEnvelope.model_validate_json(row["payload"])
        return None

    def _update_work_state(
        self,
        work_id: str,
        state: str,
        error: str | None,
    ) -> None:
        row = self._work_row(work_id)
        if row is None:
            return
        row["state"] = state
        row["lease_owner"] = None
        row["lease_expires_at"] = None
        row["last_error"] = error
        row["updated_at"] = _now_iso()
        self._save_work_row(row)
        if state == "ready":
            self._client.rpush(self._ready_work_key(row["queue"]), work_id)

    def _requeue_work_sync(
        self,
        work_id: str,
        reset_delivery_count: bool,
    ) -> QueueWorkEnvelope | None:
        row = self._work_row(work_id)
        if row is None:
            return None
        row["state"] = "ready"
        row["lease_owner"] = None
        row["lease_expires_at"] = None
        if reset_delivery_count:
            row["delivery_count"] = 0
        row["last_error"] = None
        row["updated_at"] = _now_iso()
        self._save_work_row(row)
        self._client.rpush(self._ready_work_key(row["queue"]), work_id)
        return QueueWorkEnvelope.model_validate_json(row["payload"])

    def _mark_result_applied_sync(self, result_id: str) -> None:
        row = self._result_row(result_id)
        if row is None:
            return
        now = _now_iso()
        row["state"] = "applied"
        row["applied_at"] = now
        row["updated_at"] = now
        self._save_result_row(row)

    def _requeue_expired_leases(self, *, queue: str | None, now_iso: str) -> None:
        for row in self._filtered_work_rows(queue=queue, state="leased", limit=None):
            expires = row["lease_expires_at"]
            if expires is None or expires > now_iso:
                continue
            row["state"] = "ready"
            row["lease_owner"] = None
            row["lease_expires_at"] = None
            row["updated_at"] = now_iso
            self._save_work_row(row)
            self._client.rpush(self._ready_work_key(row["queue"]), row["id"])

    def _filtered_work_rows(
        self,
        queue: str | None,
        state: str | None,
        limit: int | None,
    ) -> list[dict[str, Any]]:
        _validate_limit(limit)
        rows = [
            row
            for row in self._all_work_rows()
            if (queue is None or row["queue"] == queue)
            and (state is None or row["state"] == state)
        ]
        rows.sort(key=lambda item: (item["created_at"], item["id"]))
        return rows[:limit] if limit is not None else rows

    def _filtered_result_rows(
        self,
        queue: str | None,
        state: str,
        limit: int | None,
    ) -> list[dict[str, Any]]:
        _validate_limit(limit)
        rows = [
            row
            for row in self._all_result_rows()
            if (queue is None or row["queue"] == queue)
            and row["state"] == state
        ]
        rows.sort(key=lambda item: (item["created_at"], item["id"]))
        return rows[:limit] if limit is not None else rows

    def _stats_sync(self) -> dict[str, Any]:
        return {
            "work": _memory_stats_rows(self._all_work_rows()),
            "results": _memory_stats_rows(self._all_result_rows()),
        }

    def _all_work_rows(self) -> list[dict[str, Any]]:
        return [
            _redis_json_loads(value)
            for value in self._client.hgetall(self._work_hash_key()).values()
        ]

    def _all_result_rows(self) -> list[dict[str, Any]]:
        return [
            _redis_json_loads(value)
            for value in self._client.hgetall(self._result_hash_key()).values()
        ]

    def _work_row(self, work_id: str) -> dict[str, Any] | None:
        raw = self._client.hgetall(self._work_hash_key()).get(work_id)
        if raw is None:
            raw = self._client.hgetall(self._work_hash_key()).get(work_id.encode())
        return _redis_json_loads(raw) if raw is not None else None

    def _result_row(self, result_id: str) -> dict[str, Any] | None:
        raw = self._client.hgetall(self._result_hash_key()).get(result_id)
        if raw is None:
            raw = self._client.hgetall(self._result_hash_key()).get(result_id.encode())
        return _redis_json_loads(raw) if raw is not None else None

    def _save_work_row(self, row: dict[str, Any]) -> None:
        self._client.hset(self._work_hash_key(), row["id"], json.dumps(row))

    def _save_result_row(self, row: dict[str, Any]) -> None:
        self._client.hset(self._result_hash_key(), row["id"], json.dumps(row))

    def _remember_queue(self, queue: str | None) -> None:
        self._client.sadd(self._queues_key(), _queue_key(queue))

    def _key(self, *parts: str) -> str:
        return ":".join([self.prefix, *parts])

    def _work_hash_key(self) -> str:
        return self._key("queue_work")

    def _result_hash_key(self) -> str:
        return self._key("queue_results")

    def _queues_key(self) -> str:
        return self._key("queues")

    def _ready_work_key(self, queue: str | None) -> str:
        return self._key("queue", _queue_key(queue), "ready")

    def _ready_result_key(self, queue: str | None) -> str:
        return self._key("results", _queue_key(queue), "ready")

    def _ready_work_keys_for_claim(self, queue: str | None) -> list[str]:
        if queue is not None:
            return [self._ready_work_key(queue)]
        queue_keys = sorted(
            {
                _redis_str(value)
                for value in self._client.smembers(self._queues_key())
            }
            - {None}
        )
        if _queue_key(None) not in queue_keys:
            queue_keys.insert(0, _queue_key(None))
        return [self._key("queue", queue_key, "ready") for queue_key in queue_keys]


class SQLiteQueueTransport:
    """Durable local broker transport for queue envelopes.

    Fala still owns process claims and retries. This transport only leases
    already-exported `QueueWorkEnvelope` rows and stores `QueueResultEnvelope`
    rows until the control plane applies them.
    """

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self._ensure_schema()

    async def publish_work(self, envelope: QueueWorkEnvelope) -> None:
        await asyncio.to_thread(self._publish_work_sync, envelope)

    async def publish_result(self, envelope: QueueResultEnvelope) -> None:
        await asyncio.to_thread(self._publish_result_sync, envelope)

    async def claim_work(
        self,
        *,
        queue: str | None = None,
        worker_id: str | None = None,
        lease_seconds: float = 300.0,
        max_deliveries: int | None = None,
    ) -> QueueWorkEnvelope | None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than zero")
        if max_deliveries is not None and max_deliveries < 1:
            raise ValueError("max_deliveries must be greater than zero")
        return await asyncio.to_thread(
            self._claim_work_sync,
            queue,
            worker_id,
            lease_seconds,
            max_deliveries,
        )

    async def complete_work(self, work_id: str) -> None:
        await asyncio.to_thread(self._update_work_state, work_id, "completed", None)

    async def release_work(self, work_id: str, *, error: str | None = None) -> None:
        await asyncio.to_thread(self._update_work_state, work_id, "ready", error)

    async def fail_work(self, work_id: str, *, error: str | None = None) -> None:
        await asyncio.to_thread(self._update_work_state, work_id, "failed", error)

    async def load_work(
        self,
        *,
        queue: str | None = None,
        limit: int | None = None,
        state: str | None = None,
    ) -> list[QueueWorkEnvelope]:
        return await asyncio.to_thread(
            self._load_work_sync,
            queue,
            limit,
            state,
        )

    async def list_work_records(
        self,
        *,
        queue: str | None = None,
        limit: int | None = None,
        state: str | None = None,
        include_payload: bool = False,
    ) -> list[SQLiteQueueWorkRecord]:
        return await asyncio.to_thread(
            self._list_work_records_sync,
            queue,
            limit,
            state,
            include_payload,
        )

    async def requeue_work(
        self,
        work_id: str,
        *,
        reset_delivery_count: bool = True,
    ) -> QueueWorkEnvelope | None:
        return await asyncio.to_thread(
            self._requeue_work_sync,
            work_id,
            reset_delivery_count,
        )

    async def load_results(
        self,
        *,
        queue: str | None = None,
        limit: int | None = None,
        state: str = "ready",
    ) -> list[QueueResultEnvelope]:
        return await asyncio.to_thread(
            self._load_results_sync,
            queue,
            limit,
            state,
        )

    async def mark_result_applied(self, result_id: str) -> None:
        await asyncio.to_thread(self._mark_result_applied_sync, result_id)

    async def stats(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._stats_sync)

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            yield conn
        except BaseException:
            conn.rollback()
            raise
        else:
            conn.commit()
        finally:
            conn.close()

    def _ensure_schema(self) -> None:
        with self._connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS queue_work (
                    id TEXT PRIMARY KEY,
                    queue TEXT,
                    state TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    lease_owner TEXT,
                    lease_expires_at TEXT,
                    delivery_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_queue_work_ready
                    ON queue_work(state, queue, created_at);
                CREATE INDEX IF NOT EXISTS idx_queue_work_lease
                    ON queue_work(state, lease_expires_at);

                CREATE TABLE IF NOT EXISTS queue_results (
                    id TEXT PRIMARY KEY,
                    work_id TEXT NOT NULL,
                    queue TEXT,
                    state TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    applied_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_queue_results_ready
                    ON queue_results(state, queue, created_at);
                """
            )

    def _publish_work_sync(self, envelope: QueueWorkEnvelope) -> None:
        now = _now_iso()
        payload = envelope.model_dump_json()
        with self._connection() as conn:
            conn.execute(
                """
                INSERT INTO queue_work (
                    id, queue, state, payload, created_at, updated_at
                )
                VALUES (?, ?, 'ready', ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    queue = excluded.queue,
                    payload = excluded.payload,
                    updated_at = excluded.updated_at
                """,
                (envelope.id, envelope.queue, payload, now, now),
            )

    def _publish_result_sync(self, envelope: QueueResultEnvelope) -> None:
        now = _now_iso()
        payload = envelope.model_dump_json()
        with self._connection() as conn:
            conn.execute(
                """
                INSERT INTO queue_results (
                    id, work_id, queue, state, payload, created_at, updated_at
                )
                VALUES (?, ?, ?, 'ready', ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    work_id = excluded.work_id,
                    queue = excluded.queue,
                    payload = excluded.payload,
                    state = 'ready',
                    applied_at = NULL,
                    updated_at = excluded.updated_at
                """,
                (envelope.id, envelope.work_id, envelope.queue, payload, now, now),
            )

    def _claim_work_sync(
        self,
        queue: str | None,
        worker_id: str | None,
        lease_seconds: float,
        max_deliveries: int | None,
    ) -> QueueWorkEnvelope | None:
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        lease_expires_at = (now + timedelta(seconds=lease_seconds)).isoformat()
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            while True:
                params: list[Any] = [now_iso]
                queue_filter = ""
                if queue is not None:
                    queue_filter = "AND queue = ?"
                    params.append(queue)
                row = conn.execute(
                    f"""
                    SELECT id, payload, delivery_count
                    FROM queue_work
                    WHERE (
                        state = 'ready'
                        OR (state = 'leased' AND lease_expires_at <= ?)
                    )
                    {queue_filter}
                    ORDER BY created_at, id
                    LIMIT 1
                    """,
                    params,
                ).fetchone()
                if row is None:
                    conn.commit()
                    return None
                if (
                    max_deliveries is not None
                    and int(row["delivery_count"]) >= max_deliveries
                ):
                    conn.execute(
                        """
                        UPDATE queue_work
                        SET state = 'dead_letter',
                            lease_owner = NULL,
                            lease_expires_at = NULL,
                            last_error = ?,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            (
                                "max_deliveries exceeded "
                                f"({row['delivery_count']}/{max_deliveries})"
                            ),
                            now_iso,
                            row["id"],
                        ),
                    )
                    continue
                conn.execute(
                    """
                    UPDATE queue_work
                    SET state = 'leased',
                        lease_owner = ?,
                        lease_expires_at = ?,
                        delivery_count = delivery_count + 1,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (worker_id, lease_expires_at, now_iso, row["id"]),
                )
                conn.commit()
                return QueueWorkEnvelope.model_validate_json(row["payload"])

    def _update_work_state(
        self,
        work_id: str,
        state: str,
        error: str | None,
    ) -> None:
        now = _now_iso()
        with self._connection() as conn:
            conn.execute(
                """
                UPDATE queue_work
                SET state = ?,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    last_error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (state, error, now, work_id),
            )

    def _load_work_sync(
        self,
        queue: str | None,
        limit: int | None,
        state: str | None,
    ) -> list[QueueWorkEnvelope]:
        params: list[Any] = []
        filters: list[str] = []
        if queue is not None:
            filters.append("queue = ?")
            params.append(queue)
        if state is not None:
            filters.append("state = ?")
            params.append(state)
        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        limit_sql = ""
        if limit is not None:
            if limit < 1:
                raise ValueError("limit must be greater than zero")
            limit_sql = "LIMIT ?"
            params.append(limit)
        with self._connection() as conn:
            rows = conn.execute(
                f"""
                SELECT payload
                FROM queue_work
                {where}
                ORDER BY created_at, id
                {limit_sql}
                """,
                params,
            ).fetchall()
        return [QueueWorkEnvelope.model_validate_json(row["payload"]) for row in rows]

    def _list_work_records_sync(
        self,
        queue: str | None,
        limit: int | None,
        state: str | None,
        include_payload: bool,
    ) -> list[SQLiteQueueWorkRecord]:
        params: list[Any] = []
        filters: list[str] = []
        if queue is not None:
            filters.append("queue = ?")
            params.append(queue)
        if state is not None:
            filters.append("state = ?")
            params.append(state)
        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        limit_sql = ""
        if limit is not None:
            if limit < 1:
                raise ValueError("limit must be greater than zero")
            limit_sql = "LIMIT ?"
            params.append(limit)
        with self._connection() as conn:
            rows = conn.execute(
                f"""
                SELECT
                    id,
                    queue,
                    state,
                    payload,
                    lease_owner,
                    lease_expires_at,
                    delivery_count,
                    last_error,
                    created_at,
                    updated_at
                FROM queue_work
                {where}
                ORDER BY created_at, id
                {limit_sql}
                """,
                params,
            ).fetchall()
        return [
            SQLiteQueueWorkRecord(
                id=row["id"],
                queue=row["queue"],
                state=row["state"],
                lease_owner=row["lease_owner"],
                lease_expires_at=row["lease_expires_at"],
                delivery_count=int(row["delivery_count"]),
                last_error=row["last_error"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                work=(
                    QueueWorkEnvelope.model_validate_json(row["payload"])
                    if include_payload
                    else None
                ),
            )
            for row in rows
        ]

    def _requeue_work_sync(
        self,
        work_id: str,
        reset_delivery_count: bool,
    ) -> QueueWorkEnvelope | None:
        now = _now_iso()
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT payload
                FROM queue_work
                WHERE id = ?
                """,
                (work_id,),
            ).fetchone()
            if row is None:
                conn.commit()
                return None
            if reset_delivery_count:
                conn.execute(
                    """
                    UPDATE queue_work
                    SET state = 'ready',
                        lease_owner = NULL,
                        lease_expires_at = NULL,
                        delivery_count = 0,
                        last_error = NULL,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now, work_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE queue_work
                    SET state = 'ready',
                        lease_owner = NULL,
                        lease_expires_at = NULL,
                        last_error = NULL,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now, work_id),
                )
            conn.commit()
            return QueueWorkEnvelope.model_validate_json(row["payload"])

    def _load_results_sync(
        self,
        queue: str | None,
        limit: int | None,
        state: str,
    ) -> list[QueueResultEnvelope]:
        params: list[Any] = [state]
        queue_filter = ""
        if queue is not None:
            queue_filter = "AND queue = ?"
            params.append(queue)
        limit_sql = ""
        if limit is not None:
            if limit < 1:
                raise ValueError("limit must be greater than zero")
            limit_sql = "LIMIT ?"
            params.append(limit)
        with self._connection() as conn:
            rows = conn.execute(
                f"""
                SELECT payload
                FROM queue_results
                WHERE state = ?
                {queue_filter}
                ORDER BY created_at, id
                {limit_sql}
                """,
                params,
            ).fetchall()
        return [QueueResultEnvelope.model_validate_json(row["payload"]) for row in rows]

    def _mark_result_applied_sync(self, result_id: str) -> None:
        now = _now_iso()
        with self._connection() as conn:
            conn.execute(
                """
                UPDATE queue_results
                SET state = 'applied',
                    applied_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (now, now, result_id),
            )

    def _stats_sync(self) -> dict[str, Any]:
        with self._connection() as conn:
            work_rows = conn.execute(
                """
                SELECT COALESCE(queue, '') AS queue, state, COUNT(*) AS count
                FROM queue_work
                GROUP BY queue, state
                ORDER BY queue, state
                """
            ).fetchall()
            result_rows = conn.execute(
                """
                SELECT COALESCE(queue, '') AS queue, state, COUNT(*) AS count
                FROM queue_results
                GROUP BY queue, state
                ORDER BY queue, state
                """
            ).fetchall()
        return {
            "work": [
                {
                    "queue": row["queue"] or None,
                    "state": row["state"],
                    "count": row["count"],
                }
                for row in work_rows
            ],
            "results": [
                {
                    "queue": row["queue"] or None,
                    "state": row["state"],
                    "count": row["count"],
                }
                for row in result_rows
            ],
        }


def create_queue_broker_transport(
    target: str | Path | None = None,
) -> QueueBrokerTransport:
    """Create a broker transport from a target string.

    Supported targets:
    - `memory://name` for in-process shared state.
    - `redis://host/db` or `rediss://host/db` for shared Redis state.
    - `sqlite://path`, `sqlite:/path`, or a plain filesystem path for SQLite.
    """

    resolved = (
        str(target)
        if target is not None
        else os.environ.get("FALA_QUEUE_BROKER") or os.environ.get("FALA_QUEUE_DB")
    )
    if not resolved:
        raise ValueError(
            "queue broker target is required; set FALA_QUEUE_BROKER or FALA_QUEUE_DB"
        )
    if resolved == "memory" or resolved.startswith("memory://"):
        name = (
            resolved.removeprefix("memory://")
            if resolved.startswith("memory://")
            else "default"
        )
        return MemoryQueueTransport(name or "default")
    if resolved.startswith(("redis://", "rediss://")):
        return RedisQueueTransport(resolved)
    if resolved.startswith("sqlite://"):
        return SQLiteQueueTransport(resolved.removeprefix("sqlite://"))
    if resolved.startswith("sqlite:"):
        return SQLiteQueueTransport(resolved.removeprefix("sqlite:"))
    return SQLiteQueueTransport(resolved)


async def export_claims_to_queue(
    client: ProcessRuntimeClient,
    transport: QueueBridgeTransport,
    *,
    run_id: str,
    pipeline_id: str,
    worker_id: str | None,
    process_id: str | None = None,
    capabilities: list[str] | None = None,
    resources: ResourceSpec | dict[str, Any] | None = None,
    lease_seconds: float = 300.0,
    max_claims: int = 1,
    queue: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> list[QueueWorkEnvelope]:
    if max_claims < 1:
        raise ValueError("max_claims must be greater than zero")
    envelopes: list[QueueWorkEnvelope] = []
    for _ in range(max_claims):
        claim = await client.claim_next(
            run_id=run_id,
            pipeline_id=pipeline_id,
            worker_id=worker_id,
            process_id=process_id,
            adapter_kind="queue",
            capabilities=capabilities or [],
            resources=resources,
            lease_seconds=lease_seconds,
        )
        if claim is None:
            break
        envelope = QueueWorkEnvelope.from_claim(
            claim,
            queue=queue,
            metadata=metadata,
        )
        await transport.publish_work(envelope)
        envelopes.append(envelope)
    return envelopes


def assign_queue_work_worker(
    work: QueueWorkEnvelope,
    worker_id: str | None,
) -> QueueWorkEnvelope:
    if not worker_id or worker_id == work.worker_id or work.worker_id is not None:
        return work
    metadata = dict(work.metadata)
    metadata["worker_id_assigned_at_run"] = True
    return work.model_copy(update={"worker_id": worker_id, "metadata": metadata})


async def run_queue_work(
    work: QueueWorkEnvelope,
    *,
    adapters: AdapterRegistry | None = None,
    error_kind: str | None = "worker_error",
    renew_client: ProcessRuntimeClient | None = None,
    renew_interval_seconds: float | None = None,
    lease_seconds: float = 300.0,
) -> QueueResultEnvelope:
    captured_events: list[ProcessEvent] = []

    def capture_event(event: ProcessEvent) -> None:
        captured_events.append(event)

    renew_task = _start_queue_claim_renew_loop(
        work,
        client=renew_client,
        renew_interval_seconds=renew_interval_seconds,
        lease_seconds=lease_seconds,
    )
    try:
        if renew_client is not None:
            await _renew_queue_claim_once(
                work,
                client=renew_client,
                lease_seconds=lease_seconds,
            )
        output = await (adapters or AdapterRegistry.default()).run(
            _process_spec_from_work(work),
            work.context,
            event_sink=capture_event,
        )
    except BaseException as exc:
        return QueueResultEnvelope.failed(
            work,
            error=str(exc),
            error_kind=error_kind,
            events=captured_events,
        )
    else:
        return QueueResultEnvelope.completed(
            work,
            output,
            events=captured_events,
        )
    finally:
        await _stop_queue_claim_renew_loop(renew_task)


async def apply_queue_result(
    client: ProcessRuntimeClient,
    result: QueueResultEnvelope,
) -> dict[str, Any]:
    try:
        import httpx
    except ImportError as exc:
        raise RuntimeError("httpx is required to apply queue results") from exc
    duplicate = await _queue_result_duplicate_response(client, result, None)
    if duplicate is not None:
        return duplicate
    try:
        for event in result.events:
            await client.append_event(
                run_id=result.run_id,
                document_id=result.document_id,
                event=event,
            )
        if result.status == ProcessStatus.completed:
            output = result.output or ProcessOutput()
            return await client.write_output(
                run_id=result.run_id,
                document_id=result.document_id,
                process_id=result.process_id,
                pipeline_id=result.pipeline_id,
                worker_id=result.worker_id,
                output=output,
            )
        return await client.write_status(
            run_id=result.run_id,
            document_id=result.document_id,
            process_id=result.process_id,
            worker_id=result.worker_id,
            status=result.status,
            data=_queue_result_status_data(result),
        )
    except httpx.HTTPStatusError as exc:
        duplicate = await _queue_result_duplicate_response(client, result, exc)
        if duplicate is not None:
            return duplicate
        raise


async def apply_queue_results(
    client: ProcessRuntimeClient,
    results: Iterable[QueueResultEnvelope],
) -> list[dict[str, Any]]:
    applied: list[dict[str, Any]] = []
    for result in results:
        applied.append(await apply_queue_result(client, result))
    return applied


def _queue_result_status_data(result: QueueResultEnvelope) -> dict[str, Any]:
    return {
        "work_id": result.work_id,
        "attempt": result.attempt,
        "error": result.error,
        "error_kind": result.error_kind,
        "metadata": result.metadata,
    }


async def _queue_result_duplicate_response(
    client: ProcessRuntimeClient,
    result: QueueResultEnvelope,
    exc: httpx.HTTPStatusError | None,
) -> dict[str, Any] | None:
    if exc is not None and exc.response.status_code != 409:
        return None
    page = await client.process_page(
        run_id=result.run_id,
        document_id=result.document_id,
        process_id=result.process_id,
        limit=1,
    )
    process = page.processes[0] if page.processes else None
    if process is None:
        return None
    if (
        result.status == ProcessStatus.completed
        and process.status == ProcessStatus.completed
        and process.has_output
    ):
        return _duplicate_apply_payload(result, process.status.value)
    if result.status == ProcessStatus.failed:
        events = await client.list_events(
            run_id=result.run_id,
            document_id=result.document_id,
            process_id=result.process_id,
            limit=500,
        )
        if any(_event_matches_queue_result(event, result) for event in events.events):
            status = (
                process.status.value
                if isinstance(process.status, ProcessStatus)
                else str(process.status)
            )
            return _duplicate_apply_payload(result, status)
    return None


def _event_matches_queue_result(
    event: ProcessEvent,
    result: QueueResultEnvelope,
) -> bool:
    return (
        event.data.get("work_id") == result.work_id
        and event.data.get("attempt") == result.attempt
        and event.data.get("error_kind") == result.error_kind
    )


def _duplicate_apply_payload(
    result: QueueResultEnvelope,
    current_status: str,
) -> dict[str, Any]:
    return {
        "ok": True,
        "duplicate": True,
        "status": result.status.value,
        "current_status": current_status,
        "work_id": result.work_id,
        "result_id": result.id,
        "process_id": result.process_id,
        "document_id": result.document_id,
    }


def read_work_jsonl(source: str | Path | TextIO) -> list[QueueWorkEnvelope]:
    return [
        QueueWorkEnvelope.model_validate(item)
        for item in _read_jsonl_objects(source)
    ]


def read_result_jsonl(source: str | Path | TextIO) -> list[QueueResultEnvelope]:
    return [
        QueueResultEnvelope.model_validate(item)
        for item in _read_jsonl_objects(source)
    ]


def write_jsonl(
    items: Iterable[BaseModel],
    target: str | Path | TextIO,
) -> None:
    close = False
    if hasattr(target, "write"):
        handle = target  # type: ignore[assignment]
    else:
        path = Path(target)
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = path.open("w", encoding="utf-8")
        close = True
    try:
        for item in items:
            handle.write(json.dumps(item.model_dump(mode="json"), sort_keys=True))
            handle.write("\n")
    finally:
        if close:
            handle.close()


def _process_spec_from_work(work: QueueWorkEnvelope) -> ProcessSpec:
    return ProcessSpec(
        id=work.process.id,
        capability=work.process.capability,
        needs=work.process.needs,
        adapter=work.process.adapter,
        timeout_seconds=work.process.timeout_seconds,
        priority=work.process.priority,
        max_concurrency=work.process.max_concurrency,
        resource_pool=work.process.resource_pool,
        resources=work.process.resources,
        config=work.process.config,
    )


def _start_queue_claim_renew_loop(
    work: QueueWorkEnvelope,
    *,
    client: ProcessRuntimeClient | None,
    renew_interval_seconds: float | None,
    lease_seconds: float,
) -> asyncio.Task | None:
    if client is None:
        return None
    interval = (
        renew_interval_seconds
        if renew_interval_seconds is not None
        else max(1.0, min(60.0, lease_seconds / 2))
    )
    if interval <= 0:
        return None
    return asyncio.create_task(
        _queue_claim_renew_loop(
            work,
            client=client,
            renew_interval_seconds=interval,
            lease_seconds=lease_seconds,
        )
    )


async def _stop_queue_claim_renew_loop(task: asyncio.Task | None) -> None:
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        return


async def _queue_claim_renew_loop(
    work: QueueWorkEnvelope,
    *,
    client: ProcessRuntimeClient,
    renew_interval_seconds: float,
    lease_seconds: float,
) -> None:
    while True:
        await asyncio.sleep(renew_interval_seconds)
        try:
            renewed = await _renew_queue_claim_once(
                work,
                client=client,
                lease_seconds=lease_seconds,
            )
        except Exception:
            return
        if renewed is None:
            return


async def _renew_queue_claim_once(
    work: QueueWorkEnvelope,
    *,
    client: ProcessRuntimeClient,
    lease_seconds: float,
) -> Any:
    return await client.renew_claim(
        run_id=work.run_id,
        document_id=work.document_id,
        process_id=work.process_id,
        pipeline_id=work.pipeline_id,
        worker_id=work.worker_id,
        lease_seconds=lease_seconds,
    )


def _append_jsonl(path: Path, envelope: BaseModel) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(envelope.model_dump(mode="json"), sort_keys=True))
        handle.write("\n")


def _read_jsonl_objects(source: str | Path | TextIO) -> list[dict[str, Any]]:
    if hasattr(source, "read"):
        text = source.read()
    else:
        text = Path(source).read_text(encoding="utf-8")
    objects: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            value = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSONL at line {line_number}: {exc}") from exc
        if not isinstance(value, dict):
            raise ValueError(f"JSONL line {line_number} must contain an object")
        objects.append(value)
    return objects


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _validate_limit(limit: int | None) -> None:
    if limit is not None and limit < 1:
        raise ValueError("limit must be greater than zero")


def _memory_work_is_claimable(row: dict[str, Any], now_iso: str) -> bool:
    return row["state"] == "ready" or (
        row["state"] == "leased"
        and row["lease_expires_at"] is not None
        and row["lease_expires_at"] <= now_iso
    )


def _memory_stats_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[tuple[str | None, str], int] = {}
    for row in rows:
        key = (row["queue"], row["state"])
        counts[key] = counts.get(key, 0) + 1
    return [
        {
            "queue": queue,
            "state": state,
            "count": count,
        }
        for (queue, state), count in sorted(
            counts.items(),
            key=lambda item: ((item[0][0] or ""), item[0][1]),
        )
    ]


def _queue_key(queue: str | None) -> str:
    return queue if queue is not None else "__default__"


def _redis_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _redis_json_loads(value: Any) -> dict[str, Any]:
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, dict):
        return value
    loaded = json.loads(str(value))
    if not isinstance(loaded, dict):
        raise ValueError("Redis queue row must contain a JSON object")
    return loaded


def _redis_target_without_fala_query(target: str) -> str:
    parsed = urlparse(target)
    query = parse_qs(parsed.query, keep_blank_values=True)
    query.pop("prefix", None)
    return urlunparse(
        parsed._replace(query=urlencode(query, doseq=True))
    )


def _default_redis_client(target: str) -> RedisClient:
    try:
        import redis
    except ImportError as exc:
        raise RuntimeError(
            "Redis queue broker requires the optional dependency: "
            "install with `fala[redis]`."
        ) from exc
    return redis.from_url(target, decode_responses=True)
