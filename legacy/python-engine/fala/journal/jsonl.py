"""JsonlJournal — durable JSONL event stream with fsync write barriers.

Wire format (one line per accepted batch)::

    {"v":1,"kind":"journal_batch","batch":{...JournalBatch...}}

Durability order (EVENT_STREAM_CORE):
1. Prepare batch (sequences, journal_seq)
2. Append full line + fsync
3. Update in-memory index

On open: truncate a torn last line (no trailing newline / invalid JSON), then
rebuild the index via import_stored_batch.

Single-process claim only (same limits as InMemoryJournal).
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from fala.journal.memory import InMemoryJournal
from fala.journal.types import (
    AppendResult,
    ClaimRequest,
    ClaimResult,
    JournalBatch,
)
from fala.runtime_backend import Process, RuntimeCommand, RuntimeEvent


JSONL_WIRE_VERSION = 1


def encode_journal_line(batch: JournalBatch) -> str:
    envelope = {
        "v": JSONL_WIRE_VERSION,
        "kind": "journal_batch",
        "batch": batch.model_dump(mode="json"),
    }
    return json.dumps(envelope, sort_keys=True, separators=(",", ":")) + "\n"


def decode_journal_line(line: str) -> JournalBatch:
    payload = json.loads(line)
    if not isinstance(payload, dict):
        raise ValueError("JSONL journal line must be an object")
    if payload.get("kind") not in {None, "journal_batch"}:
        raise ValueError(f"Unsupported journal line kind: {payload.get('kind')!r}")
    body = payload.get("batch", payload)
    return JournalBatch.model_validate(body)


def repair_torn_jsonl(path: Path) -> bool:
    """Truncate a partial last line if present. Returns True if truncated."""
    if not path.exists() or path.stat().st_size == 0:
        return False
    data = path.read_bytes()
    if data.endswith(b"\n"):
        # Last line complete; still validate last non-empty line parses.
        text = data.decode("utf-8")
        lines = text.splitlines()
        if not lines:
            return False
        try:
            json.loads(lines[-1])
            return False
        except json.JSONDecodeError:
            # Drop the bad last line entirely.
            keep = "\n".join(lines[:-1])
            if keep:
                keep += "\n"
            path.write_bytes(keep.encode("utf-8"))
            return True

    # No trailing newline → torn write: drop trailing partial line.
    last_nl = data.rfind(b"\n")
    if last_nl < 0:
        path.write_bytes(b"")
        return True
    path.write_bytes(data[: last_nl + 1])
    return True


def append_line_durable(path: Path, line: str) -> None:
    """Append one complete line and fsync the file (and parent dir on POSIX)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # O_APPEND + single write for the full line, then fsync.
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
    fd = os.open(path, flags, 0o644)
    try:
        payload = line.encode("utf-8")
        if not line.endswith("\n"):
            payload += b"\n"
        written = 0
        while written < len(payload):
            written += os.write(fd, payload[written:])
        os.fsync(fd)
    finally:
        os.close(fd)
    # Best-effort directory fsync so the directory entry is durable.
    try:
        dir_fd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        pass


class JsonlJournal:
    """Journal Protocol over an append-only JSONL file + memory index."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._uri = f"jsonl://{self._path.expanduser().resolve()}"
        self._index = InMemoryJournal(stream_id=self._uri)
        self._load()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def runtime_uri(self) -> str:
        return self._uri

    @property
    def index(self) -> InMemoryJournal:
        return self._index

    def _load(self) -> None:
        if not self._path.exists():
            return
        repair_torn_jsonl(self._path)
        if not self._path.exists() or self._path.stat().st_size == 0:
            return
        text = self._path.read_text(encoding="utf-8")
        for line in text.splitlines():
            if not line.strip():
                continue
            batch = decode_journal_line(line)
            self._index.import_stored_batch(batch)

    def _durable_write(self, batch: JournalBatch) -> None:
        append_line_durable(self._path, encode_journal_line(batch))

    async def append_batch(self, batch: JournalBatch) -> AppendResult:
        async with self._index._lock:
            return self._index._append_batch_unlocked(
                batch, before_commit=self._durable_write
            )

    async def claim_next(self, request: ClaimRequest) -> ClaimResult:
        # claim_next already holds index lock internally; avoid double-lock.
        return await self._index.claim_next(
            request, before_commit=self._durable_write
        )

    async def get_command_by_idempotency(
        self, *, run_id: str, idempotency_key: str
    ) -> RuntimeCommand | None:
        return await self._index.get_command_by_idempotency(
            run_id=run_id, idempotency_key=idempotency_key
        )

    async def list_events(
        self,
        *,
        run_id: str,
        after_sequence: int | None = None,
        impulse_id: str | None = None,
        limit: int | None = None,
    ) -> list[RuntimeEvent]:
        return await self._index.list_events(
            run_id=run_id,
            after_sequence=after_sequence,
            impulse_id=impulse_id,
            limit=limit,
        )

    async def load(
        self,
        *,
        run_id: str | None = None,
        after_journal_seq: int | None = None,
        limit: int | None = None,
    ) -> list[JournalBatch]:
        return await self._index.load(
            run_id=run_id,
            after_journal_seq=after_journal_seq,
            limit=limit,
        )

    def seed_process(self, process: Process) -> None:
        self._index.seed_process(process)

    def get_process(self, process_id: str) -> Process | None:
        return self._index.get_process(process_id)


class TeeJournal:
    """Fan-out journal: durable claim/append on primary; mirror appends to secondaries.

    ``claim_next`` runs only on the primary. Secondary journals receive the
    accepted batch via ``append_batch`` when the primary commits a new batch
    (including claim multi-unit batches).
    """

    def __init__(self, primary: object, *secondaries: object) -> None:
        self.primary = primary
        self.secondaries = list(secondaries)

    @property
    def runtime_uri(self) -> str:
        uri = getattr(self.primary, "runtime_uri", None)
        if isinstance(uri, str):
            return f"tee://{uri}"
        return "tee://primary"

    async def append_batch(self, batch: JournalBatch) -> AppendResult:
        result = await self.primary.append_batch(batch)  # type: ignore[union-attr]
        if not result.replayed:
            for secondary in self.secondaries:
                await secondary.append_batch(result.batch)  # type: ignore[union-attr]
        return result

    async def claim_next(self, request: ClaimRequest) -> ClaimResult:
        result = await self.primary.claim_next(request)  # type: ignore[union-attr]
        if result.batch is not None and not result.replayed:
            for secondary in self.secondaries:
                await secondary.append_batch(result.batch)  # type: ignore[union-attr]
        return result

    async def get_command_by_idempotency(
        self, *, run_id: str, idempotency_key: str
    ) -> RuntimeCommand | None:
        return await self.primary.get_command_by_idempotency(  # type: ignore[union-attr]
            run_id=run_id, idempotency_key=idempotency_key
        )

    async def list_events(
        self,
        *,
        run_id: str,
        after_sequence: int | None = None,
        impulse_id: str | None = None,
        limit: int | None = None,
    ) -> list[RuntimeEvent]:
        return await self.primary.list_events(  # type: ignore[union-attr]
            run_id=run_id,
            after_sequence=after_sequence,
            impulse_id=impulse_id,
            limit=limit,
        )

    async def load(
        self,
        *,
        run_id: str | None = None,
        after_journal_seq: int | None = None,
        limit: int | None = None,
    ) -> list[JournalBatch]:
        return await self.primary.load(  # type: ignore[union-attr]
            run_id=run_id,
            after_journal_seq=after_journal_seq,
            limit=limit,
        )
