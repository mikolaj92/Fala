"""SqliteJournal — thin Journal Protocol wrap over Correlator.

Leading unit only is input to Correlator TX methods; non-leading units are
ignored as write inputs (Correlator regenerates side effects). No StateFact
population required (deferred until after full backend conformance gate).

See docs/EVENT_STREAM_CORE.md §PR4.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from fala.journal.types import (
    AppendResult,
    ClaimRequest,
    ClaimResult,
    CommandUnit,
    JournalBatch,
    StateFact,
)
from fala.runtime_backend import (
    Process,
    ProcessStatus,
    Run,
    RuntimeCommand,
    RuntimeEvent,
    Correlator,
)
from fala.runtime_pure import PROCESS_TRANSITION_COMMANDS


def _fact_body(units: list[CommandUnit], entity: str) -> dict[str, Any] | None:
    for unit in units:
        for fact in unit.facts:
            if fact.entity == entity and fact.op == "upsert":
                return dict(fact.body)
    return None


def _command_to_status(command_type: str) -> ProcessStatus | None:
    for status, ct in PROCESS_TRANSITION_COMMANDS.items():
        if ct == command_type:
            return status
    return None


class SqliteJournal:
    """Journal port backed by existing Correlator SQLite transactions."""

    def __init__(self, path: str | Path | Correlator) -> None:
        if isinstance(path, Correlator):
            self._backend = path
            self._path = Path(path.path)
        else:
            self._path = Path(path)
            self._backend = Correlator(self._path)

    @property
    def correlator(self) -> Correlator:
        return self._backend

    @property
    def path(self) -> Path:
        return self._path

    @property
    def runtime_uri(self) -> str:
        return f"sqlite://{self._path.expanduser().resolve()}"

    async def append_batch(self, batch: JournalBatch) -> AppendResult:
        if not batch.units:
            raise ValueError("JournalBatch.units must be non-empty")
        primary = batch.units[0]
        command = primary.command
        events = list(primary.events)
        command_type = command.command_type

        # Non-leading units are intentionally ignored as write inputs.
        if command_type == "run.create":
            body = _fact_body(batch.units, "run") or {"id": batch.run_id}
            if "id" not in body:
                body["id"] = batch.run_id
            run = Run.model_validate(body)
            submission = await self._backend.create_run(
                run, command, events=events
            )
        elif command_type == "process.schedule":
            body = _fact_body(batch.units, "process")
            if body is None:
                raise ValueError(
                    "process.schedule append_batch requires a process StateFact body"
                )
            process = Process.model_validate(body)
            submission = await self._backend.schedule_process(
                process, command, events=events
            )
        elif command_type in {
            "process.complete",
            "process.fail",
            "process.retry",
            "process.wait",
            "process.ready",
            "process.cancel",
            "process.timeout",
        }:
            status = _command_to_status(command_type)
            assert status is not None
            process_id = str(
                command.payload.get("process_id")
                or (primary.facts[0].key.get("id") if primary.facts else "")
            )
            if not process_id:
                raise ValueError(
                    f"{command_type} append_batch requires process_id in payload or fact key"
                )
            kwargs: dict[str, Any] = {}
            if "output" in command.payload and isinstance(
                command.payload["output"], dict
            ):
                kwargs["output"] = command.payload["output"]
            if "error" in command.payload and isinstance(
                command.payload["error"], dict
            ):
                kwargs["error"] = command.payload["error"]
            if "input" in command.payload and isinstance(
                command.payload["input"], dict
            ):
                kwargs["input"] = command.payload["input"]
            if "available_at" in command.payload:
                raw = command.payload["available_at"]
                if isinstance(raw, str):
                    kwargs["available_at"] = datetime.fromisoformat(raw)
            _process, submission = await self._backend.transition_process(
                run_id=command.run_id,
                process_id=process_id,
                status=status,
                command=command,
                events=events,
                **kwargs,
            )
        else:
            # Generic command+event append (no state mutation from facts).
            submission = await self._backend.submit_command(
                command, events=events
            )

        unit_out = CommandUnit(
            command=submission.command,
            events=list(submission.events),
            facts=list(primary.facts),
        )
        stored_batch = batch.model_copy(update={"units": [unit_out]})
        return AppendResult(
            batch=stored_batch,
            replayed=submission.replayed,
            units=[unit_out],
        )

    async def claim_next(self, request: ClaimRequest) -> ClaimResult:
        process = await self._backend.claim_next_ready_process(
            worker_id=request.worker_id,
            run_id=request.run_id,
            lease_seconds=request.lease_seconds,
            all_runs=request.all_runs,
        )
        if process is None:
            return ClaimResult(process=None, batch=None, replayed=False)

        # Synthesize batch from the claim command written by Correlator.
        command = await self._backend.get_command_by_idempotency(
            run_id=process.run_id,
            idempotency_key=f"process.claim:{process.id}:{process.attempt}",
        )
        events = await self._backend.list_events(run_id=process.run_id)
        claim_events = [
            e
            for e in events
            if e.process_id == process.id and e.event_type == "process.claimed"
        ]
        # Prefer events linked to this claim command when present.
        if command is not None:
            claim_events = [
                e for e in claim_events if e.command_id == command.id
            ] or claim_events[-1:]

        unit = CommandUnit(
            command=command
            or RuntimeCommand(
                run_id=process.run_id,
                command_type="process.claim",
                idempotency_key=f"process.claim:{process.id}:{process.attempt}",
                actor=request.worker_id,
                payload={"process_id": process.id},
            ),
            events=claim_events[-1:] if claim_events else [],
            facts=[
                StateFact(
                    entity="process",
                    op="upsert",
                    key={"id": process.id},
                    body=process.model_dump(mode="json"),
                )
            ],
        )
        batch = JournalBatch(run_id=process.run_id, units=[unit])
        return ClaimResult(process=process, batch=batch, replayed=False)

    async def get_command_by_idempotency(
        self, *, run_id: str, idempotency_key: str
    ) -> RuntimeCommand | None:
        return await self._backend.get_command_by_idempotency(
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
        return await self._backend.list_events(
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
        """Export-style reconstruction: one CommandUnit batch per stored command.

        ``after_journal_seq`` is ignored for SQLite v1 (no durable journal_seq
        column yet); ordering follows command created_at/id.
        """
        del after_journal_seq  # not persisted in SQLite schema yet
        if run_id is None:
            runs = await self._backend.list_runs()
            run_ids = [r.id for r in runs]
        else:
            run_ids = [run_id]

        batches: list[JournalBatch] = []
        seq = 0
        for rid in run_ids:
            commands = await self._backend.list_commands(run_id=rid)
            events = await self._backend.list_events(run_id=rid)
            events_by_cmd: dict[str, list[RuntimeEvent]] = {}
            for event in events:
                if event.command_id:
                    events_by_cmd.setdefault(event.command_id, []).append(event)
            for command in commands:
                seq += 1
                batches.append(
                    JournalBatch(
                        journal_seq=seq,
                        run_id=rid,
                        units=[
                            CommandUnit(
                                command=command,
                                events=list(events_by_cmd.get(command.id, [])),
                                facts=[],
                            )
                        ],
                    )
                )
        if limit is not None:
            batches = batches[:limit]
        return batches
