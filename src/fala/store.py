from __future__ import annotations

import asyncio
from typing import Protocol

from fala.models import (
    CombinedProjection,
    ProcessClaim,
    ProcessEvent,
    ProcessInput,
    ProcessOutput,
    ProcessStatus,
)


class StateStore(Protocol):
    async def append_event(self, event: ProcessEvent) -> None:
        ...

    async def list_events(
        self,
        *,
        run_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
        after_event_id: str | None = None,
        limit: int | None = None,
        descending: bool = False,
    ) -> list[ProcessEvent]:
        ...

    async def count_events(
        self,
        *,
        run_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
    ) -> int:
        ...

    async def set_status(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        status: ProcessStatus,
    ) -> None:
        ...

    async def clear_status(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> None:
        ...

    async def put_document_input(
        self,
        *,
        run_id: str,
        document_id: str,
        input: ProcessInput,
        pipeline_id: str | None = None,
    ) -> None:
        ...

    async def get_document_input(
        self, *, run_id: str, document_id: str
    ) -> ProcessInput | None:
        ...

    async def get_document_pipeline_id(
        self, *, run_id: str, document_id: str
    ) -> str | None:
        ...

    async def put_claim(self, claim: ProcessClaim) -> None:
        ...

    async def try_claim_process(self, claim: ProcessClaim) -> bool:
        ...

    async def get_claim(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> ProcessClaim | None:
        ...

    async def clear_claim(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> None:
        ...

    async def put_output(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        output: ProcessOutput,
    ) -> None:
        ...

    async def clear_output(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> None:
        ...

    async def get_output(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> ProcessOutput | None:
        ...

    async def get_outputs(
        self, *, run_id: str, document_id: str, process_ids: list[str]
    ) -> dict[str, ProcessOutput]:
        ...

    async def put_projection(self, projection: CombinedProjection) -> None:
        ...

    async def clear_projections(self, *, run_id: str, document_id: str) -> None:
        ...

    async def get_projection(
        self, *, run_id: str, document_id: str, projection_id: str
    ) -> CombinedProjection | None:
        ...

    async def list_documents(self, *, run_id: str) -> list[str]:
        ...

    async def list_statuses(
        self, *, run_id: str, document_id: str
    ) -> dict[str, ProcessStatus]:
        ...

    async def list_outputs(
        self, *, run_id: str, document_id: str
    ) -> dict[str, ProcessOutput]:
        ...

    async def list_claims(
        self, *, run_id: str, document_id: str
    ) -> dict[str, ProcessClaim]:
        ...

    async def list_projections(
        self, *, run_id: str, document_id: str
    ) -> dict[str, CombinedProjection]:
        ...


class InMemoryStateStore:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._events: list[ProcessEvent] = []
        self._statuses: dict[tuple[str, str, str], ProcessStatus] = {}
        self._document_inputs: dict[tuple[str, str], ProcessInput] = {}
        self._document_pipeline_ids: dict[tuple[str, str], str] = {}
        self._claims: dict[tuple[str, str, str], ProcessClaim] = {}
        self._outputs: dict[tuple[str, str, str], ProcessOutput] = {}
        self._projections: dict[tuple[str, str, str], CombinedProjection] = {}

    async def append_event(self, event: ProcessEvent) -> None:
        async with self._lock:
            self._events.append(event)

    async def list_events(
        self,
        *,
        run_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
        after_event_id: str | None = None,
        limit: int | None = None,
        descending: bool = False,
    ) -> list[ProcessEvent]:
        async with self._lock:
            events = [
                event
                for event in self._events
                if (run_id is None or event.run_id == run_id)
                and (document_id is None or event.document_id == document_id)
                and (process_id is None or event.process_id == process_id)
            ]
            if after_event_id is not None:
                for index, event in enumerate(events):
                    if event.id == after_event_id:
                        events = events[index + 1 :]
                        break
                else:
                    raise ValueError("after_event_id not found")
            if descending:
                events = list(reversed(events))
            if limit is not None:
                events = events[:limit]
            return events

    async def count_events(
        self,
        *,
        run_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
    ) -> int:
        async with self._lock:
            return sum(
                1
                for event in self._events
                if (run_id is None or event.run_id == run_id)
                and (document_id is None or event.document_id == document_id)
                and (process_id is None or event.process_id == process_id)
            )

    async def set_status(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        status: ProcessStatus,
    ) -> None:
        async with self._lock:
            self._statuses[(run_id, document_id, process_id)] = status

    async def clear_status(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> None:
        async with self._lock:
            self._statuses.pop((run_id, document_id, process_id), None)

    async def put_document_input(
        self,
        *,
        run_id: str,
        document_id: str,
        input: ProcessInput,
        pipeline_id: str | None = None,
    ) -> None:
        async with self._lock:
            self._document_inputs[(run_id, document_id)] = input
            if pipeline_id is not None:
                self._document_pipeline_ids[(run_id, document_id)] = pipeline_id

    async def get_document_input(
        self, *, run_id: str, document_id: str
    ) -> ProcessInput | None:
        async with self._lock:
            return self._document_inputs.get((run_id, document_id))

    async def get_document_pipeline_id(
        self, *, run_id: str, document_id: str
    ) -> str | None:
        async with self._lock:
            return self._document_pipeline_ids.get((run_id, document_id))

    async def put_claim(self, claim: ProcessClaim) -> None:
        async with self._lock:
            self._claims[(claim.run_id, claim.document_id, claim.process_id)] = claim

    async def try_claim_process(self, claim: ProcessClaim) -> bool:
        async with self._lock:
            key = (claim.run_id, claim.document_id, claim.process_id)
            if key in self._outputs:
                return False
            if self._statuses.get(key) != ProcessStatus.queued:
                return False
            self._claims[key] = claim
            self._statuses[key] = ProcessStatus.running
            return True

    async def get_claim(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> ProcessClaim | None:
        async with self._lock:
            return self._claims.get((run_id, document_id, process_id))

    async def clear_claim(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> None:
        async with self._lock:
            self._claims.pop((run_id, document_id, process_id), None)

    async def put_output(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        output: ProcessOutput,
    ) -> None:
        async with self._lock:
            self._outputs[(run_id, document_id, process_id)] = output

    async def clear_output(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> None:
        async with self._lock:
            self._outputs.pop((run_id, document_id, process_id), None)

    async def get_output(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> ProcessOutput | None:
        async with self._lock:
            return self._outputs.get((run_id, document_id, process_id))

    async def get_outputs(
        self, *, run_id: str, document_id: str, process_ids: list[str]
    ) -> dict[str, ProcessOutput]:
        async with self._lock:
            outputs: dict[str, ProcessOutput] = {}
            for process_id in process_ids:
                output = self._outputs.get((run_id, document_id, process_id))
                if output is not None:
                    outputs[process_id] = output
            return outputs

    async def put_projection(self, projection: CombinedProjection) -> None:
        async with self._lock:
            self._projections[(projection.run_id, projection.document_id, projection.id)] = projection

    async def clear_projections(self, *, run_id: str, document_id: str) -> None:
        async with self._lock:
            keys = [
                key
                for key in self._projections
                if key[0] == run_id and key[1] == document_id
            ]
            for key in keys:
                self._projections.pop(key, None)

    async def get_projection(
        self, *, run_id: str, document_id: str, projection_id: str
    ) -> CombinedProjection | None:
        async with self._lock:
            return self._projections.get((run_id, document_id, projection_id))

    async def list_documents(self, *, run_id: str) -> list[str]:
        async with self._lock:
            document_ids = {
                event.document_id
                for event in self._events
                if event.run_id == run_id
            }
            document_ids.update(
                document_id
                for stored_run_id, document_id, _process_id in self._statuses
                if stored_run_id == run_id
            )
            document_ids.update(
                document_id
                for stored_run_id, document_id in self._document_inputs
                if stored_run_id == run_id
            )
            document_ids.update(
                document_id
                for stored_run_id, document_id, _process_id in self._claims
                if stored_run_id == run_id
            )
            document_ids.update(
                document_id
                for stored_run_id, document_id, _process_id in self._outputs
                if stored_run_id == run_id
            )
            document_ids.update(
                document_id
                for stored_run_id, document_id, _projection_id in self._projections
                if stored_run_id == run_id
            )
            return sorted(document_ids)

    async def list_statuses(
        self, *, run_id: str, document_id: str
    ) -> dict[str, ProcessStatus]:
        async with self._lock:
            return {
                process_id: status
                for (stored_run_id, stored_document_id, process_id), status in self._statuses.items()
                if stored_run_id == run_id and stored_document_id == document_id
            }

    async def list_outputs(
        self, *, run_id: str, document_id: str
    ) -> dict[str, ProcessOutput]:
        async with self._lock:
            return {
                process_id: output
                for (stored_run_id, stored_document_id, process_id), output in self._outputs.items()
                if stored_run_id == run_id and stored_document_id == document_id
            }

    async def list_claims(
        self, *, run_id: str, document_id: str
    ) -> dict[str, ProcessClaim]:
        async with self._lock:
            return {
                process_id: claim
                for (stored_run_id, stored_document_id, process_id), claim in self._claims.items()
                if stored_run_id == run_id and stored_document_id == document_id
            }

    async def list_projections(
        self, *, run_id: str, document_id: str
    ) -> dict[str, CombinedProjection]:
        async with self._lock:
            return {
                projection_id: projection
                for (
                    stored_run_id,
                    stored_document_id,
                    projection_id,
                ), projection in self._projections.items()
                if stored_run_id == run_id and stored_document_id == document_id
            }
