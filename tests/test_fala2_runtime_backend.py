from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from fala.runtime_backend import (
    Carrier,
    Gate,
    GateStatus,
    Observation,
    Projection,
    RuntimeCommand,
    RuntimeBackendService,
    RuntimeEvent,
    SQLiteRuntimeBackend,
)


class Fala2RuntimeBackendTests(unittest.TestCase):
    def test_sqlite_backend_records_carrier_command_and_ordered_event(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                backend = SQLiteRuntimeBackend(Path(tmp_dir) / "fala2.sqlite")
                carrier = Carrier(
                    run_id="run_alpha",
                    carrier_type="invoice",
                    payload={"amount": 120},
                    metadata={"tenant": "acme"},
                )

                await backend.put_carrier(carrier)
                stored = await backend.get_carrier(
                    run_id="run_alpha", carrier_id=carrier.id
                )

                self.assertEqual(stored, carrier)

                command = RuntimeCommand(
                    run_id="run_alpha",
                    command_type="carrier.accept",
                    idempotency_key="run_alpha:carrier.accept:invoice",
                    actor="operator:mika",
                    correlation_id="corr_1",
                    payload={"carrier_id": carrier.id},
                )
                event = RuntimeEvent(
                    run_id="run_alpha",
                    carrier_id=carrier.id,
                    event_type="carrier.accepted",
                    actor="operator:mika",
                    correlation_id="corr_1",
                    payload={"accepted": True},
                )

                first = await backend.submit_command(command, events=[event])
                replay = await backend.submit_command(
                    command.model_copy(update={"id": "command_duplicate"}),
                    events=[
                        event.model_copy(update={"id": "event_duplicate"}),
                    ],
                )

                self.assertFalse(first.replayed)
                self.assertTrue(replay.replayed)
                self.assertEqual(replay.command.id, first.command.id)
                self.assertEqual(replay.events, [])

                events = await backend.list_events(run_id="run_alpha")
                self.assertEqual(len(events), 1)
                self.assertEqual(events[0].sequence, 1)
                self.assertEqual(events[0].command_id, first.command.id)
                self.assertEqual(events[0].carrier_id, carrier.id)
                self.assertEqual(events[0].actor, "operator:mika")
                self.assertEqual(events[0].correlation_id, "corr_1")

        asyncio.run(scenario())

    def test_runtime_backend_service_accepts_carrier_idempotently(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                service = RuntimeBackendService.sqlite(Path(tmp_dir) / "fala2.sqlite")
                carrier = Carrier(
                    id="carrier_case_1",
                    run_id="run_service",
                    carrier_type="arbitration_case",
                    payload={"claim_id": "CLM-1"},
                )

                first_carrier, first_submission = await service.accept_carrier(
                    carrier,
                    idempotency_key="run_service:carrier.accept:carrier_case_1",
                    actor="operator:mika",
                )
                replay_carrier, replay_submission = await service.accept_carrier(
                    carrier.model_copy(update={"payload": {"claim_id": "changed"}}),
                    idempotency_key="run_service:carrier.accept:carrier_case_1",
                    actor="operator:mika",
                )

                self.assertEqual(first_carrier, carrier)
                self.assertFalse(first_submission.replayed)
                self.assertEqual(replay_carrier, carrier)
                self.assertTrue(replay_submission.replayed)
                self.assertEqual(replay_submission.events, [])
                events = await service.backend.list_events(run_id="run_service")
                self.assertEqual(len(events), 1)
                self.assertEqual(events[0].event_type, "carrier.accepted")

        asyncio.run(scenario())

    def test_runtime_backend_service_replays_gate_and_projection_writes(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                service = RuntimeBackendService.sqlite(Path(tmp_dir) / "fala2.sqlite")
                gate = Gate(
                    id="gate_review",
                    run_id="run_service",
                    kind="human.review",
                    status=GateStatus.completed,
                )
                projection = Projection(
                    id="projection_summary",
                    run_id="run_service",
                    name="summary",
                    version=1,
                    data={"completed_gates": 1},
                    source_event_sequence=1,
                )

                first_gate, first_gate_submission = await service.save_gate(
                    gate,
                    idempotency_key="run_service:gate.save:gate_review",
                    actor="operator:mika",
                )
                replay_gate, replay_gate_submission = await service.save_gate(
                    gate.model_copy(update={"status": GateStatus.cancelled}),
                    idempotency_key="run_service:gate.save:gate_review",
                    actor="operator:mika",
                )
                first_projection, first_projection_submission = (
                    await service.save_projection(
                        projection,
                        idempotency_key="run_service:projection.save:summary",
                        correlation_id="corr_projection",
                    )
                )
                replay_projection, replay_projection_submission = (
                    await service.save_projection(
                        projection.model_copy(update={"version": 2}),
                        idempotency_key="run_service:projection.save:summary",
                        correlation_id="corr_projection",
                    )
                )

                self.assertEqual(first_gate, gate)
                self.assertFalse(first_gate_submission.replayed)
                self.assertEqual(replay_gate, gate)
                self.assertTrue(replay_gate_submission.replayed)
                self.assertEqual(first_projection, projection)
                self.assertFalse(first_projection_submission.replayed)
                self.assertEqual(replay_projection, projection)
                self.assertTrue(replay_projection_submission.replayed)
                events = await service.backend.list_events(run_id="run_service")
                self.assertEqual([event.sequence for event in events], [1, 2])
                self.assertEqual(
                    [event.event_type for event in events],
                    ["gate.saved", "projection.saved"],
                )
                self.assertEqual(events[1].correlation_id, "corr_projection")

        asyncio.run(scenario())

    def test_sqlite_backend_persists_observations_gates_and_projections(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                backend = SQLiteRuntimeBackend(Path(tmp_dir) / "fala2.sqlite")
                carrier = Carrier(
                    run_id="run_beta",
                    carrier_type="message",
                    payload={"text": "hello"},
                )
                await backend.put_carrier(carrier)

                observation = Observation(
                    run_id="run_beta",
                    carrier_id=carrier.id,
                    kind="classifier.score",
                    values={"score": 0.98},
                    metadata={"model": "local"},
                )
                await backend.put_observation(observation)

                gate = Gate(
                    run_id="run_beta",
                    carrier_id=carrier.id,
                    kind="human.approval",
                    status=GateStatus.open,
                    values={"reason": "needs review"},
                )
                await backend.put_gate(gate)
                await backend.put_gate(
                    gate.model_copy(update={"status": GateStatus.completed})
                )

                projection = Projection(
                    run_id="run_beta",
                    name="carrier_summary",
                    version=1,
                    data={"carrier_count": 1, "last_kind": observation.kind},
                    source_event_sequence=0,
                )
                await backend.put_projection(projection)

                observations = await backend.list_observations(run_id="run_beta")
                stored_gate = await backend.get_gate(run_id="run_beta", gate_id=gate.id)
                stored_projection = await backend.get_projection(
                    run_id="run_beta", name="carrier_summary"
                )

                self.assertEqual(observations, [observation])
                self.assertEqual(stored_gate.status, GateStatus.completed)
                self.assertEqual(stored_projection, projection)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
