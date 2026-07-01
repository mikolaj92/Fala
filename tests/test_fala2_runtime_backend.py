from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from fala.runtime_backend import (
    BridgeDelivery,
    BridgeDeliveryStatus,
    Carrier,
    DelegationPolicy,
    EventRef,
    Gate,
    GateStatus,
    Observation,
    Projection,
    RuntimeBudget,
    RuntimeCommand,
    RuntimeBackendService,
    RuntimeEvent,
    RuntimePool,
    RuntimeRef,
    RunRef,
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
                gates = await backend.list_gates(
                    run_id="run_beta",
                    status=GateStatus.completed,
                )
                projections = await backend.list_projections(run_id="run_beta")

                self.assertEqual(observations, [observation])
                self.assertEqual(stored_gate.status, GateStatus.completed)
                self.assertEqual(stored_projection, projection)
                self.assertEqual(gates, [gate.model_copy(update={"status": GateStatus.completed})])
                self.assertEqual(projections, [projection])

        asyncio.run(scenario())

    def test_runtime_backend_service_lists_runtime_systems(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                service = RuntimeBackendService.sqlite(Path(tmp_dir) / "fala2.sqlite")
                carrier = Carrier(
                    run_id="run_query",
                    carrier_type="message",
                    payload={"text": "hello"},
                )
                await service.accept_carrier(
                    carrier,
                    idempotency_key="run_query:carrier.accept:message",
                )
                observation, _ = await service.record_observation(
                    Observation(
                        run_id="run_query",
                        carrier_id=carrier.id,
                        kind="classifier.score",
                        values={"score": 0.98},
                    ),
                    idempotency_key="run_query:observation.record:score",
                )
                gate, _ = await service.save_gate(
                    Gate(
                        run_id="run_query",
                        carrier_id=carrier.id,
                        kind="human.approval",
                        status=GateStatus.open,
                    ),
                    idempotency_key="run_query:gate.save:approval",
                )
                projection, _ = await service.save_projection(
                    Projection(
                        run_id="run_query",
                        name="carrier_summary",
                        data={"carrier_count": 1},
                        source_event_sequence=2,
                    ),
                    idempotency_key="run_query:projection.save:carrier_summary",
                )

                self.assertEqual(
                    await service.list_observations(run_id="run_query"),
                    [observation],
                )
                self.assertEqual(
                    await service.list_gates(
                        run_id="run_query",
                        carrier_id=carrier.id,
                        status=GateStatus.open,
                    ),
                    [gate],
                )
                self.assertEqual(
                    await service.list_projections(run_id="run_query"),
                    [projection],
                )

        asyncio.run(scenario())

    def test_sqlite_bridge_delivers_carrier_between_local_runtimes_idempotently(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                source_path = Path(tmp_dir) / "source.sqlite"
                target_path = Path(tmp_dir) / "target.sqlite"
                source = RuntimeBackendService.sqlite(source_path)
                target = RuntimeBackendService.sqlite(target_path)
                source_ref = RuntimeRef(id="source", uri=f"sqlite://{source_path}")
                target_ref = RuntimeRef(id="target", uri=f"sqlite://{target_path}")
                pool = RuntimePool(
                    id="local_pair",
                    runtimes=[source_ref, target_ref],
                    carrier_types=["case"],
                )
                policy = DelegationPolicy(
                    pool_id=pool.id,
                    carrier_types=["case"],
                    budget=RuntimeBudget(
                        runtime_hops=1,
                        spawned_runs=1,
                        carrier_count=1,
                        wall_time_seconds=30,
                        attempts=2,
                        artifact_bytes=4096,
                    ),
                )
                carrier = Carrier(
                    id="carrier_case",
                    run_id="run_source",
                    carrier_type="case",
                    payload={"claim": "CLM-1"},
                )

                await source.accept_carrier(
                    carrier,
                    idempotency_key="run_source:carrier.accept:carrier_case",
                )
                source_events = await source.backend.list_events(run_id="run_source")
                delivery = BridgeDelivery(
                    id="bridge_case",
                    run_id="run_source",
                    idempotency_key="run_source:bridge:case",
                    source=RunRef(runtime=source_ref, run_id="run_source"),
                    target=RunRef(runtime=target_ref, run_id="run_target"),
                    carrier=carrier,
                    event_ref=EventRef(
                        runtime=source_ref,
                        run_id="run_source",
                        event_id=source_events[0].id,
                        sequence=source_events[0].sequence,
                    ),
                    pool_id=policy.pool_id,
                    budget=policy.budget,
                )

                outbox, enqueue = await source.enqueue_bridge_delivery(delivery)
                replay_outbox, enqueue_replay = await source.enqueue_bridge_delivery(
                    delivery.model_copy(update={"metadata": {"changed": True}}),
                    idempotency_key="run_source:bridge:case",
                )

                self.assertEqual(outbox.pool_id, "local_pair")
                self.assertEqual(outbox.budget.runtime_hops, 1)
                self.assertFalse(enqueue.replayed)
                self.assertEqual(replay_outbox, outbox)
                self.assertTrue(enqueue_replay.replayed)

                delivered, imported, delivered_submission, import_submission = (
                    await source.deliver_bridge_delivery(
                        run_id="run_source",
                        delivery_id="bridge_case",
                        target=target,
                        idempotency_key="run_source:bridge.deliver:case",
                        import_idempotency_key="run_target:bridge.import:case",
                    )
                )
                replay_delivered, replay_imported, delivered_replay, import_replay = (
                    await source.deliver_bridge_delivery(
                        run_id="run_source",
                        delivery_id="bridge_case",
                        target=target,
                        idempotency_key="run_source:bridge.deliver:case",
                        import_idempotency_key="run_target:bridge.import:case",
                    )
                )

                self.assertEqual(delivered.status, BridgeDeliveryStatus.delivered)
                self.assertEqual(imported.status, BridgeDeliveryStatus.imported)
                self.assertFalse(delivered_submission.replayed)
                self.assertFalse(import_submission.replayed)
                self.assertEqual(replay_delivered, delivered)
                self.assertEqual(replay_imported, imported)
                self.assertTrue(delivered_replay.replayed)
                self.assertTrue(import_replay.replayed)

                target_carrier = await target.backend.get_carrier(
                    run_id="run_target",
                    carrier_id="carrier_case",
                )
                self.assertIsNotNone(target_carrier)
                assert target_carrier is not None
                self.assertEqual(target_carrier.run_id, "run_target")
                self.assertEqual(
                    target_carrier.metadata["source_runtime_id"],
                    "source",
                )
                self.assertEqual(
                    await source.list_outbox_deliveries(
                        run_id="run_source",
                        status=BridgeDeliveryStatus.delivered,
                    ),
                    [delivered],
                )
                self.assertEqual(
                    await target.list_inbox_deliveries(
                        run_id="run_target",
                        status=BridgeDeliveryStatus.imported,
                    ),
                    [imported],
                )
                self.assertEqual(
                    [event.event_type for event in await source.backend.list_events(run_id="run_source")],
                    [
                        "carrier.accepted",
                        "bridge.outbox.enqueued",
                        "bridge.outbox.delivered",
                    ],
                )
                self.assertEqual(
                    [event.event_type for event in await target.backend.list_events(run_id="run_target")],
                    ["bridge.inbox.imported"],
                )

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
