from __future__ import annotations

from fala.runtime_backend import (
    Artifact,
    BridgeDelivery,
    BridgeDeliveryStatus,
    Carrier,
    CarrierRelation,
    CarrierType,
    EventRef,
    Gate,
    GateStatus,
    Observation,
    Projection,
    RuntimeBackend,
    RuntimeBudget,
    RuntimeCommand,
    RuntimeEvent,
    RuntimeRef,
    RunRef,
)


async def assert_runtime_backend_conformance(backend: RuntimeBackend) -> None:
    runtime = RuntimeRef(id="local", uri="sqlite://local")
    carrier_type = CarrierType(
        id="case",
        run_id="run_conformance",
        title="Case",
        media_types=["application/json"],
        value_schema={"type": "object"},
    )
    await backend.put_carrier_type(carrier_type)
    assert await backend.get_carrier_type(
        run_id=carrier_type.run_id,
        carrier_type_id=carrier_type.id,
    ) == carrier_type
    assert await backend.list_carrier_types(run_id=carrier_type.run_id) == [carrier_type]

    carrier = Carrier(
        id="carrier_conformance",
        run_id="run_conformance",
        carrier_type="case",
        payload={"case_id": "C-1"},
    )
    await backend.put_carrier(carrier)
    assert await backend.get_carrier(
        run_id=carrier.run_id,
        carrier_id=carrier.id,
    ) == carrier
    assert await backend.list_carriers(run_id=carrier.run_id) == [carrier]
    child_carrier = Carrier(
        id="carrier_child",
        run_id=carrier.run_id,
        carrier_type="case",
        payload={"case_id": "C-1-child"},
    )
    await backend.put_carrier(child_carrier)
    relation = CarrierRelation(
        id="relation_conformance",
        run_id=carrier.run_id,
        relation_type="derived_from",
        source_carrier_id=carrier.id,
        target_carrier_id=child_carrier.id,
    )
    await backend.put_carrier_relation(relation)
    assert await backend.get_carrier_relation(
        run_id=carrier.run_id,
        relation_id=relation.id,
    ) == relation
    assert await backend.list_carrier_relations(run_id=carrier.run_id) == [relation]
    assert await backend.list_carrier_relations(
        run_id=carrier.run_id,
        carrier_id=child_carrier.id,
    ) == [relation]

    command = RuntimeCommand(
        run_id=carrier.run_id,
        command_type="carrier.accept",
        idempotency_key="run_conformance:carrier.accept:C-1",
        actor="tester",
        correlation_id="corr-conformance",
        payload={"carrier_id": carrier.id},
    )
    first = await backend.submit_command(
        command,
        events=[
            RuntimeEvent(
                run_id=carrier.run_id,
                carrier_id=carrier.id,
                event_type="carrier.accepted",
                payload={"ok": True},
            )
        ],
    )
    replay = await backend.submit_command(
        command.model_copy(update={"id": "command_replay"}),
        events=[
            RuntimeEvent(
                run_id=carrier.run_id,
                carrier_id=carrier.id,
                event_type="carrier.accepted",
            )
        ],
    )
    events = await backend.list_events(run_id=carrier.run_id)
    assert not first.replayed
    assert replay.replayed
    assert replay.events == []
    assert [event.sequence for event in events] == [1]
    assert events[0].command_id == command.id
    assert events[0].correlation_id == "corr-conformance"

    observation = Observation(
        id="observation_score",
        run_id=carrier.run_id,
        carrier_id=carrier.id,
        kind="score",
        values={"score": 1},
    )
    await backend.put_observation(observation)
    assert await backend.list_observations(run_id=carrier.run_id) == [observation]

    artifact = Artifact(
        id="artifact_report",
        run_id=carrier.run_id,
        carrier_id=carrier.id,
        kind="report",
        uri="fala-artifact://sha256/abc",
        media_type="application/json",
        size_bytes=3,
        content_hash="sha256:abc",
    )
    await backend.put_artifact(artifact)
    await backend.put_artifact(
        artifact.model_copy(update={"uri": "fala-artifact://sha256/changed"})
    )
    assert await backend.get_artifact(
        run_id=carrier.run_id,
        artifact_id=artifact.id,
    ) == artifact
    assert await backend.list_artifacts(
        run_id=carrier.run_id,
        carrier_id=carrier.id,
        kind="report",
    ) == [artifact]

    gate = Gate(
        id="gate_review",
        run_id=carrier.run_id,
        carrier_id=carrier.id,
        kind="review",
        status=GateStatus.open,
    )
    await backend.put_gate(gate)
    await backend.put_gate(gate.model_copy(update={"status": GateStatus.completed}))
    assert await backend.get_gate(run_id=carrier.run_id, gate_id=gate.id) == gate.model_copy(
        update={"status": GateStatus.completed}
    )
    assert await backend.list_gates(
        run_id=carrier.run_id,
        status=GateStatus.completed,
    ) == [gate.model_copy(update={"status": GateStatus.completed})]

    projection = Projection(
        id="projection_case",
        run_id=carrier.run_id,
        name="case_summary",
        data={"carrier_id": carrier.id},
        source_event_sequence=1,
    )
    await backend.put_projection(projection)
    assert await backend.get_projection(
        run_id=carrier.run_id,
        name=projection.name,
    ) == projection
    assert await backend.list_projections(run_id=carrier.run_id) == [projection]

    delivery = BridgeDelivery(
        id="bridge_conformance",
        run_id=carrier.run_id,
        idempotency_key="bridge:conformance",
        source=RunRef(runtime=runtime, run_id=carrier.run_id),
        target=RunRef(runtime=RuntimeRef(id="target"), run_id="run_target"),
        carrier=carrier,
        event_ref=EventRef(runtime=runtime, run_id=carrier.run_id, sequence=1),
        budget=RuntimeBudget(runtime_hops=1, carrier_count=1),
    )
    await backend.put_outbox_delivery(delivery)
    await backend.put_inbox_delivery(
        delivery.model_copy(
            update={
                "run_id": "run_target",
                "status": BridgeDeliveryStatus.imported,
            }
        )
    )
    assert await backend.get_outbox_delivery(
        run_id=carrier.run_id,
        delivery_id=delivery.id,
    ) == delivery
    assert await backend.list_outbox_deliveries(
        run_id=carrier.run_id,
        status=BridgeDeliveryStatus.pending,
    ) == [delivery]
    assert len(
        await backend.list_inbox_deliveries(
            run_id="run_target",
            status=BridgeDeliveryStatus.imported,
        )
    ) == 1
