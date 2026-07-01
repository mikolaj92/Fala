from __future__ import annotations

from fala.runtime_backend import (
    Artifact,
    BridgeDelivery,
    BridgeDeliveryStatus,
    Carrier,
    CarrierProcessStatus,
    CarrierRunStatus,
    CarrierRelation,
    CarrierType,
    DelegationPolicy,
    EventRef,
    Gate,
    GateStatus,
    Observation,
    Process,
    Projection,
    RuntimeBackend,
    RuntimeBudget,
    RuntimeCommand,
    RuntimeEvent,
    RuntimePool,
    RuntimeRef,
    Run,
    RunRef,
)


async def assert_runtime_backend_conformance(backend: RuntimeBackend) -> None:
    runtime = RuntimeRef(id="local", uri="sqlite://local")
    run = Run(
        id="run_conformance",
        status=CarrierRunStatus.created,
        title="Conformance run",
        package_id="conformance",
        flow_id="basic",
    )
    await backend.put_run(run)
    assert await backend.get_run(run_id=run.id) == run
    assert await backend.list_runs(status=CarrierRunStatus.created) == [run]

    pool = RuntimePool(
        id="local_pool",
        runtimes=[runtime, RuntimeRef(id="target", uri="sqlite://target")],
        carrier_types=["case"],
    )
    policy = DelegationPolicy(
        id="policy_case",
        pool_id=pool.id,
        carrier_types=["case"],
        budget=RuntimeBudget(runtime_hops=1, carrier_count=1, attempts=2),
    )
    await backend.put_runtime_pool(pool)
    await backend.put_delegation_policy(policy)
    assert await backend.get_runtime_pool(pool_id=pool.id) == pool
    assert await backend.list_runtime_pools() == [pool]
    assert await backend.get_delegation_policy(policy_id=policy.id) == policy
    assert await backend.list_delegation_policies(pool_id=pool.id) == [policy]

    carrier_type = CarrierType(
        id="case",
        run_id=run.id,
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

    process = Process(
        id="process_score",
        run_id=carrier.run_id,
        carrier_id=carrier.id,
        process_type="score",
        status=CarrierProcessStatus.ready,
        max_attempts=2,
        input={"carrier_id": carrier.id},
    )
    await backend.put_process(process)
    assert await backend.list_processes(
        run_id=carrier.run_id,
        status=CarrierProcessStatus.ready,
    ) == [process]
    claimed = await backend.claim_next_ready_process(
        run_id=carrier.run_id,
        worker_id="worker_1",
        lease_seconds=30,
    )
    assert claimed is not None
    assert claimed.status == CarrierProcessStatus.running
    assert claimed.attempt == 1
    assert claimed.lease_owner == "worker_1"
    assert (
        await backend.claim_next_ready_process(
            run_id=carrier.run_id,
            worker_id="worker_2",
            lease_seconds=30,
        )
        is None
    )
    completed = await backend.complete_process(
        run_id=carrier.run_id,
        process_id=process.id,
        output={"score": 1},
    )
    assert completed.status == CarrierProcessStatus.succeeded
    assert completed.output == {"score": 1}

    retry_process = Process(
        id="process_retry",
        run_id=carrier.run_id,
        carrier_id=carrier.id,
        process_type="retryable",
        status=CarrierProcessStatus.ready,
        max_attempts=2,
    )
    await backend.put_process(retry_process)
    claimed_retry = await backend.claim_next_ready_process(
        run_id=carrier.run_id,
        worker_id="worker_1",
    )
    assert claimed_retry is not None
    failed = await backend.fail_process(
        run_id=carrier.run_id,
        process_id=retry_process.id,
        error={"message": "temporary"},
    )
    assert failed.status == CarrierProcessStatus.failed
    waiting = await backend.retry_process(
        run_id=carrier.run_id,
        process_id=retry_process.id,
        error={"message": "try again"},
    )
    assert waiting.status == CarrierProcessStatus.retry_wait
    claimed_again = await backend.claim_next_ready_process(
        run_id=carrier.run_id,
        worker_id="worker_2",
    )
    assert claimed_again is not None
    assert claimed_again.status == CarrierProcessStatus.running
    assert claimed_again.attempt == 2

    gate = Gate(
        id="gate_review",
        run_id=carrier.run_id,
        carrier_id=carrier.id,
        kind="review",
        status=GateStatus.open,
    )
    await backend.put_gate(gate)
    completed_gate = await backend.complete_gate(
        run_id=carrier.run_id,
        gate_id=gate.id,
        values={"decision": "approved"},
    )
    assert completed_gate.status == GateStatus.completed
    assert completed_gate.values == {"decision": "approved"}
    assert await backend.get_gate(run_id=carrier.run_id, gate_id=gate.id) == completed_gate
    assert await backend.list_gates(
        run_id=carrier.run_id,
        status=GateStatus.completed,
    ) == [completed_gate]

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
    rebuilt = await backend.rebuild_projections(run_id=carrier.run_id)
    assert len(rebuilt) == 1
    summary = rebuilt[0]
    assert summary.id == "projection_run_summary"
    assert summary.name == "run_summary"
    assert summary.source_event_sequence == 1
    assert summary.data["event_type_counts"] == {"carrier.accepted": 1}
    assert summary.data["carrier_count"] == 2
    assert summary.data["process_status_counts"] == {
        "running": 1,
        "succeeded": 1,
    }
    assert summary.data["resource_accounting"]["artifact_bytes"] == 3
    assert summary.data["resource_accounting"]["process_attempts"] == 3
    assert summary.data["resource_accounting"]["bridge_delivery_count"] == 0
    assert await backend.get_projection(
        run_id=carrier.run_id,
        name="run_summary",
    ) == summary

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
