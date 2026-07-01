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
    create_run = Run(
        id="run_create_conformance",
        status=CarrierRunStatus.active,
    )
    create_command = RuntimeCommand(
        run_id=create_run.id,
        command_type="run.create",
        idempotency_key="run_create_conformance:create",
    )
    create_submission = await backend.create_run(
        create_run,
        create_command,
        events=[
            RuntimeEvent(
                run_id=create_run.id,
                event_type="run.created",
                payload={"run_id": create_run.id},
            )
        ],
    )
    replay_create = await backend.create_run(
        create_run.model_copy(update={"title": "changed"}),
        create_command.model_copy(update={"id": "command_create_replay"}),
        events=[],
    )
    assert await backend.get_run(run_id=create_run.id) == create_run
    assert not create_submission.replayed
    assert replay_create.replayed
    create_events = await backend.list_events(run_id=create_run.id)
    assert [event.event_type for event in create_events] == ["run.created"]

    accept_run = Run(
        id="run_accept_conformance",
        status=CarrierRunStatus.active,
    )
    accept_carrier = Carrier(
        id="carrier_accept_conformance",
        run_id=accept_run.id,
        carrier_type="case",
    )
    accept_command = RuntimeCommand(
        run_id=accept_run.id,
        command_type="carrier.accept",
        idempotency_key="run_accept_conformance:carrier.accept",
    )
    await backend.put_run(accept_run)
    accept_submission = await backend.accept_carrier(
        accept_carrier,
        accept_command,
        events=[
            RuntimeEvent(
                run_id=accept_run.id,
                carrier_id=accept_carrier.id,
                event_type="carrier.accepted",
            )
        ],
    )
    accept_replay = await backend.accept_carrier(
        accept_carrier.model_copy(update={"payload": {"changed": True}}),
        accept_command.model_copy(update={"id": "command_accept_replay"}),
        events=[],
    )
    assert await backend.get_carrier(
        run_id=accept_run.id,
        carrier_id=accept_carrier.id,
    ) == accept_carrier
    assert not accept_submission.replayed
    assert accept_replay.replayed
    accept_events = await backend.list_events(run_id=accept_run.id)
    assert [event.event_type for event in accept_events] == ["carrier.accepted"]

    type_run = Run(
        id="run_type_conformance",
        status=CarrierRunStatus.active,
    )
    registered_type = CarrierType(
        id="registered_case",
        run_id=type_run.id,
        media_types=["application/json"],
    )
    type_command = RuntimeCommand(
        run_id=type_run.id,
        command_type="carrier_type.register",
        idempotency_key="run_type_conformance:carrier_type.register",
    )
    await backend.put_run(type_run)
    type_submission = await backend.register_carrier_type(
        registered_type,
        type_command,
        events=[
            RuntimeEvent(
                run_id=type_run.id,
                event_type="carrier_type.registered",
            )
        ],
    )
    type_replay = await backend.register_carrier_type(
        registered_type.model_copy(update={"title": "changed"}),
        type_command.model_copy(update={"id": "command_type_replay"}),
        events=[],
    )
    assert await backend.get_carrier_type(
        run_id=type_run.id,
        carrier_type_id=registered_type.id,
    ) == registered_type
    assert not type_submission.replayed
    assert type_replay.replayed

    relation_run = Run(
        id="run_relation_conformance",
        status=CarrierRunStatus.active,
    )
    relation_source = Carrier(
        id="carrier_relation_source",
        run_id=relation_run.id,
        carrier_type="case",
    )
    relation_target = Carrier(
        id="carrier_relation_target",
        run_id=relation_run.id,
        carrier_type="case",
    )
    recorded_relation = CarrierRelation(
        id="relation_recorded",
        run_id=relation_run.id,
        relation_type="derived_from",
        source_carrier_id=relation_source.id,
        target_carrier_id=relation_target.id,
    )
    relation_command = RuntimeCommand(
        run_id=relation_run.id,
        command_type="carrier_relation.record",
        idempotency_key="run_relation_conformance:relation.recorded",
    )
    await backend.put_run(relation_run)
    await backend.put_carrier(relation_source)
    await backend.put_carrier(relation_target)
    relation_submission = await backend.record_carrier_relation(
        recorded_relation,
        relation_command,
        events=[
            RuntimeEvent(
                run_id=relation_run.id,
                carrier_id=relation_source.id,
                event_type="carrier_relation.recorded",
            )
        ],
    )
    relation_replay = await backend.record_carrier_relation(
        recorded_relation.model_copy(update={"metadata": {"changed": True}}),
        relation_command.model_copy(update={"id": "command_relation_replay"}),
        events=[],
    )
    assert await backend.get_carrier_relation(
        run_id=relation_run.id,
        relation_id=recorded_relation.id,
    ) == recorded_relation
    assert not relation_submission.replayed
    assert relation_replay.replayed

    observation_run = Run(
        id="run_observation_conformance",
        status=CarrierRunStatus.active,
    )
    observation_carrier = Carrier(
        id="carrier_observation",
        run_id=observation_run.id,
        carrier_type="case",
    )
    recorded_observation = Observation(
        id="observation_recorded",
        run_id=observation_run.id,
        carrier_id=observation_carrier.id,
        kind="score",
        values={"score": 1},
    )
    observation_command = RuntimeCommand(
        run_id=observation_run.id,
        command_type="observation.record",
        idempotency_key="run_observation_conformance:observation.recorded",
    )
    await backend.put_run(observation_run)
    await backend.put_carrier(observation_carrier)
    observation_submission = await backend.record_observation(
        recorded_observation,
        observation_command,
        events=[
            RuntimeEvent(
                run_id=observation_run.id,
                carrier_id=observation_carrier.id,
                event_type="observation.recorded",
            )
        ],
    )
    observation_replay = await backend.record_observation(
        recorded_observation.model_copy(update={"values": {"score": 2}}),
        observation_command.model_copy(update={"id": "command_observation_replay"}),
        events=[],
    )
    assert await backend.list_observations(
        run_id=observation_run.id,
        carrier_id=observation_carrier.id,
    ) == [recorded_observation]
    assert not observation_submission.replayed
    assert observation_replay.replayed

    artifact_run = Run(
        id="run_artifact_conformance",
        status=CarrierRunStatus.active,
    )
    artifact_carrier = Carrier(
        id="carrier_artifact",
        run_id=artifact_run.id,
        carrier_type="case",
    )
    recorded_artifact = Artifact(
        id="artifact_recorded",
        run_id=artifact_run.id,
        carrier_id=artifact_carrier.id,
        kind="report",
        uri="fala-artifact://sha256/recorded",
        media_type="application/json",
        size_bytes=8,
        content_hash="sha256:recorded",
    )
    artifact_command = RuntimeCommand(
        run_id=artifact_run.id,
        command_type="artifact.record",
        idempotency_key="run_artifact_conformance:artifact.recorded",
    )
    await backend.put_run(artifact_run)
    await backend.put_carrier(artifact_carrier)
    artifact_submission = await backend.record_artifact(
        recorded_artifact,
        artifact_command,
        events=[
            RuntimeEvent(
                run_id=artifact_run.id,
                carrier_id=artifact_carrier.id,
                event_type="artifact.recorded",
            )
        ],
    )
    artifact_replay = await backend.record_artifact(
        recorded_artifact.model_copy(update={"uri": "fala-artifact://sha256/changed"}),
        artifact_command.model_copy(update={"id": "command_artifact_replay"}),
        events=[],
    )
    assert await backend.get_artifact(
        run_id=artifact_run.id,
        artifact_id=recorded_artifact.id,
    ) == recorded_artifact
    assert not artifact_submission.replayed
    assert artifact_replay.replayed

    schedule_run = Run(
        id="run_schedule_conformance",
        status=CarrierRunStatus.active,
    )
    schedule_carrier = Carrier(
        id="carrier_schedule",
        run_id=schedule_run.id,
        carrier_type="case",
    )
    scheduled_process = Process(
        id="process_scheduled",
        run_id=schedule_run.id,
        carrier_id=schedule_carrier.id,
        process_type="score",
        status=CarrierProcessStatus.ready,
        input={"carrier_id": schedule_carrier.id},
    )
    schedule_command = RuntimeCommand(
        run_id=schedule_run.id,
        command_type="process.schedule",
        idempotency_key="run_schedule_conformance:process.scheduled",
    )
    await backend.put_run(schedule_run)
    await backend.put_carrier(schedule_carrier)
    schedule_submission = await backend.schedule_process(
        scheduled_process,
        schedule_command,
        events=[
            RuntimeEvent(
                run_id=schedule_run.id,
                carrier_id=schedule_carrier.id,
                process_id=scheduled_process.id,
                event_type="process.scheduled",
            )
        ],
    )
    schedule_replay = await backend.schedule_process(
        scheduled_process.model_copy(update={"priority": 100}),
        schedule_command.model_copy(update={"id": "command_schedule_replay"}),
        events=[],
    )
    assert await backend.get_process(
        run_id=schedule_run.id,
        process_id=scheduled_process.id,
    ) == scheduled_process
    assert not schedule_submission.replayed
    assert schedule_replay.replayed

    transition_run = Run(
        id="run_transition_conformance",
        status=CarrierRunStatus.active,
    )
    transition_carrier = Carrier(
        id="carrier_transition",
        run_id=transition_run.id,
        carrier_type="case",
    )
    running_process = Process(
        id="process_transition",
        run_id=transition_run.id,
        carrier_id=transition_carrier.id,
        process_type="score",
        status=CarrierProcessStatus.running,
        attempt=1,
    )
    transition_command = RuntimeCommand(
        run_id=transition_run.id,
        command_type="process.complete",
        idempotency_key="run_transition_conformance:process.complete",
    )
    await backend.put_run(transition_run)
    await backend.put_carrier(transition_carrier)
    await backend.put_process(running_process)
    completed_process, transition_submission = await backend.transition_process(
        run_id=transition_run.id,
        process_id=running_process.id,
        status=CarrierProcessStatus.succeeded,
        command=transition_command,
        events=[
            RuntimeEvent(
                run_id=transition_run.id,
                carrier_id=transition_carrier.id,
                process_id=running_process.id,
                event_type="process.completed",
            )
        ],
        output={"score": 1},
    )
    replayed_process, transition_replay = await backend.transition_process(
        run_id=transition_run.id,
        process_id=running_process.id,
        status=CarrierProcessStatus.succeeded,
        command=transition_command.model_copy(
            update={"id": "command_transition_replay"}
        ),
        events=[],
        output={"score": 2},
    )
    assert completed_process.status == CarrierProcessStatus.succeeded
    assert completed_process.output == {"score": 1}
    assert replayed_process == completed_process
    assert not transition_submission.replayed
    assert transition_replay.replayed

    gate_run = Run(
        id="run_gate_conformance",
        status=CarrierRunStatus.active,
    )
    saved_gate = Gate(
        id="gate_recorded",
        run_id=gate_run.id,
        kind="review",
        status=GateStatus.open,
    )
    gate_command = RuntimeCommand(
        run_id=gate_run.id,
        command_type="gate.open",
        idempotency_key="run_gate_conformance:gate.open",
    )
    await backend.put_run(gate_run)
    gate_submission = await backend.save_gate(
        saved_gate,
        gate_command,
        events=[
            RuntimeEvent(
                run_id=gate_run.id,
                event_type="gate.opened",
            )
        ],
    )
    gate_replay = await backend.save_gate(
        saved_gate.model_copy(update={"metadata": {"changed": True}}),
        gate_command.model_copy(update={"id": "command_gate_replay"}),
        events=[],
    )
    completed_gate, gate_transition = await backend.transition_gate(
        run_id=gate_run.id,
        gate_id=saved_gate.id,
        status=GateStatus.completed,
        command=RuntimeCommand(
            run_id=gate_run.id,
            command_type="gate.complete",
            idempotency_key="run_gate_conformance:gate.complete",
        ),
        events=[
            RuntimeEvent(
                run_id=gate_run.id,
                event_type="gate.completed",
            )
        ],
        values={"decision": "approved"},
    )
    replayed_gate, gate_transition_replay = await backend.transition_gate(
        run_id=gate_run.id,
        gate_id=saved_gate.id,
        status=GateStatus.completed,
        command=RuntimeCommand(
            run_id=gate_run.id,
            command_type="gate.complete",
            idempotency_key="run_gate_conformance:gate.complete",
        ),
        events=[],
        values={"decision": "changed"},
    )
    assert (
        await backend.get_gate(run_id=gate_run.id, gate_id=saved_gate.id)
        == completed_gate
    )
    assert completed_gate.status == GateStatus.completed
    assert completed_gate.values == {"decision": "approved"}
    assert replayed_gate == completed_gate
    assert not gate_submission.replayed
    assert gate_replay.replayed
    assert not gate_transition.replayed
    assert gate_transition_replay.replayed

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
    assert await backend.get_command(
        run_id=carrier.run_id,
        command_id=command.id,
    ) == command
    assert await backend.get_command_by_idempotency(
        run_id=carrier.run_id,
        idempotency_key=command.idempotency_key,
    ) == command
    assert await backend.list_commands(run_id=carrier.run_id) == [command]
    assert await backend.list_commands(
        run_id=carrier.run_id,
        command_type="carrier.accept",
    ) == [command]
    assert await backend.list_commands(
        run_id=carrier.run_id,
        actor="tester",
    ) == [command]
    assert await backend.list_events(
        run_id=carrier.run_id,
        carrier_id=carrier.id,
    ) == events
    assert await backend.list_events(
        run_id=carrier.run_id,
        after_sequence=0,
        limit=1,
    ) == events

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

    cancel_process = Process(
        id="process_cancel",
        run_id=carrier.run_id,
        carrier_id=carrier.id,
        process_type="cancelable",
        status=CarrierProcessStatus.ready,
    )
    await backend.put_process(cancel_process)
    cancelled_process = await backend.cancel_process(
        run_id=carrier.run_id,
        process_id=cancel_process.id,
        error={"reason": "operator"},
    )
    assert cancelled_process.status == CarrierProcessStatus.cancelled
    assert cancelled_process.error == {"reason": "operator"}
    timeout_process = Process(
        id="process_timeout",
        run_id=carrier.run_id,
        carrier_id=carrier.id,
        process_type="timeoutable",
        status=CarrierProcessStatus.running,
    )
    await backend.put_process(timeout_process)
    timed_out_process = await backend.timeout_process(
        run_id=carrier.run_id,
        process_id=timeout_process.id,
        error={"reason": "timeout"},
    )
    assert timed_out_process.status == CarrierProcessStatus.timed_out
    assert timed_out_process.error == {"reason": "timeout"}

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
    cancel_gate = Gate(
        id="gate_cancel",
        run_id=carrier.run_id,
        carrier_id=carrier.id,
        kind="review",
        status=GateStatus.open,
    )
    await backend.put_gate(cancel_gate)
    cancelled_gate = await backend.cancel_gate(
        run_id=carrier.run_id,
        gate_id=cancel_gate.id,
        values={"reason": "operator"},
    )
    assert cancelled_gate.status == GateStatus.cancelled
    assert cancelled_gate.values == {"reason": "operator"}
    expire_gate = Gate(
        id="gate_expire",
        run_id=carrier.run_id,
        carrier_id=carrier.id,
        kind="review",
        status=GateStatus.open,
    )
    await backend.put_gate(expire_gate)
    expired_gate = await backend.expire_gate(
        run_id=carrier.run_id,
        gate_id=expire_gate.id,
        values={"reason": "timeout"},
    )
    assert expired_gate.status == GateStatus.expired
    assert expired_gate.values == {"reason": "timeout"}

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
        "cancelled": 1,
        "running": 1,
        "succeeded": 1,
        "timed_out": 1,
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
    await backend.put_run(Run(id="run_target"))
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
