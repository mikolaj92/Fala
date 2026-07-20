from __future__ import annotations

from fala.runtime_backend import (
    Reaction,
    BridgeDelivery,
    BridgeDeliveryStatus,
    Impulse,
    ProcessStatus,
    RunStatus,
    ImpulseRelation,
    ImpulseType,
    DelegationPolicy,
    EventRef,
    Homeostat,
    HomeostatStatus,
    Association,
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
        status=RunStatus.active,
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

    run_status_run = Run(
        id="run_status_conformance",
        status=RunStatus.created,
    )
    run_status_command = RuntimeCommand(
        run_id=run_status_run.id,
        command_type="run.status.set",
        idempotency_key="run_status_conformance:active",
    )
    await backend.put_run(run_status_run)
    active_run, run_status_submission = await backend.transition_run(
        run_id=run_status_run.id,
        status=RunStatus.active,
        command=run_status_command,
        events=[
            RuntimeEvent(
                run_id=run_status_run.id,
                event_type="run.status.changed",
            )
        ],
    )
    replayed_active_run, run_status_replay = await backend.transition_run(
        run_id=run_status_run.id,
        status=RunStatus.active,
        command=run_status_command.model_copy(update={"id": "command_run_replay"}),
        events=[],
    )
    assert active_run.status == RunStatus.active
    assert active_run.started_at is not None
    assert replayed_active_run == active_run
    assert not run_status_submission.replayed
    assert run_status_replay.replayed

    accept_run = Run(
        id="run_accept_conformance",
        status=RunStatus.active,
    )
    accept_impulse = Impulse(
        id="impulse_accept_conformance",
        run_id=accept_run.id,
        impulse_type="case",
    )
    accept_command = RuntimeCommand(
        run_id=accept_run.id,
        command_type="impulse.accept",
        idempotency_key="run_accept_conformance:impulse.accept",
    )
    await backend.put_run(accept_run)
    accept_submission = await backend.accept_impulse(
        accept_impulse,
        accept_command,
        events=[
            RuntimeEvent(
                run_id=accept_run.id,
                impulse_id=accept_impulse.id,
                event_type="impulse.accepted",
            )
        ],
    )
    accept_replay = await backend.accept_impulse(
        accept_impulse.model_copy(update={"payload": {"changed": True}}),
        accept_command.model_copy(update={"id": "command_accept_replay"}),
        events=[],
    )
    assert await backend.get_impulse(
        run_id=accept_run.id,
        impulse_id=accept_impulse.id,
    ) == accept_impulse
    assert not accept_submission.replayed
    assert accept_replay.replayed
    accept_events = await backend.list_events(run_id=accept_run.id)
    assert [event.event_type for event in accept_events] == ["impulse.accepted"]

    type_run = Run(
        id="run_type_conformance",
        status=RunStatus.active,
    )
    registered_type = ImpulseType(
        id="registered_case",
        run_id=type_run.id,
        media_types=["application/json"],
    )
    type_command = RuntimeCommand(
        run_id=type_run.id,
        command_type="impulse_type.register",
        idempotency_key="run_type_conformance:impulse_type.register",
    )
    await backend.put_run(type_run)
    type_submission = await backend.register_impulse_type(
        registered_type,
        type_command,
        events=[
            RuntimeEvent(
                run_id=type_run.id,
                event_type="impulse_type.registered",
            )
        ],
    )
    type_replay = await backend.register_impulse_type(
        registered_type.model_copy(update={"title": "changed"}),
        type_command.model_copy(update={"id": "command_type_replay"}),
        events=[],
    )
    assert await backend.get_impulse_type(
        run_id=type_run.id,
        impulse_type_id=registered_type.id,
    ) == registered_type
    assert not type_submission.replayed
    assert type_replay.replayed

    relation_run = Run(
        id="run_relation_conformance",
        status=RunStatus.active,
    )
    relation_source = Impulse(
        id="impulse_relation_source",
        run_id=relation_run.id,
        impulse_type="case",
    )
    relation_target = Impulse(
        id="impulse_relation_target",
        run_id=relation_run.id,
        impulse_type="case",
    )
    recorded_relation = ImpulseRelation(
        id="relation_recorded",
        run_id=relation_run.id,
        relation_type="derived_from",
        source_impulse_id=relation_source.id,
        target_impulse_id=relation_target.id,
    )
    relation_command = RuntimeCommand(
        run_id=relation_run.id,
        command_type="impulse_relation.record",
        idempotency_key="run_relation_conformance:relation.recorded",
    )
    await backend.put_run(relation_run)
    await backend.put_impulse(relation_source)
    await backend.put_impulse(relation_target)
    relation_submission = await backend.record_impulse_relation(
        recorded_relation,
        relation_command,
        events=[
            RuntimeEvent(
                run_id=relation_run.id,
                impulse_id=relation_source.id,
                event_type="impulse_relation.recorded",
            )
        ],
    )
    relation_replay = await backend.record_impulse_relation(
        recorded_relation.model_copy(update={"metadata": {"changed": True}}),
        relation_command.model_copy(update={"id": "command_relation_replay"}),
        events=[],
    )
    assert await backend.get_impulse_relation(
        run_id=relation_run.id,
        relation_id=recorded_relation.id,
    ) == recorded_relation
    assert not relation_submission.replayed
    assert relation_replay.replayed

    association_run = Run(
        id="run_association_conformance",
        status=RunStatus.active,
    )
    association_impulse = Impulse(
        id="impulse_association",
        run_id=association_run.id,
        impulse_type="case",
    )
    recorded_association = Association(
        id="association_recorded",
        run_id=association_run.id,
        impulse_id=association_impulse.id,
        kind="score",
        values={"score": 1},
    )
    association_command = RuntimeCommand(
        run_id=association_run.id,
        command_type="association.record",
        idempotency_key="run_association_conformance:association.recorded",
    )
    await backend.put_run(association_run)
    await backend.put_impulse(association_impulse)
    association_submission = await backend.record_association(
        recorded_association,
        association_command,
        events=[
            RuntimeEvent(
                run_id=association_run.id,
                impulse_id=association_impulse.id,
                event_type="association.recorded",
            )
        ],
    )
    association_replay = await backend.record_association(
        recorded_association.model_copy(update={"values": {"score": 2}}),
        association_command.model_copy(update={"id": "command_association_replay"}),
        events=[],
    )
    assert await backend.list_associations(
        run_id=association_run.id,
        impulse_id=association_impulse.id,
    ) == [recorded_association]
    assert not association_submission.replayed
    assert association_replay.replayed

    reaction_run = Run(
        id="run_reaction_conformance",
        status=RunStatus.active,
    )
    reaction_impulse = Impulse(
        id="impulse_reaction",
        run_id=reaction_run.id,
        impulse_type="case",
    )
    recorded_reaction = Reaction(
        id="reaction_recorded",
        run_id=reaction_run.id,
        impulse_id=reaction_impulse.id,
        kind="report",
        uri="fala-reaction://sha256/recorded",
        media_type="application/json",
        size_bytes=8,
        content_hash="sha256:recorded",
    )
    reaction_command = RuntimeCommand(
        run_id=reaction_run.id,
        command_type="reaction.record",
        idempotency_key="run_reaction_conformance:reaction.recorded",
    )
    await backend.put_run(reaction_run)
    await backend.put_impulse(reaction_impulse)
    reaction_submission = await backend.record_reaction(
        recorded_reaction,
        reaction_command,
        events=[
            RuntimeEvent(
                run_id=reaction_run.id,
                impulse_id=reaction_impulse.id,
                event_type="reaction.recorded",
            )
        ],
    )
    reaction_replay = await backend.record_reaction(
        recorded_reaction.model_copy(update={"uri": "fala-reaction://sha256/changed"}),
        reaction_command.model_copy(update={"id": "command_reaction_replay"}),
        events=[],
    )
    assert await backend.get_reaction(
        run_id=reaction_run.id,
        reaction_id=recorded_reaction.id,
    ) == recorded_reaction
    assert not reaction_submission.replayed
    assert reaction_replay.replayed

    schedule_run = Run(
        id="run_schedule_conformance",
        status=RunStatus.active,
    )
    schedule_impulse = Impulse(
        id="impulse_schedule",
        run_id=schedule_run.id,
        impulse_type="case",
    )
    scheduled_process = Process(
        id="process_scheduled",
        run_id=schedule_run.id,
        impulse_id=schedule_impulse.id,
        process_type="score",
        status=ProcessStatus.ready,
        input={"impulse_id": schedule_impulse.id},
    )
    schedule_command = RuntimeCommand(
        run_id=schedule_run.id,
        command_type="process.schedule",
        idempotency_key="run_schedule_conformance:process.scheduled",
    )
    await backend.put_run(schedule_run)
    await backend.put_impulse(schedule_impulse)
    schedule_submission = await backend.schedule_process(
        scheduled_process,
        schedule_command,
        events=[
            RuntimeEvent(
                run_id=schedule_run.id,
                impulse_id=schedule_impulse.id,
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
        status=RunStatus.active,
    )
    transition_impulse = Impulse(
        id="impulse_transition",
        run_id=transition_run.id,
        impulse_type="case",
    )
    running_process = Process(
        id="process_transition",
        run_id=transition_run.id,
        impulse_id=transition_impulse.id,
        process_type="score",
        status=ProcessStatus.running,
        attempt=1,
    )
    transition_command = RuntimeCommand(
        run_id=transition_run.id,
        command_type="process.complete",
        idempotency_key="run_transition_conformance:process.complete",
    )
    await backend.put_run(transition_run)
    await backend.put_impulse(transition_impulse)
    await backend.put_process(running_process)
    completed_process, transition_submission = await backend.transition_process(
        run_id=transition_run.id,
        process_id=running_process.id,
        status=ProcessStatus.succeeded,
        command=transition_command,
        events=[
            RuntimeEvent(
                run_id=transition_run.id,
                impulse_id=transition_impulse.id,
                process_id=running_process.id,
                event_type="process.completed",
            )
        ],
        output={"score": 1},
    )
    replayed_process, transition_replay = await backend.transition_process(
        run_id=transition_run.id,
        process_id=running_process.id,
        status=ProcessStatus.succeeded,
        command=transition_command.model_copy(
            update={"id": "command_transition_replay"}
        ),
        events=[],
        output={"score": 2},
    )
    assert completed_process.status == ProcessStatus.succeeded
    assert completed_process.output == {"score": 1}
    assert replayed_process == completed_process
    assert not transition_submission.replayed
    assert transition_replay.replayed

    homeostat_run = Run(
        id="run_homeostat_conformance",
        status=RunStatus.active,
    )
    saved_homeostat = Homeostat(
        id="homeostat_recorded",
        run_id=homeostat_run.id,
        kind="review",
        status=HomeostatStatus.open,
    )
    homeostat_command = RuntimeCommand(
        run_id=homeostat_run.id,
        command_type="homeostat.open",
        idempotency_key="run_homeostat_conformance:homeostat.open",
    )
    await backend.put_run(homeostat_run)
    homeostat_submission = await backend.save_homeostat(
        saved_homeostat,
        homeostat_command,
        events=[
            RuntimeEvent(
                run_id=homeostat_run.id,
                event_type="homeostat.opened",
            )
        ],
    )
    homeostat_replay = await backend.save_homeostat(
        saved_homeostat.model_copy(update={"metadata": {"changed": True}}),
        homeostat_command.model_copy(update={"id": "command_homeostat_replay"}),
        events=[],
    )
    completed_homeostat, homeostat_transition = await backend.transition_homeostat(
        run_id=homeostat_run.id,
        homeostat_id=saved_homeostat.id,
        status=HomeostatStatus.completed,
        command=RuntimeCommand(
            run_id=homeostat_run.id,
            command_type="homeostat.complete",
            idempotency_key="run_homeostat_conformance:homeostat.complete",
        ),
        events=[
            RuntimeEvent(
                run_id=homeostat_run.id,
                event_type="homeostat.completed",
            )
        ],
        values={"decision": "approved"},
    )
    replayed_homeostat, homeostat_transition_replay = await backend.transition_homeostat(
        run_id=homeostat_run.id,
        homeostat_id=saved_homeostat.id,
        status=HomeostatStatus.completed,
        command=RuntimeCommand(
            run_id=homeostat_run.id,
            command_type="homeostat.complete",
            idempotency_key="run_homeostat_conformance:homeostat.complete",
        ),
        events=[],
        values={"decision": "changed"},
    )
    assert (
        await backend.get_homeostat(run_id=homeostat_run.id, homeostat_id=saved_homeostat.id)
        == completed_homeostat
    )
    assert completed_homeostat.status == HomeostatStatus.completed
    assert completed_homeostat.values == {"decision": "approved"}
    assert replayed_homeostat == completed_homeostat
    assert not homeostat_submission.replayed
    assert homeostat_replay.replayed
    assert not homeostat_transition.replayed
    assert homeostat_transition_replay.replayed

    projection_run = Run(
        id="run_projection_conformance",
        status=RunStatus.active,
    )
    saved_projection = Projection(
        run_id=projection_run.id,
        name="manual_projection",
        data={"count": 1},
        source_event_sequence=0,
    )
    projection_command = RuntimeCommand(
        run_id=projection_run.id,
        command_type="projection.save",
        idempotency_key="run_projection_conformance:projection.save",
    )
    await backend.put_run(projection_run)
    projection_submission = await backend.save_projection(
        saved_projection,
        projection_command,
        events=[
            RuntimeEvent(
                run_id=projection_run.id,
                event_type="projection.saved",
            )
        ],
    )
    projection_replay = await backend.save_projection(
        saved_projection.model_copy(update={"data": {"count": 2}}),
        projection_command.model_copy(update={"id": "command_projection_replay"}),
        events=[],
    )
    assert await backend.get_projection(
        run_id=projection_run.id,
        name=saved_projection.name,
    ) == saved_projection
    assert not projection_submission.replayed
    assert projection_replay.replayed

    rebuild_command = RuntimeCommand(
        run_id=projection_run.id,
        command_type="projection.rebuild",
        idempotency_key="run_projection_conformance:projection.rebuild",
    )
    rebuilt_projections, rebuild_submission = (
        await backend.rebuild_projections_with_command(
            run_id=projection_run.id,
            names=["run_summary"],
            command=rebuild_command,
            events=[
                RuntimeEvent(
                    run_id=projection_run.id,
                    event_type="projection.rebuilt",
                )
            ],
        )
    )
    replayed_rebuild, rebuild_replay = await backend.rebuild_projections_with_command(
        run_id=projection_run.id,
        names=["run_summary"],
        command=rebuild_command.model_copy(update={"id": "command_rebuild_replay"}),
        events=[],
    )
    assert [projection.name for projection in rebuilt_projections] == ["run_summary"]
    assert replayed_rebuild == rebuilt_projections
    assert not rebuild_submission.replayed
    assert rebuild_replay.replayed

    run = Run(
        id="run_conformance",
        status=RunStatus.created,
        title="Conformance run",
        package_id="conformance",
        correlation_path_id="basic",
    )
    await backend.put_run(run)
    assert await backend.get_run(run_id=run.id) == run
    assert await backend.list_runs(status=RunStatus.created) == [run]

    pool = RuntimePool(
        id="local_pool",
        runtimes=[runtime, RuntimeRef(id="target", uri="sqlite://target")],
        impulse_types=["case"],
    )
    policy = DelegationPolicy(
        id="policy_case",
        pool_id=pool.id,
        impulse_types=["case"],
        budget=RuntimeBudget(runtime_hops=1, impulse_count=1, attempts=2),
    )
    await backend.put_runtime_pool(pool)
    await backend.put_delegation_policy(policy)
    assert await backend.get_runtime_pool(pool_id=pool.id) == pool
    assert await backend.list_runtime_pools() == [pool]
    assert await backend.get_delegation_policy(policy_id=policy.id) == policy
    assert await backend.list_delegation_policies(pool_id=pool.id) == [policy]

    impulse_type = ImpulseType(
        id="case",
        run_id=run.id,
        title="Case",
        media_types=["application/json"],
        value_schema={"type": "object"},
    )
    await backend.put_impulse_type(impulse_type)
    assert await backend.get_impulse_type(
        run_id=impulse_type.run_id,
        impulse_type_id=impulse_type.id,
    ) == impulse_type
    assert await backend.list_impulse_types(run_id=impulse_type.run_id) == [impulse_type]

    impulse = Impulse(
        id="impulse_conformance",
        run_id="run_conformance",
        impulse_type="case",
        payload={"case_id": "C-1"},
    )
    await backend.put_impulse(impulse)
    assert await backend.get_impulse(
        run_id=impulse.run_id,
        impulse_id=impulse.id,
    ) == impulse
    assert await backend.list_impulses(run_id=impulse.run_id) == [impulse]
    child_impulse = Impulse(
        id="impulse_child",
        run_id=impulse.run_id,
        impulse_type="case",
        payload={"case_id": "C-1-child"},
    )
    await backend.put_impulse(child_impulse)
    relation = ImpulseRelation(
        id="relation_conformance",
        run_id=impulse.run_id,
        relation_type="derived_from",
        source_impulse_id=impulse.id,
        target_impulse_id=child_impulse.id,
    )
    await backend.put_impulse_relation(relation)
    assert await backend.get_impulse_relation(
        run_id=impulse.run_id,
        relation_id=relation.id,
    ) == relation
    assert await backend.list_impulse_relations(run_id=impulse.run_id) == [relation]
    assert await backend.list_impulse_relations(
        run_id=impulse.run_id,
        impulse_id=child_impulse.id,
    ) == [relation]

    command = RuntimeCommand(
        run_id=impulse.run_id,
        command_type="impulse.accept",
        idempotency_key="run_conformance:impulse.accept:C-1",
        actor="tester",
        correlation_id="corr-conformance",
        payload={"impulse_id": impulse.id},
    )
    first = await backend.submit_command(
        command,
        events=[
            RuntimeEvent(
                run_id=impulse.run_id,
                impulse_id=impulse.id,
                event_type="impulse.accepted",
                payload={"ok": True},
            )
        ],
    )
    replay = await backend.submit_command(
        command.model_copy(update={"id": "command_replay"}),
        events=[
            RuntimeEvent(
                run_id=impulse.run_id,
                impulse_id=impulse.id,
                event_type="impulse.accepted",
            )
        ],
    )
    events = await backend.list_events(run_id=impulse.run_id)
    assert not first.replayed
    assert replay.replayed
    assert replay.events == []
    assert [event.sequence for event in events] == [1]
    assert events[0].command_id == command.id
    assert events[0].correlation_id == "corr-conformance"
    assert await backend.get_command(
        run_id=impulse.run_id,
        command_id=command.id,
    ) == command
    assert await backend.get_command_by_idempotency(
        run_id=impulse.run_id,
        idempotency_key=command.idempotency_key,
    ) == command
    assert await backend.list_commands(run_id=impulse.run_id) == [command]
    assert await backend.list_commands(
        run_id=impulse.run_id,
        command_type="impulse.accept",
    ) == [command]
    assert await backend.list_commands(
        run_id=impulse.run_id,
        actor="tester",
    ) == [command]
    assert await backend.list_events(
        run_id=impulse.run_id,
        impulse_id=impulse.id,
    ) == events
    assert await backend.list_events(
        run_id=impulse.run_id,
        after_sequence=0,
        limit=1,
    ) == events

    association = Association(
        id="association_score",
        run_id=impulse.run_id,
        impulse_id=impulse.id,
        kind="score",
        values={"score": 1},
    )
    await backend.put_association(association)
    assert await backend.list_associations(run_id=impulse.run_id) == [association]

    reaction = Reaction(
        id="reaction_report",
        run_id=impulse.run_id,
        impulse_id=impulse.id,
        kind="report",
        uri="fala-reaction://sha256/abc",
        media_type="application/json",
        size_bytes=3,
        content_hash="sha256:abc",
    )
    await backend.put_reaction(reaction)
    await backend.put_reaction(
        reaction.model_copy(update={"uri": "fala-reaction://sha256/changed"})
    )
    assert await backend.get_reaction(
        run_id=impulse.run_id,
        reaction_id=reaction.id,
    ) == reaction
    assert await backend.list_reactions(
        run_id=impulse.run_id,
        impulse_id=impulse.id,
        kind="report",
    ) == [reaction]

    process = Process(
        id="process_score",
        run_id=impulse.run_id,
        impulse_id=impulse.id,
        process_type="score",
        status=ProcessStatus.ready,
        max_attempts=2,
        input={"impulse_id": impulse.id},
    )
    await backend.put_process(process)
    assert await backend.list_processes(
        run_id=impulse.run_id,
        status=ProcessStatus.ready,
    ) == [process]
    claimed = await backend.claim_next_ready_process(
        run_id=impulse.run_id,
        worker_id="worker_1",
        lease_seconds=30,
    )
    assert claimed is not None
    assert claimed.status == ProcessStatus.running
    assert claimed.attempt == 1
    assert claimed.lease_owner == "worker_1"
    assert (
        await backend.claim_next_ready_process(
            run_id=impulse.run_id,
            worker_id="worker_2",
            lease_seconds=30,
        )
        is None
    )
    completed = await backend.complete_process(
        run_id=impulse.run_id,
        process_id=process.id,
        output={"score": 1},
    )
    assert completed.status == ProcessStatus.succeeded
    assert completed.output == {"score": 1}

    retry_process = Process(
        id="process_retry",
        run_id=impulse.run_id,
        impulse_id=impulse.id,
        process_type="retryable",
        status=ProcessStatus.ready,
        max_attempts=2,
    )
    await backend.put_process(retry_process)
    claimed_retry = await backend.claim_next_ready_process(
        run_id=impulse.run_id,
        worker_id="worker_1",
    )
    assert claimed_retry is not None
    failed = await backend.fail_process(
        run_id=impulse.run_id,
        process_id=retry_process.id,
        error={"message": "temporary"},
    )
    assert failed.status == ProcessStatus.failed
    waiting = await backend.retry_process(
        run_id=impulse.run_id,
        process_id=retry_process.id,
        error={"message": "try again"},
    )
    assert waiting.status == ProcessStatus.retry_wait
    claimed_again = await backend.claim_next_ready_process(
        run_id=impulse.run_id,
        worker_id="worker_2",
    )
    assert claimed_again is not None
    assert claimed_again.status == ProcessStatus.running
    assert claimed_again.attempt == 2

    cancel_process = Process(
        id="process_cancel",
        run_id=impulse.run_id,
        impulse_id=impulse.id,
        process_type="cancelable",
        status=ProcessStatus.ready,
    )
    await backend.put_process(cancel_process)
    cancelled_process = await backend.cancel_process(
        run_id=impulse.run_id,
        process_id=cancel_process.id,
        error={"reason": "operator"},
    )
    assert cancelled_process.status == ProcessStatus.cancelled
    assert cancelled_process.error == {"reason": "operator"}
    timeout_process = Process(
        id="process_timeout",
        run_id=impulse.run_id,
        impulse_id=impulse.id,
        process_type="timeoutable",
        status=ProcessStatus.running,
    )
    await backend.put_process(timeout_process)
    timed_out_process = await backend.timeout_process(
        run_id=impulse.run_id,
        process_id=timeout_process.id,
        error={"reason": "timeout"},
    )
    assert timed_out_process.status == ProcessStatus.timed_out
    assert timed_out_process.error == {"reason": "timeout"}

    homeostat = Homeostat(
        id="homeostat_review",
        run_id=impulse.run_id,
        impulse_id=impulse.id,
        kind="review",
        status=HomeostatStatus.open,
    )
    await backend.put_homeostat(homeostat)
    completed_homeostat = await backend.complete_homeostat(
        run_id=impulse.run_id,
        homeostat_id=homeostat.id,
        values={"decision": "approved"},
    )
    assert completed_homeostat.status == HomeostatStatus.completed
    assert completed_homeostat.values == {"decision": "approved"}
    assert await backend.get_homeostat(run_id=impulse.run_id, homeostat_id=homeostat.id) == completed_homeostat
    assert await backend.list_homeostats(
        run_id=impulse.run_id,
        status=HomeostatStatus.completed,
    ) == [completed_homeostat]
    cancel_homeostat = Homeostat(
        id="homeostat_cancel",
        run_id=impulse.run_id,
        impulse_id=impulse.id,
        kind="review",
        status=HomeostatStatus.open,
    )
    await backend.put_homeostat(cancel_homeostat)
    cancelled_homeostat = await backend.cancel_homeostat(
        run_id=impulse.run_id,
        homeostat_id=cancel_homeostat.id,
        values={"reason": "operator"},
    )
    assert cancelled_homeostat.status == HomeostatStatus.cancelled
    assert cancelled_homeostat.values == {"reason": "operator"}
    expire_homeostat = Homeostat(
        id="homeostat_expire",
        run_id=impulse.run_id,
        impulse_id=impulse.id,
        kind="review",
        status=HomeostatStatus.open,
    )
    await backend.put_homeostat(expire_homeostat)
    expired_homeostat = await backend.expire_homeostat(
        run_id=impulse.run_id,
        homeostat_id=expire_homeostat.id,
        values={"reason": "timeout"},
    )
    assert expired_homeostat.status == HomeostatStatus.expired
    assert expired_homeostat.values == {"reason": "timeout"}

    projection = Projection(
        id="projection_case",
        run_id=impulse.run_id,
        name="case_summary",
        data={"impulse_id": impulse.id},
        source_event_sequence=1,
    )
    await backend.put_projection(projection)
    assert await backend.get_projection(
        run_id=impulse.run_id,
        name=projection.name,
    ) == projection
    assert await backend.list_projections(run_id=impulse.run_id) == [projection]
    rebuilt = await backend.rebuild_projections(run_id=impulse.run_id)
    assert len(rebuilt) == 1
    summary = rebuilt[0]
    assert summary.id == "projection_run_summary"
    assert summary.name == "run_summary"
    assert summary.source_event_sequence == 4
    assert summary.data["event_type_counts"] == {
        "impulse.accepted": 1,
        "process.claimed": 3,
    }
    assert summary.data["impulse_count"] == 2
    assert summary.data["process_status_counts"] == {
        "cancelled": 1,
        "running": 1,
        "succeeded": 1,
        "timed_out": 1,
    }
    assert summary.data["resource_accounting"]["reaction_bytes"] == 3
    assert summary.data["resource_accounting"]["process_attempts"] == 3
    assert summary.data["resource_accounting"]["bridge_delivery_count"] == 0
    assert await backend.get_projection(
        run_id=impulse.run_id,
        name="run_summary",
    ) == summary

    delivery = BridgeDelivery(
        id="bridge_conformance",
        run_id=impulse.run_id,
        idempotency_key="bridge:conformance",
        source=RunRef(runtime=runtime, run_id=impulse.run_id),
        target=RunRef(runtime=RuntimeRef(id="target"), run_id="run_target"),
        impulse=impulse,
        event_ref=EventRef(runtime=runtime, run_id=impulse.run_id, sequence=1),
        budget=RuntimeBudget(runtime_hops=1, impulse_count=1),
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
        run_id=impulse.run_id,
        delivery_id=delivery.id,
    ) == delivery
    assert await backend.list_outbox_deliveries(
        run_id=impulse.run_id,
        status=BridgeDeliveryStatus.pending,
    ) == [delivery]
    assert len(
        await backend.list_inbox_deliveries(
            run_id="run_target",
            status=BridgeDeliveryStatus.imported,
        )
    ) == 1

    commanded_delivery = delivery.model_copy(
        update={
            "id": "bridge_commanded",
            "idempotency_key": "bridge:commanded",
        }
    )
    enqueue_command = RuntimeCommand(
        run_id=commanded_delivery.run_id,
        command_type="bridge.outbox.enqueue",
        idempotency_key=commanded_delivery.idempotency_key,
    )
    enqueue_submission = await backend.enqueue_outbox_delivery(
        commanded_delivery,
        enqueue_command,
        events=[
            RuntimeEvent(
                run_id=commanded_delivery.run_id,
                impulse_id=commanded_delivery.impulse.id,
                event_type="bridge.outbox.enqueued",
            )
        ],
    )
    enqueue_replay = await backend.enqueue_outbox_delivery(
        commanded_delivery.model_copy(update={"metadata": {"changed": True}}),
        enqueue_command.model_copy(update={"id": "command_bridge_enqueue_replay"}),
        events=[],
    )
    assert await backend.get_outbox_delivery(
        run_id=commanded_delivery.run_id,
        delivery_id=commanded_delivery.id,
    ) == commanded_delivery
    assert not enqueue_submission.replayed
    assert enqueue_replay.replayed

    imported_impulse = impulse.model_copy(update={"run_id": "run_target"})
    imported_delivery = commanded_delivery.model_copy(
        update={
            "run_id": "run_target",
            "impulse": imported_impulse,
            "status": BridgeDeliveryStatus.imported,
            "attempts": 1,
        }
    )
    import_command = RuntimeCommand(
        run_id=imported_delivery.run_id,
        command_type="bridge.inbox.import",
        idempotency_key="bridge:commanded:import",
    )
    import_submission = await backend.import_inbox_delivery(
        imported_delivery,
        imported_impulse,
        import_command,
        events=[
            RuntimeEvent(
                run_id=imported_delivery.run_id,
                impulse_id=imported_impulse.id,
                event_type="bridge.inbox.imported",
            )
        ],
    )
    assert await backend.get_inbox_delivery(
        run_id=imported_delivery.run_id,
        delivery_id=imported_delivery.id,
    ) == imported_delivery
    assert not import_submission.replayed

    delivered_delivery = commanded_delivery.model_copy(
        update={
            "status": BridgeDeliveryStatus.delivered,
            "attempts": 1,
        }
    )
    deliver_command = RuntimeCommand(
        run_id=delivered_delivery.run_id,
        command_type="bridge.outbox.deliver",
        idempotency_key="bridge:commanded:deliver",
    )
    deliver_submission = await backend.deliver_outbox_delivery(
        delivered_delivery,
        deliver_command,
        events=[
            RuntimeEvent(
                run_id=delivered_delivery.run_id,
                impulse_id=delivered_delivery.impulse.id,
                event_type="bridge.outbox.delivered",
            )
        ],
    )
    assert await backend.get_outbox_delivery(
        run_id=delivered_delivery.run_id,
        delivery_id=delivered_delivery.id,
    ) == delivered_delivery
    assert not deliver_submission.replayed
