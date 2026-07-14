from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from fala.adapters import EffectorRunRequest
from fala.driver import close_delegations, enqueue_fala_runtime_process, run_until_idle
from fala.errors import FalaBudgetExceeded
from fala.models import EffectorAdapterSpec
from fala.runtime_backend import (
    BridgeDelivery,
    ProcessStatus,
    RunStatus,
    DelegationPolicy,
    Impulse,
    Process,
    Projection,
    Run,
    RunRef,
    RuntimeBackendService,
    RuntimeBudget,
    RuntimePool,
    RuntimeRef,
)


async def _completed_process(
    service: RuntimeBackendService,
    *,
    run_id: str,
    process_id: str,
    impulse_id: str,
    worker_id: str = "worker",
) -> Process:
    await service.schedule_process(
        Process(
            id=process_id,
            run_id=run_id,
            impulse_id=impulse_id,
            process_type="score",
            status=ProcessStatus.ready,
        ),
        idempotency_key=f"process.schedule:{process_id}",
    )
    claimed = await service.claim_next_ready_process(worker_id=worker_id, run_id=run_id)
    assert claimed is not None and claimed.id == process_id
    stored, _ = await service.complete_process(
        run_id=run_id,
        process_id=process_id,
        output={},
        idempotency_key=f"process.complete:{process_id}",
        actor=worker_id,
    )
    return stored


async def _delegation_source(
    service: RuntimeBackendService,
    *,
    run_id: str,
    target_uri: str,
    budget: RuntimeBudget,
    targets: dict[str, str],
) -> None:
    """Create a run with one ready fala_runtime process per ``targets`` entry.

    ``targets`` maps process id -> child target_run_id.
    """
    await service.create_run(Run(id=run_id), idempotency_key="run.create")
    await service.save_runtime_pool(
        RuntimePool(
            id="target_pool",
            runtimes=[RuntimeRef(id="target", uri=target_uri)],
            impulse_types=["case"],
        )
    )
    await service.save_delegation_policy(
        DelegationPolicy(
            id="delegate_cases",
            pool_id="target_pool",
            impulse_types=["case"],
            budget=budget,
        )
    )
    for index, (process_id, target_run_id) in enumerate(targets.items()):
        impulse_id = f"impulse_{process_id}"
        await service.accept_impulse(
            Impulse(
                id=impulse_id,
                run_id=run_id,
                impulse_type="case",
                payload={"claim": f"DELEGATE-{index}"},
            ),
            idempotency_key=f"impulse.accept:{impulse_id}",
        )
        await service.schedule_process(
            Process(
                id=process_id,
                run_id=run_id,
                impulse_id=impulse_id,
                process_type="fala_runtime",
                status=ProcessStatus.ready,
                input={
                    "adapter": {
                        "kind": "fala_runtime",
                        "runtime_ref": "target_pool",
                    },
                    "config": {"target_run_id": target_run_id},
                },
            ),
            idempotency_key=f"process.schedule:{process_id}",
        )


class TestRunBoundaryAssociation(unittest.TestCase):
    def test_observe_run_unknown_run_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = RuntimeBackendService.sqlite(Path(tmp) / "runtime.db")

            async def run() -> None:
                with self.assertRaisesRegex(ValueError, "Unknown run"):
                    await service.observe_run(run_id="missing")

            asyncio.run(run())

    def test_observe_run_derives_status_counts_and_watermark(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = RuntimeBackendService.sqlite(Path(tmp) / "runtime.db")

            async def run() -> None:
                await service.create_run(Run(id="run_obs"), idempotency_key="run.create")
                empty = await service.observe_run(run_id="run_obs")
                self.assertEqual(empty.status, RunStatus.created)
                self.assertEqual(empty.derived_status, RunStatus.created)
                self.assertEqual(empty.process_status_counts, {})

                await service.accept_impulse(
                    Impulse(id="impulse_obs", run_id="run_obs", impulse_type="case"),
                    idempotency_key="impulse.accept:impulse_obs",
                )
                await _completed_process(
                    service,
                    run_id="run_obs",
                    process_id="process_obs",
                    impulse_id="impulse_obs",
                )

                boundary = await service.observe_run(run_id="run_obs")
                self.assertEqual(boundary.status, RunStatus.created)
                self.assertEqual(boundary.derived_status, RunStatus.completed)
                self.assertEqual(boundary.process_status_counts, {"succeeded": 1})
                events = await service.backend.list_events(run_id="run_obs")
                self.assertEqual(boundary.event_watermark, events[-1].sequence)

            asyncio.run(run())

    def test_observe_run_derives_failed_and_waiting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = RuntimeBackendService.sqlite(Path(tmp) / "runtime.db")

            async def run() -> None:
                for run_id in ("run_failed", "run_waiting"):
                    await service.create_run(
                        Run(id=run_id), idempotency_key="run.create"
                    )
                    await service.accept_impulse(
                        Impulse(id=f"impulse_{run_id}", run_id=run_id, impulse_type="case"),
                        idempotency_key="impulse.accept",
                    )
                    await service.schedule_process(
                        Process(
                            id=f"process_{run_id}",
                            run_id=run_id,
                            impulse_id=f"impulse_{run_id}",
                            process_type="score",
                            status=ProcessStatus.ready,
                        ),
                        idempotency_key="process.schedule",
                    )
                    claimed = await service.claim_next_ready_process(
                        worker_id="worker", run_id=run_id
                    )
                    assert claimed is not None

                await service.fail_process(
                    run_id="run_failed",
                    process_id="process_run_failed",
                    error={"message": "boom"},
                    idempotency_key="process.fail",
                    actor="worker",
                )
                await service.wait_process(
                    run_id="run_waiting",
                    process_id="process_run_waiting",
                    idempotency_key="process.wait",
                    actor="worker",
                )

                failed = await service.observe_run(run_id="run_failed")
                self.assertEqual(failed.derived_status, RunStatus.failed)
                self.assertEqual(failed.process_status_counts, {"failed": 1})
                waiting = await service.observe_run(run_id="run_waiting")
                self.assertEqual(waiting.derived_status, RunStatus.waiting)

            asyncio.run(run())

    def test_observe_run_stored_terminal_status_wins(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = RuntimeBackendService.sqlite(Path(tmp) / "runtime.db")

            async def run() -> None:
                await service.create_run(Run(id="run_term"), idempotency_key="run.create")
                await service.accept_impulse(
                    Impulse(id="impulse_term", run_id="run_term", impulse_type="case"),
                    idempotency_key="impulse.accept:impulse_term",
                )
                await _completed_process(
                    service,
                    run_id="run_term",
                    process_id="process_term",
                    impulse_id="impulse_term",
                )
                await service.set_run_status(
                    run_id="run_term",
                    status=RunStatus.cancelled,
                    idempotency_key="run.cancelled",
                )
                boundary = await service.observe_run(run_id="run_term")
                self.assertEqual(boundary.status, RunStatus.cancelled)
                self.assertEqual(boundary.derived_status, RunStatus.cancelled)

            asyncio.run(run())


class TestProjectionStaleness(unittest.TestCase):
    def test_get_projection_stamps_staleness_from_event_watermark(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = RuntimeBackendService.sqlite(Path(tmp) / "runtime.db")

            async def run() -> None:
                await service.create_run(Run(id="run_proj"), idempotency_key="run.create")
                events = await service.backend.list_events(run_id="run_proj")
                await service.save_projection(
                    Projection(
                        run_id="run_proj",
                        name="summary",
                        data={"note": "fresh"},
                        source_event_sequence=events[-1].sequence,
                    ),
                    idempotency_key="projection.save:summary",
                )
                fresh = await service.get_projection(run_id="run_proj", name="summary")
                assert fresh is not None
                self.assertFalse(fresh.stale)

                await service.accept_impulse(
                    Impulse(id="impulse_proj", run_id="run_proj", impulse_type="case"),
                    idempotency_key="impulse.accept:impulse_proj",
                )
                stale = await service.get_projection(run_id="run_proj", name="summary")
                assert stale is not None
                self.assertTrue(stale.stale)
                self.assertIsNone(
                    await service.get_projection(run_id="run_proj", name="missing")
                )

            asyncio.run(run())


class TestSpawnedRunsBudget(unittest.TestCase):
    def test_enqueue_respects_spawned_runs_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = RuntimeBackendService.sqlite(Path(tmp) / "source.db")

            async def run() -> None:
                await _delegation_source(
                    source,
                    run_id="run_budget",
                    target_uri=f"sqlite://{Path(tmp) / 'target.db'}",
                    budget=RuntimeBudget(
                        runtime_hops=1,
                        impulse_count=1,
                        attempts=1,
                        spawned_runs=1,
                    ),
                    targets={"process_a": "run_child_a", "process_b": "run_child_b"},
                )

                claimed_a = await source.claim_next_ready_process(
                    worker_id="worker", run_id="run_budget"
                )
                assert claimed_a is not None
                first = await enqueue_fala_runtime_process(
                    service=source,
                    process=claimed_a,
                    request=_delegation_request(claimed_a),
                    actor="worker",
                )
                self.assertTrue(first.waiting)
                self.assertEqual(first.output["target_run_id"], "run_child_a")

                # Idempotent re-enqueue of an already-spawned target is free.
                replay = await enqueue_fala_runtime_process(
                    service=source,
                    process=claimed_a,
                    request=_delegation_request(claimed_a),
                    actor="worker",
                )
                self.assertEqual(replay.output["delivery_id"], first.output["delivery_id"])

                await source.wait_process(
                    run_id="run_budget",
                    process_id=claimed_a.id,
                    output=first.output,
                    idempotency_key=f"process.wait:{claimed_a.id}:{claimed_a.attempt}",
                    actor="worker",
                )
                claimed_b = await source.claim_next_ready_process(
                    worker_id="worker", run_id="run_budget"
                )
                assert claimed_b is not None
                with self.assertRaises(FalaBudgetExceeded) as caught:
                    await enqueue_fala_runtime_process(
                        service=source,
                        process=claimed_b,
                        request=_delegation_request(claimed_b),
                        actor="worker",
                    )
                self.assertEqual(caught.exception.details["spawned_runs"], 1)
                self.assertEqual(
                    caught.exception.details["spawned_run_ids"], ["run_child_a"]
                )

            asyncio.run(run())

    def test_retry_reenqueue_reuses_spawned_target_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = RuntimeBackendService.sqlite(Path(tmp) / "source.db")

            async def run() -> None:
                await _delegation_source(
                    source,
                    run_id="run_retry",
                    target_uri=f"sqlite://{Path(tmp) / 'target.db'}",
                    budget=RuntimeBudget(spawned_runs=1),
                    targets={"process_a": "run_child_a"},
                )
                claimed = await source.claim_next_ready_process(
                    worker_id="worker", run_id="run_retry"
                )
                assert claimed is not None
                # No target_run_id in config: the driver mints the child run id
                # on the first attempt and must reuse it on retries.
                request = EffectorRunRequest(
                    process_id=claimed.id,
                    impulse_id=claimed.impulse_id,
                    adapter=EffectorAdapterSpec.model_validate(
                        claimed.input["adapter"]
                    ),
                    input={},
                    config={},
                )
                first = await enqueue_fala_runtime_process(
                    service=source,
                    process=claimed,
                    request=request,
                    actor="worker",
                )
                retry = claimed.model_copy(update={"attempt": claimed.attempt + 1})
                second = await enqueue_fala_runtime_process(
                    service=source,
                    process=retry,
                    request=request,
                    actor="worker",
                )
                self.assertEqual(
                    second.output["delivery_id"], first.output["delivery_id"]
                )
                self.assertEqual(
                    second.output["target_run_id"], first.output["target_run_id"]
                )

            asyncio.run(run())


class TestBridgeHopBudget(unittest.TestCase):
    def test_enqueue_rejects_exhausted_hop_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = RuntimeBackendService.sqlite(Path(tmp) / "source.db")

            async def run() -> None:
                await source.create_run(
                    Run(id="run_hops"), idempotency_key="run.create"
                )
                impulse = Impulse(
                    id="impulse_hops",
                    run_id="run_hops",
                    impulse_type="case",
                )
                await source.accept_impulse(
                    impulse,
                    idempotency_key="impulse.accept:impulse_hops",
                )
                runtime_ref = RuntimeRef(id="peer", uri="sqlite://peer.db")
                delivery = BridgeDelivery(
                    run_id="run_hops",
                    source=RunRef(runtime=runtime_ref, run_id="run_hops"),
                    target=RunRef(runtime=runtime_ref, run_id="run_child"),
                    impulse=impulse,
                    budget=RuntimeBudget(runtime_hops=0),
                )
                with self.assertRaises(FalaBudgetExceeded) as caught:
                    await source.enqueue_bridge_delivery(delivery, actor="worker")
                self.assertEqual(caught.exception.details["runtime_hops"], 0)

            asyncio.run(run())


def _delegation_request(process: Process) -> EffectorRunRequest:
    return EffectorRunRequest(
        process_id=process.id,
        impulse_id=process.impulse_id,
        adapter=EffectorAdapterSpec.model_validate(process.input["adapter"]),
        input={},
        config=dict(process.input.get("config") or {}),
    )


class TestCloseDelegations(unittest.TestCase):
    def _drive_to_waiting(
        self, source: RuntimeBackendService, run_id: str
    ) -> Process:
        outcome = asyncio.run(
            run_until_idle(source, worker_id="worker", run_id=run_id)
        )
        self.assertEqual(outcome.stopped_reason, "idle")
        self.assertEqual(len(outcome.waiting), 1)
        return outcome.waiting[0]

    def test_close_delegations_completes_parent_from_child_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = RuntimeBackendService.sqlite(Path(tmp) / "source.db")
            target = RuntimeBackendService.sqlite(Path(tmp) / "target.db")

            async def setup() -> None:
                await target.create_run(Run(id="run_child"), idempotency_key="run.create")
                await _delegation_source(
                    source,
                    run_id="run_parent",
                    target_uri=f"sqlite://{Path(tmp) / 'target.db'}",
                    budget=RuntimeBudget(runtime_hops=1, impulse_count=1, attempts=1),
                    targets={"process_delegate": "run_child"},
                )

            asyncio.run(setup())
            waiting = self._drive_to_waiting(source, "run_parent")
            self.assertEqual(waiting.output["status"], "submitted")
            self.assertEqual(waiting.output["target_run_id"], "run_child")

            async def close(actor: str) -> list[Process]:
                return await close_delegations(
                    source,
                    run_id="run_parent",
                    target_service=target,
                    actor=actor,
                )

            # Child run still open: nothing to close yet.
            self.assertEqual(asyncio.run(close("closer")), [])

            async def finish_child() -> None:
                await source.deliver_bridge_delivery(
                    run_id="run_parent",
                    delivery_id=waiting.output["delivery_id"],
                    target=target,
                    idempotency_key=f"bridge.deliver:{waiting.output['delivery_id']}",
                )
                await target.accept_impulse(
                    Impulse(id="impulse_child", run_id="run_child", impulse_type="case"),
                    idempotency_key="impulse.accept:impulse_child",
                )
                await _completed_process(
                    target,
                    run_id="run_child",
                    process_id="process_child",
                    impulse_id="impulse_child",
                )

            asyncio.run(finish_child())
            closed = asyncio.run(close("closer"))
            self.assertEqual([process.id for process in closed], ["process_delegate"])
            self.assertEqual(closed[0].status, ProcessStatus.succeeded)
            boundary = closed[0].output["run.boundary"]
            self.assertEqual(boundary["derived_status"], "completed")
            self.assertEqual(boundary["process_status_counts"], {"succeeded": 1})
            # The delegation envelope survives alongside the boundary.
            self.assertEqual(
                closed[0].output["delivery_id"], waiting.output["delivery_id"]
            )
            # Nothing left waiting: closing again is a no-op.
            self.assertEqual(asyncio.run(close("closer")), [])

    def test_close_delegations_fails_parent_when_child_run_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = RuntimeBackendService.sqlite(Path(tmp) / "source.db")
            target = RuntimeBackendService.sqlite(Path(tmp) / "target.db")

            async def setup() -> None:
                await target.create_run(Run(id="run_child"), idempotency_key="run.create")
                await _delegation_source(
                    source,
                    run_id="run_parent",
                    target_uri=f"sqlite://{Path(tmp) / 'target.db'}",
                    budget=RuntimeBudget(runtime_hops=1, impulse_count=1, attempts=1),
                    targets={"process_delegate": "run_child"},
                )

            asyncio.run(setup())
            self._drive_to_waiting(source, "run_parent")

            async def fail_child_and_close() -> list[Process]:
                await target.set_run_status(
                    run_id="run_child",
                    status=RunStatus.failed,
                    idempotency_key="run.failed",
                    reason="child exploded",
                )
                return await close_delegations(
                    source,
                    run_id="run_parent",
                    target_service=target,
                    actor="closer",
                )

            closed = asyncio.run(fail_child_and_close())
            self.assertEqual(len(closed), 1)
            self.assertEqual(closed[0].status, ProcessStatus.failed)
            self.assertEqual(closed[0].error["type"], "DelegatedRunNotCompleted")
            self.assertIn("run_child", closed[0].error["message"])
            self.assertEqual(closed[0].error["run.boundary"]["status"], "failed")


if __name__ == "__main__":
    unittest.main()
