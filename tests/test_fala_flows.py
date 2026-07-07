from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from fala.flows import (
    FlowAdvance,
    FlowInstance,
    advance_flow,
    advance_flow_for_process,
    find_flow_step_process,
    flow_step_processes,
    instantiate_flow,
)
from fala.models import CarrierAdapterSpec, CarrierFlowSpec, CarrierFlowStepSpec
from fala.runtime_backend import (
    CarrierProcessStatus,
    Process,
    Run,
    RuntimeBackendService,
)


def _flow_double(request) -> dict:
    return {"value": request.input["value"] * 2}


def _python_step(
    step_id: str,
    *,
    needs: list[str] | None = None,
    timeout_seconds: float | None = None,
    config: dict | None = None,
) -> CarrierFlowStepSpec:
    return CarrierFlowStepSpec(
        id=step_id,
        capability="python_function",
        adapter=CarrierAdapterSpec(
            kind="python_function",
            ref="tests.test_fala_flows._flow_double",
        ),
        needs=needs or [],
        timeout_seconds=timeout_seconds,
        config=config or {},
    )


def _two_step_flow() -> CarrierFlowSpec:
    return CarrierFlowSpec(
        id="flow_pair",
        steps=[
            _python_step("first"),
            _python_step("second", needs=["first"]),
        ],
    )


def _diamond_flow() -> CarrierFlowSpec:
    # a -> {b, c} -> d. ``d`` directly needs only b and c; ``a`` reaches it purely
    # transitively, which is exactly what ``accumulate_upstream_artifacts`` exposes.
    return CarrierFlowSpec(
        id="flow_diamond",
        accumulate_upstream_artifacts=True,
        steps=[
            _python_step("a"),
            _python_step("b", needs=["a"]),
            _python_step("c", needs=["a"]),
            _python_step("d", needs=["b", "c"]),
        ],
    )


async def _service(root: Path, run_id: str) -> RuntimeBackendService:
    service = RuntimeBackendService.sqlite(root / "state.sqlite")
    await service.create_run(Run(id=run_id), idempotency_key=f"{run_id}:create")
    return service


class FalaFlowInstantiationTests(unittest.TestCase):
    def test_instantiate_flow_sets_statuses_markers_and_timeouts(self) -> None:
        flow = CarrierFlowSpec(
            id="flow_main",
            steps=[
                _python_step("root", timeout_seconds=7.5, config={"base": 1}),
                _python_step("dependent", needs=["root"]),
                CarrierFlowStepSpec(
                    id="approval",
                    capability="manual_gate",
                    adapter=CarrierAdapterSpec(kind="manual_gate"),
                    needs=["root"],
                    timeout_seconds=30.0,
                ),
            ],
        )

        async def scenario(root: Path) -> FlowInstance:
            service = await _service(root, "run_flow")
            return await instantiate_flow(
                service,
                run_id="run_flow",
                flow=flow,
                step_inputs={"root": {"value": 3}},
                step_configs={"root": {"extra": 2}},
                max_attempts=2,
                priority=5,
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            instance = asyncio.run(scenario(Path(tmp_dir)))

        self.assertEqual(instance.flow_id, "run_flow:flow_main")
        by_step = {
            process.metadata["flow"]["step_id"]: process
            for process in instance.processes
        }
        self.assertEqual(set(by_step), {"root", "dependent", "approval"})

        root_process = by_step["root"]
        self.assertEqual(root_process.id, "run_flow:flow_main:root")
        self.assertEqual(root_process.status, CarrierProcessStatus.ready)
        self.assertEqual(root_process.process_type, "python_function")
        self.assertEqual(root_process.priority, 5)
        self.assertEqual(root_process.max_attempts, 2)
        self.assertEqual(root_process.input["value"], 3)
        self.assertEqual(root_process.input["config"], {"base": 1, "extra": 2})
        self.assertEqual(root_process.input["adapter"]["timeout_seconds"], 7.5)
        self.assertEqual(
            root_process.metadata["flow"],
            {
                "flow_id": "run_flow:flow_main",
                "flow_spec_id": "flow_main",
                "step_id": "root",
                "needs": [],
                "accumulate_upstream_artifacts": False,
            },
        )

        dependent = by_step["dependent"]
        self.assertEqual(dependent.status, CarrierProcessStatus.pending)
        self.assertEqual(dependent.metadata["flow"]["needs"], ["root"])

        approval = by_step["approval"]
        self.assertEqual(approval.status, CarrierProcessStatus.pending)
        self.assertIsNone(approval.input["adapter"]["timeout_seconds"])

    def test_instantiate_flow_replays_idempotently(self) -> None:
        async def scenario(root: Path) -> tuple[FlowInstance, FlowInstance, int]:
            service = await _service(root, "run_replay")
            first = await instantiate_flow(
                service,
                run_id="run_replay",
                flow=_two_step_flow(),
                step_inputs={"first": {"value": 1}},
            )
            second = await instantiate_flow(
                service,
                run_id="run_replay",
                flow=_two_step_flow(),
                step_inputs={"first": {"value": 1}},
            )
            processes = await service.list_processes(run_id="run_replay")
            return first, second, len(processes)

        with tempfile.TemporaryDirectory() as tmp_dir:
            first, second, count = asyncio.run(scenario(Path(tmp_dir)))

        self.assertEqual(count, 2)
        self.assertEqual(
            [process.id for process in first.processes],
            [process.id for process in second.processes],
        )

    def test_instantiate_flow_rejects_unknown_and_reserved_keys(self) -> None:
        async def scenario(root: Path, **kwargs) -> None:
            service = await _service(root, "run_invalid")
            await instantiate_flow(
                service,
                run_id="run_invalid",
                flow=_two_step_flow(),
                **kwargs,
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            with self.assertRaises(ValueError) as unknown_input:
                asyncio.run(scenario(root / "a", step_inputs={"missing": {"value": 1}}))
            self.assertIn("unknown flow steps", str(unknown_input.exception))
            with self.assertRaises(ValueError) as unknown_config:
                asyncio.run(scenario(root / "b", step_configs={"missing": {"flag": True}}))
            self.assertIn("unknown flow steps", str(unknown_config.exception))
            for reserved_key in ("adapter", "config", "needs", "upstream_artifacts"):
                with self.assertRaises(ValueError) as reserved:
                    asyncio.run(
                        scenario(
                            root / f"c_{reserved_key}",
                            step_inputs={"first": {reserved_key: {}}},
                        )
                    )
                self.assertIn("reserved keys", str(reserved.exception))


class FalaReadyProcessTests(unittest.TestCase):
    def test_ready_process_requires_pending_and_injects_input(self) -> None:
        async def scenario(root: Path) -> tuple[Process, Process, str]:
            service = await _service(root, "run_ready")
            await service.schedule_process(
                Process(
                    id="process_pending",
                    run_id="run_ready",
                    process_type="python_function",
                    status=CarrierProcessStatus.pending,
                    input={"value": 1},
                ),
                idempotency_key="run_ready:process.schedule:process_pending",
            )
            readied, _ = await service.ready_process(
                run_id="run_ready",
                process_id="process_pending",
                input={"value": 1, "needs": {"first": {"value": 2}}},
                idempotency_key="run_ready:process.ready:process_pending",
            )
            replayed, submission = await service.ready_process(
                run_id="run_ready",
                process_id="process_pending",
                input={"value": 99},
                idempotency_key="run_ready:process.ready:process_pending",
            )
            self.assertTrue(submission.replayed)
            error = ""
            try:
                await service.ready_process(
                    run_id="run_ready",
                    process_id="process_pending",
                    idempotency_key="run_ready:process.ready:process_pending:again",
                )
            except ValueError as exc:
                error = str(exc)
            return readied, replayed, error

        with tempfile.TemporaryDirectory() as tmp_dir:
            readied, replayed, error = asyncio.run(scenario(Path(tmp_dir)))

        self.assertEqual(readied.status, CarrierProcessStatus.ready)
        self.assertEqual(readied.input["needs"], {"first": {"value": 2}})
        self.assertEqual(replayed.input["needs"], {"first": {"value": 2}})
        self.assertIn("cannot become ready", error)

    def test_ready_process_unknown_process_raises(self) -> None:
        async def scenario(root: Path) -> None:
            service = await _service(root, "run_missing")
            await service.ready_process(
                run_id="run_missing",
                process_id="process_missing",
                idempotency_key="run_missing:process.ready:process_missing",
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            with self.assertRaises(ValueError) as ctx:
                asyncio.run(scenario(Path(tmp_dir)))
            self.assertIn("Unknown process", str(ctx.exception))


class FalaAdvanceFlowTests(unittest.TestCase):
    def test_advance_flow_readies_step_with_needs_output_injected(self) -> None:
        async def scenario(root: Path) -> tuple[FlowAdvance, Process]:
            service = await _service(root, "run_advance")
            instance = await instantiate_flow(
                service,
                run_id="run_advance",
                flow=_two_step_flow(),
                step_inputs={"first": {"value": 4}},
            )
            first = instance.processes[0]
            claimed = await service.claim_next_ready_process(worker_id="tester")
            assert claimed is not None and claimed.id == first.id
            await service.complete_process(
                run_id="run_advance",
                process_id=first.id,
                output={"value": 8},
                idempotency_key=f"run_advance:process.complete:{first.id}:1",
            )
            advance = await advance_flow(
                service,
                run_id="run_advance",
                flow_id=instance.flow_id,
            )
            second = await service.backend.get_process(
                run_id="run_advance",
                process_id="run_advance:flow_pair:second",
            )
            assert second is not None
            return advance, second

        with tempfile.TemporaryDirectory() as tmp_dir:
            advance, second = asyncio.run(scenario(Path(tmp_dir)))

        self.assertEqual(len(advance.readied), 1)
        self.assertEqual(advance.blocked, [])
        self.assertEqual(second.status, CarrierProcessStatus.ready)
        self.assertEqual(second.input["needs"]["first"]["value"], 8)

    def test_advance_flow_injects_transitive_upstream_artifacts(self) -> None:
        async def scenario(root: Path) -> Process:
            service = await _service(root, "run_diamond")
            instance = await instantiate_flow(
                service,
                run_id="run_diamond",
                flow=_diamond_flow(),
            )
            flow_id = instance.flow_id

            async def claim_and_complete() -> None:
                claimed = await service.claim_next_ready_process(worker_id="tester")
                assert claimed is not None
                step_id = claimed.metadata["flow"]["step_id"]
                await service.complete_process(
                    run_id="run_diamond",
                    process_id=claimed.id,
                    output={
                        "value": 1,
                        "artifacts": [{"kind": f"{step_id}-art"}],
                    },
                    idempotency_key=(
                        f"run_diamond:process.complete:{claimed.id}:1"
                    ),
                )

            # a runs, then advancing readies b and c, then d.
            await claim_and_complete()  # a
            await advance_flow(service, run_id="run_diamond", flow_id=flow_id)
            await claim_and_complete()  # b or c
            await claim_and_complete()  # the other of b/c
            await advance_flow(service, run_id="run_diamond", flow_id=flow_id)

            processed = await service.backend.get_process(
                run_id="run_diamond",
                process_id="run_diamond:flow_diamond:d",
            )
            assert processed is not None
            return processed

        with tempfile.TemporaryDirectory() as tmp_dir:
            d = asyncio.run(scenario(Path(tmp_dir)))

        self.assertEqual(d.status, CarrierProcessStatus.ready)
        # a is NOT a direct need of d -- yet its artifact is visible transitively,
        # in topological order (deepest ancestor first).
        self.assertEqual(
            d.input["upstream_artifacts"],
            [{"kind": "a-art"}, {"kind": "b-art"}, {"kind": "c-art"}],
        )
        # Direct-needs injection is untouched: d sees b and c fully, but not a.
        self.assertEqual(set(d.input["needs"]), {"b", "c"})

    def test_advance_flow_omits_upstream_artifacts_when_flag_off(self) -> None:
        async def scenario(root: Path) -> Process:
            service = await _service(root, "run_no_accum")
            instance = await instantiate_flow(
                service,
                run_id="run_no_accum",
                flow=_two_step_flow(),
                step_inputs={"first": {"value": 4}},
            )
            first = instance.processes[0]
            claimed = await service.claim_next_ready_process(worker_id="tester")
            assert claimed is not None and claimed.id == first.id
            await service.complete_process(
                run_id="run_no_accum",
                process_id=first.id,
                output={"value": 8, "artifacts": [{"kind": "first-art"}]},
                idempotency_key=f"run_no_accum:process.complete:{first.id}:1",
            )
            await advance_flow(
                service,
                run_id="run_no_accum",
                flow_id=instance.flow_id,
            )
            second = await service.backend.get_process(
                run_id="run_no_accum",
                process_id="run_no_accum:flow_pair:second",
            )
            assert second is not None
            return second

        with tempfile.TemporaryDirectory() as tmp_dir:
            second = asyncio.run(scenario(Path(tmp_dir)))

        self.assertEqual(second.status, CarrierProcessStatus.ready)
        self.assertNotIn("upstream_artifacts", second.input)

    def test_advance_flow_reports_unmet_and_dead_needs_without_readying(self) -> None:
        async def scenario(root: Path) -> tuple[FlowAdvance, FlowAdvance, Process]:
            service = await _service(root, "run_blocked")
            instance = await instantiate_flow(
                service,
                run_id="run_blocked",
                flow=_two_step_flow(),
                step_inputs={"first": {"value": 4}},
            )
            unmet = await advance_flow(
                service,
                run_id="run_blocked",
                flow_id=instance.flow_id,
            )
            claimed = await service.claim_next_ready_process(worker_id="tester")
            assert claimed is not None
            await service.cancel_process(
                run_id="run_blocked",
                process_id=claimed.id,
                idempotency_key=f"run_blocked:process.cancel:{claimed.id}",
            )
            dead = await advance_flow(
                service,
                run_id="run_blocked",
                flow_id=instance.flow_id,
            )
            second = await service.backend.get_process(
                run_id="run_blocked",
                process_id="run_blocked:flow_pair:second",
            )
            assert second is not None
            return unmet, dead, second

        with tempfile.TemporaryDirectory() as tmp_dir:
            unmet, dead, second = asyncio.run(scenario(Path(tmp_dir)))

        self.assertEqual(unmet.readied, [])
        self.assertEqual(len(unmet.blocked), 1)
        self.assertEqual(unmet.blocked[0].step_id, "second")
        self.assertEqual(unmet.blocked[0].unmet, ["first"])
        self.assertEqual(unmet.blocked[0].dead, [])

        self.assertEqual(dead.readied, [])
        self.assertEqual(len(dead.blocked), 1)
        self.assertEqual(dead.blocked[0].dead, ["first"])
        self.assertEqual(second.status, CarrierProcessStatus.pending)

    def test_advance_flow_treats_exhausted_failed_need_as_dead(self) -> None:
        async def scenario(root: Path) -> FlowAdvance:
            service = await _service(root, "run_failed")
            instance = await instantiate_flow(
                service,
                run_id="run_failed",
                flow=_two_step_flow(),
                step_inputs={"first": {"value": 4}},
            )
            claimed = await service.claim_next_ready_process(worker_id="tester")
            assert claimed is not None
            await service.fail_process(
                run_id="run_failed",
                process_id=claimed.id,
                error={"type": "RuntimeError", "message": "boom"},
                idempotency_key=f"run_failed:process.fail:{claimed.id}:1",
            )
            return await advance_flow(
                service,
                run_id="run_failed",
                flow_id=instance.flow_id,
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            advance = asyncio.run(scenario(Path(tmp_dir)))

        self.assertEqual(advance.readied, [])
        self.assertEqual(len(advance.blocked), 1)
        self.assertEqual(advance.blocked[0].dead, ["first"])

    def test_advance_flow_unknown_need_fails_closed(self) -> None:
        async def scenario(root: Path) -> None:
            service = await _service(root, "run_orphan")
            await service.schedule_process(
                Process(
                    id="flow_x:child",
                    run_id="run_orphan",
                    process_type="python_function",
                    status=CarrierProcessStatus.pending,
                    input={
                        "adapter": {
                            "kind": "python_function",
                            "ref": "tests.test_fala_flows._flow_double",
                        },
                    },
                    metadata={
                        "flow": {
                            "flow_id": "flow_x",
                            "flow_spec_id": "flow_x",
                            "step_id": "child",
                            "needs": ["ghost"],
                        }
                    },
                ),
                idempotency_key="run_orphan:process.schedule:flow_x:child",
            )
            await advance_flow(service, run_id="run_orphan", flow_id="flow_x")

        with tempfile.TemporaryDirectory() as tmp_dir:
            with self.assertRaises(ValueError) as ctx:
                asyncio.run(scenario(Path(tmp_dir)))
            self.assertIn("unknown step", str(ctx.exception))

    def test_advance_flow_for_process_ignores_non_flow_processes(self) -> None:
        async def scenario(root: Path) -> FlowAdvance | None:
            service = await _service(root, "run_plain")
            await service.schedule_process(
                Process(
                    id="process_plain",
                    run_id="run_plain",
                    process_type="python_function",
                    status=CarrierProcessStatus.ready,
                    input={
                        "adapter": {
                            "kind": "python_function",
                            "ref": "tests.test_fala_flows._flow_double",
                        },
                        "value": 1,
                    },
                ),
                idempotency_key="run_plain:process.schedule:process_plain",
            )
            process = await service.backend.get_process(
                run_id="run_plain",
                process_id="process_plain",
            )
            assert process is not None
            return await advance_flow_for_process(service, process=process)

        with tempfile.TemporaryDirectory() as tmp_dir:
            self.assertIsNone(asyncio.run(scenario(Path(tmp_dir))))


class FalaFlowStepProcessLookupTests(unittest.TestCase):
    """Pure host-side selectors over a run's process list -- no service needed.

    These are the primitives a finished-run reader uses instead of re-deriving
    the ``{flow_id}:{step_id}`` process-id format: they read the same
    ``metadata['flow']`` marker :func:`advance_flow` uses to assemble members.
    """

    def _member(self, flow_id: str, step_id: str) -> Process:
        return Process(
            id=f"{flow_id}:{step_id}",
            run_id="run_lookup",
            process_type="python_function",
            status=CarrierProcessStatus.succeeded,
            metadata={"flow": {"flow_id": flow_id, "step_id": step_id}},
        )

    def test_flow_step_processes_keys_by_step_and_filters_foreign(self) -> None:
        wanted_first = self._member("flow_a", "first")
        wanted_second = self._member("flow_a", "second")
        other_flow = self._member("flow_b", "first")
        no_marker = Process(
            id="plain",
            run_id="run_lookup",
            process_type="python_function",
            status=CarrierProcessStatus.ready,
        )
        no_step_id = Process(
            id="bad",
            run_id="run_lookup",
            process_type="python_function",
            status=CarrierProcessStatus.ready,
            metadata={"flow": {"flow_id": "flow_a"}},
        )
        members = flow_step_processes(
            [wanted_first, other_flow, no_marker, no_step_id, wanted_second],
            "flow_a",
        )
        # Only flow_a's marked, step-identified members survive, keyed by step id.
        self.assertEqual(set(members), {"first", "second"})
        self.assertEqual(members["first"].id, "flow_a:first")
        self.assertEqual(members["second"].id, "flow_a:second")

    def test_flow_step_processes_last_of_a_kind_wins(self) -> None:
        first = self._member("flow_a", "dup")
        second = self._member("flow_a", "dup")
        members = flow_step_processes([first, second], "flow_a")
        self.assertIs(members["dup"], second)

    def test_find_flow_step_process_returns_match_or_none(self) -> None:
        member = self._member("flow_a", "report")
        self.assertIs(
            find_flow_step_process([member], flow_id="flow_a", step_id="report"),
            member,
        )
        self.assertIsNone(
            find_flow_step_process([member], flow_id="flow_a", step_id="missing")
        )
        self.assertIsNone(
            find_flow_step_process([member], flow_id="other_flow", step_id="report")
        )


if __name__ == "__main__":
    unittest.main()
