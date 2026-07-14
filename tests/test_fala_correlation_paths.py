from __future__ import annotations

import asyncio
import sys
import tempfile
import unittest
from pathlib import Path

import fala.correlation_paths as correlation_paths
from fala.correlation_paths import (
    PYTHON_COMMAND_PLACEHOLDER,
    CorrelationPathAdvance,
    CorrelationPathInstance,
    advance_correlation_path,
    advance_correlation_path_for_process,
    find_correlation_path_effector_process,
    correlation_path_effector_processes,
    instantiate_correlation_path,
)
from fala.sdk import INJECTED_INPUT_KEYS
from fala.models import EffectorAdapterSpec, CorrelationPathSpec, EffectorSpec
from fala.runtime_backend import (
    ProcessStatus,
    Process,
    Run,
    RuntimeBackendService,
)


def _correlation_path_double(request) -> dict:
    return {"value": request.input["value"] * 2}


def _python_effector(
    effector_id: str,
    *,
    conduction: list[str] | None = None,
    timeout_seconds: float | None = None,
    config: dict | None = None,
) -> EffectorSpec:
    return EffectorSpec(
        id=effector_id,
        capability="python_function",
        adapter=EffectorAdapterSpec(
            kind="python_function",
            ref="tests.test_fala_correlation_paths._correlation_path_double",
        ),
        conduction=conduction or [],
        timeout_seconds=timeout_seconds,
        config=config or {},
    )


def _two_effector_correlation_path() -> CorrelationPathSpec:
    return CorrelationPathSpec(
        id="correlation_path_pair",
        effectors=[
            _python_effector("first"),
            _python_effector("second", conduction=["first"]),
        ],
    )


def _diamond_correlation_path() -> CorrelationPathSpec:
    # a -> {b, c} -> d. ``d`` has direct conduction only from b and c; ``a`` reaches it purely
    # transitively, which is exactly what ``accumulate_upstream_reactions`` exposes.
    return CorrelationPathSpec(
        id="correlation_path_diamond",
        accumulate_upstream_reactions=True,
        effectors=[
            _python_effector("a"),
            _python_effector("b", conduction=["a"]),
            _python_effector("c", conduction=["a"]),
            _python_effector("d", conduction=["b", "c"]),
        ],
    )


async def _service(root: Path, run_id: str) -> RuntimeBackendService:
    service = RuntimeBackendService.sqlite(root / "state.sqlite")
    await service.create_run(Run(id=run_id), idempotency_key=f"{run_id}:create")
    return service


class FalaCorrelationPathInstantiationTests(unittest.TestCase):
    def test_instantiate_correlation_path_sets_statuses_markers_and_timeouts(self) -> None:
        correlation_path = CorrelationPathSpec(
            id="correlation_path_main",
            effectors=[
                _python_effector("root", timeout_seconds=7.5, config={"base": 1}),
                _python_effector("dependent", conduction=["root"]),
                EffectorSpec(
                    id="approval",
                    capability="manual_homeostat",
                    adapter=EffectorAdapterSpec(kind="manual_homeostat"),
                    conduction=["root"],
                    timeout_seconds=30.0,
                ),
            ],
        )

        async def scenario(root: Path) -> CorrelationPathInstance:
            service = await _service(root, "run_correlation_path")
            return await instantiate_correlation_path(
                service,
                run_id="run_correlation_path",
                correlation_path=correlation_path,
                effector_inputs={"root": {"value": 3}},
                effector_configs={"root": {"extra": 2}},
                max_attempts=2,
                priority=5,
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            instance = asyncio.run(scenario(Path(tmp_dir)))

        self.assertEqual(instance.correlation_path_id, "run_correlation_path:correlation_path_main")
        by_effector = {
            process.metadata["correlation_path"]["effector_id"]: process
            for process in instance.processes
        }
        self.assertEqual(set(by_effector), {"root", "dependent", "approval"})

        root_process = by_effector["root"]
        self.assertEqual(root_process.id, "run_correlation_path:correlation_path_main:root")
        self.assertEqual(root_process.status, ProcessStatus.ready)
        self.assertEqual(root_process.process_type, "python_function")
        self.assertEqual(root_process.priority, 5)
        self.assertEqual(root_process.max_attempts, 2)
        self.assertEqual(root_process.input["value"], 3)
        self.assertEqual(root_process.input["config"], {"base": 1, "extra": 2})
        self.assertEqual(root_process.input["adapter"]["timeout_seconds"], 7.5)
        self.assertEqual(
            root_process.metadata["correlation_path"],
            {
                "correlation_path_id": "run_correlation_path:correlation_path_main",
                "correlation_path_spec_id": "correlation_path_main",
                "effector_id": "root",
                "seq": 0,
                "conduction": [],
                "accumulate_upstream_reactions": False,
            },
        )

        dependent = by_effector["dependent"]
        self.assertEqual(dependent.status, ProcessStatus.pending)
        self.assertEqual(dependent.metadata["correlation_path"]["conduction"], ["root"])

        approval = by_effector["approval"]
        self.assertEqual(approval.status, ProcessStatus.pending)
        self.assertIsNone(approval.input["adapter"]["timeout_seconds"])

    def test_instantiate_correlation_path_respects_max_attempts_by_effector(self) -> None:
        correlation_path = CorrelationPathSpec(
            id="correlation_path_ma",
            effectors=[
                _python_effector("root"),
                _python_effector("dependent", conduction=["root"]),
            ],
        )

        async def scenario(root: Path) -> CorrelationPathInstance:
            service = await _service(root, "run_ma")
            return await instantiate_correlation_path(
                service,
                run_id="run_ma",
                correlation_path=correlation_path,
                max_attempts=1,
                max_attempts_by_effector={"dependent": 5},
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            instance = asyncio.run(scenario(Path(tmp_dir)))

        by_effector = {
            process.metadata["correlation_path"]["effector_id"]: process
            for process in instance.processes
        }
        self.assertEqual(by_effector["root"].max_attempts, 1)
        self.assertEqual(by_effector["dependent"].max_attempts, 5)
    def test_instantiate_correlation_path_resolves_python_placeholder(self) -> None:
        def _subprocess_effector(effector_id: str, command: list[str]) -> EffectorSpec:
            return EffectorSpec(
                id=effector_id,
                capability="subprocess",
                adapter=EffectorAdapterSpec(kind="subprocess", command=command),
            )

        correlation_path = CorrelationPathSpec(
            id="correlation_path_python",
            effectors=[
                _subprocess_effector(
                    "portable",
                    [PYTHON_COMMAND_PLACEHOLDER, "-m", "pkg.mod", PYTHON_COMMAND_PLACEHOLDER],
                ),
                _subprocess_effector("bare", ["python", "-m", "pkg.mod"]),
            ],
        )

        async def scenario(root: Path) -> CorrelationPathInstance:
            service = await _service(root, "run_python")
            return await instantiate_correlation_path(
                service,
                run_id="run_python",
                correlation_path=correlation_path,
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            instance = asyncio.run(scenario(Path(tmp_dir)))

        by_effector = {
            process.metadata["correlation_path"]["effector_id"]: process
            for process in instance.processes
        }
        # Every explicit placeholder token resolves to the driving interpreter.
        self.assertEqual(
            by_effector["portable"].input["adapter"]["command"],
            [sys.executable, "-m", "pkg.mod", sys.executable],
        )
        # Bare python tokens are not placeholders and stay untouched.
        self.assertEqual(
            by_effector["bare"].input["adapter"]["command"],
            ["python", "-m", "pkg.mod"],
        )

    def test_reserved_effector_input_keys_cover_injected_keys(self) -> None:
        # The reserved set is derived from the SDK's injected-key constant, so
        # the two can never drift apart.
        self.assertLessEqual(
            set(INJECTED_INPUT_KEYS), set(correlation_paths._RESERVED_EFFECTOR_INPUT_KEYS)
        )
        self.assertIn("adapter", correlation_paths._RESERVED_EFFECTOR_INPUT_KEYS)
        self.assertIn("config", correlation_paths._RESERVED_EFFECTOR_INPUT_KEYS)

    def test_instantiate_correlation_path_replays_idempotently(self) -> None:
        async def scenario(root: Path) -> tuple[CorrelationPathInstance, CorrelationPathInstance, int]:
            service = await _service(root, "run_replay")
            first = await instantiate_correlation_path(
                service,
                run_id="run_replay",
                correlation_path=_two_effector_correlation_path(),
                effector_inputs={"first": {"value": 1}},
            )
            second = await instantiate_correlation_path(
                service,
                run_id="run_replay",
                correlation_path=_two_effector_correlation_path(),
                effector_inputs={"first": {"value": 1}},
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

    def test_instantiate_correlation_path_rejects_unknown_and_reserved_keys(self) -> None:
        async def scenario(root: Path, **kwargs) -> None:
            service = await _service(root, "run_invalid")
            await instantiate_correlation_path(
                service,
                run_id="run_invalid",
                correlation_path=_two_effector_correlation_path(),
                **kwargs,
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            with self.assertRaises(ValueError) as unknown_input:
                asyncio.run(scenario(root / "a", effector_inputs={"missing": {"value": 1}}))
            self.assertIn("unknown correlation_path effectors", str(unknown_input.exception))
            with self.assertRaises(ValueError) as unknown_config:
                asyncio.run(scenario(root / "b", effector_configs={"missing": {"flag": True}}))
            self.assertIn("unknown correlation_path effectors", str(unknown_config.exception))
            for reserved_key in ("adapter", "config", "conduction", "upstream_reactions"):
                with self.assertRaises(ValueError) as reserved:
                    asyncio.run(
                        scenario(
                            root / f"c_{reserved_key}",
                            effector_inputs={"first": {reserved_key: {}}},
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
                    status=ProcessStatus.pending,
                    input={"value": 1},
                ),
                idempotency_key="run_ready:process.schedule:process_pending",
            )
            readied, _ = await service.ready_process(
                run_id="run_ready",
                process_id="process_pending",
                input={"value": 1, "conduction": {"first": {"value": 2}}},
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

        self.assertEqual(readied.status, ProcessStatus.ready)
        self.assertEqual(readied.input["conduction"], {"first": {"value": 2}})
        self.assertEqual(replayed.input["conduction"], {"first": {"value": 2}})
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


class FalaAdvanceCorrelationPathTests(unittest.TestCase):
    def test_advance_correlation_path_readies_effector_with_conduction_output_injected(self) -> None:
        async def scenario(root: Path) -> tuple[CorrelationPathAdvance, Process]:
            service = await _service(root, "run_advance")
            instance = await instantiate_correlation_path(
                service,
                run_id="run_advance",
                correlation_path=_two_effector_correlation_path(),
                effector_inputs={"first": {"value": 4}},
                auto_advance=False,
            )
            first = instance.processes[0]
            claimed = await service.claim_next_ready_process(worker_id="tester", all_runs=True)
            assert claimed is not None and claimed.id == first.id
            await service.complete_process(
                run_id="run_advance",
                process_id=first.id,
                output={"value": 8},
                idempotency_key=f"run_advance:process.complete:{first.id}:1",
                actor="tester",
            )
            advance = await advance_correlation_path(
                service,
                run_id="run_advance",
                correlation_path_id=instance.correlation_path_id,
            )
            second = await service.backend.get_process(
                run_id="run_advance",
                process_id="run_advance:correlation_path_pair:second",
            )
            assert second is not None
            return advance, second

        with tempfile.TemporaryDirectory() as tmp_dir:
            advance, second = asyncio.run(scenario(Path(tmp_dir)))

        self.assertEqual(len(advance.readied), 1)
        self.assertEqual(advance.blocked, [])
        self.assertEqual(second.status, ProcessStatus.ready)
        self.assertEqual(second.input["conduction"]["first"]["value"], 8)

    def test_advance_correlation_path_injects_transitive_upstream_reactions(self) -> None:
        async def scenario(root: Path) -> Process:
            service = await _service(root, "run_diamond")
            instance = await instantiate_correlation_path(
                service,
                run_id="run_diamond",
                correlation_path=_diamond_correlation_path(),
            )
            correlation_path_id = instance.correlation_path_id

            async def claim_and_complete() -> None:
                claimed = await service.claim_next_ready_process(worker_id="tester", all_runs=True)
                assert claimed is not None
                effector_id = claimed.metadata["correlation_path"]["effector_id"]
                await service.complete_process(
                    run_id="run_diamond",
                    process_id=claimed.id,
                    output={
                        "value": 1,
                        "reactions": [{"kind": f"{effector_id}-art"}],
                    },
                    idempotency_key=(
                        f"run_diamond:process.complete:{claimed.id}:1"
                    ),
                    actor="tester",
                )

            # a runs, then advancing readies b and c, then d.
            await claim_and_complete()  # a
            await advance_correlation_path(service, run_id="run_diamond", correlation_path_id=correlation_path_id)
            await claim_and_complete()  # b or c
            await claim_and_complete()  # the other of b/c
            await advance_correlation_path(service, run_id="run_diamond", correlation_path_id=correlation_path_id)

            processed = await service.backend.get_process(
                run_id="run_diamond",
                process_id="run_diamond:correlation_path_diamond:d",
            )
            assert processed is not None
            return processed

        with tempfile.TemporaryDirectory() as tmp_dir:
            d = asyncio.run(scenario(Path(tmp_dir)))

        self.assertEqual(d.status, ProcessStatus.ready)
        # a is NOT a direct conduction upstream of d -- yet its reaction is visible transitively,
        # in topological order (deepest ancestor first).
        self.assertEqual(
            d.input["upstream_reactions"],
            [{"kind": "a-art"}, {"kind": "b-art"}, {"kind": "c-art"}],
        )
        # Direct-conduction injection is untouched: d sees b and c fully, but not a.
        self.assertEqual(set(d.input["conduction"]), {"b", "c"})

    def test_advance_correlation_path_omits_upstream_reactions_when_flag_off(self) -> None:
        async def scenario(root: Path) -> Process:
            service = await _service(root, "run_no_accum")
            instance = await instantiate_correlation_path(
                service,
                run_id="run_no_accum",
                correlation_path=_two_effector_correlation_path(),
                effector_inputs={"first": {"value": 4}},
            )
            first = instance.processes[0]
            claimed = await service.claim_next_ready_process(worker_id="tester", all_runs=True)
            assert claimed is not None and claimed.id == first.id
            await service.complete_process(
                run_id="run_no_accum",
                process_id=first.id,
                output={"value": 8, "reactions": [{"kind": "first-art"}]},
                idempotency_key=f"run_no_accum:process.complete:{first.id}:1",
                actor="tester",
            )
            await advance_correlation_path(
                service,
                run_id="run_no_accum",
                correlation_path_id=instance.correlation_path_id,
            )
            second = await service.backend.get_process(
                run_id="run_no_accum",
                process_id="run_no_accum:correlation_path_pair:second",
            )
            assert second is not None
            return second

        with tempfile.TemporaryDirectory() as tmp_dir:
            second = asyncio.run(scenario(Path(tmp_dir)))

        self.assertEqual(second.status, ProcessStatus.ready)
        self.assertNotIn("upstream_reactions", second.input)

    def test_advance_correlation_path_reports_unmet_and_dead_conduction_without_readying(self) -> None:
        async def scenario(root: Path) -> tuple[CorrelationPathAdvance, CorrelationPathAdvance, Process]:
            service = await _service(root, "run_blocked")
            instance = await instantiate_correlation_path(
                service,
                run_id="run_blocked",
                correlation_path=_two_effector_correlation_path(),
                effector_inputs={"first": {"value": 4}},
            )
            unmet = await advance_correlation_path(
                service,
                run_id="run_blocked",
                correlation_path_id=instance.correlation_path_id,
            )
            claimed = await service.claim_next_ready_process(worker_id="tester", all_runs=True)
            assert claimed is not None
            await service.cancel_process(
                run_id="run_blocked",
                process_id=claimed.id,
                idempotency_key=f"run_blocked:process.cancel:{claimed.id}",
            )
            dead = await advance_correlation_path(
                service,
                run_id="run_blocked",
                correlation_path_id=instance.correlation_path_id,
            )
            second = await service.backend.get_process(
                run_id="run_blocked",
                process_id="run_blocked:correlation_path_pair:second",
            )
            assert second is not None
            return unmet, dead, second

        with tempfile.TemporaryDirectory() as tmp_dir:
            unmet, dead, second = asyncio.run(scenario(Path(tmp_dir)))

        self.assertEqual(unmet.readied, [])
        self.assertEqual(len(unmet.blocked), 1)
        self.assertEqual(unmet.blocked[0].effector_id, "second")
        self.assertEqual(unmet.blocked[0].unmet, ["first"])
        self.assertEqual(unmet.blocked[0].dead, [])

        self.assertEqual(dead.readied, [])
        self.assertEqual(len(dead.blocked), 1)
        self.assertEqual(dead.blocked[0].dead, ["first"])
        self.assertEqual(second.status, ProcessStatus.pending)

    def test_advance_correlation_path_treats_exhausted_failed_need_as_dead(self) -> None:
        async def scenario(root: Path) -> CorrelationPathAdvance:
            service = await _service(root, "run_failed")
            instance = await instantiate_correlation_path(
                service,
                run_id="run_failed",
                correlation_path=_two_effector_correlation_path(),
                effector_inputs={"first": {"value": 4}},
                auto_advance=False,
            )
            claimed = await service.claim_next_ready_process(worker_id="tester", all_runs=True)
            assert claimed is not None
            await service.fail_process(
                run_id="run_failed",
                process_id=claimed.id,
                error={"type": "RuntimeError", "message": "boom"},
                idempotency_key=f"run_failed:process.fail:{claimed.id}:1",
                actor="tester",
            )
            return await advance_correlation_path(
                service,
                run_id="run_failed",
                correlation_path_id=instance.correlation_path_id,
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            advance = asyncio.run(scenario(Path(tmp_dir)))

        self.assertEqual(advance.readied, [])
        self.assertEqual(len(advance.blocked), 1)
        self.assertEqual(advance.blocked[0].dead, ["first"])

    def test_advance_correlation_path_unknown_need_fails_closed(self) -> None:
        async def scenario(root: Path) -> None:
            service = await _service(root, "run_orphan")
            await service.schedule_process(
                Process(
                    id="correlation_path_x:child",
                    run_id="run_orphan",
                    process_type="python_function",
                    status=ProcessStatus.pending,
                    input={
                        "adapter": {
                            "kind": "python_function",
                            "ref": "tests.test_fala_correlation_paths._correlation_path_double",
                        },
                    },
                    metadata={
                        "correlation_path": {
                            "correlation_path_id": "correlation_path_x",
                            "correlation_path_spec_id": "correlation_path_x",
                            "effector_id": "child",
                            "conduction": ["ghost"],
                        }
                    },
                ),
                idempotency_key="run_orphan:process.schedule:correlation_path_x:child",
            )
            await advance_correlation_path(service, run_id="run_orphan", correlation_path_id="correlation_path_x")

        with tempfile.TemporaryDirectory() as tmp_dir:
            with self.assertRaises(ValueError) as ctx:
                asyncio.run(scenario(Path(tmp_dir)))
            self.assertIn("unknown effector", str(ctx.exception))
    def test_advance_correlation_path_injects_regulation_from_marker(self) -> None:
        async def scenario_ready(root: Path) -> dict[str, Any]:
            service = await _service(root, "run_reg_ready")
            instance = await instantiate_correlation_path(
                service,
                run_id="run_reg_ready",
                correlation_path=_two_effector_correlation_path(),
                effector_inputs={"first": {"value": 4}},
                regulation_by_effector={"second": {"entropy": 0.42, "damping": "high"}},
                auto_advance=False,
            )
            first = instance.processes[0]
            await service.claim_next_ready_process(worker_id="tester", all_runs=True)
            await service.complete_process(
                run_id="run_reg_ready",
                process_id=first.id,
                output={"value": 8},
                idempotency_key=f"run_reg_ready:process.complete:{first.id}:1",
                actor="tester",
            )
            await advance_correlation_path(
                service,
                run_id="run_reg_ready",
                correlation_path_id=instance.correlation_path_id,
            )
            second = await service.backend.get_process(
                run_id="run_reg_ready",
                process_id="run_reg_ready:correlation_path_pair:second",
            )
            assert second is not None
            return second.input.get("regulation") or {}

        async def scenario_dead(root: Path) -> dict[str, Any]:
            service = await _service(root, "run_reg_dead")
            instance = await instantiate_correlation_path(
                service,
                run_id="run_reg_dead",
                correlation_path=_two_effector_correlation_path(),
                effector_inputs={"first": {"value": 4}},
                # default auto_advance=True so the atomic dead-cancellation fires on fail(first)
            )
            first = instance.processes[0]
            # schedule a fresh pending that depends on first, carrying regulation in its marker
            await service.schedule_process(
                Process(
                    id="run_reg_dead:correlation_path_pair:third",
                    run_id="run_reg_dead",
                    process_type="python_function",
                    status=ProcessStatus.pending,
                    input={"adapter": {"kind": "python_function", "ref": "tests.test_fala_correlation_paths._correlation_path_double"}},
                    metadata={
                        "correlation_path": {
                            "correlation_path_id": instance.correlation_path_id,
                            "effector_id": "third",
                            "conduction": ["first"],
                            "regulation": {"entropy": 0.7, "damping": "kill"},
                        }
                    },
                ),
                idempotency_key="run_reg_dead:process.schedule:correlation_path_pair:third",
            )
            # claim and fail the upstream -> transition_process should atomically dead-cancel third
            # and embed the regulation from third's marker into the cancel error
            claimed = await service.claim_next_ready_process(worker_id="tester", all_runs=True)
            assert claimed is not None and claimed.id == first.id
            await service.fail_process(
                run_id="run_reg_dead",
                process_id=first.id,
                error={"message": "boom"},
                idempotency_key=f"run_reg_dead:process.fail:{first.id}:1",
                actor="tester",
            )
            third = await service.backend.get_process(run_id="run_reg_dead", process_id="run_reg_dead:correlation_path_pair:third")
            assert third is not None
            return (third.error or {})

        with tempfile.TemporaryDirectory() as tmp_dir_ready:
            reg_ready = asyncio.run(scenario_ready(Path(tmp_dir_ready)))
        with tempfile.TemporaryDirectory() as tmp_dir_dead:
            reg_dead = asyncio.run(scenario_dead(Path(tmp_dir_dead)))
        self.assertEqual(reg_ready, {"entropy": 0.42, "damping": "high"})
        self.assertIn("regulation", reg_dead)
        self.assertEqual(reg_dead["regulation"], {"entropy": 0.7, "damping": "kill"})

    def test_advance_correlation_path_propagates_regulation_and_dynamic_max_attempts(self) -> None:
        """Regulation from marker is injected, propagated via conduction, and
        regulation["max_attempts"] overrides the downstream process retry budget.
        This is the core quantitative damping / variety control mechanism.
        """
        async def scenario(root: Path) -> tuple[dict[str, Any], int, int]:
            service = await _service(root, "run_reg_dyn")
            instance = await instantiate_correlation_path(
                service,
                run_id="run_reg_dyn",
                correlation_path=_two_effector_correlation_path(),
                effector_inputs={"first": {"value": 1}},
                # Give downstream a regulation payload that both carries metadata
                # and asks for higher retry budget (damping signal).
                regulation_by_effector={
                    "second": {
                        "entropy": 0.13,
                        "damping": "aggressive",
                        "max_attempts": 4,
                    }
                },
                max_attempts=1,  # global default is low
                auto_advance=True,
            )
            first = instance.processes[0]
            # complete first -> atomic ready of second (with regulation + override)
            await service.claim_next_ready_process(worker_id="tester", all_runs=True)
            await service.complete_process(
                run_id="run_reg_dyn",
                process_id=first.id,
                output={"value": 2},
                idempotency_key=f"run_reg_dyn:process.complete:{first.id}:1",
                actor="tester",
            )
            second = await service.backend.get_process(
                run_id="run_reg_dyn",
                process_id="run_reg_dyn:correlation_path_pair:second",
            )
            assert second is not None
            reg = second.input.get("regulation") or {}
            # claim the ready downstream (regulation already injected at atomic ready)
            claimed = await service.claim_next_ready_process(worker_id="tester", all_runs=True)
            assert claimed is not None and claimed.id == second.id
            # now fail it; with override=4 it should still have attempts left (attempt becomes 1)
            await service.fail_process(
                run_id="run_reg_dyn",
                process_id=second.id,
                error={"message": "transient"},
                idempotency_key=f"run_reg_dyn:process.fail:{second.id}:1",
                actor="tester",
            )
            second_after = await service.backend.get_process(
                run_id="run_reg_dyn",
                process_id=second.id,
            )
            assert second_after is not None
            return (reg, second.max_attempts, second_after.attempt)

        with tempfile.TemporaryDirectory() as tmp_dir:
            reg, ma_at_ready, attempt_after_one_fail = asyncio.run(scenario(Path(tmp_dir)))
        self.assertEqual(reg, {"entropy": 0.13, "damping": "aggressive", "max_attempts": 4})
        self.assertEqual(ma_at_ready, 4)  # overridden from global 1
        # with max_attempts=4 it is still eligible for retry (not yet exhausted)

    def test_advance_correlation_path_for_process_ignores_non_correlation_path_processes(self) -> None:
        async def scenario(root: Path) -> CorrelationPathAdvance | None:
            service = await _service(root, "run_plain")
            await service.schedule_process(
                Process(
                    id="process_plain",
                    run_id="run_plain",
                    process_type="python_function",
                    status=ProcessStatus.ready,
                    input={
                        "adapter": {
                            "kind": "python_function",
                            "ref": "tests.test_fala_correlation_paths._correlation_path_double",
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
            return await advance_correlation_path_for_process(service, process=process)

        with tempfile.TemporaryDirectory() as tmp_dir:
            self.assertIsNone(asyncio.run(scenario(Path(tmp_dir))))


class FalaCorrelationPathEffectorProcessLookupTests(unittest.TestCase):
    """Pure host-side selectors over a run's process list -- no service needed.

    These are the primitives a finished-run reader uses instead of re-deriving
    the ``{correlation_path_id}:{effector_id}`` process-id format: they read the same
    ``metadata['correlation_path']`` marker :func:`advance_correlation_path` uses to assemble members.
    """

    def _member(self, correlation_path_id: str, effector_id: str) -> Process:
        return Process(
            id=f"{correlation_path_id}:{effector_id}",
            run_id="run_lookup",
            process_type="python_function",
            status=ProcessStatus.succeeded,
            metadata={"correlation_path": {"correlation_path_id": correlation_path_id, "effector_id": effector_id}},
        )

    def test_correlation_path_effector_processes_keys_by_effector_and_filters_foreign(self) -> None:
        wanted_first = self._member("correlation_path_a", "first")
        wanted_second = self._member("correlation_path_a", "second")
        other_correlation_path = self._member("correlation_path_b", "first")
        no_marker = Process(
            id="plain",
            run_id="run_lookup",
            process_type="python_function",
            status=ProcessStatus.ready,
        )
        no_effector_id = Process(
            id="bad",
            run_id="run_lookup",
            process_type="python_function",
            status=ProcessStatus.ready,
            metadata={"correlation_path": {"correlation_path_id": "correlation_path_a"}},
        )
        members = correlation_path_effector_processes(
            [wanted_first, other_correlation_path, no_marker, no_effector_id, wanted_second],
            "correlation_path_a",
        )
        # Only correlation_path_a's marked, effector-identified members survive, keyed by effector id.
        self.assertEqual(set(members), {"first", "second"})
        self.assertEqual(members["first"].id, "correlation_path_a:first")
        self.assertEqual(members["second"].id, "correlation_path_a:second")

    def test_correlation_path_effector_processes_last_of_a_kind_wins(self) -> None:
        first = self._member("correlation_path_a", "dup")
        second = self._member("correlation_path_a", "dup")
        members = correlation_path_effector_processes([first, second], "correlation_path_a")
        self.assertIs(members["dup"], second)

    def test_find_correlation_path_effector_process_returns_match_or_none(self) -> None:
        member = self._member("correlation_path_a", "report")
        self.assertIs(
            find_correlation_path_effector_process([member], correlation_path_id="correlation_path_a", effector_id="report"),
            member,
        )
        self.assertIsNone(
            find_correlation_path_effector_process([member], correlation_path_id="correlation_path_a", effector_id="missing")
        )
        self.assertIsNone(
            find_correlation_path_effector_process([member], correlation_path_id="other_correlation_path", effector_id="report")
        )


if __name__ == "__main__":
    unittest.main()
