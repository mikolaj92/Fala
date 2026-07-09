from __future__ import annotations

import asyncio
import sys
import tempfile
import unittest
from pathlib import Path

from fala import (
    RunFlowResult,
    find_flow_step_process,
    flow_step_processes,
    run_flow,
)
from fala.flows import PYTHON_COMMAND_PLACEHOLDER
from fala.models import CarrierAdapterSpec, CarrierFlowSpec, CarrierFlowStepSpec
from fala.runtime_backend import (
    CarrierProcessStatus,
    CarrierRunStatus,
    Run,
    RuntimeBackendService,
)


def _double(request) -> dict:
    return {"value": request.input["value"] * 2}


def _sum_needs(request) -> dict:
    needs = request.input["needs"]
    return {"total": sum(item["value"] for item in needs.values())}


def _boom(request) -> dict:
    raise RuntimeError("boom")


def _echo_config(request) -> dict:
    return {"scaled": request.config.get("scale", 0) * request.input["value"]}


def _step(step_id: str, ref: str, *, needs: list[str] | None = None) -> CarrierFlowStepSpec:
    return CarrierFlowStepSpec(
        id=step_id,
        capability="python_function",
        adapter=CarrierAdapterSpec(kind="python_function", ref=ref),
        needs=needs or [],
    )


def _diamond() -> CarrierFlowSpec:
    return CarrierFlowSpec(
        id="flow_diamond",
        steps=[
            _step("left", "tests.test_fala_run_flow._double"),
            _step("right", "tests.test_fala_run_flow._double"),
            _step("join", "tests.test_fala_run_flow._sum_needs", needs=["left", "right"]),
        ],
    )


def _bare_service(root: Path) -> RuntimeBackendService:
    # run_flow creates the run itself, so the store starts with no run record.
    return RuntimeBackendService.sqlite(root / "state.sqlite")


class RunFlowTests(unittest.TestCase):
    def test_run_flow_creates_run_drives_dag_and_completes(self) -> None:
        async def scenario(root: Path):
            service = _bare_service(root)
            result = await run_flow(
                service,
                run=Run(id="rf_ok"),
                flow=_diamond(),
                worker_id="tester",
                step_inputs={"left": {"value": 2}, "right": {"value": 3}},
            )
            stored = await service.backend.get_run(run_id="rf_ok")
            join = await service.backend.get_process(
                run_id="rf_ok",
                process_id=f"{result.flow.flow_id}:join",
            )
            return result, stored, join

        with tempfile.TemporaryDirectory() as tmp_dir:
            result, stored, join = asyncio.run(scenario(Path(tmp_dir)))

        self.assertIsInstance(result, RunFlowResult)
        # Fala owns the run's terminal state: it created the run and marked it.
        self.assertEqual(result.status, CarrierRunStatus.completed)
        self.assertEqual(result.run.status, CarrierRunStatus.completed)
        self.assertIsNotNone(stored)
        self.assertEqual(stored.status, CarrierRunStatus.completed)
        # Drove the whole DAG; step_inputs reached the leaves and joined.
        self.assertEqual(result.outcome.failed, [])
        self.assertEqual(result.outcome.waiting, [])
        self.assertEqual(len(result.outcome.completed), 3)
        self.assertEqual(result.flow.flow_id, "rf_ok:flow_diamond")
        self.assertIsNotNone(join)
        self.assertEqual(join.status, CarrierProcessStatus.succeeded)
        self.assertEqual(join.output["total"], 10)

    def test_run_flow_result_carries_flow_step_processes(self) -> None:
        async def scenario(root: Path):
            service = _bare_service(root)
            result = await run_flow(
                service,
                run=Run(id="rf_procs"),
                flow=_diamond(),
                worker_id="tester",
                step_inputs={"left": {"value": 2}, "right": {"value": 3}},
            )
            listed = await service.list_processes(run_id="rf_procs")
            return result, listed

        with tempfile.TemporaryDirectory() as tmp_dir:
            result, listed = asyncio.run(scenario(Path(tmp_dir)))

        # The result exposes the run's processes directly, so a host-side reader
        # never re-lists them to reach a step's output.
        self.assertEqual(
            {p.id for p in result.processes},
            {p.id for p in listed},
        )
        # And they are addressable by flow marker, not by reconstructing ids.
        members = flow_step_processes(result.processes, result.flow.flow_id)
        self.assertEqual(set(members), {"left", "right", "join"})
        self.assertEqual(members["join"].output["total"], 10)
        join = find_flow_step_process(
            result.processes, flow_id=result.flow.flow_id, step_id="join"
        )
        self.assertIsNotNone(join)
        self.assertEqual(join.status, CarrierProcessStatus.succeeded)

    def test_run_flow_propagates_step_configs(self) -> None:
        flow = CarrierFlowSpec(
            id="flow_solo",
            steps=[_step("solo", "tests.test_fala_run_flow._echo_config")],
        )

        async def scenario(root: Path):
            service = _bare_service(root)
            result = await run_flow(
                service,
                run=Run(id="rf_cfg"),
                flow=flow,
                worker_id="tester",
                step_inputs={"solo": {"value": 5}},
                step_configs={"solo": {"scale": 3}},
            )
            solo = await service.backend.get_process(
                run_id="rf_cfg",
                process_id=f"{result.flow.flow_id}:solo",
            )
            return result, solo

        with tempfile.TemporaryDirectory() as tmp_dir:
            result, solo = asyncio.run(scenario(Path(tmp_dir)))

        self.assertEqual(result.status, CarrierRunStatus.completed)
        self.assertIsNotNone(solo)
        # config (per-run, per-step) reached the step alongside its input.
        self.assertEqual(solo.output["scaled"], 15)

    def test_run_flow_resolves_python_placeholder_in_subprocess_step(self) -> None:
        script = (
            "import json, os\n"
            "from pathlib import Path\n"
            "output = Path(os.environ['FALA_STEP_OUTPUT_DIR'])\n"
            "output.mkdir(parents=True, exist_ok=True)\n"
            "(output / 'result.json').write_text(json.dumps({'ok': True}))\n"
        )
        flow = CarrierFlowSpec(
            id="flow_portable",
            steps=[
                CarrierFlowStepSpec(
                    id="portable",
                    capability="subprocess",
                    adapter=CarrierAdapterSpec(
                        kind="subprocess",
                        command=[PYTHON_COMMAND_PLACEHOLDER, "-c", script],
                        timeout_seconds=30,
                    ),
                    needs=[],
                )
            ],
        )

        async def scenario(root: Path):
            service = _bare_service(root)
            result = await run_flow(
                service,
                run=Run(id="rf_portable"),
                flow=flow,
                worker_id="tester",
                work_dir=root / "work",
            )
            stored = await service.backend.get_process(
                run_id="rf_portable",
                process_id=f"{result.flow.flow_id}:portable",
            )
            return result, stored

        with tempfile.TemporaryDirectory() as tmp_dir:
            result, stored = asyncio.run(scenario(Path(tmp_dir)))

        # The placeholder resolved to the driving interpreter and the step ran.
        self.assertEqual(result.status, CarrierRunStatus.completed)
        self.assertIsNotNone(stored)
        self.assertEqual(stored.status, CarrierProcessStatus.succeeded)
        self.assertEqual(stored.input["adapter"]["command"][0], sys.executable)
        self.assertEqual(stored.output["ok"], True)

    def test_run_flow_fails_run_when_a_step_fails(self) -> None:
        flow = CarrierFlowSpec(
            id="flow_boom",
            steps=[_step("boom", "tests.test_fala_run_flow._boom")],
        )

        async def scenario(root: Path):
            service = _bare_service(root)
            result = await run_flow(
                service,
                run=Run(id="rf_fail"),
                flow=flow,
                worker_id="tester",
            )
            stored = await service.backend.get_run(run_id="rf_fail")
            return result, stored

        with tempfile.TemporaryDirectory() as tmp_dir:
            result, stored = asyncio.run(scenario(Path(tmp_dir)))

        self.assertTrue(result.outcome.failed)
        self.assertEqual(result.status, CarrierRunStatus.failed)
        self.assertEqual(result.run.status, CarrierRunStatus.failed)
        self.assertEqual(stored.status, CarrierRunStatus.failed)

    def test_run_flow_times_out_when_max_ticks_exhausted(self) -> None:
        async def scenario(root: Path):
            service = _bare_service(root)
            result = await run_flow(
                service,
                run=Run(id="rf_timeout"),
                flow=_diamond(),
                worker_id="tester",
                step_inputs={"left": {"value": 1}, "right": {"value": 1}},
                max_ticks=1,
            )
            stored = await service.backend.get_run(run_id="rf_timeout")
            return result, stored

        with tempfile.TemporaryDirectory() as tmp_dir:
            result, stored = asyncio.run(scenario(Path(tmp_dir)))

        # One tick cannot drain the diamond: steps remain incomplete and the
        # tick budget is spent -> Fala times the run out rather than completing.
        self.assertEqual(result.outcome.stopped_reason, "max_ticks")
        self.assertFalse(result.outcome.ok)
        self.assertEqual(result.outcome.failed, [])
        self.assertEqual(result.status, CarrierRunStatus.timed_out)
        self.assertEqual(stored.status, CarrierRunStatus.timed_out)

    def test_run_flow_marks_run_waiting_when_a_gate_parks_the_flow(self) -> None:
        flow = CarrierFlowSpec(
            id="flow_gate",
            steps=[
                CarrierFlowStepSpec(
                    id="gate",
                    capability="manual_gate",
                    adapter=CarrierAdapterSpec(kind="manual_gate"),
                    needs=[],
                )
            ],
        )

        async def scenario(root: Path):
            service = _bare_service(root)
            result = await run_flow(
                service,
                run=Run(id="rf_gate"),
                flow=flow,
                worker_id="tester",
            )
            stored = await service.backend.get_run(run_id="rf_gate")
            return result, stored

        with tempfile.TemporaryDirectory() as tmp_dir:
            result, stored = asyncio.run(scenario(Path(tmp_dir)))

        # A parked gate is a *suspended* run, not a failed one: Fala leaves it in
        # the non-terminal `waiting` state so it stays resumable, never `failed`.
        self.assertEqual(result.outcome.stopped_reason, "idle")
        self.assertEqual(len(result.outcome.waiting), 1)
        self.assertEqual(result.status, CarrierRunStatus.waiting)
        self.assertEqual(result.run.status, CarrierRunStatus.waiting)
        self.assertEqual(stored.status, CarrierRunStatus.waiting)

    def test_run_flow_is_idempotent_on_a_terminal_run(self) -> None:
        flow = CarrierFlowSpec(
            id="flow_chain",
            steps=[
                _step("a", "tests.test_fala_run_flow._double"),
                _step("b", "tests.test_fala_run_flow._sum_needs", needs=["a"]),
            ],
        )

        async def scenario(root: Path):
            service = _bare_service(root)
            run = Run(id="rf_replay")
            first = await run_flow(
                service,
                run=run,
                flow=flow,
                worker_id="tester",
                step_inputs={"a": {"value": 5}},
                max_ticks=1,
            )
            # Same run, same flow, again: create_run replays the terminal run.
            second = await run_flow(
                service,
                run=run,
                flow=flow,
                worker_id="tester",
                step_inputs={"a": {"value": 5}},
                max_ticks=1,
            )
            step_b = await service.backend.get_process(
                run_id="rf_replay",
                process_id=f"{second.flow.flow_id}:b",
            )
            return first, second, step_b

        with tempfile.TemporaryDirectory() as tmp_dir:
            first, second, step_b = asyncio.run(scenario(Path(tmp_dir)))

        # One tick cannot drive a->b, so the first pass times the run out.
        self.assertEqual(first.status, CarrierRunStatus.timed_out)
        # Re-invoking on the now-terminal run must neither re-drive the
        # leftover-ready step nor attempt an illegal terminal->terminal
        # transition: it returns the finished run untouched.
        self.assertEqual(second.status, CarrierRunStatus.timed_out)
        self.assertEqual(second.run.status, CarrierRunStatus.timed_out)
        self.assertEqual(second.outcome.stopped_reason, "already_terminal")
        self.assertEqual(second.outcome.ticks, 0)
        # The early-terminal branch still exposes the run's processes, so a reader
        # of a replayed terminal run sees the same steps as the first pass.
        self.assertTrue(second.processes)
        self.assertEqual(
            {p.id for p in second.processes},
            {p.id for p in first.processes},
        )
        # `b` was left untouched -- never claimed or completed on the terminal run.
        self.assertEqual(step_b.status, CarrierProcessStatus.ready)


if __name__ == "__main__":
    unittest.main()
