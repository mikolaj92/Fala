from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path

from fala import (
    RunCorrelationPathResult,
    find_correlation_path_effector_process,
    correlation_path_effector_processes,
    run_correlation_path,
)
from fala.correlation_paths import PYTHON_COMMAND_PLACEHOLDER
from fala.models import EffectorAdapterSpec, CorrelationPathSpec, EffectorSpec
from fala.runtime_backend import (
    ProcessStatus,
    RunStatus,
    Run,
    RuntimeBackendService,
)


def _double(request) -> dict:
    return {"value": request.input["value"] * 2}


def _sum_conduction(request) -> dict:
    conduction = request.input["conduction"]
    return {"total": sum(item["value"] for item in conduction.values())}


def _boom(request) -> dict:
    raise RuntimeError("boom")


def _echo_config(request) -> dict:
    return {"scaled": request.config.get("scale", 0) * request.input["value"]}

def _flaky_double(request) -> dict:
    # module-level so it is importable via "tests.test_fala_run_correlation_path._flaky_double"
    # for the python_function adapter.
    # The test wraps call counting via a mutable container passed in input.
    # Supports two modes for cross-boundary counting:
    # - list (in-process mutation, e.g. direct service calls)
    # - str|Path (file-backed JSON int, survives across run_correlation_path invocations)
    state = request.input.get("__flaky_state__", [0])
    if isinstance(state, (str, Path)):
        p = Path(state)
        try:
            n = int(json.loads(p.read_text() or "0"))
        except Exception:
            n = 0
        n += 1
        p.write_text(json.dumps(n))
        calls = n
    else:
        # list or other mutable container
        state[0] += 1
        calls = state[0]
    if calls == 1:
        raise RuntimeError("transient failure (first call)")
    return {"value": request.input["value"] * 2}

def _effector(effector_id: str, ref: str, *, conduction: list[str] | None = None) -> EffectorSpec:
    return EffectorSpec(
        id=effector_id,
        capability="python_function",
        adapter=EffectorAdapterSpec(kind="python_function", ref=ref),
        conduction=conduction or [],
    )


def _diamond() -> CorrelationPathSpec:
    return CorrelationPathSpec(
        id="correlation_path_diamond",
        effectors=[
            _effector("left", "tests.test_fala_run_correlation_path._double"),
            _effector("right", "tests.test_fala_run_correlation_path._double"),
            _effector("join", "tests.test_fala_run_correlation_path._sum_conduction", conduction=["left", "right"]),
        ],
    )


def _bare_service(root: Path) -> RuntimeBackendService:
    # run_correlation_path creates the run itself, so the store starts with no run record.
    return RuntimeBackendService.sqlite(root / "state.sqlite")


class RunCorrelationPathTests(unittest.TestCase):
    def test_run_correlation_path_creates_run_drives_dag_and_completes(self) -> None:
        async def scenario(root: Path):
            service = _bare_service(root)
            result = await run_correlation_path(
                service,
                run=Run(id="rf_ok"),
                correlation_path=_diamond(),
                worker_id="tester",
                effector_inputs={"left": {"value": 2}, "right": {"value": 3}},
            )
            stored = await service.backend.get_run(run_id="rf_ok")
            join = await service.backend.get_process(
                run_id="rf_ok",
                process_id=f"{result.correlation_path.correlation_path_id}:join",
            )
            return result, stored, join

        with tempfile.TemporaryDirectory() as tmp_dir:
            result, stored, join = asyncio.run(scenario(Path(tmp_dir)))

        self.assertIsInstance(result, RunCorrelationPathResult)
        # Fala owns the run's terminal state: it created the run and marked it.
        self.assertEqual(result.status, RunStatus.completed)
        self.assertEqual(result.run.status, RunStatus.completed)
        self.assertIsNotNone(stored)
        self.assertEqual(stored.status, RunStatus.completed)
        # Drove the whole DAG; effector_inputs reached the leaves and joined.
        self.assertEqual(result.outcome.failed, [])
        self.assertEqual(result.outcome.waiting, [])
        self.assertEqual(len(result.outcome.completed), 3)
        self.assertEqual(result.correlation_path.correlation_path_id, "rf_ok:correlation_path_diamond")
        self.assertIsNotNone(join)
        self.assertEqual(join.status, ProcessStatus.succeeded)
        self.assertEqual(join.output["total"], 10)

    def test_run_correlation_path_result_carries_correlation_path_effector_processes(self) -> None:
        async def scenario(root: Path):
            service = _bare_service(root)
            result = await run_correlation_path(
                service,
                run=Run(id="rf_procs"),
                correlation_path=_diamond(),
                worker_id="tester",
                effector_inputs={"left": {"value": 2}, "right": {"value": 3}},
            )
            listed = await service.list_processes(run_id="rf_procs")
            return result, listed

        with tempfile.TemporaryDirectory() as tmp_dir:
            result, listed = asyncio.run(scenario(Path(tmp_dir)))

        # The result exposes the run's processes directly, so a host-side reader
        # never re-lists them to reach an effector's output.
        self.assertEqual(
            {p.id for p in result.processes},
            {p.id for p in listed},
        )
        # And they are addressable by correlation_path marker, not by reconstructing ids.
        members = correlation_path_effector_processes(result.processes, result.correlation_path.correlation_path_id)
        self.assertEqual(set(members), {"left", "right", "join"})
        self.assertEqual(members["join"].output["total"], 10)
        join = find_correlation_path_effector_process(
            result.processes, correlation_path_id=result.correlation_path.correlation_path_id, effector_id="join"
        )
        self.assertIsNotNone(join)
        self.assertEqual(join.status, ProcessStatus.succeeded)

    def test_run_correlation_path_propagates_effector_configs(self) -> None:
        correlation_path = CorrelationPathSpec(
            id="correlation_path_solo",
            effectors=[_effector("solo", "tests.test_fala_run_correlation_path._echo_config")],
        )

        async def scenario(root: Path):
            service = _bare_service(root)
            result = await run_correlation_path(
                service,
                run=Run(id="rf_cfg"),
                correlation_path=correlation_path,
                worker_id="tester",
                effector_inputs={"solo": {"value": 5}},
                effector_configs={"solo": {"scale": 3}},
            )
            solo = await service.backend.get_process(
                run_id="rf_cfg",
                process_id=f"{result.correlation_path.correlation_path_id}:solo",
            )
            return result, solo

        with tempfile.TemporaryDirectory() as tmp_dir:
            result, solo = asyncio.run(scenario(Path(tmp_dir)))

        self.assertEqual(result.status, RunStatus.completed)
        self.assertIsNotNone(solo)
        # config (per-run, per-effector) reached the effector alongside its input.
        self.assertEqual(solo.output["scaled"], 15)

    def test_run_correlation_path_resolves_python_placeholder_in_subprocess_effector(self) -> None:
        script = (
            "import json, os\n"
            "from pathlib import Path\n"
            "output = Path(os.environ['FALA_EFFECTOR_OUTPUT_DIR'])\n"
            "output.mkdir(parents=True, exist_ok=True)\n"
            "(output / 'result.json').write_text(json.dumps({'ok': True}))\n"
        )
        correlation_path = CorrelationPathSpec(
            id="correlation_path_portable",
            effectors=[
                EffectorSpec(
                    id="portable",
                    capability="subprocess",
                    adapter=EffectorAdapterSpec(
                        kind="subprocess",
                        command=[PYTHON_COMMAND_PLACEHOLDER, "-c", script],
                        timeout_seconds=30,
                    ),
                    conduction=[],
                )
            ],
        )

        async def scenario(root: Path):
            service = _bare_service(root)
            result = await run_correlation_path(
                service,
                run=Run(id="rf_portable"),
                correlation_path=correlation_path,
                worker_id="tester",
                work_dir=root / "work",
            )
            stored = await service.backend.get_process(
                run_id="rf_portable",
                process_id=f"{result.correlation_path.correlation_path_id}:portable",
            )
            return result, stored

        with tempfile.TemporaryDirectory() as tmp_dir:
            result, stored = asyncio.run(scenario(Path(tmp_dir)))

        # The placeholder resolved to the driving interpreter and the effector ran.
        self.assertEqual(result.status, RunStatus.completed)
        self.assertIsNotNone(stored)
        self.assertEqual(stored.status, ProcessStatus.succeeded)
        self.assertEqual(stored.input["adapter"]["command"][0], sys.executable)
        self.assertEqual(stored.output["ok"], True)

    def test_run_correlation_path_fails_run_when_an_effector_fails(self) -> None:
        correlation_path = CorrelationPathSpec(
            id="correlation_path_boom",
            effectors=[_effector("boom", "tests.test_fala_run_correlation_path._boom")],
        )

        async def scenario(root: Path):
            service = _bare_service(root)
            result = await run_correlation_path(
                service,
                run=Run(id="rf_fail"),
                correlation_path=correlation_path,
                worker_id="tester",
            )
            stored = await service.backend.get_run(run_id="rf_fail")
            return result, stored

        with tempfile.TemporaryDirectory() as tmp_dir:
            result, stored = asyncio.run(scenario(Path(tmp_dir)))

        self.assertTrue(result.outcome.failed)
        self.assertEqual(result.status, RunStatus.failed)
        self.assertEqual(result.run.status, RunStatus.failed)
        self.assertEqual(stored.status, RunStatus.failed)

    def test_run_correlation_path_times_out_when_max_ticks_exhausted(self) -> None:
        async def scenario(root: Path):
            service = _bare_service(root)
            result = await run_correlation_path(
                service,
                run=Run(id="rf_timeout"),
                correlation_path=_diamond(),
                worker_id="tester",
                effector_inputs={"left": {"value": 1}, "right": {"value": 1}},
                max_ticks=1,
            )
            stored = await service.backend.get_run(run_id="rf_timeout")
            return result, stored

        with tempfile.TemporaryDirectory() as tmp_dir:
            result, stored = asyncio.run(scenario(Path(tmp_dir)))

        # One tick cannot drain the diamond: effectors remain incomplete and the
        # tick budget is spent -> Fala times the run out rather than completing.
        self.assertEqual(result.outcome.stopped_reason, "max_ticks")
        self.assertFalse(result.outcome.ok)
        self.assertEqual(result.outcome.failed, [])
        self.assertEqual(result.status, RunStatus.timed_out)
        self.assertEqual(stored.status, RunStatus.timed_out)

    def test_run_correlation_path_respects_regulation_max_attempts_damping(self) -> None:
        """End-to-end: regulation_by_effector + regulation["max_attempts"] lets a
        transiently failing effector retry and eventually succeed, even when the
        global max_attempts would have killed it. This is quantitative damping
        (Mazur/Kossecki) realized in the conduction graph.
        """
        correlation_path = CorrelationPathSpec(
            id="correlation_path_flaky",
            effectors=[
                _effector("flaky", "tests.test_fala_run_correlation_path._flaky_double"),
            ],
        )

        async def scenario(root: Path):
            service = _bare_service(root)
            # Use a file-backed counter so the module-level _flaky_double can observe
            # call count across Process serialization (driver + python_function adapter).
            counter_path = root / "flaky_calls.json"
            counter_path.write_text("0")
            result = await run_correlation_path(
                service,
                run=Run(id="rf_damp"),
                correlation_path=correlation_path,
                worker_id="tester",
                effector_inputs={
                    "flaky": {
                        "value": 7,
                        "__flaky_state__": str(counter_path),
                    }
                },
                # Global default (1) is insufficient; regulation overrides per-effector.
                regulation_by_effector={"flaky": {"max_attempts": 3, "note": "damping active"}},
            )
            stored = await service.backend.get_process(
                run_id="rf_damp",
                process_id=f"{result.correlation_path.correlation_path_id}:flaky",
            )
            calls = int(json.loads(counter_path.read_text() or "0"))
            return result, stored, calls

        with tempfile.TemporaryDirectory() as tmp_dir:
            result, stored, calls = asyncio.run(scenario(Path(tmp_dir)))

        self.assertEqual(result.status, RunStatus.completed)
        self.assertIsNotNone(stored)
        self.assertEqual(stored.status, ProcessStatus.succeeded)
        self.assertEqual(stored.output["value"], 14)
        self.assertEqual(stored.max_attempts, 3)  # regulation overrode global 1
        self.assertGreaterEqual(calls, 2)  # at least one retry happened (fail once, succeed once)
        # Schedule-time injection: regulation dict must be visible in the root effector input
        # (roots are ready at birth; downstreams receive it on ready via advance).
        self.assertIn("regulation", stored.input or {})
        self.assertEqual(stored.input.get("regulation"), {"max_attempts": 3, "note": "damping active"})
    def test_run_correlation_path_marks_run_waiting_when_a_homeostat_parks_the_correlation_path(self) -> None:
        correlation_path = CorrelationPathSpec(
            id="correlation_path_homeostat",
            effectors=[
                EffectorSpec(
                    id="homeostat",
                    capability="manual_homeostat",
                    adapter=EffectorAdapterSpec(kind="manual_homeostat"),
                    conduction=[],
                )
            ],
        )

        async def scenario(root: Path):
            service = _bare_service(root)
            result = await run_correlation_path(
                service,
                run=Run(id="rf_homeostat"),
                correlation_path=correlation_path,
                worker_id="tester",
            )
            stored = await service.backend.get_run(run_id="rf_homeostat")
            return result, stored

        with tempfile.TemporaryDirectory() as tmp_dir:
            result, stored = asyncio.run(scenario(Path(tmp_dir)))

        # A parked homeostat is a *suspended* run, not a failed one: Fala leaves it in
        # the non-terminal `waiting` state so it stays resumable, never `failed`.
        self.assertEqual(result.outcome.stopped_reason, "idle")
        self.assertEqual(len(result.outcome.waiting), 1)
        self.assertEqual(result.status, RunStatus.waiting)
        self.assertEqual(result.run.status, RunStatus.waiting)
        self.assertEqual(stored.status, RunStatus.waiting)

    def test_run_correlation_path_is_idempotent_on_a_terminal_run(self) -> None:
        correlation_path = CorrelationPathSpec(
            id="correlation_path_chain",
            effectors=[
                _effector("a", "tests.test_fala_run_correlation_path._double"),
                _effector("b", "tests.test_fala_run_correlation_path._sum_conduction", conduction=["a"]),
            ],
        )

        async def scenario(root: Path):
            service = _bare_service(root)
            run = Run(id="rf_replay")
            first = await run_correlation_path(
                service,
                run=run,
                correlation_path=correlation_path,
                worker_id="tester",
                effector_inputs={"a": {"value": 5}},
                max_ticks=1,
            )
            # Same run, same correlation_path, again: create_run replays the terminal run.
            second = await run_correlation_path(
                service,
                run=run,
                correlation_path=correlation_path,
                worker_id="tester",
                effector_inputs={"a": {"value": 5}},
                max_ticks=1,
            )
            effector_b = await service.backend.get_process(
                run_id="rf_replay",
                process_id=f"{second.correlation_path.correlation_path_id}:b",
            )
            return first, second, effector_b

        with tempfile.TemporaryDirectory() as tmp_dir:
            first, second, effector_b = asyncio.run(scenario(Path(tmp_dir)))

        # One tick cannot drive a->b, so the first pass times the run out.
        self.assertEqual(first.status, RunStatus.timed_out)
        # Re-invoking on the now-terminal run must neither re-drive the
        # leftover-ready effector nor attempt an illegal terminal->terminal
        # transition: it returns the finished run untouched.
        self.assertEqual(second.status, RunStatus.timed_out)
        self.assertEqual(second.run.status, RunStatus.timed_out)
        self.assertEqual(second.outcome.stopped_reason, "already_terminal")
        self.assertEqual(second.outcome.ticks, 0)
        # The early-terminal branch still exposes the run's processes, so a reader
        # of a replayed terminal run sees the same effectors as the first pass.
        self.assertTrue(second.processes)
        self.assertEqual(
            {p.id for p in second.processes},
            {p.id for p in first.processes},
        )
        # `b` was left untouched -- never claimed or completed on the terminal run.
        self.assertEqual(effector_b.status, ProcessStatus.ready)


if __name__ == "__main__":
    unittest.main()
