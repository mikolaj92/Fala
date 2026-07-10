from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from fala.driver import RunUntilIdleResult, process_error_text, run_until_idle
from fala.correlation_paths import instantiate_correlation_path
from fala.models import EffectorAdapterSpec, CorrelationPathSpec, EffectorSpec
from fala.runtime_backend import (
    ProcessStatus,
    Process,
    Run,
    RuntimeBackendService,
)


def _driver_double(request) -> dict:
    return {"value": request.input["value"] * 2}


def _driver_sum_conduction(request) -> dict:
    conduction = request.input["conduction"]
    return {"total": sum(item["value"] for item in conduction.values())}


def _driver_boom(request) -> dict:
    raise RuntimeError("boom")


def _python_effector(
    effector_id: str,
    ref: str,
    *,
    conduction: list[str] | None = None,
) -> EffectorSpec:
    return EffectorSpec(
        id=effector_id,
        capability="python_function",
        adapter=EffectorAdapterSpec(kind="python_function", ref=ref),
        conduction=conduction or [],
    )


async def _service(root: Path, run_id: str) -> RuntimeBackendService:
    service = RuntimeBackendService.sqlite(root / "state.sqlite")
    await service.create_run(Run(id=run_id), idempotency_key=f"{run_id}:create")
    return service


class FalaDriverTests(unittest.TestCase):
    def test_run_until_idle_validates_arguments(self) -> None:
        async def scenario(root: Path, **kwargs) -> RunUntilIdleResult:
            service = await _service(root, "run_args")
            return await run_until_idle(service, worker_id="tester", **kwargs)

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            with self.assertRaises(ValueError) as ticks:
                asyncio.run(scenario(root / "a", max_ticks=0))
            self.assertIn("max_ticks", str(ticks.exception))
            with self.assertRaises(ValueError) as lease:
                asyncio.run(scenario(root / "b", lease_seconds=0))
            self.assertIn("lease_seconds", str(lease.exception))

    def test_run_until_idle_executes_correlation_path_end_to_end_with_conduction(self) -> None:
        correlation_path = CorrelationPathSpec(
            id="correlation_path_diamond",
            effectors=[
                _python_effector("left", "tests.test_fala_driver._driver_double"),
                _python_effector("right", "tests.test_fala_driver._driver_double"),
                _python_effector(
                    "join",
                    "tests.test_fala_driver._driver_sum_conduction",
                    conduction=["left", "right"],
                ),
            ],
        )

        async def scenario(root: Path) -> tuple[RunUntilIdleResult, Process]:
            service = await _service(root, "run_correlation_path")
            await instantiate_correlation_path(
                service,
                run_id="run_correlation_path",
                correlation_path=correlation_path,
                effector_inputs={"left": {"value": 2}, "right": {"value": 3}},
            )
            result = await run_until_idle(service, worker_id="tester", run_id="run_correlation_path")
            join = await service.backend.get_process(
                run_id="run_correlation_path",
                process_id="run_correlation_path:correlation_path_diamond:join",
            )
            assert join is not None
            return result, join

        with tempfile.TemporaryDirectory() as tmp_dir:
            result, join = asyncio.run(scenario(Path(tmp_dir)))

        self.assertTrue(result.ok)
        self.assertEqual(result.stopped_reason, "idle")
        self.assertEqual(result.ticks, 3)
        self.assertEqual(len(result.completed), 3)
        self.assertEqual(result.failed, [])
        self.assertEqual(result.waiting, [])
        self.assertEqual(join.status, ProcessStatus.succeeded)
        self.assertEqual(join.output["total"], 10)

    def test_run_until_idle_leaves_correlation_path_pending_when_advance_disabled(self) -> None:
        async def scenario(root: Path) -> tuple[RunUntilIdleResult, Process]:
            service = await _service(root, "run_manual")
            await instantiate_correlation_path(
                service,
                run_id="run_manual",
                correlation_path=CorrelationPathSpec(
                    id="correlation_path_pair",
                    effectors=[
                        _python_effector("first", "tests.test_fala_driver._driver_double"),
                        _python_effector(
                            "second",
                            "tests.test_fala_driver._driver_sum_conduction",
                            conduction=["first"],
                        ),
                    ],
                ),
                effector_inputs={"first": {"value": 2}},
            )
            result = await run_until_idle(
                service,
                worker_id="tester",
                run_id="run_manual",
                advance_correlation_paths=False,
            )
            second = await service.backend.get_process(
                run_id="run_manual",
                process_id="run_manual:correlation_path_pair:second",
            )
            assert second is not None
            return result, second

        with tempfile.TemporaryDirectory() as tmp_dir:
            result, second = asyncio.run(scenario(Path(tmp_dir)))

        self.assertEqual(result.stopped_reason, "idle")
        self.assertEqual(len(result.completed), 1)
        self.assertEqual(second.status, ProcessStatus.pending)

    def test_run_until_idle_retries_then_fails_exhausted_process(self) -> None:
        async def scenario(root: Path) -> tuple[RunUntilIdleResult, Process]:
            service = await _service(root, "run_boom")
            await service.schedule_process(
                Process(
                    id="process_boom",
                    run_id="run_boom",
                    process_type="python_function",
                    status=ProcessStatus.ready,
                    max_attempts=2,
                    input={
                        "adapter": {
                            "kind": "python_function",
                            "ref": "tests.test_fala_driver._driver_boom",
                        },
                    },
                ),
                idempotency_key="run_boom:process.schedule:process_boom",
            )
            result = await run_until_idle(service, worker_id="tester", run_id="run_boom")
            process = await service.backend.get_process(
                run_id="run_boom",
                process_id="process_boom",
            )
            assert process is not None
            return result, process

        with tempfile.TemporaryDirectory() as tmp_dir:
            result, process = asyncio.run(scenario(Path(tmp_dir)))

        self.assertEqual(result.stopped_reason, "idle")
        self.assertEqual(result.ticks, 2)
        self.assertEqual(
            [item.status for item in result.failed],
            [ProcessStatus.retry_wait, ProcessStatus.failed],
        )
        self.assertEqual(process.status, ProcessStatus.failed)
        self.assertEqual(process.error["message"], "boom")

    def test_run_until_idle_manual_homeostat_waits_and_opens_homeostat(self) -> None:
        async def scenario(root: Path) -> tuple[RunUntilIdleResult, Process, object]:
            service = await _service(root, "run_homeostat")
            await service.schedule_process(
                Process(
                    id="process_homeostat",
                    run_id="run_homeostat",
                    process_type="manual_homeostat",
                    status=ProcessStatus.ready,
                    input={"adapter": {"kind": "manual_homeostat"}},
                ),
                idempotency_key="run_homeostat:process.schedule:process_homeostat",
            )
            result = await run_until_idle(service, worker_id="tester", run_id="run_homeostat")
            process = await service.backend.get_process(
                run_id="run_homeostat",
                process_id="process_homeostat",
            )
            assert process is not None
            homeostat = await service.backend.get_homeostat(
                run_id="run_homeostat",
                homeostat_id="homeostat:process_homeostat",
            )
            return result, process, homeostat

        with tempfile.TemporaryDirectory() as tmp_dir:
            result, process, homeostat = asyncio.run(scenario(Path(tmp_dir)))

        self.assertEqual(result.stopped_reason, "idle")
        self.assertEqual(len(result.waiting), 1)
        self.assertEqual(process.status, ProcessStatus.waiting)
        self.assertIsNotNone(homeostat)

    def test_run_until_idle_stops_at_max_ticks(self) -> None:
        async def scenario(root: Path) -> RunUntilIdleResult:
            service = await _service(root, "run_ticks")
            for index in range(2):
                await service.schedule_process(
                    Process(
                        id=f"process_{index}",
                        run_id="run_ticks",
                        process_type="python_function",
                        status=ProcessStatus.ready,
                        input={
                            "adapter": {
                                "kind": "python_function",
                                "ref": "tests.test_fala_driver._driver_double",
                            },
                            "value": index,
                        },
                    ),
                    idempotency_key=f"run_ticks:process.schedule:process_{index}",
                )
            return await run_until_idle(
                service,
                worker_id="tester",
                run_id="run_ticks",
                max_ticks=1,
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            result = asyncio.run(scenario(Path(tmp_dir)))

        self.assertFalse(result.ok)
        self.assertEqual(result.stopped_reason, "max_ticks")
        self.assertEqual(result.ticks, 1)
        self.assertEqual(len(result.completed), 1)

    def test_run_until_idle_stops_between_ticks_after_completing_effector(self) -> None:
        async def scenario(root: Path) -> tuple[RunUntilIdleResult, Process | None]:
            service = await _service(root, "run_stop")
            for index in range(2):
                await service.schedule_process(
                    Process(
                        id=f"process_{index}",
                        run_id="run_stop",
                        process_type="python_function",
                        status=ProcessStatus.ready,
                        input={
                            "adapter": {
                                "kind": "python_function",
                                "ref": "tests.test_fala_driver._driver_double",
                            },
                            "value": index,
                        },
                    ),
                    idempotency_key=f"run_stop:process.schedule:process_{index}",
                )

            checks = 0

            def should_stop() -> bool:
                nonlocal checks
                checks += 1
                return checks > 1

            result = await run_until_idle(
                service,
                worker_id="tester",
                run_id="run_stop",
                should_stop=should_stop,
            )
            remaining = await service.backend.get_process(
                run_id="run_stop",
                process_id="process_1",
            )
            return result, remaining

        with tempfile.TemporaryDirectory() as tmp_dir:
            result, remaining = asyncio.run(scenario(Path(tmp_dir)))

        self.assertTrue(result.ok)
        self.assertEqual(result.stopped_reason, "stopped")
        self.assertEqual(result.ticks, 1)
        self.assertEqual(len(result.completed), 1)
        self.assertEqual(result.completed[0].status, ProcessStatus.succeeded)
        self.assertIsNotNone(remaining)
        assert remaining is not None
        self.assertEqual(remaining.status, ProcessStatus.ready)

    def test_run_until_idle_writes_effector_work_dirs(self) -> None:
        async def scenario(root: Path) -> RunUntilIdleResult:
            service = await _service(root, "run_work")
            await service.schedule_process(
                Process(
                    id="process_work",
                    run_id="run_work",
                    process_type="python_function",
                    status=ProcessStatus.ready,
                    input={
                        "adapter": {
                            "kind": "python_function",
                            "ref": "tests.test_fala_driver._driver_double",
                        },
                        "value": 1,
                    },
                ),
                idempotency_key="run_work:process.schedule:process_work",
            )
            return await run_until_idle(
                service,
                worker_id="tester",
                run_id="run_work",
                work_dir=root / "work",
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            result = asyncio.run(scenario(root))
            self.assertTrue((root / "work" / "process_work").is_dir())

        self.assertEqual(len(result.completed), 1)


class ProcessErrorTextTests(unittest.TestCase):
    """The {type, message} envelope run_until_idle records, rendered to one line."""

    def _failed(self, error: dict) -> Process:
        return Process(
            id="p",
            run_id="r",
            process_type="python_function",
            status=ProcessStatus.failed,
            error=error,
        )

    def test_renders_type_and_message(self) -> None:
        self.assertEqual(
            process_error_text(
                self._failed({"type": "RuntimeError", "message": "boom"})
            ),
            "RuntimeError: boom",
        )

    def test_falls_back_to_bare_message_then_json_dump(self) -> None:
        # Message without a type: just the message.
        self.assertEqual(
            process_error_text(self._failed({"message": "boom"})),
            "boom",
        )
        # No message at all: a deterministic, sorted JSON dump of the envelope.
        self.assertEqual(
            process_error_text(self._failed({"code": 7, "at": "effector"})),
            '{"at": "effector", "code": 7}',
        )

    def test_sentinel_when_error_envelope_is_empty(self) -> None:
        self.assertEqual(
            process_error_text(self._failed({})),
            "unknown effector failure",
        )


if __name__ == "__main__":
    unittest.main()
