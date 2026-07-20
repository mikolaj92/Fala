from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

from fala.adapters import EffectorRunRequest, create_effector_adapter
from fala.reactions import content_address_json
from fala.cli import _structural_diff
from fala.models import EffectorAdapterSpec
from fala.runtime_backend import (
    ProcessStatus,
    Impulse,
    Process,
    Reaction,
    Run,
    RuntimeBackendService,
)

_ENV_DUMP_SCRIPT = """
import json
import os
from pathlib import Path

output = Path(os.environ["FALA_EFFECTOR_OUTPUT_DIR"])
output.mkdir(parents=True, exist_ok=True)
(output / "result.json").write_text(json.dumps({
    "has_path": "PATH" in os.environ,
    "leaked": os.environ.get("FALA_TEST_AMBIENT"),
    "inherited": os.environ.get("FALA_TEST_INHERITED"),
}))
""".strip()


class TestSubprocessEnvAllowlist(unittest.TestCase):
    def _run_env_dump(self, adapter_spec: EffectorAdapterSpec) -> dict:
        async def scenario() -> dict:
            with tempfile.TemporaryDirectory() as tmp:
                script = Path(tmp) / "effector.py"
                script.write_text(_ENV_DUMP_SCRIPT, encoding="utf-8")
                spec = adapter_spec.model_copy(
                    update={"command": [sys.executable, str(script)]}
                )
                result = await create_effector_adapter("subprocess").run(
                    EffectorRunRequest(process_id="process_env", adapter=spec)
                )
                return result.output

        return asyncio.run(scenario())

    def test_ambient_environment_is_withheld_by_default(self) -> None:
        os.environ["FALA_TEST_AMBIENT"] = "should-not-leak"
        try:
            output = self._run_env_dump(
                EffectorAdapterSpec(
                    kind="subprocess",
                    command=["placeholder"],
                    timeout_seconds=10,
                )
            )
        finally:
            os.environ.pop("FALA_TEST_AMBIENT", None)
        self.assertTrue(output["has_path"])
        self.assertIsNone(output["leaked"])

    def test_inherit_env_passes_named_variables_through(self) -> None:
        os.environ["FALA_TEST_INHERITED"] = "explicitly-allowed"
        try:
            output = self._run_env_dump(
                EffectorAdapterSpec(
                    kind="subprocess",
                    command=["placeholder"],
                    inherit_env=["FALA_TEST_INHERITED"],
                    timeout_seconds=10,
                )
            )
        finally:
            os.environ.pop("FALA_TEST_INHERITED", None)
        self.assertEqual(output["inherited"], "explicitly-allowed")

    def test_inherit_env_rejected_outside_subprocess(self) -> None:
        with self.assertRaisesRegex(ValueError, "cannot define inherit_env"):
            EffectorAdapterSpec(
                kind="python_function",
                ref="tests.test_fala_replay_lab._nope",
                inherit_env=["FALA_TEST_INHERITED"],
            )


class TestAttemptFingerprints(unittest.TestCase):
    def test_completion_and_failure_events_carry_attempt_digests(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = RuntimeBackendService.sqlite(Path(tmp) / "runtime.db")

            async def run() -> None:
                await service.create_run(Run(id="run_lab"), idempotency_key="run.create")
                await service.accept_impulse(
                    Impulse(id="impulse_lab", run_id="run_lab", impulse_type="case"),
                    idempotency_key="impulse.accept:impulse_lab",
                )
                for process_id in ("process_done", "process_broken"):
                    await service.schedule_process(
                        Process(
                            id=process_id,
                            run_id="run_lab",
                            impulse_id="impulse_lab",
                            process_type="score",
                            status=ProcessStatus.ready,
                            input={"value": 41},
                        ),
                        idempotency_key=f"process.schedule:{process_id}",
                    )
                    claimed = await service.claim_next_ready_process(
                        worker_id="worker", run_id="run_lab"
                    )
                    assert claimed is not None
                await service.complete_process(
                    run_id="run_lab",
                    process_id="process_done",
                    output={"value": 42},
                    idempotency_key="process.complete:process_done",
                    actor="worker",
                )
                await service.fail_process(
                    run_id="run_lab",
                    process_id="process_broken",
                    error={"message": "boom"},
                    idempotency_key="process.fail:process_broken",
                    actor="worker",
                )

                events = await service.backend.list_events(run_id="run_lab")
                by_type = {event.event_type: event for event in events}
                completed = by_type["process.completed"].payload
                self.assertEqual(completed["attempt"], 1)
                self.assertEqual(
                    completed["input_digest"], content_address_json({"value": 41})
                )
                self.assertEqual(
                    completed["output_digest"], content_address_json({"value": 42})
                )
                failed = by_type["process.failed"].payload
                self.assertEqual(
                    failed["error_digest"], content_address_json({"message": "boom"})
                )
                self.assertEqual(
                    failed["input_digest"], content_address_json({"value": 41})
                )

            asyncio.run(run())

    def test_reaction_recorded_event_carries_content_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = RuntimeBackendService.sqlite(Path(tmp) / "runtime.db")

            async def run() -> None:
                await service.create_run(Run(id="run_art"), idempotency_key="run.create")
                await service.accept_impulse(
                    Impulse(id="impulse_art", run_id="run_art", impulse_type="case"),
                    idempotency_key="impulse.accept:impulse_art",
                )
                await service.record_reaction(
                    Reaction(
                        id="reaction_art",
                        run_id="run_art",
                        impulse_id="impulse_art",
                        kind="report",
                        uri="fala-reaction://sha256/abc123",
                        content_hash="sha256:abc123",
                    ),
                    idempotency_key="reaction.record:reaction_art",
                )
                events = await service.backend.list_events(run_id="run_art")
                recorded = next(
                    event for event in events if event.event_type == "reaction.recorded"
                )
                self.assertEqual(recorded.payload["content_hash"], "sha256:abc123")

            asyncio.run(run())


class TestStructuralDiff(unittest.TestCase):
    def test_structural_diff_reports_leaf_paths(self) -> None:
        recorded = {
            "value": 1,
            "only_recorded": True,
            "nested": {"list": [1, 2], "same": "x"},
        }
        rerun = {
            "value": 2,
            "only_rerun": True,
            "nested": {"list": [1, 2, 3], "same": "x"},
        }
        diff = _structural_diff(recorded, rerun)
        by_path = {entry["path"]: entry for entry in diff}
        self.assertEqual(
            set(by_path),
            {"$.value", "$.only_recorded", "$.only_rerun", "$.nested.list[2]"},
        )
        self.assertEqual(by_path["$.value"]["change"], "changed")
        self.assertEqual(by_path["$.only_recorded"]["change"], "removed")
        self.assertEqual(by_path["$.only_rerun"]["change"], "added")
        self.assertEqual(by_path["$.nested.list[2]"]["change"], "added")
        self.assertEqual(_structural_diff(recorded, json.loads(json.dumps(recorded))), [])


if __name__ == "__main__":
    unittest.main()
