from __future__ import annotations

import asyncio
import sys
import tempfile
import unittest
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fala import CarrierProcessStatus, FalaRuntime, Run
from fala.models import CarrierWorkflowPackageSpec


def _package() -> CarrierWorkflowPackageSpec:
    return CarrierWorkflowPackageSpec.model_validate(
        {
            "id": "flow_package",
            "capabilities": [
                {"id": "cap_ingest"},
                {"id": "cap_enrich"},
                {"id": "cap_export"},
            ],
            "flows": [
                {
                    "id": "enrichment",
                    "steps": [
                        {
                            "id": "ingest",
                            "capability": "cap_ingest",
                            "adapter": {
                                "kind": "python_function",
                                "ref": "steps.ingest",
                            },
                        },
                        {
                            "id": "enrich",
                            "capability": "cap_enrich",
                            "adapter": {
                                "kind": "python_function",
                                "ref": "steps.enrich",
                            },
                            "needs": ["ingest"],
                            "config": {"mode": "fast"},
                        },
                        {
                            "id": "export",
                            "capability": "cap_export",
                            "adapter": {
                                "kind": "python_function",
                                "ref": "steps.export",
                            },
                            "needs": ["ingest", "enrich"],
                        },
                    ],
                }
            ],
        }
    )


class FlowOrchestrationTests(unittest.TestCase):
    def _runtime(self, tmp_dir: str) -> FalaRuntime:
        return FalaRuntime.sqlite(Path(tmp_dir) / "fala.sqlite")

    async def _instantiated_runtime(self, tmp_dir: str) -> FalaRuntime:
        runtime = self._runtime(tmp_dir)
        await runtime.create_run(
            Run(id="run_flow"),
            idempotency_key="run_flow:run.create",
        )
        await runtime.instantiate_flow(
            _package(),
            "enrichment",
            run_id="run_flow",
            values={"source": "s3://bucket/data"},
        )
        return runtime

    def test_instantiate_flow_schedules_processes_with_needs_statuses(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = await self._instantiated_runtime(tmp_dir)
                processes = {
                    process.metadata["flow_step_id"]: process
                    for process in await runtime.list_processes(run_id="run_flow")
                }

                self.assertEqual(
                    set(processes), {"ingest", "enrich", "export"}
                )
                self.assertEqual(
                    processes["ingest"].status, CarrierProcessStatus.ready
                )
                self.assertEqual(
                    processes["enrich"].status, CarrierProcessStatus.pending
                )
                self.assertEqual(
                    processes["export"].status, CarrierProcessStatus.pending
                )
                self.assertEqual(
                    processes["ingest"].input["adapter"]["kind"], "python_function"
                )
                self.assertEqual(
                    processes["ingest"].input["source"], "s3://bucket/data"
                )
                self.assertEqual(processes["enrich"].input["config"], {"mode": "fast"})
                self.assertEqual(processes["enrich"].process_type, "cap_enrich")
                self.assertEqual(
                    processes["export"].metadata["flow_needs"], ["ingest", "enrich"]
                )

                replayed = await runtime.service.instantiate_flow(
                    _package(), "enrichment", run_id="run_flow"
                )
                self.assertEqual(len(replayed), 3)
                self.assertEqual(
                    len(await runtime.list_processes(run_id="run_flow")), 3
                )

        asyncio.run(scenario())

    def test_complete_process_readies_dependents_and_injects_needs(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = await self._instantiated_runtime(tmp_dir)

                ingest = await runtime.claim_next_ready_process(worker_id="w1")
                self.assertEqual(ingest.metadata["flow_step_id"], "ingest")
                await runtime.complete_process(
                    run_id="run_flow",
                    process_id=ingest.id,
                    output={"values": {"source": "s3://bucket/data", "chars": 18}},
                    idempotency_key=f"run_flow:process.complete:{ingest.id}:0",
                )

                enrich = await runtime.claim_next_ready_process(worker_id="w1")
                self.assertIsNotNone(enrich)
                self.assertEqual(enrich.metadata["flow_step_id"], "enrich")
                self.assertEqual(
                    enrich.input["needs"],
                    {"ingest": {"source": "s3://bucket/data", "chars": 18}},
                )

                export = await runtime.backend.get_process(
                    run_id="run_flow",
                    process_id="process_enrichment_export",
                )
                self.assertEqual(export.status, CarrierProcessStatus.pending)
                self.assertEqual(
                    export.input["needs"],
                    {"ingest": {"source": "s3://bucket/data", "chars": 18}},
                )

                await runtime.complete_process(
                    run_id="run_flow",
                    process_id=enrich.id,
                    output={"values": {"label": "DATA"}},
                    idempotency_key=f"run_flow:process.complete:{enrich.id}:0",
                )

                export = await runtime.claim_next_ready_process(worker_id="w1")
                self.assertIsNotNone(export)
                self.assertEqual(export.metadata["flow_step_id"], "export")
                self.assertEqual(export.input["needs"]["enrich"], {"label": "DATA"})
                self.assertIsNone(
                    await runtime.claim_next_ready_process(worker_id="w1")
                )

        asyncio.run(scenario())

    def test_failed_need_never_readies_dependents(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = await self._instantiated_runtime(tmp_dir)

                ingest = await runtime.claim_next_ready_process(worker_id="w1")
                await runtime.fail_process(
                    run_id="run_flow",
                    process_id=ingest.id,
                    error={"message": "boom"},
                    idempotency_key=f"run_flow:process.fail:{ingest.id}:0",
                )

                self.assertIsNone(
                    await runtime.claim_next_ready_process(worker_id="w1")
                )
                for step_id in ("enrich", "export"):
                    process = await runtime.backend.get_process(
                        run_id="run_flow",
                        process_id=f"process_enrichment_{step_id}",
                    )
                    self.assertEqual(process.status, CarrierProcessStatus.pending)
                    self.assertNotIn("needs", process.input)

        asyncio.run(scenario())

    def test_instantiate_flow_rejects_unknown_and_cyclic_flows(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = self._runtime(tmp_dir)
                await runtime.create_run(
                    Run(id="run_flow"),
                    idempotency_key="run_flow:run.create",
                )
                with self.assertRaises(ValueError):
                    await runtime.instantiate_flow(
                        _package(), "missing", run_id="run_flow"
                    )

                cyclic = CarrierWorkflowPackageSpec.model_validate(
                    {
                        "id": "cyclic_package",
                        "capabilities": [{"id": "cap_a"}, {"id": "cap_b"}],
                        "flows": [
                            {
                                "id": "loop",
                                "allow_feedback_cycles": True,
                                "steps": [
                                    {
                                        "id": "a",
                                        "capability": "cap_a",
                                        "adapter": {
                                            "kind": "python_function",
                                            "ref": "steps.a",
                                        },
                                        "needs": ["b"],
                                    },
                                    {
                                        "id": "b",
                                        "capability": "cap_b",
                                        "adapter": {
                                            "kind": "python_function",
                                            "ref": "steps.b",
                                        },
                                        "needs": ["a"],
                                    },
                                ],
                            }
                        ],
                    }
                )
                with self.assertRaises(ValueError):
                    await runtime.instantiate_flow(cyclic, "loop", run_id="run_flow")

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
