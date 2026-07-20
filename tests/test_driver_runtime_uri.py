"""Driver runtime_uri / separate journal tests (PR7 / #79)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fala import (
    AutonomousCorrelator,
    Impulse,
    InMemoryJournal,
    JournalBackedBackend,
    JournalConfig,
    Process,
    ProcessStatus,
    Run,
)
from fala.adapters import EffectorRunRequest
from fala.driver import backend_runtime_uri, enqueue_fala_runtime_process
from fala.models import EffectorAdapterSpec
from fala.runtime import _default_impulse_reaction_root
from fala.runtime_backend import Correlator


class DriverRuntimeUriTests(unittest.IsolatedAsyncioTestCase):
    def test_backend_runtime_uri_from_journal_backed(self) -> None:
        backend = JournalBackedBackend(InMemoryJournal())
        self.assertTrue(backend_runtime_uri(backend).startswith("memory://"))

    def test_backend_runtime_uri_from_correlator(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "db.sqlite"
            backend = Correlator(path)
            uri = backend_runtime_uri(backend)
            self.assertTrue(uri.startswith("sqlite://"))
            self.assertIn("db.sqlite", uri)

    def test_reaction_root_from_sqlite_journal_backend(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.sqlite"
            runtime = AutonomousCorrelator.from_journal(path)
            root = _default_impulse_reaction_root(runtime.backend)
            self.assertEqual(root, path.resolve().parent / "reactions")

    async def test_enqueue_fala_runtime_on_memory_journal(self) -> None:
        parent = AutonomousCorrelator.from_journal(InMemoryJournal(stream_id="memory://parent"))
        child = AutonomousCorrelator.from_journal(InMemoryJournal(stream_id="memory://child"))
        self.assertNotEqual(parent.runtime_uri, child.runtime_uri)

        await parent.create_run(Run(id="run_parent"), idempotency_key="c")
        impulse = Impulse(id="imp_1", run_id="run_parent", impulse_type="case")
        await parent.accept_impulse(impulse, idempotency_key="a")
        process = Process(
            id="proc_1",
            run_id="run_parent",
            impulse_id=impulse.id,
            process_type="delegate",
            status=ProcessStatus.running,
            attempt=1,
            lease_owner="worker",
            input={
                "adapter": {
                    "kind": "fala_runtime",
                    "runtime_ref": child.runtime_uri,
                },
                "config": {},
            },
        )
        await parent.backend.put_process(process)
        # Keep process claimable state consistent for enqueue (running is fine)
        request = EffectorRunRequest(
            process_id=process.id,
            impulse_id=impulse.id,
            adapter=EffectorAdapterSpec(
                kind="fala_runtime",
                runtime_ref=child.runtime_uri,
            ),
            input={},
            config={},
        )
        result = await enqueue_fala_runtime_process(
            service=parent.service,
            process=process,
            request=request,
            actor="worker",
        )
        self.assertTrue(result.waiting)
        self.assertEqual(result.output["status"], "submitted")
        outbox = await parent.service.list_outbox_deliveries(run_id="run_parent")
        self.assertEqual(len(outbox), 1)
        self.assertEqual(outbox[0].source.runtime.uri, parent.runtime_uri)


if __name__ == "__main__":
    unittest.main()
