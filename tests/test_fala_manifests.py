from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from fala.manifests import (
    attach_run_manifest,
    fetch_run_manifests,
    get_run_manifest,
    load_run_manifests,
    put_run_manifest,
    upsert_run_manifest,
)
from fala.runtime_backend import CarrierRunStatus, Run, RuntimeBackendService


class RunManifestTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "runtime.sqlite3"
        self.service = RuntimeBackendService.sqlite(self.db_path)

    def test_attach_requires_existing_run(self) -> None:
        with self.assertRaises(ValueError) as caught:
            asyncio.run(
                attach_run_manifest(self.service, run_id="missing", key="k", manifest={"a": 1})
            )
        self.assertIn("'missing' not found in the state store", str(caught.exception))

    def test_attach_merges_metadata_and_preserves_other_keys(self) -> None:
        asyncio.run(self.service.backend.put_run(Run(id="run-1", metadata={"other": {"keep": 1}})))
        asyncio.run(
            attach_run_manifest(self.service, run_id="run-1", key="k", manifest={"a": 1})
        )
        stored = asyncio.run(self.service.backend.get_run(run_id="run-1"))
        assert stored is not None
        self.assertEqual(stored.metadata["k"], {"a": 1})
        self.assertEqual(stored.metadata["other"], {"keep": 1})

    def test_put_creates_synthetic_completed_run(self) -> None:
        asyncio.run(
            put_run_manifest(
                self.service, run_id="run-2", key="k", manifest={"a": 2}, title="Synthetic"
            )
        )
        stored = asyncio.run(self.service.backend.get_run(run_id="run-2"))
        assert stored is not None
        self.assertIs(stored.status, CarrierRunStatus.completed)
        self.assertEqual(stored.title, "Synthetic")
        self.assertEqual(stored.metadata["k"], {"a": 2})

    def test_upsert_creates_then_updates_preserving_other_keys(self) -> None:
        asyncio.run(
            upsert_run_manifest(self.service, run_id="run-3", key="k", manifest={"v": 1})
        )
        created = asyncio.run(self.service.backend.get_run(run_id="run-3"))
        assert created is not None
        self.assertIs(created.status, CarrierRunStatus.created)

        metadata = dict(created.metadata)
        metadata["other"] = {"keep": 1}
        asyncio.run(self.service.backend.put_run(created.model_copy(update={"metadata": metadata})))
        asyncio.run(
            upsert_run_manifest(self.service, run_id="run-3", key="k", manifest={"v": 2})
        )
        updated = asyncio.run(self.service.backend.get_run(run_id="run-3"))
        assert updated is not None
        self.assertEqual(updated.metadata["k"], {"v": 2})
        self.assertEqual(updated.metadata["other"], {"keep": 1})

    def test_get_returns_none_for_missing_run_or_non_dict_manifest(self) -> None:
        self.assertIsNone(
            asyncio.run(get_run_manifest(self.service, run_id="missing", key="k"))
        )
        asyncio.run(self.service.backend.put_run(Run(id="run-4", metadata={"k": "not-a-dict"})))
        self.assertIsNone(asyncio.run(get_run_manifest(self.service, run_id="run-4", key="k")))

    def test_fetch_and_load_skip_runs_without_manifest(self) -> None:
        asyncio.run(self.service.backend.put_run(Run(id="plain")))
        asyncio.run(put_run_manifest(self.service, run_id="run-5", key="k", manifest={"v": 5}))
        self.assertEqual(asyncio.run(fetch_run_manifests(self.service, "k")), [{"v": 5}])
        self.assertEqual(load_run_manifests(self.db_path, "k"), [{"v": 5}])


if __name__ == "__main__":
    unittest.main()
