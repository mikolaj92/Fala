from __future__ import annotations

import asyncio
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fala.carrier_runtime import FalaRuntime
from fala.runtime_backend import (
    Artifact,
    CarrierRunStatus,
    Run,
    RuntimeArtifactBlob,
    RuntimeArtifactStore,
    RuntimeBackendService,
    RuntimeCommand,
    SQLiteRuntimeBackend,
)


class TestJournalMaintenance(unittest.TestCase):
    def test_maintain_journal_dry_run_has_no_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime.db"
            service = RuntimeBackendService(SQLiteRuntimeBackend(db_path))

            async def run() -> tuple[object, object | None]:
                old_run = Run(id="old", status=CarrierRunStatus.completed)
                await service.create_run(
                    old_run,
                    idempotency_key="old:create",
                    actor="test",
                )
                with sqlite3.connect(db_path) as connection:
                    connection.execute(
                        "UPDATE runs SET created_at = ?, updated_at = ?, finished_at = ? WHERE id = ?",
                        ("2000-01-01T00:00:00+00:00",) * 3 + ("old",),
                    )
                plan = await service.maintain_journal(older_than_days=1.0, dry_run=True)
                return plan, await service.backend.get_run(run_id="old")

            plan, old_run = asyncio.run(run())
            self.assertTrue(plan.dry_run)
            self.assertEqual(plan.runs_archived, 0)
            self.assertIsNotNone(plan.retention)
            self.assertEqual(plan.retention.candidate_count, 1)
            self.assertIsNotNone(old_run)

    def test_maintain_journal_deletes_old_terminal_runs_and_vacuums(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime.db"
            service = RuntimeBackendService(SQLiteRuntimeBackend(db_path))

            async def run() -> tuple[object, object | None, object | None]:
                for run_id in ("old", "new"):
                    await service.create_run(
                        Run(id=run_id, status=CarrierRunStatus.completed),
                        idempotency_key=f"{run_id}:create",
                        actor="test",
                    )
                with sqlite3.connect(db_path) as connection:
                    connection.execute(
                        "UPDATE runs SET created_at = ?, updated_at = ?, finished_at = ? WHERE id = ?",
                        ("2000-01-01T00:00:00+00:00",) * 3 + ("old",),
                    )
                plan = await service.maintain_journal(
                    older_than_days=1.0,
                    dry_run=False,
                    vacuum=True,
                )
                return (
                    plan,
                    await service.backend.get_run(run_id="old"),
                    await service.backend.get_run(run_id="new"),
                )

            plan, old_run, new_run = asyncio.run(run())
            self.assertFalse(plan.dry_run)
            self.assertEqual(plan.runs_archived, 1)
            self.assertIsNone(old_run)
            self.assertIsNotNone(new_run)
            self.assertIsNotNone(plan.vacuum_result)
            self.assertTrue(plan.vacuum_result["vacuumed"])

    def test_keep_last_preserves_most_recent_terminal_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime.db"
            service = RuntimeBackendService(SQLiteRuntimeBackend(db_path))

            async def run() -> tuple[object | None, object | None, object | None]:
                for run_id in ("old_1", "old_2", "old_3"):
                    await service.create_run(
                        Run(id=run_id, status=CarrierRunStatus.completed),
                        idempotency_key=f"{run_id}:create",
                        actor="test",
                    )
                with sqlite3.connect(db_path) as connection:
                    for i, run_id in enumerate(("old_1", "old_2", "old_3"), start=1):
                        ts = f"2000-01-0{i}T00:00:00+00:00"
                        connection.execute(
                            "UPDATE runs SET created_at = ?, updated_at = ?, finished_at = ? WHERE id = ?",
                            (ts, ts, ts, run_id),
                        )
                await service.maintain_journal(
                    older_than_days=1.0,
                    keep_last=1,
                    dry_run=False,
                    vacuum=False,
                )
                return (
                    await service.backend.get_run(run_id="old_1"),
                    await service.backend.get_run(run_id="old_2"),
                    await service.backend.get_run(run_id="old_3"),
                )

            old_1, old_2, old_3 = asyncio.run(run())
            self.assertIsNone(old_1)
            self.assertIsNone(old_2)
            self.assertIsNotNone(old_3)

    def test_artifact_gc_removes_unreferenced_blobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime.db"
            blob_dir = Path(tmp) / "artifacts" / "blobs" / "sha256"
            referenced_digest = "a" * 64
            orphan_digest = "b" * 64
            referenced_path = blob_dir / referenced_digest[:2] / referenced_digest
            orphan_path = blob_dir / orphan_digest[:2] / orphan_digest
            referenced_path.parent.mkdir(parents=True, exist_ok=True)
            orphan_path.parent.mkdir(parents=True, exist_ok=True)
            referenced_path.write_bytes(b"keep")
            orphan_path.write_bytes(b"delete")
            store = RuntimeArtifactStore(
                root=Path(tmp) / "artifacts",
                blobs={
                    referenced_digest: RuntimeArtifactBlob(referenced_digest, 4, str(referenced_path)),
                    orphan_digest: RuntimeArtifactBlob(orphan_digest, 6, str(orphan_path)),
                },
            )
            service = RuntimeBackendService(SQLiteRuntimeBackend(db_path))

            async def run() -> object:
                await service.create_run(
                    Run(id="run", status=CarrierRunStatus.completed),
                    idempotency_key="run:create",
                    actor="test",
                )
                await service.record_artifact(
                    Artifact(
                        id="artifact_keep",
                        run_id="run",
                        kind="text",
                        uri=f"fala-artifact://sha256/{referenced_digest}",
                        content_hash=f"sha256:{referenced_digest}",
                    ),
                    idempotency_key="artifact:record",
                    actor="test",
                )
                return await service.maintain_journal(
                    older_than_days=1.0,
                    dry_run=False,
                    vacuum=False,
                    artifact_store=store,
                )

            plan = asyncio.run(run())
            self.assertTrue(referenced_path.exists())
            self.assertFalse(orphan_path.exists())
            self.assertEqual(plan.artifact_gc.deleted_count, 1)
            self.assertEqual(plan.bytes_reclaimed, 6)

    def test_embedded_facade_exposes_maintain_journal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runtime = FalaRuntime.sqlite(Path(tmp) / "runtime.db")

            async def run() -> object:
                return await runtime.maintain_journal(older_than_days=1.0, dry_run=True)

            plan = asyncio.run(run())
            self.assertTrue(plan.dry_run)
            self.assertEqual(plan.older_than_days, 1.0)


if __name__ == "__main__":
    unittest.main()
