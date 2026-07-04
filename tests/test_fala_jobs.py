from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fala.jobs import BaseJob, JobStatus, enqueue_job, load_job, retry_job, run_job
from fala.manifests import load_run_manifests
from fala.runtime_backend import CarrierRunStatus, RuntimeBackendService

import asyncio

KEY = "test_job"
PREFIX = "test-job-"


class DemoJob(BaseJob):
    payload: str = ""
    result: str | None = None


class JobTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "runtime.sqlite3"

    def _enqueue(self, **overrides: object) -> DemoJob:
        job = DemoJob(job_id="job-1", payload="doc.docx", **overrides)  # type: ignore[arg-type]
        return enqueue_job(
            job, db_path=self.db_path, key=KEY, run_id_prefix=PREFIX, title="Demo job job-1"
        )

    def _load(self) -> DemoJob:
        return load_job(self.db_path, "job-1", key=KEY, run_id_prefix=PREFIX, job_type=DemoJob)

    def _run(self, execute) -> DemoJob:
        return run_job(
            self.db_path,
            "job-1",
            key=KEY,
            run_id_prefix=PREFIX,
            job_type=DemoJob,
            execute=execute,
        )

    def _retry(self, execute) -> DemoJob:
        return retry_job(
            self.db_path,
            "job-1",
            key=KEY,
            run_id_prefix=PREFIX,
            job_type=DemoJob,
            execute=execute,
        )

    def test_enqueue_records_initial_event_and_persists(self) -> None:
        job = self._enqueue()
        self.assertEqual(job.status, "queued")
        self.assertEqual(job.events[-1].message, "Job created and waiting in the queue.")
        loaded = self._load()
        self.assertEqual(loaded.payload, "doc.docx")
        run = asyncio.run(
            RuntimeBackendService.sqlite(self.db_path).backend.get_run(run_id="test-job-job-1")
        )
        assert run is not None
        self.assertIs(run.status, CarrierRunStatus.created)
        self.assertEqual(run.title, "Demo job job-1")
        self.assertEqual(len(load_run_manifests(self.db_path, KEY)), 1)

    def test_enqueue_scheduled_message(self) -> None:
        when = datetime.now(UTC) + timedelta(hours=1)
        job = self._enqueue(scheduled_for=when)
        self.assertEqual(
            job.events[-1].message,
            f"Job scheduled for {when.isoformat()} and waiting in the queue.",
        )

    def test_run_before_schedule_keeps_job_queued(self) -> None:
        when = datetime.now(UTC) + timedelta(hours=1)
        self._enqueue(scheduled_for=when)
        job = self._run(lambda job: ("completed", "done"))
        self.assertEqual(job.status, "queued")
        self.assertEqual(job.attempts, 0)
        self.assertEqual(job.events[-1].message, f"Job remains queued until {when.isoformat()}.")

    def test_run_success_records_execute_outcome(self) -> None:
        self._enqueue()

        def execute(job: DemoJob) -> tuple[JobStatus, str]:
            job.result = f"analyzed {job.payload}"
            return "completed", "Batch completed successfully."

        job = self._run(execute)
        self.assertEqual(job.status, "completed")
        self.assertEqual(job.attempts, 1)
        self.assertIsNone(job.last_error)
        self.assertEqual(job.events[-1].message, "Batch completed successfully.")
        loaded = self._load()
        self.assertEqual(loaded.result, "analyzed doc.docx")
        self.assertIn("Job attempt 1 started.", [event.message for event in loaded.events])

    def test_run_partial_failure_status_from_execute(self) -> None:
        self._enqueue()
        job = self._run(
            lambda job: ("partial_failure", "Batch completed with one or more isolated failures.")
        )
        self.assertEqual(job.status, "partial_failure")
        self.assertEqual(
            job.events[-1].message, "Batch completed with one or more isolated failures."
        )

    def test_run_failure_records_last_error(self) -> None:
        self._enqueue()

        def execute(job: DemoJob) -> tuple[JobStatus, str]:
            raise RuntimeError("boom")

        job = self._run(execute)
        self.assertEqual(job.status, "failed")
        self.assertEqual(job.last_error, "boom")
        self.assertEqual(job.events[-1].message, "Job execution failed: boom")

    def test_retry_requeues_and_runs_next_attempt(self) -> None:
        self._enqueue()

        def failing(job: DemoJob) -> tuple[JobStatus, str]:
            raise RuntimeError("boom")

        self._run(failing)
        job = self._retry(lambda job: ("completed", "Recovered."))
        self.assertEqual(job.status, "completed")
        self.assertEqual(job.attempts, 2)
        self.assertIsNone(job.last_error)
        self.assertIn("Job re-queued for retry.", [event.message for event in job.events])

    def test_retry_budget_guard_runs_before_status_guard(self) -> None:
        self._enqueue(max_retries=0)
        self._run(lambda job: ("completed", "done"))
        with self.assertRaises(ValueError) as caught:
            self._retry(lambda job: ("completed", "done"))
        self.assertEqual(str(caught.exception), "Job job-1 exhausted retry budget.")

    def test_retry_rejects_non_retryable_status(self) -> None:
        self._enqueue(max_retries=2)
        self._run(lambda job: ("completed", "done"))
        with self.assertRaises(ValueError) as caught:
            self._retry(lambda job: ("completed", "done"))
        self.assertEqual(
            str(caught.exception), "Job job-1 is not retryable from status completed."
        )

    def test_load_missing_job_fails_closed(self) -> None:
        self._enqueue()
        with self.assertRaises(ValueError) as caught:
            load_job(self.db_path, "nope", key=KEY, run_id_prefix=PREFIX, job_type=DemoJob)
        self.assertEqual(str(caught.exception), "Job not found: nope")

    def test_subclass_fields_survive_base_type_round_trip(self) -> None:
        self._enqueue()
        loaded = load_job(self.db_path, "job-1", key=KEY, run_id_prefix=PREFIX, job_type=BaseJob)
        self.assertEqual(loaded.model_dump()["payload"], "doc.docx")


if __name__ == "__main__":
    unittest.main()
