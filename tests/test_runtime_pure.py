"""Unit tests for pure process/run transition and claim helpers (PR2 / #74)."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from fala.runtime_backend import Process, ProcessStatus, RunStatus, RuntimeCommand
from fala.runtime_pure import (
    apply_lease_expired_fail,
    apply_process_claim,
    lease_expired_error,
    process_is_claimable,
    process_lease_expired_no_retries,
    process_transition_command_type,
    select_next_claimable,
    validate_process_can_finish,
    validate_process_can_ready,
    validate_process_can_retry,
    validate_process_can_wait,
    validate_process_transition_command,
    validate_run_status_transition,
)


def _now() -> datetime:
    return datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _process(**kwargs: object) -> Process:
    base = {
        "run_id": "run_1",
        "process_type": "effector",
        "status": ProcessStatus.ready,
        "attempt": 0,
        "max_attempts": 3,
        "available_at": _now(),
        "created_at": _now(),
    }
    base.update(kwargs)
    return Process.model_validate(base)


class RuntimePureTests(unittest.TestCase):
    def test_run_status_transition_matrix(self) -> None:
        validate_run_status_transition(RunStatus.created, RunStatus.active)
        with self.assertRaises(ValueError):
            validate_run_status_transition(RunStatus.completed, RunStatus.active)
        with self.assertRaises(ValueError):
            validate_run_status_transition(RunStatus.active, RunStatus.created)

    def test_process_transition_command_types(self) -> None:
        self.assertEqual(
            process_transition_command_type(ProcessStatus.succeeded),
            "process.complete",
        )
        cmd = RuntimeCommand(
            run_id="run_1",
            command_type="process.complete",
            idempotency_key="k",
        )
        validate_process_transition_command(
            run_id="run_1", status=ProcessStatus.succeeded, command=cmd
        )
        with self.assertRaises(ValueError):
            validate_process_transition_command(
                run_id="run_1",
                status=ProcessStatus.succeeded,
                command=cmd.model_copy(update={"command_type": "process.fail"}),
            )

    def test_finish_requires_running_or_waiting_and_lease(self) -> None:
        running = _process(
            status=ProcessStatus.running, lease_owner="w1", attempt=1
        )
        validate_process_can_finish(running, actor="w1")
        with self.assertRaises(ValueError):
            validate_process_can_finish(running, actor="other")
        with self.assertRaises(ValueError):
            validate_process_can_finish(
                _process(status=ProcessStatus.ready), actor=None
            )

    def test_retry_and_wait_and_ready_guards(self) -> None:
        validate_process_can_retry(
            _process(status=ProcessStatus.running, lease_owner="w", attempt=1),
            actor="w",
        )
        with self.assertRaises(ValueError):
            validate_process_can_retry(
                _process(status=ProcessStatus.running, attempt=3, max_attempts=3),
                actor=None,
            )
        validate_process_can_wait(_process(status=ProcessStatus.running))
        with self.assertRaises(ValueError):
            validate_process_can_wait(_process(status=ProcessStatus.ready))
        validate_process_can_ready(_process(status=ProcessStatus.pending))
        with self.assertRaises(ValueError):
            validate_process_can_ready(_process(status=ProcessStatus.ready))

    def test_claimable_and_lease_reap_eligibility(self) -> None:
        now = _now()
        self.assertTrue(process_is_claimable(_process(status=ProcessStatus.ready), now=now))
        self.assertTrue(
            process_is_claimable(
                _process(
                    status=ProcessStatus.retry_wait,
                    available_at=now - timedelta(seconds=1),
                ),
                now=now,
            )
        )
        self.assertFalse(
            process_is_claimable(
                _process(
                    status=ProcessStatus.retry_wait,
                    available_at=now + timedelta(hours=1),
                ),
                now=now,
            )
        )
        expired_running = _process(
            status=ProcessStatus.running,
            attempt=1,
            max_attempts=3,
            lease_expires_at=now - timedelta(seconds=1),
        )
        self.assertTrue(process_is_claimable(expired_running, now=now))
        no_retries = _process(
            status=ProcessStatus.running,
            attempt=3,
            max_attempts=3,
            lease_owner="w",
            lease_expires_at=now - timedelta(seconds=1),
        )
        self.assertFalse(process_is_claimable(no_retries, now=now))
        self.assertTrue(process_lease_expired_no_retries(no_retries, now=now))
        err = lease_expired_error(no_retries)
        self.assertEqual(err["type"], "lease_expired")
        failed = apply_lease_expired_fail(no_retries, now=now)
        self.assertEqual(failed.status, ProcessStatus.failed)
        self.assertIsNone(failed.lease_owner)

    def test_apply_claim_and_select_order(self) -> None:
        now = _now()
        low = _process(id="p_low", priority=1, available_at=now)
        high = _process(id="p_high", priority=10, available_at=now)
        claimed = apply_process_claim(high, worker_id="w1", lease_seconds=60, now=now)
        self.assertEqual(claimed.status, ProcessStatus.running)
        self.assertEqual(claimed.attempt, 1)
        self.assertEqual(claimed.lease_owner, "w1")
        chosen = select_next_claimable([low, high], now=now)
        assert chosen is not None
        self.assertEqual(chosen.id, "p_high")
        with self.assertRaises(ValueError):
            apply_process_claim(high, worker_id="w", lease_seconds=0, now=now)


if __name__ == "__main__":
    unittest.main()
