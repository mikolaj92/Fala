"""Unit tests for journal batch types and Protocol shape (PR1)."""

from __future__ import annotations

import unittest
from typing import get_type_hints

from pydantic import ValidationError

from fala.journal import (
    KNOWN_STATE_FACT_ENTITIES,
    AppendResult,
    ClaimRequest,
    ClaimResult,
    CommandUnit,
    Journal,
    JournalBatch,
    StateFact,
    leading_command,
    leading_idempotency_key,
)
from fala.runtime_backend import Process, RuntimeCommand, RuntimeEvent


def _command(
    *,
    run_id: str = "run_1",
    command_type: str = "run.create",
    idempotency_key: str = "key_1",
) -> RuntimeCommand:
    return RuntimeCommand(
        run_id=run_id,
        command_type=command_type,
        idempotency_key=idempotency_key,
    )


def _event(
    *,
    run_id: str = "run_1",
    event_type: str = "run.created",
    command_id: str | None = None,
) -> RuntimeEvent:
    return RuntimeEvent(
        run_id=run_id,
        event_type=event_type,
        command_id=command_id,
    )


class JournalTypesTests(unittest.TestCase):
    def test_state_fact_upsert_round_trip(self) -> None:
        fact = StateFact(
            entity="run",
            op="upsert",
            key={"id": "run_1"},
            body={"id": "run_1", "status": "created"},
        )
        loaded = StateFact.model_validate(fact.model_dump(mode="json"))
        self.assertEqual(loaded.entity, "run")
        self.assertEqual(loaded.op, "upsert")
        self.assertEqual(loaded.key["id"], "run_1")
        self.assertIn("run", KNOWN_STATE_FACT_ENTITIES)

    def test_state_fact_rejects_unknown_op(self) -> None:
        with self.assertRaises(ValidationError):
            StateFact(
                entity="run",
                op="merge",  # type: ignore[arg-type]
                key={"id": "run_1"},
            )

    def test_command_unit_with_events_and_facts(self) -> None:
        command = _command()
        unit = CommandUnit(
            command=command,
            events=[_event(command_id=command.id)],
            facts=[
                StateFact(
                    entity="run",
                    op="upsert",
                    key={"id": "run_1"},
                    body={"id": "run_1"},
                )
            ],
        )
        self.assertEqual(len(unit.events), 1)
        self.assertEqual(len(unit.facts), 1)

    def test_journal_batch_requires_at_least_one_unit(self) -> None:
        with self.assertRaises(ValidationError):
            JournalBatch(run_id="run_1", units=[])

    def test_journal_batch_multi_unit_claim_shape(self) -> None:
        """claim_next style: fail reaps then claim unit."""
        batch = JournalBatch(
            run_id="run_1",
            units=[
                CommandUnit(
                    command=_command(
                        command_type="process.fail",
                        idempotency_key="process.fail:p1:1",
                    ),
                    events=[_event(event_type="process.failed")],
                ),
                CommandUnit(
                    command=_command(
                        command_type="process.claim",
                        idempotency_key="process.claim:p2:1",
                    ),
                    events=[_event(event_type="process.claimed")],
                ),
            ],
        )
        self.assertEqual(len(batch.units), 2)
        self.assertEqual(leading_command(batch).command_type, "process.fail")
        self.assertEqual(
            leading_idempotency_key(batch),
            ("run_1", "process.fail:p1:1"),
        )

    def test_append_result_replayed_primary_empty_events_contract(self) -> None:
        command = _command()
        batch = JournalBatch(
            run_id="run_1",
            units=[CommandUnit(command=command, events=[_event()])],
        )
        # On replay, AppendResult.units carry empty event lists (normative).
        result = AppendResult(
            batch=batch,
            replayed=True,
            units=[CommandUnit(command=command, events=[])],
        )
        self.assertTrue(result.replayed)
        self.assertEqual(result.units[0].events, [])

    def test_claim_request_defaults(self) -> None:
        req = ClaimRequest(worker_id="worker_a")
        self.assertEqual(req.lease_seconds, 300.0)
        self.assertFalse(req.all_runs)
        self.assertIsNone(req.run_id)

    def test_claim_result_none_when_idle(self) -> None:
        result = ClaimResult()
        self.assertIsNone(result.process)
        self.assertIsNone(result.batch)
        self.assertFalse(result.replayed)

    def test_claim_result_with_process(self) -> None:
        process = Process(run_id="run_1", process_type="effector")
        batch = JournalBatch(
            run_id="run_1",
            units=[
                CommandUnit(
                    command=_command(
                        command_type="process.claim",
                        idempotency_key="process.claim:x:1",
                    )
                )
            ],
        )
        result = ClaimResult(process=process, batch=batch)
        self.assertEqual(result.process.run_id, "run_1")
        self.assertEqual(len(result.batch.units), 1)

    def test_journal_is_runtime_checkable_protocol(self) -> None:
        self.assertTrue(hasattr(Journal, "append_batch"))
        self.assertTrue(hasattr(Journal, "claim_next"))
        self.assertTrue(hasattr(Journal, "get_command_by_idempotency"))
        self.assertTrue(hasattr(Journal, "list_events"))
        self.assertTrue(hasattr(Journal, "load"))
        # Protocol methods are present on the structural type.
        hints = get_type_hints(Journal.append_batch)
        self.assertIn("batch", hints)
        self.assertIn("return", hints)

    def test_json_round_trip_batch(self) -> None:
        command = _command()
        batch = JournalBatch(
            run_id="run_1",
            journal_seq=1,
            stream_id="stream_parent",
            units=[
                CommandUnit(
                    command=command,
                    events=[
                        _event(command_id=command.id).model_copy(
                            update={"sequence": 1}
                        )
                    ],
                    facts=[
                        StateFact(
                            entity="process",
                            op="upsert",
                            key={"id": "proc_1"},
                            body={"id": "proc_1", "status": "ready"},
                        )
                    ],
                )
            ],
        )
        payload = batch.model_dump(mode="json")
        restored = JournalBatch.model_validate(payload)
        self.assertEqual(restored.journal_seq, 1)
        self.assertEqual(restored.stream_id, "stream_parent")
        self.assertEqual(restored.units[0].facts[0].entity, "process")


if __name__ == "__main__":
    unittest.main()
