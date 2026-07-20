"""stream.merged composition helpers (PR10 / #90)."""

from __future__ import annotations

import unittest

from fala.journal import (
    STREAM_MERGED_KIND,
    CommandUnit,
    JournalBatch,
    flatten_stream_merged,
    nest_child_batch,
    stream_merged_envelope,
)
from fala.runtime_backend import RuntimeCommand, RuntimeEvent
from fala.runtime_models import Run, RunStatus


class JournalStreamTests(unittest.TestCase):
    def test_nest_child_batch_sets_parent_refs(self) -> None:
        child = JournalBatch(
            run_id="child_run",
            stream_id="memory://child",
            units=[
                CommandUnit(
                    command=RuntimeCommand(
                        run_id="child_run",
                        command_type="run.create",
                        idempotency_key="c",
                    ),
                    events=[
                        RuntimeEvent(run_id="child_run", event_type="run.created")
                    ],
                )
            ],
        )
        nested = nest_child_batch(
            child,
            parent_stream_id="memory://parent",
            parent_process_id="proc_delegate",
        )
        self.assertEqual(nested.parent_stream_id, "memory://parent")
        self.assertEqual(nested.parent_process_id, "proc_delegate")
        self.assertEqual(nested.stream_id, "memory://child")

    def test_stream_merged_round_trip(self) -> None:
        parent = JournalBatch(
            run_id="run_p",
            stream_id="jsonl://parent",
            units=[
                CommandUnit(
                    command=RuntimeCommand(
                        run_id="run_p",
                        command_type="run.create",
                        idempotency_key="p",
                    )
                )
            ],
        )
        child = JournalBatch(
            run_id="run_c",
            stream_id="jsonl://child",
            units=[
                CommandUnit(
                    command=RuntimeCommand(
                        run_id="run_c",
                        command_type="run.create",
                        idempotency_key="c",
                    )
                )
            ],
        )
        envelope = stream_merged_envelope(
            [parent],
            [child],
            parent_process_id="proc_1",
        )
        self.assertEqual(envelope["kind"], STREAM_MERGED_KIND)
        self.assertEqual(envelope["parent_process_id"], "proc_1")
        self.assertEqual(
            envelope["children"][0]["parent_process_id"], "proc_1"
        )
        flat = flatten_stream_merged(envelope)
        self.assertEqual(len(flat), 2)
        self.assertEqual(flat[0].run_id, "run_p")
        self.assertEqual(flat[1].parent_process_id, "proc_1")

    def test_runtime_models_import_stable(self) -> None:
        self.assertEqual(Run(id="r1").status, RunStatus.created)


if __name__ == "__main__":
    unittest.main()
