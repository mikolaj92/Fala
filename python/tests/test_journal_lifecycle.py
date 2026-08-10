from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest


def _rows(db):
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        return (
            dict(conn.execute("SELECT * FROM runs WHERE id='run'").fetchone()),
            dict(
                conn.execute(
                    "SELECT * FROM processes WHERE run_id='run' AND id='process'"
                ).fetchone()
            ),
        )


def test_run_lifecycle_preserves_opaque_metadata_and_finalize_idempotency(
    tmp_path,
) -> None:
    import fala

    db = tmp_path / "nested" / "journal.sqlite"
    fala.ensure_journal(db)
    fala.upsert_run_metadata(
        db, run_id="run", title="Run", status="active", metadata={"consumer": {"x": 1}}
    )
    fala.finalize_run(db, run_id="run", status="failed", reason="stopped")
    fala.finalize_run(db, run_id="run", status="failed", reason="stopped")
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT status,title,metadata,finished_at FROM runs WHERE id='run'"
        ).fetchone()
    assert row[0:2] == ("failed", "Run")
    assert json.loads(row[2]) == {"consumer": {"x": 1}, "finalize_reason": "stopped"}
    assert row[3]


def test_park_then_atomic_completion_matches_journal_semantics(tmp_path) -> None:
    import fala

    db = tmp_path / "journal.sqlite"
    fala.upsert_run_metadata(
        db, run_id="run", status="waiting", metadata={"opaque": True}
    )
    fala.park_process(
        db,
        run_id="run",
        process_id="process",
        process_type="manual",
        metadata={"domain": "uninterpreted"},
        output={},
    )
    now = "2026-01-01T00:00:00Z"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO homeostats (run_id,id,kind,status,values_json,metadata,attempt,"
            "max_attempts,created_at,updated_at) VALUES ('run','blocker','manual','open','{}','{}',1,1,?,?)",
            (now, now),
        )
    result = fala.complete_waiting_process(
        db,
        run_id="run",
        process_id="process",
        blocker_id="blocker",
        output={"approved": True},
    )
    assert result == {
        "run_id": "run",
        "process_id": "process",
        "changed": True,
        "process_status": "succeeded",
        "run_status": "completed",
    }
    run, process = _rows(db)
    assert run["status"] == "completed" and run["finished_at"]
    assert process["status"] == "succeeded" and process["finished_at"]
    assert json.loads(process["output_json"]) == {"approved": True}
    with sqlite3.connect(db) as conn:
        blocker = conn.execute(
            "SELECT status,values_json FROM homeostats WHERE run_id='run' AND id='blocker'"
        ).fetchone()
    assert blocker[0] == "completed"
    assert json.loads(blocker[1]) == {"approved": True}
    assert (
        fala.complete_waiting_process(
            db,
            run_id="run",
            process_id="process",
            blocker_id="blocker",
            output={"approved": True},
        )["changed"]
        is False
    )


def test_concurrent_completion_has_exactly_one_change(tmp_path) -> None:
    import fala

    db = tmp_path / "journal.sqlite"
    fala.upsert_run_metadata(db, run_id="run", status="waiting", metadata={})
    fala.park_process(db, run_id="run", process_id="process")

    def complete():
        return fala.complete_waiting_process(db, run_id="run", process_id="process")

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: complete(), range(16)))
    assert sum(result["changed"] for result in results) == 1
    assert {result["process_status"] for result in results} == {"succeeded"}


def test_missing_rows_and_non_json_payloads_fail_closed(tmp_path) -> None:
    import fala

    db = tmp_path / "journal.sqlite"
    fala.ensure_journal(db)
    with pytest.raises(ValueError, match="run 'missing' not found"):
        fala.transition_run(db, run_id="missing", status="failed")
    with pytest.raises(TypeError, match="metadata is not JSON-recordable"):
        fala.upsert_run_metadata(db, run_id="run", metadata={"bad": object()})
    fala.upsert_run_metadata(db, run_id="run", metadata={})
    with pytest.raises(ValueError, match="process 'missing' not found"):
        fala.complete_waiting_process(db, run_id="run", process_id="missing")
