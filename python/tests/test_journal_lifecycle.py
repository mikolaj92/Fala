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


def test_statuses_terminal_monotonicity_and_finished_at(tmp_path) -> None:
    import fala
    db = tmp_path / "journal.sqlite"
    with pytest.raises(ValueError, match="invalid status"):
        fala.upsert_run_metadata(db, run_id="run", status="bogus", metadata={})
    fala.upsert_run_metadata(db, run_id="run", status="active", metadata={})
    fala.finalize_run(db, run_id="run", status="failed")
    with pytest.raises(ValueError, match="cannot be overwritten or reopened"):
        fala.upsert_run_metadata(db, run_id="run", status="active", metadata={})
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT status,finished_at FROM runs WHERE id='run'").fetchone()[1]
    fala.upsert_run_metadata(db, run_id="run2", status="active", metadata={})
    fala.park_process(db, run_id="run2", process_id="process")
    fala.complete_waiting_process(db, run_id="run2", process_id="process")
    with pytest.raises(ValueError, match="cannot be overwritten or reopened"):
        fala.park_process(db, run_id="run2", process_id="process")


def test_process_requires_run_and_completion_requires_blocker(tmp_path) -> None:
    import fala
    db = tmp_path / "journal.sqlite"
    fala.ensure_journal(db)
    with pytest.raises(ValueError, match="run 'missing' not found"):
        fala.park_process(db, run_id="missing", process_id="process")
    fala.upsert_run_metadata(db, run_id="run", status="waiting", metadata={})
    fala.park_process(db, run_id="run", process_id="process")
    with pytest.raises(ValueError, match="blocker 'missing' not found"):
        fala.complete_waiting_process(db, run_id="run", process_id="process", blocker_id="missing")
    run, process = _rows(db)
    assert (run["status"], process["status"]) == ("waiting", "waiting")


def test_completion_exact_replay_and_competing_outcome_conflicts(tmp_path) -> None:
    import fala
    db = tmp_path / "journal.sqlite"
    fala.upsert_run_metadata(db, run_id="run", status="waiting", metadata={})
    fala.park_process(db, run_id="run", process_id="process")

    def complete(failed: bool):
        args = dict(process_status="failed", run_status="failed", output={"winner": "failed"}) if failed else dict(output={"winner": "success"})
        try:
            return fala.complete_waiting_process(db, run_id="run", process_id="process", **args)
        except ValueError:
            return None

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda i: complete(bool(i % 2)), range(32)))
    assert sum(r is not None and r["changed"] for r in results) == 1
    run, process = _rows(db)
    durable = json.loads(process["output_json"])
    assert (process["status"], run["status"]) in {("succeeded", "completed"), ("failed", "failed")}
    assert durable == {"winner": "success" if process["status"] == "succeeded" else "failed"}
    # Different output at the same terminal target is a conflict, not status-only replay.
    with pytest.raises(ValueError, match="conflicts"):
        fala.complete_waiting_process(db, run_id="run", process_id="process", process_status=process["status"], run_status=run["status"], output={"different": True})


def test_replaced_database_is_reensured_and_malformed_v6_refused(tmp_path) -> None:
    import fala
    db = tmp_path / "journal.sqlite"
    fala.ensure_journal(db)
    db.unlink()
    sqlite3.connect(db).close()
    fala.upsert_run_metadata(db, run_id="run", metadata={})
    bad = tmp_path / "bad.sqlite"
    with sqlite3.connect(bad) as conn:
        conn.execute("CREATE TABLE runs (id TEXT PRIMARY KEY, status TEXT, schema_version INTEGER, metadata TEXT, created_at TEXT, updated_at TEXT)")
        conn.execute("PRAGMA user_version=6")
    with pytest.raises(RuntimeError, match="incompatible runs"):
        fala.upsert_run_metadata(bad, run_id="run", metadata={})
    with sqlite3.connect(bad) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 6
        assert conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == [("runs",)]
