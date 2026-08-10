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


def test_full_schema_contract_rejects_noncore_shape_before_mutation(tmp_path) -> None:
    import fala
    db = tmp_path / "malformed.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE impulses (id TEXT PRIMARY KEY)")
        conn.execute("INSERT INTO impulses VALUES ('sentinel')")
    with pytest.raises(RuntimeError, match="incompatible impulses"):
        fala.ensure_journal(db)
    with sqlite3.connect(db) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 0
        assert conn.execute("SELECT * FROM impulses").fetchall() == [("sentinel",)]
        assert conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == [("impulses",)]


def test_full_schema_contract_validates_owned_indexes_triggers_and_foreign_keys(tmp_path) -> None:
    import fala
    db = tmp_path / "journal.sqlite"
    fala.ensure_journal(db)
    with sqlite3.connect(db) as conn:
        conn.execute("DROP INDEX idx_runs_status")
        conn.execute("CREATE INDEX idx_runs_status ON runs(status)")
    with pytest.raises(RuntimeError, match="incompatible index idx_runs_status"):
        fala.ensure_journal(db)

    db2 = tmp_path / "trigger.sqlite"
    fala.ensure_journal(db2)
    with sqlite3.connect(db2) as conn:
        conn.execute("DROP TRIGGER runtime_events_no_delete")
        conn.execute("CREATE TRIGGER runtime_events_no_delete BEFORE DELETE ON runtime_events BEGIN SELECT 1; END")
    with pytest.raises(RuntimeError, match="incompatible trigger runtime_events_no_delete"):
        fala.ensure_journal(db2)

    # Recreate a schema-owned FK table without its declared relationship.
    db3 = tmp_path / "fk.sqlite"
    fala.ensure_journal(db3)
    with sqlite3.connect(db3) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("ALTER TABLE reactions RENAME TO reactions_old")
        conn.execute("CREATE TABLE reactions (run_id TEXT NOT NULL,id TEXT NOT NULL,kind TEXT NOT NULL,uri TEXT NOT NULL,impulse_id TEXT,media_type TEXT,size_bytes INTEGER,content_hash TEXT,metadata TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(run_id,id))")
        conn.execute("DROP TABLE reactions_old")
    with pytest.raises(RuntimeError, match="foreign keys"):
        fala.ensure_journal(db3)


@pytest.mark.parametrize(
    ("blocker_status", "process_status", "run_status"),
    [("completed", "succeeded", "completed"), ("cancelled", "cancelled", "cancelled"), ("expired", "timed_out", "timed_out")],
)
def test_native_terminal_pairings_are_exact(tmp_path, blocker_status, process_status, run_status) -> None:
    import fala
    db = tmp_path / f"{blocker_status}.sqlite"
    fala.upsert_run_metadata(db, run_id="run", status="waiting", metadata={})
    fala.park_process(db, run_id="run", process_id="process")
    result = fala.complete_waiting_process(db, run_id="run", process_id="process", blocker_status=blocker_status, process_status=process_status, run_status=run_status)
    assert (result["process_status"], result["run_status"]) == (process_status, run_status)


def test_invalid_terminal_pairings_and_any_partial_lease_fail_closed(tmp_path) -> None:
    import fala
    db = tmp_path / "journal.sqlite"
    fala.upsert_run_metadata(db, run_id="run", status="waiting", metadata={})
    fala.park_process(db, run_id="run", process_id="process")
    for kwargs in (
        {"blocker_status": "completed", "process_status": "failed", "run_status": "failed"},
        {"blocker_status": "completed", "process_status": "succeeded", "run_status": "failed"},
    ):
        with pytest.raises(ValueError, match="terminal pairing"):
            fala.complete_waiting_process(db, run_id="run", process_id="process", **kwargs)
    for owner, expiry in (("worker", None), (None, "2099"), ("worker", "2099")):
        with sqlite3.connect(db) as conn:
            conn.execute("UPDATE processes SET lease_owner=?,lease_expires_at=? WHERE run_id='run' AND id='process'", (owner, expiry))
        with pytest.raises(ValueError, match="wholly unleased"):
            fala.complete_waiting_process(db, run_id="run", process_id="process")


def test_waiting_upsert_rejects_existing_lease_and_terminal_update_clears_it(tmp_path) -> None:
    import fala
    db = tmp_path / "journal.sqlite"
    fala.upsert_run_metadata(db, run_id="run", status="active", metadata={})
    fala.upsert_process(db, run_id="run", process_id="process", status="running")
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE processes SET lease_owner='worker',lease_expires_at='2099' WHERE run_id='run' AND id='process'")
    with pytest.raises(ValueError, match="leased process"):
        fala.park_process(db, run_id="run", process_id="process")
    fala.upsert_process(db, run_id="run", process_id="process", status="succeeded")
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT lease_owner,lease_expires_at FROM processes WHERE run_id='run' AND id='process'").fetchone() == (None, None)


_RUNTIME_LEGACY = {
    "minimal": "CREATE TABLE runtime_events (run_id TEXT NOT NULL, sequence INTEGER NOT NULL, id TEXT PRIMARY KEY, event_type TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL)",
    "process-only": "CREATE TABLE runtime_events (run_id TEXT NOT NULL, sequence INTEGER NOT NULL, id TEXT PRIMARY KEY, event_type TEXT NOT NULL, process_id TEXT, payload TEXT NOT NULL, created_at TEXT NOT NULL)",
    "schema-only": "CREATE TABLE runtime_events (run_id TEXT NOT NULL, sequence INTEGER NOT NULL, id TEXT PRIMARY KEY, event_type TEXT NOT NULL, schema_version INTEGER NOT NULL DEFAULT 1, payload TEXT NOT NULL, created_at TEXT NOT NULL)",
}


@pytest.mark.parametrize("label", list(_RUNTIME_LEGACY))
def test_public_ensure_rebuilds_exact_native_runtime_legacy_and_preserves_rows(tmp_path, label) -> None:
    import fala
    db = tmp_path / f"{label}.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute(_RUNTIME_LEGACY[label])
        conn.execute("INSERT INTO runtime_events (run_id,sequence,id,event_type,payload,created_at) VALUES ('legacy',1,'legacy-event','run.created','{\"kept\":true}','now')")
    fala.ensure_journal(db)
    with sqlite3.connect(db) as conn:
        row = conn.execute("SELECT payload,process_id,schema_version,actor,correlation_id FROM runtime_events WHERE id='legacy-event'").fetchone()
        assert row == ('{"kept":true}', None, 1, None, None)
        assert [(r[1], r[5]) for r in conn.execute("PRAGMA table_info(runtime_events)") if r[5]] == [("run_id", 1), ("sequence", 2)]
        assert conn.execute("PRAGMA foreign_key_list(runtime_events)").fetchall()
    fala.ensure_journal(db)
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM runtime_events WHERE id='legacy-event'").fetchone()[0] == 1


def test_public_ensure_rebuilds_exact_native_process_legacy_and_preserves_rows(tmp_path) -> None:
    import fala
    db = tmp_path / "process.sqlite"
    sql = "CREATE TABLE processes (run_id TEXT NOT NULL, id TEXT PRIMARY KEY, process_type TEXT NOT NULL, impulse_id TEXT, status TEXT NOT NULL, priority INTEGER NOT NULL, attempt INTEGER NOT NULL, max_attempts INTEGER NOT NULL, available_at TEXT NOT NULL, lease_owner TEXT, lease_expires_at TEXT, input_json TEXT NOT NULL, output_json TEXT NOT NULL, error_json TEXT NOT NULL, metadata TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, started_at TEXT, finished_at TEXT)"
    with sqlite3.connect(db) as conn:
        conn.execute(sql)
        conn.execute("INSERT INTO processes (run_id,id,process_type,status,priority,attempt,max_attempts,available_at,input_json,output_json,error_json,metadata,created_at,updated_at) VALUES ('legacy','legacy-process','manual','ready',0,0,1,'now','{}','{}','{}','{}','now','now')")
    fala.ensure_journal(db)
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT run_id,id,status,output_schema_json FROM processes").fetchall() == [("legacy", "legacy-process", "ready", "{}")]
        assert [(r[1], r[5]) for r in conn.execute("PRAGMA table_info(processes)") if r[5]] == [("run_id", 1), ("id", 2)]


@pytest.mark.parametrize("mutation", ["extra", "nullable", "wrong-pk"])
def test_legacy_acceptance_is_closed_and_malformed_lookalikes_are_unchanged(tmp_path, mutation) -> None:
    import fala
    db = tmp_path / f"bad-{mutation}.sqlite"
    sql = _RUNTIME_LEGACY["minimal"]
    if mutation == "extra":
        sql = sql[:-1] + ", bogus TEXT)"
    elif mutation == "nullable":
        sql = sql.replace("payload TEXT NOT NULL", "payload TEXT")
    else:
        sql = sql.replace("id TEXT PRIMARY KEY", "id TEXT")
    with sqlite3.connect(db) as conn:
        conn.execute(sql)
        conn.execute("INSERT INTO runtime_events (run_id,sequence,id,event_type,payload,created_at) VALUES ('legacy',1,'sentinel','type','{}','now')")
    with pytest.raises(RuntimeError, match="incompatible runtime_events"):
        fala.ensure_journal(db)
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT id,payload FROM runtime_events").fetchall() == [("sentinel", "{}")]
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 0
        expected = ["run_id", "sequence", "id", "event_type", "payload", "created_at"]
        if mutation == "extra":
            expected.append("bogus")
        assert [r[1] for r in conn.execute("PRAGMA table_info(runtime_events)")] == expected
