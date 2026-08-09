from __future__ import annotations

import sqlite3


def _seed(db) -> None:
    from fala.host import _ensure_durable_schema
    _ensure_durable_schema(db)
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT INTO runs (id,status,schema_version,metadata,created_at,updated_at) VALUES ('run','active',6,'{}','2020-01-01T00:00:00Z','2020-01-01T00:00:00Z')")
        for process_id, owner, expiry, max_attempts in [
            ("expired", "worker-a", "2026-01-01T00:00:00Z", 2),
            ("live", "worker-a", "2026-01-02T00:00:00Z", 2),
            ("other", "worker-b", "2026-01-01T00:00:00Z", 2),
            ("exhausted", "worker-a", "2026-01-01T00:00:00Z", 1),
        ]:
            conn.execute(
                "INSERT INTO processes (run_id,id,process_type,status,priority,attempt,max_attempts,available_at,lease_owner,lease_expires_at,input_json,output_json,error_json,metadata,created_at,updated_at,started_at,output_schema_json) VALUES ('run',?,'native','running',0,1,?,'2026-01-01T00:00:00Z',?,?,'{}','{}','{}','{}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{}')",
                (process_id, max_attempts, owner, expiry),
            )


def test_recover_expired_owned_claims_once_and_preserve_live(tmp_path) -> None:
    import fala
    db = tmp_path / "journal.sqlite"; _seed(db)
    result = fala.recover_incomplete(db, worker_id="worker-a", now="2026-01-01T12:00:00Z")
    assert [(i["process_id"], i["status"]) for i in result["items"]] == [
        ("exhausted", "failed"), ("expired", "retry_wait")
    ]
    assert result["requeued_count"] == 1
    assert result["unrecoverable_count"] == 1
    with sqlite3.connect(db) as conn:
        rows = dict(conn.execute("SELECT id,status FROM processes"))
    assert rows == {"expired": "retry_wait", "live": "running", "other": "running", "exhausted": "failed"}

    replay = fala.recover_incomplete(db, worker_id="worker-a", now="2026-01-01T12:00:00Z")
    assert replay["recovered_count"] == 0
    assert replay["items"] == []


def test_recovered_process_is_replay_ready(tmp_path) -> None:
    import fala
    db = tmp_path / "journal.sqlite"; _seed(db)
    fala.recover_incomplete(db, worker_id="worker-a", now="2026-01-01T12:00:00Z")
    with sqlite3.connect(db) as conn:
        row = conn.execute("SELECT status,lease_owner,lease_expires_at,available_at,error_json FROM processes WHERE id='expired'").fetchone()
    assert row[0:4] == ("retry_wait", None, None, "2026-01-01T12:00:00Z")
    assert "lease_expired" in row[4]
