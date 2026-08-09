from __future__ import annotations

import sqlite3

import pytest


def _schema(db) -> None:
    from fala.host import _ensure_durable_schema
    _ensure_durable_schema(db)


def _run(db, run_id: str, status: str, at: str) -> None:
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO runs (id,status,schema_version,metadata,created_at,updated_at,finished_at) VALUES (?,?,6,'{}',?,?,?)",
            (run_id, status, at, at, at if status in {"completed", "failed", "cancelled", "timed_out"} else None),
        )


def test_maintenance_dry_run_apply_terminal_only_and_keep_last(tmp_path) -> None:
    import fala
    db = tmp_path / "journal.sqlite"
    _schema(db)
    _run(db, "old", "completed", "2020-01-01T00:00:00Z")
    _run(db, "kept", "failed", "2020-01-02T00:00:00Z")
    _run(db, "active", "active", "2020-01-01T00:00:00Z")

    plan = fala.maintain_journal(db, older_than_days=1, keep_last=1)
    assert plan["dry_run"] is True
    assert [item["run_id"] for item in plan["runs"]] == ["old"]
    assert plan["deleted_run_count"] == 0
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT count(*) FROM runs").fetchone()[0] == 3

    applied = fala.maintain_journal(db, older_than_days=1, keep_last=1, dry_run=False)
    assert applied["deleted_run_count"] == 1
    assert applied["runs"][0]["deleted"] is True
    with sqlite3.connect(db) as conn:
        assert [r[0] for r in conn.execute("SELECT id FROM runs ORDER BY id")] == ["active", "kept"]
        triggers = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'")}
    assert {"runtime_events_no_delete", "runtime_commands_no_delete"} <= triggers


def test_maintenance_validates_before_native_work(tmp_path) -> None:
    import fala
    with pytest.raises(FileNotFoundError):
        fala.maintain_journal(tmp_path / "missing.sqlite", older_than_days=1)
    db = tmp_path / "journal.sqlite"; _schema(db)
    with pytest.raises(ValueError, match="non-negative"):
        fala.maintain_journal(db, older_than_days=-1)
    with pytest.raises(ValueError, match="keep_last"):
        fala.maintain_journal(db, older_than_days=1, keep_last=-2)
