from __future__ import annotations

import json
import hashlib
import sqlite3
from pathlib import Path

from fala import inspect_leases
import fala.inspection as inspection


def _db(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.executescript("""
    PRAGMA user_version=6;
    CREATE TABLE schema_migrations(id TEXT PRIMARY KEY, version INTEGER, name TEXT, applied_at TEXT);
    INSERT INTO schema_migrations VALUES('runtime_backend',6,'runtime_backend','2026-01-01T00:00:00Z');
    CREATE TABLE runs(id TEXT PRIMARY KEY, status TEXT);
    CREATE TABLE processes(
      run_id TEXT, id TEXT, status TEXT, lease_owner TEXT,
      lease_expires_at TEXT, attempt INTEGER, max_attempts INTEGER
    );
    INSERT INTO runs VALUES('active','active'),('terminal','completed');
    """)
    return connection


def _insert(db, values):
    db.execute("INSERT INTO processes VALUES(?,?,?,?,?,?,?)", values)


def test_classifies_schema_v6_process_leases(tmp_path: Path) -> None:
    path = tmp_path / "journal.sqlite"
    db = _db(path)
    _insert(db, ('active','live','running','worker-a','2026-01-01T00:00:02Z',1,3))
    _insert(db, ('active','expired','running','worker-b','2025-12-31T23:59:59+00:00',2,3))
    _insert(db, ('active','terminal-process','succeeded','old','2026-02-01T00:00:00Z',1,3))
    _insert(db, ('terminal','foreign-run','running','worker-c','2026-02-01T00:00:00Z',1,3))
    _insert(db, ('active','bad-time','running','worker-d','not-a-time',1,3))
    db.commit(); db.close()

    result = inspect_leases(path, now='2026-01-01T00:00:00Z')
    assert [lease['process_id'] for lease in result['current']] == ['live']
    assert [lease['process_id'] for lease in result['expired']] == ['expired']
    assert [item['code'] for item in result['uncertainty']] == [
        'invalid_lease_expiry', 'lease_on_non_running_process',
        'lease_outside_active_run',
    ]
    assert result['semantics'] == {
        'run': 'context_only_not_leased',
        'process': 'claimed_while_status_running_with_owner_and_expiry',
        'reaction': 'durable_artifact_not_leased',
    }
    json.dumps(result, sort_keys=True)


def test_reads_committed_wal_without_mutation(tmp_path: Path) -> None:
    path = tmp_path / 'wal.sqlite'
    db = _db(path)
    db.execute('PRAGMA journal_mode=WAL')
    _insert(db, ('active','wal-live','running','worker','2026-01-02T00:00:00Z',1,2))
    db.commit()
    wal = Path(str(path) + '-wal')
    assert wal.exists()
    database_bytes_before = hashlib.sha256(path.read_bytes()).hexdigest()
    before = db.execute('SELECT quote(run_id), quote(id), quote(status), quote(lease_owner), quote(lease_expires_at), quote(attempt), quote(max_attempts) FROM processes ORDER BY id').fetchall()
    result = inspect_leases(path, now='2026-01-01T00:00:00Z')
    after = db.execute('SELECT quote(run_id), quote(id), quote(status), quote(lease_owner), quote(lease_expires_at), quote(attempt), quote(max_attempts) FROM processes ORDER BY id').fetchall()
    assert [x['process_id'] for x in result['current']] == ['wal-live']
    # Read-only inspection may change SQLite lock/SHM metadata, never DB/row content.
    assert hashlib.sha256(path.read_bytes()).hexdigest() == database_bytes_before
    assert before == after
    db.close()


def test_missing_corrupt_and_invalid_now_fail_closed(tmp_path: Path) -> None:
    missing = inspect_leases(tmp_path / 'missing.sqlite', now='2026-01-01T00:00:00Z')
    assert not missing['complete'] and missing['errors'][0]['code'] == 'database_unavailable'
    corrupt = tmp_path / 'corrupt.sqlite'; corrupt.write_bytes(b'not sqlite')
    damaged = inspect_leases(corrupt, now='2026-01-01T00:00:00Z')
    assert not damaged['complete'] and damaged['errors'][0]['code'] == 'database_error'
    invalid = inspect_leases(corrupt, now='naive')
    assert invalid['errors'][0]['code'] == 'invalid_now'


def test_null_running_is_uncertainty_and_row_limit_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / 'limits.sqlite'
    db = _db(path)
    _insert(db, ('active', 'missing', 'running', None, None, 1, 2))
    _insert(db, ('active', 'live', 'running', 'worker', '2026-01-02T00:00:00Z', 1, 2))
    db.commit(); db.close()

    result = inspect_leases(path, now='2026-01-01T00:00:00Z')
    assert not result['complete']
    assert result['uncertainty'][0]['code'] == 'incomplete_process_lease'
    limited = inspect_leases(path, now='2026-01-01T00:00:00Z', max_rows=1)
    assert not limited['complete']
    assert limited['errors'][0]['code'] == 'row_limit_exceeded'


def test_preexisting_wal_commit_survives_concurrent_checkpoint(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / 'checkpoint.sqlite'
    writer = _db(path)
    writer.execute('PRAGMA journal_mode=WAL')
    _insert(writer, ('active', 'preexisting-live', 'running', 'worker', '2026-01-02T00:00:00Z', 1, 2))
    writer.commit()
    original = inspection._pin_read_snapshot

    def pin_then_checkpoint(connection):
        original(connection)
        # Snapshot is already materialized. A checkpoint cannot tear it.
        writer.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()

    monkeypatch.setattr(inspection, '_pin_read_snapshot', pin_then_checkpoint)
    result = inspect_leases(path, now='2026-01-01T00:00:00Z')
    assert result['complete']
    assert [x['process_id'] for x in result['current']] == ['preexisting-live']
    writer.close()


def test_commit_after_snapshot_is_consistently_excluded(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / 'during.sqlite'
    writer = _db(path)
    writer.execute('PRAGMA journal_mode=WAL')
    writer.commit()
    original = inspection._pin_read_snapshot

    def pin_then_commit(connection):
        original(connection)
        _insert(writer, ('active', 'later', 'running', 'worker', '2026-01-02T00:00:00Z', 1, 2))
        writer.commit()

    monkeypatch.setattr(inspection, '_pin_read_snapshot', pin_then_commit)
    result = inspect_leases(path, now='2026-01-01T00:00:00Z')
    assert result['complete']
    assert result['current'] == []
    assert writer.execute("SELECT count(*) FROM processes WHERE id='later'").fetchone() == (1,)
    writer.close()


def test_wal_created_after_snapshot_is_not_partially_observed(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / 'wal-created.sqlite'
    setup = _db(path)
    setup.execute('PRAGMA journal_mode=WAL')
    setup.commit(); setup.close()
    wal = Path(str(path) + '-wal')
    assert not wal.exists()
    original = inspection._pin_read_snapshot

    def pin_then_create_wal(connection):
        original(connection)
        writer = sqlite3.connect(path)
        _insert(writer, ('active', 'new-wal-row', 'running', 'worker', '2026-01-02T00:00:00Z', 1, 2))
        writer.commit()
        assert wal.exists()
        writer.close()

    monkeypatch.setattr(inspection, '_pin_read_snapshot', pin_then_create_wal)
    result = inspect_leases(path, now='2026-01-01T00:00:00Z')
    assert result['complete']
    assert result['current'] == []


def test_malformed_storage_classes_and_statuses_fail_closed_json_safe(tmp_path: Path) -> None:
    cases = {
        "blob-status": ('active', 'id', sqlite3.Binary(b'running'), None, None, 1, 2),
        "blob-id": ('active', sqlite3.Binary(b'id'), 'running', 'worker', '2026-01-02T00:00:00Z', 1, 2),
        "blob-attempt": ('active', 'id', 'running', 'worker', '2026-01-02T00:00:00Z', sqlite3.Binary(b'1'), 2),
        "unknown-status": ('active', 'id', 'mystery', None, None, 1, 2),
    }
    for name, values in cases.items():
        path = tmp_path / f"{name}.sqlite"
        db = _db(path); _insert(db, values); db.commit(); db.close()
        result = inspect_leases(path, now='2026-01-01T00:00:00Z')
        assert not result['complete'], name
        assert result['current'] == [], name
        assert result['uncertainty'][0]['code'] == 'malformed_process_row', name
        json.dumps(result, sort_keys=True, allow_nan=False)


def test_malformed_run_context_fails_closed_json_safe(tmp_path: Path) -> None:
    path = tmp_path / 'malformed-run.sqlite'
    db = _db(path)
    db.execute("UPDATE runs SET status=? WHERE id='active'", (sqlite3.Binary(b'active'),))
    _insert(db, ('active', 'id', 'running', 'worker', '2026-01-02T00:00:00Z', 1, 2))
    db.commit(); db.close()
    result = inspect_leases(path, now='2026-01-01T00:00:00Z')
    assert not result['complete']
    assert result['uncertainty'][0]['code'] == 'malformed_process_row'
    assert result['uncertainty'][0]['run_status'] == {
        'storage_class': 'blob', 'hex': b'active'.hex()
    }
    json.dumps(result, sort_keys=True, allow_nan=False)


def test_known_terminal_rows_are_validated_and_may_be_omitted(tmp_path: Path) -> None:
    path = tmp_path / 'terminal.sqlite'
    db = _db(path)
    _insert(db, ('active', 'done', 'succeeded', None, None, 1, 2))
    db.commit(); db.close()
    result = inspect_leases(path, now='2026-01-01T00:00:00Z', max_rows=1)
    assert result['complete']
    assert result['current'] == result['expired'] == []
