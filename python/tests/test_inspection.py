from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

from fala import inspect_leases


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
    INSERT INTO runs VALUES('active','active'),('terminal','succeeded');
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
    before = {p.name: (p.stat().st_size, hashlib.sha256(p.read_bytes()).hexdigest())
              for p in (path, wal, Path(str(path) + '-shm')) if p.exists()}
    result = inspect_leases(path, now='2026-01-01T00:00:00Z')
    after = {p.name: (p.stat().st_size, hashlib.sha256(p.read_bytes()).hexdigest())
             for p in (path, wal, Path(str(path) + '-shm')) if p.exists()}
    assert [x['process_id'] for x in result['current']] == ['wal-live']
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
