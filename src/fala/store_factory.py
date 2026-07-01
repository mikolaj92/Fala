from __future__ import annotations

import os
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from fala.schema_migrations import (
    RUNTIME_SCHEMA_VERSION,
    runtime_schema_migration_rows,
)
from fala.sqlite_store import SQLiteStateStore
from fala.store import StateStore


SQLITE_SCHEMES = {"sqlite", "sqlite3"}

EXPECTED_RUNTIME_TABLES = (
    "operator_audit_events",
    "process_claims",
    "process_document_inputs",
    "process_documents",
    "process_events",
    "process_outputs",
    "process_projections",
    "process_runs",
    "process_statuses",
    "process_stream_checkpoints",
    "process_stream_chunks",
    "process_worker_heartbeats",
    "runtime_schema_migrations",
)


def create_state_store(
    target: str | os.PathLike[str],
    *,
    ensure_schema: bool = True,
) -> StateStore:
    """Create the shipped first-party runtime state store.

    Fala 2.0 keeps a backend plugin boundary, but the core distribution ships
    only SQLite. Non-SQLite backends belong in external RuntimeBackend plugins.
    """

    del ensure_schema
    target_text = os.fspath(target)
    parsed = urlparse(target_text)
    if parsed.scheme in SQLITE_SCHEMES:
        return SQLiteStateStore(_sqlite_path_from_url(target_text))
    if parsed.scheme:
        raise ValueError(_unsupported_backend_message(parsed.scheme))
    return SQLiteStateStore(target_text)


def default_state_store_target(
    target: str | os.PathLike[str] | None = None,
    *,
    default_sqlite_path: str = "fala.db",
) -> str:
    return os.fspath(
        target
        or os.environ.get("FALA_DATABASE_URL")
        or os.environ.get("FALA_DB")
        or default_sqlite_path
    )


def state_store_diagnostics_target(store: StateStore) -> str | None:
    target = getattr(store, "dsn", None)
    if target:
        return os.fspath(target)
    target = getattr(store, "path", None)
    if target:
        return os.fspath(target)
    return None


def runtime_db_diagnostics(
    target: str | os.PathLike[str],
    *,
    ensure_schema: bool = False,
) -> dict[str, Any]:
    target_text = os.fspath(target)
    parsed = urlparse(target_text)
    if parsed.scheme in SQLITE_SCHEMES:
        sqlite_path = Path(_sqlite_path_from_url(target_text))
    elif parsed.scheme:
        return _db_diagnostics_error(
            store_kind="unsupported",
            target=target_text,
            ensure_schema=ensure_schema,
            error=_unsupported_backend_message(parsed.scheme),
        )
    else:
        sqlite_path = Path(target_text)
    return _sqlite_db_diagnostics(sqlite_path, ensure_schema=ensure_schema)


def _sqlite_db_diagnostics(path: Path, *, ensure_schema: bool) -> dict[str, Any]:
    existed_before = path.exists()
    if ensure_schema:
        SQLiteStateStore(path)
    elif not existed_before:
        return _db_diagnostics_error(
            store_kind="sqlite",
            target=str(path),
            ensure_schema=ensure_schema,
            error="SQLite database file does not exist.",
        )
    try:
        with closing(sqlite3.connect(path)) as conn:
            table_rows = conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                """
            ).fetchall()
            tables = sorted(str(row[0]) for row in table_rows)
            table_set = set(tables)
            missing_tables = [
                table for table in EXPECTED_RUNTIME_TABLES if table not in table_set
            ]
            migrations = _sqlite_schema_migration_status(conn, table_set)
            run_count = (
                int(conn.execute("SELECT COUNT(*) FROM process_runs").fetchone()[0])
                if "process_runs" in table_set
                else None
            )
            document_count = (
                int(conn.execute("SELECT COUNT(*) FROM process_documents").fetchone()[0])
                if "process_documents" in table_set
                else None
            )
            process_count = (
                int(conn.execute("SELECT COUNT(*) FROM process_statuses").fetchone()[0])
                if "process_statuses" in table_set
                else None
            )
    except Exception as exc:
        return _db_diagnostics_error(
            store_kind="sqlite",
            target=str(path),
            ensure_schema=ensure_schema,
            error=str(exc),
        )
    return _db_diagnostics_payload(
        store_kind="sqlite",
        target=str(path),
        ensure_schema=ensure_schema,
        created=ensure_schema and not existed_before,
        tables=tables,
        missing_tables=missing_tables,
        migrations=migrations,
        run_count=run_count,
        document_count=document_count,
        process_count=process_count,
    )


def _sqlite_schema_migration_status(
    conn: sqlite3.Connection,
    table_set: set[str],
) -> dict[str, Any]:
    user_version_row = conn.execute("PRAGMA user_version").fetchone()
    user_version = int(user_version_row[0]) if user_version_row is not None else 0
    if "runtime_schema_migrations" not in table_set:
        return _schema_migration_status([], user_version=user_version)
    rows = conn.execute(
        """
        SELECT version, migration_id, description, checksum, applied_at, payload
        FROM runtime_schema_migrations
        ORDER BY version ASC
        """
    ).fetchall()
    applied = [
        {
            "version": int(row[0]),
            "migration_id": str(row[1]),
            "description": str(row[2]),
            "checksum": str(row[3]),
            "applied_at": str(row[4]),
            "payload": str(row[5]),
        }
        for row in rows
    ]
    return _schema_migration_status(applied, user_version=user_version)


def _schema_migration_status(
    applied: list[dict[str, Any]],
    *,
    user_version: int | None,
) -> dict[str, Any]:
    expected = runtime_schema_migration_rows()
    expected_by_version = {int(item["version"]): item for item in expected}
    applied_by_version = {int(item["version"]): item for item in applied}
    missing = [
        item
        for item in expected
        if int(item["version"]) not in applied_by_version
    ]
    checksum_mismatches = [
        {
            "version": version,
            "migration_id": str(applied_by_version[version].get("migration_id") or ""),
            "expected_checksum": str(expected_by_version[version]["checksum"]),
            "actual_checksum": str(applied_by_version[version].get("checksum") or ""),
        }
        for version in sorted(set(expected_by_version).intersection(applied_by_version))
        if str(applied_by_version[version].get("checksum") or "")
        != str(expected_by_version[version]["checksum"])
    ]
    current_version = max(applied_by_version, default=0)
    user_version_ok = user_version in (None, RUNTIME_SCHEMA_VERSION)
    ok = (
        current_version >= RUNTIME_SCHEMA_VERSION
        and not missing
        and not checksum_mismatches
        and user_version_ok
    )
    return {
        "ok": ok,
        "current_version": current_version,
        "latest_version": RUNTIME_SCHEMA_VERSION,
        "user_version": user_version,
        "user_version_ok": user_version_ok,
        "applied_count": len(applied),
        "expected_count": len(expected),
        "missing_count": len(missing),
        "checksum_mismatch_count": len(checksum_mismatches),
        "applied": applied,
        "missing": missing,
        "checksum_mismatches": checksum_mismatches,
    }


def _db_diagnostics_payload(
    *,
    store_kind: str,
    target: str,
    ensure_schema: bool,
    created: bool,
    tables: list[str],
    missing_tables: list[str],
    migrations: dict[str, Any],
    run_count: int | None,
    document_count: int | None,
    process_count: int | None,
) -> dict[str, Any]:
    return {
        "ok": not missing_tables and bool(migrations.get("ok")),
        "store_kind": store_kind,
        "target": target,
        "ensure_schema": ensure_schema,
        "created": created,
        "schema": {
            "ok": not missing_tables and bool(migrations.get("ok")),
            "current_version": migrations.get("current_version"),
            "latest_version": migrations.get("latest_version"),
            "expected_table_count": len(EXPECTED_RUNTIME_TABLES),
            "present_table_count": len(set(tables) & set(EXPECTED_RUNTIME_TABLES)),
            "missing_tables": missing_tables,
            "tables": tables,
            "migrations": migrations,
        },
        "counts": {
            "runs": run_count,
            "documents": document_count,
            "processes": process_count,
        },
    }


def _db_diagnostics_error(
    *,
    store_kind: str,
    target: str,
    ensure_schema: bool,
    error: str,
) -> dict[str, Any]:
    return {
        "ok": False,
        "store_kind": store_kind,
        "target": target,
        "ensure_schema": ensure_schema,
        "created": False,
        "schema": {
            "ok": False,
            "current_version": 0,
            "latest_version": RUNTIME_SCHEMA_VERSION,
            "expected_table_count": len(EXPECTED_RUNTIME_TABLES),
            "present_table_count": 0,
            "missing_tables": list(EXPECTED_RUNTIME_TABLES),
            "tables": [],
            "migrations": _schema_migration_status([], user_version=None),
        },
        "counts": {
            "runs": None,
            "documents": None,
            "processes": None,
        },
        "error": error,
    }


def _sqlite_path_from_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in SQLITE_SCHEMES:
        raise ValueError(f"Unsupported SQLite URL scheme: {parsed.scheme}")
    if parsed.netloc and parsed.netloc != "localhost":
        raise ValueError("SQLite URL host must be empty or localhost")
    if parsed.netloc == "localhost":
        path = parsed.path
    elif url.startswith(f"{parsed.scheme}:////"):
        path = parsed.path
    else:
        path = parsed.path.lstrip("/")
    if not path:
        raise ValueError("SQLite URL must include a database path")
    return str(Path(unquote(path)))


def _unsupported_backend_message(scheme: str) -> str:
    return (
        "Fala ships only the SQLite state store first-party. "
        f"Unsupported backend URL scheme: {scheme!r}. "
        "Use sqlite:// or a filesystem path, or provide a non-SQLite "
        "RuntimeBackend as an external plugin."
    )
