"""Deterministic, bounded public reads of Fala schema-v6 journals."""
from __future__ import annotations

import json
import math
import sqlite3
from os import fspath
from pathlib import Path
from typing import Any
from urllib.parse import quote

_SCHEMA_VERSION = 6
_RUN_STATUSES = frozenset({"created", "active", "waiting", "completed", "failed", "cancel_requested", "cancelled", "timed_out"})
_PROCESS_STATUSES = frozenset({"pending", "ready", "running", "waiting", "retry_wait", "cancel_requested", "succeeded", "failed", "cancelled", "timed_out"})
_RUN_COLUMNS = ("id", "status", "title", "package_id", "package_version", "package_digest", "correlation_path_id", "correlation_path_digest", "runtime_version", "backend_version", "schema_version", "metadata", "created_at", "updated_at", "started_at", "finished_at")
_PROCESS_COLUMNS = ("run_id", "id", "process_type", "impulse_id", "status", "priority", "attempt", "max_attempts", "available_at", "lease_owner", "lease_expires_at", "input_json", "output_json", "error_json", "metadata", "created_at", "updated_at", "started_at", "finished_at", "output_schema_json")
_JSON_FIELDS = frozenset({"metadata", "input_json", "output_json", "error_json", "output_schema_json"})


def _limit(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1 or value > 100_000:
        raise ValueError("fala read: max_rows must be an integer from 1 through 100000")
    return value


def _path(db_path: str | bytes | Path) -> Path:
    path = Path(fspath(db_path)).expanduser().resolve(strict=True)
    if not path.is_file():
        raise FileNotFoundError(f"fala read: journal is not a regular file: {path}")
    return path


def _safe(value: Any, field: str) -> Any:
    if field in _JSON_FIELDS:
        if not isinstance(value, str):
            raise RuntimeError(f"fala read: {field} is not JSON text")
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"fala read: malformed {field}") from exc
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    if isinstance(value, list):
        return [_safe(item, "") for item in value]
    if isinstance(value, dict):
        return {str(key): _safe(item, "") for key, item in value.items()}
    raise RuntimeError(f"fala read: non-JSON-safe value in {field}")


def _open(path: Path) -> sqlite3.Connection:
    uri = f"file:{quote(str(path), safe='/')}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("BEGIN")
    conn.execute("PRAGMA schema_version").fetchone()  # materialize the snapshot
    version = conn.execute("PRAGMA user_version").fetchone()
    migration = conn.execute("SELECT version FROM schema_migrations WHERE id='runtime_backend'").fetchone()
    if version is None or version[0] != _SCHEMA_VERSION or migration is None or migration[0] != _SCHEMA_VERSION:
        conn.close()
        raise RuntimeError("fala read: schema v6 is required")
    return conn


def _row(row: sqlite3.Row, columns: tuple[str, ...]) -> dict[str, Any]:
    result = {name: _safe(row[name], name) for name in columns}
    if not isinstance(result.get("id"), str) or not result["id"]:
        raise RuntimeError("fala read: row id must be non-empty text")
    statuses = _PROCESS_STATUSES if "process_type" in result else _RUN_STATUSES
    if result.get("status") not in statuses:
        raise RuntimeError("fala read: row has an invalid status")
    if "schema_version" in result and result["schema_version"] != _SCHEMA_VERSION:
        raise RuntimeError("fala read: run has an invalid schema version")
    if "run_id" in result and (not isinstance(result["run_id"], str) or not result["run_id"]):
        raise RuntimeError("fala read: process run_id must be non-empty text")
    # Stable ergonomic aliases; raw storage names remain present for compatibility.
    if "input_json" in result:
        result["input"] = result["input_json"]
        result["output"] = result["output_json"]
    json.dumps(result, allow_nan=False, sort_keys=True)
    return result


def list_runs(db_path: str | bytes | Path, *, max_rows: int = 10_000) -> list[dict[str, Any]]:
    """Return runs ordered by durable id from one pinned read-only transaction."""
    limit = _limit(max_rows); path = _path(db_path)
    with _open(path) as conn:
        rows = conn.execute(f"SELECT {','.join(_RUN_COLUMNS)} FROM runs ORDER BY id ASC LIMIT ?", (limit + 1,)).fetchall()
        if len(rows) > limit:
            raise RuntimeError("fala read: run row limit exceeded")
        return [_row(row, _RUN_COLUMNS) for row in rows]


def get_run(db_path: str | bytes | Path, run_id: str) -> dict[str, Any] | None:
    """Return one run by exact id, or ``None``, from a pinned read snapshot."""
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("fala read: run_id must be non-empty text")
    path = _path(db_path)
    with _open(path) as conn:
        row = conn.execute(f"SELECT {','.join(_RUN_COLUMNS)} FROM runs WHERE id=?", (run_id,)).fetchone()
        return None if row is None else _row(row, _RUN_COLUMNS)


def list_processes(db_path: str | bytes | Path, run_id: str, *, max_rows: int = 10_000) -> list[dict[str, Any]]:
    """Return a run's processes ordered by id from one pinned read snapshot."""
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("fala read: run_id must be non-empty text")
    limit = _limit(max_rows); path = _path(db_path)
    with _open(path) as conn:
        rows = conn.execute(f"SELECT {','.join(_PROCESS_COLUMNS)} FROM processes WHERE run_id=? ORDER BY id ASC LIMIT ?", (run_id, limit + 1)).fetchall()
        if len(rows) > limit:
            raise RuntimeError("fala read: process row limit exceeded")
        return [_row(row, _PROCESS_COLUMNS) for row in rows]

__all__ = ["get_run", "list_processes", "list_runs"]
