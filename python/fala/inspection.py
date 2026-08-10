"""Read-only operational inspection of Fala schema-v6 process leases."""

from __future__ import annotations

import json
import math
import sqlite3
from datetime import datetime, timezone
from os import fspath
from pathlib import Path
from typing import Any
from urllib.parse import quote

_ENVELOPE_VERSION = 1
_SCHEMA_VERSION = 6
_PROCESS_STATUSES = frozenset({
    "pending", "ready", "running", "waiting", "retry_wait", "succeeded",
    "failed", "cancel_requested", "cancelled", "timed_out",
})
_RUN_STATUSES = frozenset({
    "created", "active", "waiting", "completed", "failed",
    "cancel_requested", "cancelled", "timed_out",
})
# Every process row is evidence: filtering here could hide corrupt status values.
_LEASE_QUERY = """
SELECT p.run_id, p.id, p.status, p.lease_owner, p.lease_expires_at,
       p.attempt, p.max_attempts, r.status
FROM processes AS p
LEFT JOIN runs AS r ON r.id = p.run_id
ORDER BY p.run_id ASC, p.id ASC
"""
_SCHEMA_QUERY = """
SELECT version FROM schema_migrations
WHERE id = 'runtime_backend'
"""


def _utc(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("timestamp is empty")
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    else:
        raise TypeError("now must be a datetime, ISO-8601 string, or None")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include a UTC offset")
    return parsed.astimezone(timezone.utc)


def _stamp(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _envelope(now: datetime | None = None) -> dict[str, Any]:
    return {
        "version": _ENVELOPE_VERSION,
        "schema_version": _SCHEMA_VERSION,
        "observed_at": _stamp(now) if now is not None else None,
        "semantics": {
            "run": "context_only_not_leased",
            "process": "claimed_while_status_running_with_owner_and_expiry",
            "reaction": "durable_artifact_not_leased",
        },
        "current": [], "expired": [], "uncertainty": [], "errors": [],
        "complete": False,
    }


def _json_safe(value: Any) -> Any:
    """Convert even corrupt SQLite/exception context to JSON-safe values."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, bytes):
        return {"storage_class": "blob", "hex": value.hex()}
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return str(value)


def _issue(code: str, message: str, **context: Any) -> dict[str, Any]:
    result: dict[str, Any] = {"code": code, "message": message}
    result.update({key: _json_safe(value) for key, value in context.items()})
    return result


def _finish(result: dict[str, Any]) -> dict[str, Any]:
    safe = _json_safe(result)
    json.dumps(safe, allow_nan=False)
    return safe


def _pin_read_snapshot(connection: sqlite3.Connection) -> None:
    """Start and materialize a SQLite read transaction."""
    connection.execute("BEGIN")
    connection.execute("PRAGMA schema_version").fetchone()


def inspect_leases(
    db_path: str | bytes | Path,
    now: datetime | str | None = None,
    *,
    max_rows: int = 100_000,
) -> dict[str, Any]:
    """Inspect current Fala leases through one pinned read-only transaction.

    All process rows up to ``max_rows`` are validated before known rows without
    lease state are omitted. The JSON-safe API-v1 result fails closed on any
    malformed evidence.
    """
    result = _envelope()
    try:
        observed = _utc(now)
        result["observed_at"] = _stamp(observed)
    except (TypeError, ValueError, OverflowError) as exc:
        result["errors"].append(_issue("invalid_now", str(exc)))
        return _finish(result)

    if isinstance(max_rows, bool) or not isinstance(max_rows, int) or max_rows < 1:
        result["errors"].append(_issue("invalid_limit", "max_rows must be a positive integer"))
        return _finish(result)

    try:
        path = Path(fspath(db_path)).expanduser().resolve(strict=True)
        if not path.is_file():
            raise FileNotFoundError(f"not a regular file: {path}")
    except (TypeError, ValueError, OSError) as exc:
        result["errors"].append(_issue("database_unavailable", str(exc)))
        return _finish(result)

    rows: list[tuple[Any, ...]] = []
    uri = f"file:{quote(str(path), safe='/')}?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=0)
        try:
            _pin_read_snapshot(connection)
            row = connection.execute(_SCHEMA_QUERY).fetchone()
            user_version = connection.execute("PRAGMA user_version").fetchone()
            if (row is None or row[0] != _SCHEMA_VERSION or user_version is None
                    or user_version[0] != _SCHEMA_VERSION):
                result["errors"].append(_issue("unsupported_schema", "Fala schema v6 is required"))
                return _finish(result)

            for row_number, process_row in enumerate(connection.execute(_LEASE_QUERY), start=1):
                if row_number > max_rows:
                    result["errors"].append(_issue(
                        "row_limit_exceeded", "lease inspection row limit exceeded",
                        max_rows=max_rows,
                    ))
                    break
                rows.append(process_row)
        finally:
            connection.close()
    except sqlite3.Error as exc:
        result["errors"].append(_issue("database_error", str(exc)))
        return _finish(result)

    for run_id, process_id, status, owner, expires, attempt, max_attempts, run_status in rows:
        context = {"run_id": run_id, "process_id": process_id}
        malformed: list[str] = []
        if not isinstance(run_id, str) or not run_id:
            malformed.append("run_id must be non-empty TEXT")
        if not isinstance(process_id, str) or not process_id:
            malformed.append("process_id must be non-empty TEXT")
        if not isinstance(status, str) or status not in _PROCESS_STATUSES:
            malformed.append("process status must be an allowed TEXT value")
        if not isinstance(run_status, str) or run_status not in _RUN_STATUSES:
            malformed.append("run status must be an allowed TEXT value")
        if owner is not None and (not isinstance(owner, str) or not owner):
            malformed.append("lease owner must be NULL or non-empty TEXT")
        if expires is not None and (not isinstance(expires, str) or not expires):
            malformed.append("lease expiry must be NULL or non-empty TEXT")
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 0:
            malformed.append("attempt must be a non-negative INTEGER")
        if (isinstance(max_attempts, bool) or not isinstance(max_attempts, int)
                or max_attempts < 1):
            malformed.append("max_attempts must be a positive INTEGER")
        if (isinstance(attempt, int) and not isinstance(attempt, bool)
                and isinstance(max_attempts, int) and not isinstance(max_attempts, bool)
                and max_attempts >= 1 and attempt > max_attempts):
            malformed.append("attempt must not exceed max_attempts")
        if malformed:
            result["uncertainty"].append(_issue(
                "malformed_process_row", "; ".join(malformed), status=status,
                run_status=run_status, attempt=attempt, max_attempts=max_attempts,
                lease_owner=owner, lease_expires_at=expires, **context,
            ))
            continue

        if status != "running":
            if owner is not None or expires is not None:
                result["uncertainty"].append(_issue(
                    "lease_on_non_running_process",
                    "lease fields exist on a process that is not running",
                    status=status, **context,
                ))
            continue
        if owner is None or expires is None:
            result["uncertainty"].append(_issue(
                "incomplete_process_lease",
                "running process lease requires owner and expiry", **context,
            ))
            continue
        if run_status != "active":
            result["uncertainty"].append(_issue(
                "lease_outside_active_run",
                "running process lease does not belong to an active run",
                run_status=run_status, **context,
            ))
            continue
        try:
            expiry = _utc(expires)
        except (TypeError, ValueError, OverflowError) as exc:
            result["uncertainty"].append(_issue(
                "invalid_lease_expiry", str(exc), lease_expires_at=expires, **context,
            ))
            continue
        lease = {
            "kind": "process", "run_id": run_id, "process_id": process_id,
            "owner": owner, "lease_expires_at": _stamp(expiry),
            "attempt": attempt, "max_attempts": max_attempts,
            "process_status": status, "run_status": run_status,
        }
        (result["expired"] if expiry <= observed else result["current"]).append(lease)

    result["complete"] = not result["errors"] and not result["uncertainty"]
    return _finish(result)


__all__ = ["inspect_leases"]
