"""Read-only operational inspection of Fala schema-v6 process leases."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from os import fspath
from pathlib import Path
from typing import Any
from urllib.parse import quote

_ENVELOPE_VERSION = 1
_SCHEMA_VERSION = 6
# Fala-owned schema-v6 query. Consumers never need to know the durable schema.
_LEASE_QUERY = """
SELECT p.run_id, p.id, p.status, p.lease_owner, p.lease_expires_at,
       p.attempt, p.max_attempts, r.status
FROM processes AS p
LEFT JOIN runs AS r ON r.id = p.run_id
WHERE p.status = 'running'
   OR p.lease_owner IS NOT NULL OR p.lease_expires_at IS NOT NULL
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
        "current": [],
        "expired": [],
        "uncertainty": [],
        "errors": [],
        "complete": False,
    }


def _issue(code: str, message: str, **context: Any) -> dict[str, Any]:
    result: dict[str, Any] = {"code": code, "message": message}
    result.update(context)
    return result


def _pin_read_snapshot(connection: sqlite3.Connection) -> None:
    """Start and materialize a SQLite read transaction.

    ``BEGIN`` alone is deferred.  Reading ``schema_version`` establishes the
    snapshot before inspection performs schema validation or reads lease rows.
    """
    connection.execute("BEGIN")
    connection.execute("PRAGMA schema_version").fetchone()


def inspect_leases(
    db_path: str | bytes | Path,
    now: datetime | str | None = None,
    *,
    max_rows: int = 100_000,
) -> dict[str, Any]:
    """Inspect current Fala leases through one pinned read-only transaction.

    The JSON-safe, API-v1 result fails closed: database/schema/limit failures
    are reported in ``errors`` and ``complete`` remains false.  ``max_rows``
    bounds in-memory evidence retained by inspection without imposing a limit
    on the database or WAL file size.  SQLite may update lock/SHM metadata as
    part of safely reading a live WAL database; application rows are never
    written by this function.
    """
    result = _envelope()
    try:
        observed = _utc(now)
        result["observed_at"] = _stamp(observed)
    except (TypeError, ValueError, OverflowError) as exc:
        result["errors"].append(_issue("invalid_now", str(exc)))
        return result

    if isinstance(max_rows, bool) or not isinstance(max_rows, int) or max_rows < 1:
        result["errors"].append(
            _issue("invalid_limit", "max_rows must be a positive integer")
        )
        return result

    try:
        path = Path(fspath(db_path)).expanduser().resolve(strict=True)
        if not path.is_file():
            raise FileNotFoundError(f"not a regular file: {path}")
    except (TypeError, ValueError, OSError) as exc:
        result["errors"].append(_issue("database_unavailable", str(exc)))
        return result

    rows: list[tuple[Any, ...]] = []
    uri = f"file:{quote(str(path), safe='/')}?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=0)
        try:
            _pin_read_snapshot(connection)
            row = connection.execute(_SCHEMA_QUERY).fetchone()
            user_version = connection.execute("PRAGMA user_version").fetchone()
            if (
                row is None
                or row[0] != _SCHEMA_VERSION
                or user_version is None
                or user_version[0] != _SCHEMA_VERSION
            ):
                result["errors"].append(
                    _issue("unsupported_schema", "Fala schema v6 is required")
                )
                return result

            cursor = connection.execute(_LEASE_QUERY)
            for row_number, lease_row in enumerate(cursor, start=1):
                if row_number > max_rows:
                    result["errors"].append(_issue(
                        "row_limit_exceeded",
                        "lease inspection row limit exceeded",
                        max_rows=max_rows,
                    ))
                    break
                rows.append(lease_row)
        finally:
            connection.close()
    except sqlite3.Error as exc:
        result["errors"].append(_issue("database_error", str(exc)))
        return result

    for (
        run_id, process_id, status, owner, expires, attempt, max_attempts, run_status
    ) in rows:
        context = {"run_id": run_id, "process_id": process_id}
        if status != "running":
            result["uncertainty"].append(_issue(
                "lease_on_non_running_process",
                "lease fields exist on a process that is not running",
                status=status, **context,
            ))
            continue
        if not isinstance(owner, str) or not owner:
            result["uncertainty"].append(_issue(
                "incomplete_process_lease",
                "running process lease has no owner",
                **context,
            ))
            continue
        if not isinstance(expires, str) or not expires:
            result["uncertainty"].append(_issue(
                "incomplete_process_lease",
                "running process lease has no expiry",
                **context,
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
                "invalid_lease_expiry", str(exc), lease_expires_at=expires, **context
            ))
            continue
        lease = {
            "kind": "process",
            "run_id": run_id,
            "process_id": process_id,
            "owner": owner,
            "lease_expires_at": _stamp(expiry),
            "attempt": attempt,
            "max_attempts": max_attempts,
            "process_status": status,
            "run_status": run_status,
        }
        (result["expired"] if expiry <= observed else result["current"]).append(lease)

    result["complete"] = not result["errors"] and not result["uncertainty"]
    return result


__all__ = ["inspect_leases"]
