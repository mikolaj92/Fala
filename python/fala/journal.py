"""Safe, domain-blind lifecycle operations for schema-v6 Fala journals.

Durable writes go through the Mojo host journal. This module keeps the public
Python API and fail-closed argument checks; it does not copy schema-v6 SQL.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict

from fala._build import ensure_native, ensure_sqlite_fire_library
from fala.host import _with_sqlite_cwd

RUN_STATUSES = frozenset({"created", "active", "waiting", "completed", "failed", "cancel_requested", "cancelled", "timed_out"})
PROCESS_STATUSES = frozenset({"pending", "ready", "running", "waiting", "retry_wait", "cancel_requested", "succeeded", "failed", "cancelled", "timed_out"})
BLOCKER_STATUSES = frozenset({"open", "completed", "cancelled", "expired"})
TERMINAL_RUN_STATUSES = frozenset({"completed", "failed", "cancelled", "timed_out"})
TERMINAL_PROCESS_STATUSES = frozenset({"succeeded", "failed", "cancelled", "timed_out"})
TERMINAL_BLOCKER_STATUSES = frozenset({"completed", "cancelled", "expired"})
_ENSURE_LOCK = threading.Lock()


class LifecycleResult(TypedDict):
    run_id: str
    process_id: str
    changed: bool
    process_status: str
    run_status: str


def _db(db_path: str | Path) -> Path:
    return Path(db_path).expanduser().resolve()


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _required(value: str, label: str) -> str:
    result = str(value).strip()
    if not result:
        raise ValueError(f"fala journal: {label} must not be blank")
    return result


def _status(value: str, label: str, allowed: frozenset[str]) -> str:
    result = _required(value, label)
    if result not in allowed:
        raise ValueError(f"fala journal: invalid {label} {result!r}")
    return result


def _json(value: Mapping[str, Any] | None, label: str) -> str:
    try:
        return json.dumps(dict(value or {}), ensure_ascii=False, allow_nan=False, sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"fala journal: {label} is not JSON-recordable") from exc


def _native_call(operation: str, request: dict[str, Any]) -> Any:
    ensure_sqlite_fire_library()
    native = ensure_native()
    fn = getattr(native, operation)

    def _call() -> Any:
        try:
            return fn(json.dumps(request, ensure_ascii=False, sort_keys=True))
        except Exception as exc:
            message = str(exc)
            if "incompatible" in message or "schema-v6" in message or "initialization incomplete" in message:
                raise RuntimeError(message) from exc
            if "fala journal:" in message or "fala.record_in_process:" in message:
                if "is not JSON-recordable" in message:
                    raise TypeError(message) from exc
                raise ValueError(message) from exc
            raise RuntimeError(f"fala journal: {operation} failed: {message}") from exc

    return _with_sqlite_cwd(_call)


def ensure_journal(db_path: str | Path) -> None:
    """Ensure and validate schema v6 on every call (no pathname cache)."""
    db = _db(db_path)
    db.parent.mkdir(parents=True, exist_ok=True)
    with _ENSURE_LOCK:
        _native_call("ensure_journal", {"db_path": str(db)})


def upsert_run_metadata(
    db_path: str | Path,
    *,
    run_id: str,
    metadata: Mapping[str, Any],
    title: str | None = None,
    status: str = "active",
) -> None:
    db, rid = _db(db_path), _required(run_id, "run_id")
    state, encoded = _status(status, "status", RUN_STATUSES), _json(metadata, "metadata")
    request: dict[str, Any] = {
        "db_path": str(db),
        "run_id": rid,
        "status": state,
        "metadata_json": encoded,
        "now": _now(),
        "title_present": title is not None,
    }
    if title is not None:
        request["title"] = str(title)
    _native_call("upsert_run_metadata", request)


def transition_run(
    db_path: str | Path,
    *,
    run_id: str,
    status: str,
    metadata_updates: Mapping[str, Any] | None = None,
) -> None:
    db, rid = _db(db_path), _required(run_id, "run_id")
    state = _status(status, "status", RUN_STATUSES)
    updates = dict(metadata_updates or {})
    encoded = _json(updates, "metadata_updates")
    _native_call(
        "transition_run",
        {
            "db_path": str(db),
            "run_id": rid,
            "status": state,
            "updates_json": encoded,
            "now": _now(),
        },
    )


def finalize_run(db_path: str | Path, *, run_id: str, status: str, reason: str | None = None) -> None:
    state = _status(status, "status", TERMINAL_RUN_STATUSES)
    transition_run(
        db_path,
        run_id=run_id,
        status=state,
        metadata_updates={"finalize_reason": reason} if reason else None,
    )


def upsert_process(
    db_path: str | Path,
    *,
    run_id: str,
    process_id: str,
    status: str,
    output: Mapping[str, Any] | None = None,
    error: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
    inputs: Mapping[str, Any] | None = None,
    attempt: int = 1,
    process_type: str = "external",
) -> None:
    db, rid, pid = _db(db_path), _required(run_id, "run_id"), _required(process_id, "process_id")
    state = _status(status, "status", PROCESS_STATUSES)
    ptype = _required(process_type, "process_type")
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
        raise ValueError("fala journal: attempt must be a positive integer")
    _native_call(
        "upsert_process",
        {
            "db_path": str(db),
            "run_id": rid,
            "process_id": pid,
            "status": state,
            "process_type": ptype,
            "attempt": attempt,
            "input_json": _json(inputs, "inputs"),
            "output_json": _json(output, "output"),
            "error_json": _json(error, "error"),
            "metadata_json": _json(metadata, "metadata"),
            "now": _now(),
        },
    )


def park_process(db_path: str | Path, **kwargs: Any) -> None:
    upsert_process(db_path, status="waiting", **kwargs)


def complete_waiting_process(
    db_path: str | Path,
    *,
    run_id: str,
    process_id: str,
    blocker_id: str | None = None,
    output: Mapping[str, Any] | None = None,
    process_status: str = "succeeded",
    blocker_status: str = "completed",
    run_status: str = "completed",
) -> LifecycleResult:
    db, rid, pid = _db(db_path), _required(run_id, "run_id"), _required(process_id, "process_id")
    pstate = _status(process_status, "process_status", TERMINAL_PROCESS_STATUSES)
    rstate = _status(run_status, "run_status", TERMINAL_RUN_STATUSES)
    bstate = _status(blocker_status, "blocker_status", TERMINAL_BLOCKER_STATUSES)
    bid = _required(blocker_id, "blocker_id") if blocker_id is not None else ""
    encoded = _json({"completed": True} if output is None else output, "output")
    result = _native_call(
        "complete_waiting_process",
        {
            "db_path": str(db),
            "run_id": rid,
            "process_id": pid,
            "process_status": pstate,
            "run_status": rstate,
            "blocker_id": bid,
            "has_blocker": blocker_id is not None,
            "blocker_status": bstate,
            "output_json": encoded,
            "now": _now(),
        },
    )
    if not isinstance(result, dict):
        raise RuntimeError(f"fala journal: complete_waiting_process returned {result!r}")
    return {
        "run_id": str(result["run_id"]),
        "process_id": str(result["process_id"]),
        "changed": bool(result["changed"]),
        "process_status": str(result["process_status"]),
        "run_status": str(result["run_status"]),
    }


__all__ = [
    "RUN_STATUSES",
    "PROCESS_STATUSES",
    "BLOCKER_STATUSES",
    "TERMINAL_PROCESS_STATUSES",
    "TERMINAL_RUN_STATUSES",
    "LifecycleResult",
    "complete_waiting_process",
    "ensure_journal",
    "finalize_run",
    "park_process",
    "transition_run",
    "upsert_process",
    "upsert_run_metadata",
]
