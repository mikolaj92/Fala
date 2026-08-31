"""Thin in-process Fala host API.

- **Memory path:** ``host_drive`` / ``open_memory`` (Mojo native extension auto-builds on first use, which dynamically pulls sqlite.fire sources).
- **Durable path:** ``open_sqlite`` / ``host_run_package`` / ``record_in_process`` / ``delete_terminal_run`` /
  ``maintain_journal`` / ``recover_incomplete``
  (optional SQLite journal sink via sqlite.fire; auto-builds the native library
  on first use — #106 / #108).

Heavy multi-organ CLI / bridge / projections stay on the Mojo CLI surface.
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence, TypeVar, TypedDict

from fala._build import (
    ensure_native,
    ensure_process_host_library,
    ensure_sqlite_fire_library,
)

# Process-global cwd/env mutations in ``_with_sqlite_cwd`` are not thread-safe
# without serialization: concurrent durable host entrypoints race chdir restore
# against relative dylib ``dlopen`` (#128).
_SQLITE_CWD_LOCK = threading.RLock()
_IN_PROCESS_LOCKS_GUARD = threading.Lock()
_IN_PROCESS_LOCKS: dict[Path, threading.Lock] = {}
_T = TypeVar("_T")


def host_drive(
    *,
    run_id: str,
    path: Mapping[str, Any],
    impulse: Mapping[str, Any] | None = None,
    outputs: Mapping[str, Any] | None = None,
    stream_id: str = "memory://host",
    title: str = "",
    max_ticks: int = 16,
) -> dict[str, Any]:
    """Memory host: create_run → impulse → instantiate path → drive_until_idle."""
    request: dict[str, Any] = {
        "stream_id": stream_id,
        "run_id": run_id,
        "title": title,
        "path": dict(path),
        "max_ticks": max_ticks,
    }
    if impulse is not None:
        request["impulse"] = dict(impulse)
    if outputs is not None:
        request["outputs"] = dict(outputs)
    return host_drive_json(request)


def host_drive_json(request: str | Mapping[str, Any]) -> dict[str, Any]:
    """Low-level JSON entry (see ``_native.host_drive_json``)."""
    if isinstance(request, Mapping):
        payload = json.dumps(request)
    else:
        payload = request
    native = ensure_native()
    out = native.host_drive(payload)
    if not isinstance(out, dict):
        raise RuntimeError("fala: host_drive result is not an object")
    return out


def open_memory(
    *,
    run_id: str = "run",
    stream_id: str = "memory://host",
    title: str = "",
) -> "MemoryHost":
    """Convenience factory for a multi-step memory host session (builder style)."""
    return MemoryHost(run_id=run_id, stream_id=stream_id, title=title)


class MemoryHost:
    """Python-side builder that collapses into one ``host_drive`` call.

    Keeps the public surface close to open → accept → drive without holding a
    live Mojo runtime across calls (no dual session state).
    """

    def __init__(
        self,
        *,
        run_id: str = "run",
        stream_id: str = "memory://host",
        title: str = "",
    ) -> None:
        self.run_id = run_id
        self.stream_id = stream_id
        self.title = title
        self._impulse: dict[str, Any] | None = None
        self._path: dict[str, Any] | None = None
        self._outputs: dict[str, Any] = {}
        self._max_ticks = 16

    def accept_impulse(
        self,
        *,
        impulse_id: str,
        impulse_type: str = "case",
        payload: Mapping[str, Any] | str | None = None,
    ) -> "MemoryHost":
        if payload is None:
            payload_obj: Any = {}
        elif isinstance(payload, str):
            payload_obj = payload
        else:
            payload_obj = dict(payload)
        self._impulse = {
            "id": impulse_id,
            "type": impulse_type,
            "payload": payload_obj,
        }
        return self

    def set_path(
        self,
        path_id: str,
        effectors: Sequence[Mapping[str, Any]],
    ) -> "MemoryHost":
        self._path = {"id": path_id, "effectors": [dict(e) for e in effectors]}
        return self

    def register_output(
        self, effector_id: str, output: Mapping[str, Any] | str
    ) -> "MemoryHost":
        self._outputs[effector_id] = output if isinstance(output, str) else dict(output)
        return self

    def drive(self, max_ticks: int = 16) -> dict[str, Any]:
        if self._path is None:
            raise ValueError("MemoryHost: set_path() required before drive()")
        self._max_ticks = max_ticks
        return host_drive(
            run_id=self.run_id,
            stream_id=self.stream_id,
            title=self.title,
            path=self._path,
            impulse=self._impulse,
            outputs=self._outputs or None,
            max_ticks=max_ticks,
        )


def _with_sqlite_cwd(fn, process_host_library: Path | None = None):  # type: ignore[no-untyped-def]
    """Run *fn* with cwd at vendor/sqlite.fire (dylib load path).

    Subprocess effectors without an explicit ``FALA_EFFECTOR_ROOT`` create
    ``.fala-effector-*`` workdirs under cwd. Durable hosts chdir into the
    package vendor tree for dylib discovery, so when the env root is unset we
    install a process-local temporary root for the duration of *fn* and clean
    it up afterward (#119). An already-configured root is preserved as-is.
    When supplied, ``process_host_library`` is passed through the hardened
    native loader without overriding an explicit caller setting.

    The whole critical section (chdir, env install/restore, *fn*) is serialized
    with a module-level ``RLock`` so concurrent ``host_run_package`` /
    ``open_sqlite`` callers cannot restore another thread's previous cwd before
    relative dylib ``dlopen`` completes (#128).
    """
    from fala._build import repo_root

    with _SQLITE_CWD_LOCK:
        sqlite_cwd = repo_root() / "vendor" / "sqlite.fire"
        prev = os.getcwd()
        previous_effector_root = os.environ.get("FALA_EFFECTOR_ROOT")
        previous_process_host_library = os.environ.get("FALA_PROCESS_HOST_LIBRARY")
        owned_root: tempfile.TemporaryDirectory[str] | None = None
        try:
            if sqlite_cwd.is_dir():
                os.chdir(sqlite_cwd)
            if previous_effector_root is None or not previous_effector_root.strip():
                owned_root = tempfile.TemporaryDirectory(prefix="fala-effectors-")
                os.environ["FALA_EFFECTOR_ROOT"] = owned_root.name
            if (
                previous_process_host_library is None
                or not previous_process_host_library.strip()
            ) and process_host_library is not None:
                os.environ["FALA_PROCESS_HOST_LIBRARY"] = str(
                    process_host_library.resolve()
                )
            return fn()
        finally:
            if previous_process_host_library is None:
                os.environ.pop("FALA_PROCESS_HOST_LIBRARY", None)
            else:
                os.environ["FALA_PROCESS_HOST_LIBRARY"] = previous_process_host_library
            if previous_effector_root is None:
                os.environ.pop("FALA_EFFECTOR_ROOT", None)
            else:
                os.environ["FALA_EFFECTOR_ROOT"] = previous_effector_root
            if owned_root is not None:
                owned_root.cleanup()
            os.chdir(prev)


def open_sqlite(path: str | Path) -> dict[str, Any]:
    """Probe-open a durable SQLite journal via the Mojo engine (creates if needed).

    Ensures ``libsqlite_fire`` is present (automatically builds once, dynamically
    cloning the source repository from GitHub if needed). Memory path does not call this.
    """
    p = Path(path).expanduser().resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    ensure_sqlite_fire_library()
    native = ensure_native()

    def _call() -> dict[str, Any]:
        out = native.open_sqlite_journal(str(p))
        if not isinstance(out, dict) or not out.get("ok"):
            raise RuntimeError(f"fala.open_sqlite failed: {out!r}")
        return out

    return _with_sqlite_cwd(_call)


def record_in_process(
    *,
    db_path: str | Path,
    run_id: str,
    process_id: str,
    operation: Callable[[], _T],
    inputs: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
    process_type: str = "in_process",
) -> _T:
    """Run one Python callback and durably record its terminal process row.

    The callback runs in the calling process and is invoked exactly once.  Its
    JSON-recordable return value is stored as ``output_json`` and returned
    unchanged.  If it raises, a failed row containing the exception type and
    message is committed and the original exception is re-raised.

    Recording is fail-closed: inputs and metadata are validated before the
    callback, and a non-JSON-recordable return value turns the row into a
    failure.  Calls for the same database are single-flight and reject rather
    than wait when another callback is active.  The referenced run must already
    exist. Durable INSERT/UPDATE is native (#189).
    """
    from datetime import datetime, timezone

    db = Path(db_path).expanduser().resolve()
    rid = str(run_id).strip()
    pid = str(process_id).strip()
    ptype = str(process_type).strip()
    if not rid or not pid or not ptype:
        raise ValueError(
            "fala.record_in_process: run_id, process_id, and process_type must not be blank"
        )
    if not callable(operation):
        raise TypeError("fala.record_in_process: operation must be callable")

    input_json = _record_json(dict(inputs or {}), "inputs")
    metadata_json = _record_json(dict(metadata or {}), "metadata")
    db.parent.mkdir(parents=True, exist_ok=True)

    with _IN_PROCESS_LOCKS_GUARD:
        flight = _IN_PROCESS_LOCKS.setdefault(db, threading.Lock())
    if not flight.acquire(blocking=False):
        raise RuntimeError(f"fala.record_in_process: execution already active for {db}")

    def now() -> str:
        return (
            datetime.now(timezone.utc)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z")
        )

    def _native(operation_name: str, request: dict[str, Any]) -> None:
        ensure_sqlite_fire_library()
        native = ensure_native()
        fn = getattr(native, operation_name)

        def _call() -> None:
            try:
                fn(json.dumps(request, ensure_ascii=False, sort_keys=True))
            except Exception as exc:
                message = str(exc)
                if "disappeared" in message:
                    raise RuntimeError(message) from exc
                if message.startswith("fala.record_in_process:"):
                    raise ValueError(message) from exc
                raise RuntimeError(
                    f"fala.record_in_process: {operation_name} failed: {message}"
                ) from exc

        _with_sqlite_cwd(_call)

    try:
        started = now()
        _native(
            "record_process_start",
            {
                "db_path": str(db),
                "run_id": rid,
                "process_id": pid,
                "process_type": ptype,
                "input_json": input_json,
                "metadata_json": metadata_json,
                "now": started,
            },
        )
        try:
            result = operation()
        except BaseException as exc:
            error_json = _record_json(
                {"message": str(exc), "type": type(exc).__name__}, "exception"
            )
            try:
                _native(
                    "record_process_finish",
                    {
                        "db_path": str(db),
                        "run_id": rid,
                        "process_id": pid,
                        "status": "failed",
                        "output_json": "{}",
                        "error_json": error_json,
                        "now": now(),
                    },
                )
            except Exception as recording_error:
                exc.add_note(
                    f"Fala could not record process failure: {recording_error}"
                )
            raise

        try:
            output_json = _record_json(result, "operation result")
        except (TypeError, ValueError) as exc:
            error_json = _record_json(
                {"message": str(exc), "type": type(exc).__name__}, "exception"
            )
            _native(
                "record_process_finish",
                {
                    "db_path": str(db),
                    "run_id": rid,
                    "process_id": pid,
                    "status": "failed",
                    "output_json": "{}",
                    "error_json": error_json,
                    "now": now(),
                },
            )
            raise
        _native(
            "record_process_finish",
            {
                "db_path": str(db),
                "run_id": rid,
                "process_id": pid,
                "status": "succeeded",
                "output_json": output_json,
                "error_json": "{}",
                "now": now(),
            },
        )
        return result
    finally:
        flight.release()


def _ensure_durable_schema(db: Path) -> None:
    """Initialize the native schema through the host journal contract."""
    from fala.journal import ensure_journal

    ensure_journal(db)


def _record_json(value: Any, label: str) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as exc:
        raise TypeError(
            f"fala.record_in_process: {label} is not JSON-recordable"
        ) from exc


def host_run_package(
    *,
    db_path: str | Path,
    package_path: str | Path,
    path_id: str,
    run_id: str = "run",
    inputs: Mapping[str, Any] | None = None,
    effector_inputs: Mapping[str, Mapping[str, Any]] | None = None,
    effector_configs: Mapping[str, Mapping[str, Any] | str] | None = None,
    command_overrides: Mapping[str, Sequence[str]] | None = None,
    max_ticks: int = 32,
    worker_id: str = "python-host",
) -> dict[str, Any]:
    """Drive one correlation path from a TOML package on a SQLite journal (Mojo).

    Ensures both native libraries required by the durable subprocess path before
    loading the Mojo host.  The returned ``effector_results`` mapping is keyed by
    package effector id; each value contains process ``id`` and ``status`` plus
    decoded ``output`` and ``error`` JSON values for the stored process.

    For a non-terminal process, stored ``{}`` is the typed no-result / no-error
    placeholder and is returned as empty objects. Malformed stored JSON fails
    closed naming ``run_id``, ``process_id``, and the field, without embedding
    payload text.
    """
    from datetime import datetime, timezone

    db = Path(db_path).expanduser().resolve()
    pkg = Path(package_path).expanduser().resolve()
    db.parent.mkdir(parents=True, exist_ok=True)
    if not pkg.is_file():
        raise FileNotFoundError(f"fala package not found: {pkg}")

    import os

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    request: dict[str, Any] = {
        "db_path": str(db),
        "package_path": str(pkg),
        "path_id": path_id,
        "run_id": run_id,
        "max_ticks": max_ticks,
        "worker_id": worker_id,
        "created_at": now,
        "now": now,
        "lease_expires_at": "2099-01-01T00:00:00Z",
        # Ambient host env for subprocess inherit_env / base keys (#108 / v0.7.6).
        # Mojo materializes these into adapter.env before dispatch.
        "host_environment": dict(os.environ),
    }
    if inputs:
        # Native JSON types; Mojo host converts Values to value_json via to_string.
        request["inputs"] = dict(inputs)
    if effector_inputs:
        request["effector_inputs"] = {
            step: dict(payload) for step, payload in effector_inputs.items()
        }
    if effector_configs:
        # Config objects stay objects; string configs pass through as JSON text.
        request["effector_configs"] = {
            step: (cfg if isinstance(cfg, str) else dict(cfg))
            for step, cfg in effector_configs.items()
        }
    if command_overrides:
        request["command_overrides"] = {
            k: list(v) for k, v in command_overrides.items()
        }

    configured_process_host = os.environ.get("FALA_PROCESS_HOST_LIBRARY", "")
    if configured_process_host.strip():
        process_host_library = Path(configured_process_host)
        if not process_host_library.is_absolute():
            raise ValueError("FALA_PROCESS_HOST_LIBRARY must be an absolute path")
        if not process_host_library.is_file():
            raise ValueError(
                "FALA_PROCESS_HOST_LIBRARY must name an existing regular file"
            )
    else:
        process_host_library = ensure_process_host_library()
    ensure_sqlite_fire_library()
    native = ensure_native()

    def _call() -> dict[str, Any]:
        out = native.host_run_package(json.dumps(request))
        if not isinstance(out, dict):
            raise RuntimeError(f"fala.host_run_package failed: {out!r}")
        return out

    return _with_sqlite_cwd(_call, process_host_library)


class MaintenanceRun(TypedDict):
    run_id: str
    status: str
    created_at: str
    updated_at: str
    finished_at: str
    deleted: bool
    row_counts: dict[str, int]


class JournalMaintenanceResult(TypedDict):
    ok: bool
    dry_run: bool
    older_than_days: float
    keep_last: int
    vacuum: bool
    before: str
    candidate_count: int
    deleted_run_count: int
    row_counts: dict[str, int]
    runs: list[MaintenanceRun]
    reaction_gc: dict[str, int]
    vacuumed: bool


class RecoveryItem(TypedDict):
    run_id: str
    process_id: str
    previous_status: str
    status: str
    attempt: int
    max_attempts: int


class IncompleteRecoveryResult(TypedDict):
    ok: bool
    worker_id: str
    now: str
    recovered_count: int
    requeued_count: int
    unrecoverable_count: int
    items: list[RecoveryItem]


def _durable_database(db_path: str | Path, operation: str) -> Path:
    db = Path(db_path).expanduser().resolve()
    if not db.is_file():
        raise FileNotFoundError(f"fala.{operation}: database not found: {db}")
    return db


def maintain_journal(
    db_path: str | Path,
    *,
    older_than_days: float,
    keep_last: int = -1,
    vacuum: bool = False,
    dry_run: bool = True,
    reaction_root: str | Path | None = None,
) -> JournalMaintenanceResult:
    """Plan or apply native terminal-run retention and optional maintenance.

    ``dry_run=True`` is the safe default. ``keep_last`` preserves the newest N
    terminal runs; nonterminal runs are never candidates. The consumer never
    receives a SQL escape hatch: selection, deletion, trigger restoration,
    reaction GC, and VACUUM remain native store operations.
    """
    if isinstance(older_than_days, bool) or not isinstance(older_than_days, (int, float)):
        raise TypeError("fala.maintain_journal: older_than_days must be numeric")
    age = float(older_than_days)
    if not __import__("math").isfinite(age) or age < 0:
        raise ValueError("fala.maintain_journal: older_than_days must be finite and non-negative")
    if isinstance(keep_last, bool) or not isinstance(keep_last, int) or keep_last < -1:
        raise ValueError("fala.maintain_journal: keep_last must be -1 or non-negative")
    if not isinstance(vacuum, bool) or not isinstance(dry_run, bool):
        raise TypeError("fala.maintain_journal: vacuum and dry_run must be bool")
    db = _durable_database(db_path, "maintain_journal")
    root = ""
    if reaction_root is not None:
        reaction_path = Path(reaction_root).expanduser().resolve()
        if not reaction_path.is_dir():
            raise FileNotFoundError(f"fala.maintain_journal: reaction root not found: {reaction_path}")
        root = str(reaction_path)
    ensure_sqlite_fire_library()
    native = ensure_native()
    request = {"db_path": str(db), "older_than_days": age, "keep_last": keep_last,
               "vacuum": vacuum, "dry_run": dry_run, "reaction_root": root}

    def _call() -> JournalMaintenanceResult:
        try:
            out = native.maintain_journal(json.dumps(request, sort_keys=True))
        except Exception as exc:
            raise RuntimeError(f"fala.maintain_journal failed: {exc}") from exc
        if not isinstance(out, dict) or out.get("ok") is not True:
            raise RuntimeError(f"fala.maintain_journal failed: {out!r}")
        return out  # type: ignore[return-value]

    return _with_sqlite_cwd(_call)


def recover_incomplete(
    db_path: str | Path,
    *,
    worker_id: str,
    now: str,
    retry_available_at: str | None = None,
) -> IncompleteRecoveryResult:
    """Resolve this worker's expired running claims through native journal APIs.

    Live leases and other workers' leases remain untouched. Expired claims are
    requeued when their persisted retry policy/attempt budget permits, otherwise
    they become terminally failed. A repeated call is an idempotent no-op.
    Timestamps must be timezone-qualified ISO-8601 strings; lexical ordering is
    intentionally restricted to normalized UTC (``Z``) values.
    """
    worker = str(worker_id or "").strip()
    if not worker:
        raise ValueError("fala.recover_incomplete: worker_id must not be blank")
    from datetime import datetime

    def timestamp(value: str, label: str) -> str:
        if not isinstance(value, str) or not value.endswith("Z"):
            raise ValueError(f"fala.recover_incomplete: {label} must be an ISO-8601 UTC timestamp ending in Z")
        try:
            datetime.fromisoformat(value[:-1] + "+00:00")
        except ValueError as exc:
            raise ValueError(f"fala.recover_incomplete: invalid {label}") from exc
        return value

    at = timestamp(now, "now")
    due = timestamp(retry_available_at or at, "retry_available_at")
    if due < at:
        raise ValueError("fala.recover_incomplete: retry_available_at must not precede now")
    db = _durable_database(db_path, "recover_incomplete")
    ensure_sqlite_fire_library()
    native = ensure_native()
    request = {"db_path": str(db), "worker_id": worker, "now": at, "retry_available_at": due}

    def _call() -> IncompleteRecoveryResult:
        try:
            out = native.recover_incomplete(json.dumps(request, sort_keys=True))
        except Exception as exc:
            raise RuntimeError(f"fala.recover_incomplete failed: {exc}") from exc
        if not isinstance(out, dict) or out.get("ok") is not True:
            raise RuntimeError(f"fala.recover_incomplete failed: {out!r}")
        return out  # type: ignore[return-value]

    return _with_sqlite_cwd(_call)



def delete_terminal_run(db_path: str | Path, run_id: str) -> dict[str, Any]:
    """Delete one terminal durable run via the Mojo store transaction.

    Allowed statuses: ``completed``, ``failed``, ``cancelled``, ``timed_out``.
    Blank / unknown / non-terminal runs raise ``ValueError`` or ``RuntimeError``
    without durable writes; append-only triggers stay restored on failure.

    Returns deterministic table counts including ``run_id`` and ``total``.
    """
    rid = str(run_id or "").strip()
    if not rid:
        raise ValueError("fala.delete_terminal_run: run_id must not be blank")

    db = Path(db_path).expanduser().resolve()
    if not db.is_file():
        raise FileNotFoundError(f"fala.delete_terminal_run: database not found: {db}")

    ensure_sqlite_fire_library()
    native = ensure_native()
    request = {"db_path": str(db), "run_id": rid}

    def _call() -> dict[str, Any]:
        try:
            out = native.delete_terminal_run(json.dumps(request))
        except Exception as exc:  # Mojo raises Error → Python exception
            message = str(exc)
            if "run_id must not be empty" in message:
                raise ValueError(
                    "fala.delete_terminal_run: run_id must not be blank"
                ) from exc
            if "unknown run" in message:
                raise ValueError(
                    f"fala.delete_terminal_run: unknown run: {rid}"
                ) from exc
            if "not terminal" in message:
                raise ValueError(
                    f"fala.delete_terminal_run: run is not terminal: {rid}"
                ) from exc
            raise RuntimeError(f"fala.delete_terminal_run failed: {message}") from exc
        if not isinstance(out, dict) or not out.get("ok"):
            raise RuntimeError(f"fala.delete_terminal_run failed: {out!r}")
        return out

    return _with_sqlite_cwd(_call)
