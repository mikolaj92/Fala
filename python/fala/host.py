"""Thin in-process Fala host API.

- **Memory path:** ``host_drive`` / ``open_memory`` (no sqlite.fire required).
- **Durable path:** ``open_sqlite`` / ``host_run_package`` / ``delete_terminal_run``
  (optional SQLite journal sink via sqlite.fire; auto-builds the native library
  on first use — #106 / #108).

Heavy multi-organ CLI / bridge / projections stay on the Mojo CLI surface.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence

from fala._build import (
    ensure_native,
    ensure_process_host_library,
    ensure_sqlite_fire_library,
)


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
    raw = native.host_drive_json(payload)
    if not isinstance(raw, str):
        raw = str(raw)
    out = json.loads(raw)
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

    def register_output(self, effector_id: str, output: Mapping[str, Any] | str) -> "MemoryHost":
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


def _with_sqlite_cwd(fn):  # type: ignore[no-untyped-def]
    """Run *fn* with cwd at vendor/sqlite.fire (dylib load path).

    Subprocess effectors without an explicit ``FALA_EFFECTOR_ROOT`` create
    ``.fala-effector-*`` workdirs under cwd. Durable hosts chdir into the
    package vendor tree for dylib discovery, so when the env root is unset we
    install a process-local temporary root for the duration of *fn* and clean
    it up afterward (#119). An already-configured root is preserved as-is.
    """
    from fala._build import repo_root

    sqlite_cwd = repo_root() / "vendor" / "sqlite.fire"
    prev = os.getcwd()
    previous_effector_root = os.environ.get("FALA_EFFECTOR_ROOT")
    owned_root: tempfile.TemporaryDirectory[str] | None = None
    try:
        if sqlite_cwd.is_dir():
            os.chdir(sqlite_cwd)
        if previous_effector_root is None or not previous_effector_root.strip():
            owned_root = tempfile.TemporaryDirectory(prefix="fala-effectors-")
            os.environ["FALA_EFFECTOR_ROOT"] = owned_root.name
        return fn()
    finally:
        if previous_effector_root is None:
            os.environ.pop("FALA_EFFECTOR_ROOT", None)
        else:
            os.environ["FALA_EFFECTOR_ROOT"] = previous_effector_root
        if owned_root is not None:
            owned_root.cleanup()
        os.chdir(prev)


def open_sqlite(path: str | Path) -> dict[str, Any]:
    """Probe-open a durable SQLite journal via the Mojo engine (creates if needed).

    Ensures ``libsqlite_fire`` is present (builds once if hatch-shipped sources
    are available). Memory path does not call this.
    """
    p = Path(path).expanduser().resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    ensure_sqlite_fire_library()
    native = ensure_native()

    def _call() -> dict[str, Any]:
        raw = native.open_sqlite_journal(str(p))
        if not isinstance(raw, str):
            raw = str(raw)
        out = json.loads(raw)
        if not isinstance(out, dict) or not out.get("ok"):
            raise RuntimeError(f"fala.open_sqlite failed: {raw!r}")
        return out

    return _with_sqlite_cwd(_call)


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
    loading the Mojo host.
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
        request["command_overrides"] = {k: list(v) for k, v in command_overrides.items()}

    ensure_process_host_library()
    ensure_sqlite_fire_library()
    native = ensure_native()

    def _call() -> dict[str, Any]:
        raw = native.host_run_package_json(json.dumps(request))
        if not isinstance(raw, str):
            raw = str(raw)
        out = json.loads(raw)
        if not isinstance(out, dict):
            raise RuntimeError(f"fala.host_run_package failed: {raw!r}")
        return out

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
            raw = native.delete_terminal_run_json(json.dumps(request))
        except Exception as exc:  # Mojo raises Error → Python exception
            message = str(exc)
            if "run_id must not be empty" in message:
                raise ValueError("fala.delete_terminal_run: run_id must not be blank") from exc
            if "unknown run" in message:
                raise ValueError(f"fala.delete_terminal_run: unknown run: {rid}") from exc
            if "not terminal" in message:
                raise ValueError(
                    f"fala.delete_terminal_run: run is not terminal: {rid}"
                ) from exc
            raise RuntimeError(f"fala.delete_terminal_run failed: {message}") from exc
        if not isinstance(raw, str):
            raw = str(raw)
        out = json.loads(raw)
        if not isinstance(out, dict) or not out.get("ok"):
            raise RuntimeError(f"fala.delete_terminal_run failed: {raw!r}")
        return out

    return _with_sqlite_cwd(_call)

