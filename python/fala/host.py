"""Thin in-process Fala host API (memory path only).

Heavy ops (SQLite multi-organ, bridge, projections, CLI) stay on subprocess/CLI.
"""

from __future__ import annotations

import json
from typing import Any, Mapping, Sequence

from fala._build import ensure_native


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
