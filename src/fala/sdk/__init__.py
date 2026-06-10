from __future__ import annotations

import json
import os
import re
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, TextIO
from urllib.parse import unquote, urlparse

PROCESS_RUNTIME_EVENT_PREFIX = "PROCESS_RUNTIME_EVENT "
StepHandler = Callable[[dict[str, Any]], dict[str, Any]]


def run_stdio(handler: StepHandler) -> int:
    """Run one process-runtime step over stdin/stdout JSON."""
    try:
        context = json.loads(sys.stdin.read() or "{}")
        output_value = handler(context)
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(json.dumps(output_value, ensure_ascii=False))
    return 0


def emit_event(
    event_type: str,
    *,
    status: str | None = None,
    data: dict[str, Any] | None = None,
    stream: TextIO | None = None,
) -> None:
    payload: dict[str, Any] = {"type": event_type}
    if status is not None:
        payload["status"] = status
    if data is not None:
        payload["data"] = data
    print(
        f"{PROCESS_RUNTIME_EVENT_PREFIX}{json.dumps(payload, ensure_ascii=False)}",
        file=stream or sys.stderr,
        flush=True,
    )


def input_values(context: dict[str, Any]) -> dict[str, Any]:
    return dict((context.get("input") or {}).get("values") or {})


def initial(context: dict[str, Any]) -> dict[str, Any]:
    return dict(input_values(context).get("initial") or {})


def needs(context: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return dict(input_values(context).get("needs") or {})


def input_artifacts(context: dict[str, Any]) -> list[dict[str, Any]]:
    return list((context.get("input") or {}).get("artifacts") or [])


def artifact_root(context: dict[str, Any], process_id: str) -> Path:
    runtime_dir = os.environ.get("PROCESS_RUNTIME_ARTIFACT_DIR")
    if runtime_dir:
        root = Path(runtime_dir).expanduser()
        root.mkdir(parents=True, exist_ok=True)
        return root

    base = Path(os.environ.get("PROCESS_RUNTIME_ARTIFACT_ROOT", ".flow-runs/process-artifacts"))
    root = (
        base
        / slug(str(context.get("run_id") or "run"))
        / slug(str(context.get("document_id") or "document"))
        / process_id
    )
    root.mkdir(parents=True, exist_ok=True)
    return root


def artifact(kind: str, path: Path, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "kind": kind,
        "uri": path.resolve().as_uri(),
        "metadata": metadata or {},
    }


def output(
    *,
    values: dict[str, Any],
    artifacts: list[dict[str, Any]] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "values": values,
        "artifacts": artifacts or [],
        "metadata": metadata or {},
    }


def skipped(context: dict[str, Any], process_id: str, reason: str) -> dict[str, Any]:
    path = write_json(artifact_root(context, process_id) / "empty.json", [])
    return output(
        values={"status": "skipped", "reason": reason},
        artifacts=[artifact("empty", path)],
    )


def path_from_output(output_value: dict[str, Any], kind: str) -> Path | None:
    for artifact_value in output_value.get("artifacts") or []:
        if artifact_value.get("kind") == kind:
            return path_from_uri(str(artifact_value.get("uri") or ""))
    values = output_value.get("values") if isinstance(output_value.get("values"), dict) else output_value
    value = values.get(f"{kind}_path") if isinstance(values, dict) else None
    return optional_path(str(value or ""))


def read_needed_text(needs_value: dict[str, dict[str, Any]], process_id: str, kind: str) -> str:
    path = path_from_output(needs_value.get(process_id, {}), kind)
    if not path or not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def read_needed_json(
    needs_value: dict[str, dict[str, Any]],
    process_id: str,
    kind: str,
    *,
    default: Any | None = None,
) -> Any:
    path = path_from_output(needs_value.get(process_id, {}), kind)
    if not path or not path.exists():
        return {} if default is None else default
    return json.loads(path.read_text(encoding="utf-8"))


def write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def write_json(path: Path, value: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def path_from_uri(uri: str) -> Path | None:
    if not uri:
        return None
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        return Path(unquote(parsed.path))
    if not parsed.scheme:
        return Path(uri).expanduser()
    return None


def optional_path(value: str) -> Path | None:
    if not value:
        return None
    return path_from_uri(value) or Path(value).expanduser()


def slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._") or "item"


__all__ = [
    "PROCESS_RUNTIME_EVENT_PREFIX",
    "StepHandler",
    "artifact",
    "artifact_root",
    "emit_event",
    "initial",
    "input_artifacts",
    "input_values",
    "needs",
    "optional_path",
    "output",
    "path_from_output",
    "path_from_uri",
    "read_needed_json",
    "read_needed_text",
    "run_stdio",
    "skipped",
    "slug",
    "write_json",
    "write_text",
]
