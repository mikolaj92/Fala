from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

StepHandler = Callable[[dict[str, Any]], dict[str, Any]]


def load_manifest(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    source_env = env or os.environ
    manifest_path = source_env.get("FALA_STEP_MANIFEST")
    if not manifest_path:
        raise RuntimeError("FALA_STEP_MANIFEST is required")
    loaded = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise RuntimeError("FALA_STEP_MANIFEST must contain a JSON object")
    return loaded


def input_values(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return _dict(manifest.get("input"))


def needs(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return _dict(input_values(manifest).get("needs"))


def config(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return _dict(manifest.get("config"))


def output(
    *,
    values: dict[str, Any] | None = None,
    observations: list[dict[str, Any]] | None = None,
    artifacts: list[dict[str, Any]] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "values": values or {},
        "observations": observations or [],
        "artifacts": artifacts or [],
        "metadata": metadata or {},
    }


def write_result(
    result: Mapping[str, Any],
    *,
    env: Mapping[str, str] | None = None,
) -> Path:
    source_env = env or os.environ
    output_dir = source_env.get("FALA_STEP_OUTPUT_DIR")
    if not output_dir:
        raise RuntimeError("FALA_STEP_OUTPUT_DIR is required")
    path = Path(output_dir) / "result.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(dict(result), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return path


def run_manifest_step(handler: StepHandler) -> int:
    try:
        manifest = load_manifest()
        write_result(handler(manifest))
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


__all__ = [
    "StepHandler",
    "config",
    "input_values",
    "load_manifest",
    "needs",
    "output",
    "run_manifest_step",
    "write_result",
]
