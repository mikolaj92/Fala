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


def upstream_artifacts(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Artifacts of every transitive ancestor, in topological order.

    Populated only when the flow opts in via ``accumulate_upstream_artifacts``;
    otherwise (and for root steps) this is empty.
    """
    return _artifact_list(input_values(manifest).get("upstream_artifacts"))


def find_artifact(manifest: Mapping[str, Any], kind: str) -> dict[str, Any] | None:
    """Most recent upstream artifact of ``kind``, or ``None`` if absent.

    Scans :func:`upstream_artifacts` newest-first, so when several ancestors
    emit the same ``kind`` the most-downstream (latest) producer wins.
    """
    return _find_latest(upstream_artifacts(manifest), kind)


def output_artifacts(step_output: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Artifacts a completed step wrote, in emission order.

    The host-side twin of :func:`upstream_artifacts`: it reads the ``artifacts``
    list of a step's *output* envelope (as produced by :func:`output`), whereas
    ``upstream_artifacts`` reads them from a downstream step's injected input.
    """
    return _artifact_list(_dict(step_output).get("artifacts"))


def find_output_artifact(
    step_output: Mapping[str, Any], kind: str
) -> dict[str, Any] | None:
    """Most recent artifact of ``kind`` a step emitted, or ``None`` if absent.

    Scans :func:`output_artifacts` newest-first, mirroring :func:`find_artifact`
    -- when a step wrote several artifacts of the same ``kind`` the last wins.
    """
    return _find_latest(output_artifacts(step_output), kind)


def output_metadata(step_output: Mapping[str, Any]) -> dict[str, Any]:
    """The opaque ``metadata`` a completed step attached to its output.

    Fala never interprets this channel; it round-trips whatever a step passed to
    :func:`output`'s ``metadata=``. Reading it back host-side is the twin of a
    step reading its own inputs.
    """
    return _dict(_dict(step_output).get("metadata"))


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


def _artifact_list(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    return [dict(item) for item in raw if isinstance(item, Mapping)]


def _find_latest(
    artifacts: list[dict[str, Any]], kind: str
) -> dict[str, Any] | None:
    for artifact in reversed(artifacts):
        if artifact.get("kind") == kind:
            return artifact
    return None


__all__ = [
    "StepHandler",
    "config",
    "find_artifact",
    "find_output_artifact",
    "input_values",
    "load_manifest",
    "needs",
    "output",
    "output_artifacts",
    "output_metadata",
    "run_manifest_step",
    "upstream_artifacts",
    "write_result",
]
