"""Pure-Python Fala effector boundary (FALA_EFFECTOR_* contract).

Not a second engine: helpers for Python subprocess organs hosted by the
Mojo correlator (same env contract as Mojo process host).
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from .protocol import Request, Result, build_result, parse, speak, validate

EffectorHandler = Callable[[Request], Result]


def load_manifest(env: Mapping[str, str] | None = None) -> Request:
    source_env = os.environ if env is None else env
    manifest_path = source_env.get("FALA_EFFECTOR_MANIFEST")
    if not manifest_path:
        raise RuntimeError("FALA_EFFECTOR_MANIFEST is required")
    loaded = parse(Path(manifest_path).read_text(encoding="utf-8"), "request")
    if not isinstance(loaded, Request):
        raise TypeError("FALA_EFFECTOR_MANIFEST must be a Request")
    return loaded


def input_values(manifest: Mapping[str, Any] | Request) -> dict[str, Any]:
    if isinstance(manifest, Request):
        return dict(manifest.payload)
    return _dict(manifest.get("payload"))


INJECTED_INPUT_KEYS: frozenset[str] = frozenset(
    {"conduction", "upstream_reactions", "regulation"}
)
"""The ``input`` keys Fala injects when readying a correlation_path effector.

``conduction`` carries direct upstream outputs; ``upstream_reactions`` carries
transitive-ancestor reactions when opted in; and ``regulation`` carries the
runtime regulation envelope. Everything else in ``input`` is authored.
"""


def declared_inputs(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """The effector inputs the correlation_path author declared: ``input`` minus Fala's keys.

    Fala merges the correlation_path's ``effector_inputs`` with :data:`INJECTED_INPUT_KEYS`
    into one ``input`` mapping when it readies an effector; this recovers the
    authored values. Missing or malformed ``input`` yields ``{}``, matching
    :func:`input_values`.
    """
    return {
        key: value
        for key, value in input_values(manifest).items()
        if key not in INJECTED_INPUT_KEYS
    }


def conduction(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return _dict(input_values(manifest).get("conduction"))


def upstream_reactions(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Reactions of every transitive ancestor, in topological order.

    Populated only when the correlation_path opts in via ``accumulate_upstream_reactions``;
    otherwise (and for root effectors) this is empty.
    """
    return _reaction_list(input_values(manifest).get("upstream_reactions"))


def find_reaction(manifest: Mapping[str, Any], kind: str) -> dict[str, Any] | None:
    """Most recent upstream reaction of ``kind``, or ``None`` if absent.

    Scans :func:`upstream_reactions` newest-first, so when several ancestors
    emit the same ``kind`` the most-downstream (latest) producer wins.
    """
    return _find_latest(upstream_reactions(manifest), kind)


def output_reactions(effector_output: Mapping[str, Any] | Result) -> list[dict[str, Any]]:
    """Evidence a completed effector wrote, in emission order.

    Reads ``payload.evidence`` from a typed :class:`Result` or a decoded envelope.
    """
    if isinstance(effector_output, Result):
        payload = dict(effector_output.payload)
    else:
        payload = _dict(_dict(effector_output).get("payload"))
    return _reaction_list(payload.get("evidence"))


def find_output_reaction(
    effector_output: Mapping[str, Any], kind: str
) -> dict[str, Any] | None:
    """Most recent reaction of ``kind`` an effector emitted, or ``None`` if absent.

    Scans :func:`output_reactions` newest-first, mirroring :func:`find_reaction`
    -- when an effector wrote several reactions of the same ``kind`` the last wins.
    """
    return _find_latest(output_reactions(effector_output), kind)


def config(manifest: Mapping[str, Any] | Request) -> dict[str, Any]:
    if isinstance(manifest, Request):
        return dict(manifest.config)
    return _dict(manifest.get("config"))


def request_identity(manifest: Mapping[str, Any]) -> dict[str, str]:
    return {
        "id": str(manifest.get("id") or ""),
        "from": str(manifest.get("from") or ""),
        "to": str(manifest.get("to") or ""),
        "job": str(manifest.get("job") or ""),
    }


def output(
    request: Mapping[str, Any] | Request,
    payload: Mapping[str, Any],
    *,
    status: str = "ok",
) -> Result:
    """Build a typed Fala result from the child's request."""
    return build_result(request, payload=payload, status=status)


def write_result(
    result: Result,
    *,
    env: Mapping[str, str] | None = None,
) -> Path:
    source_env = os.environ if env is None else env
    output_dir = source_env.get("FALA_EFFECTOR_OUTPUT_DIR")
    if not output_dir:
        raise RuntimeError("FALA_EFFECTOR_OUTPUT_DIR is required")
    if not isinstance(result, Result):
        if isinstance(result, Request | Mapping):
            validate(result, "result")
        raise TypeError("write_result accepts only Result")
    spoken = speak(result, "result")
    path = Path(output_dir) / "result.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(spoken.to_message(), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return path


def run_manifest_effector(handler: EffectorHandler) -> int:
    try:
        manifest = load_manifest()
        write_result(handler(manifest))
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _reaction_list(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    return [dict(item) for item in raw if isinstance(item, Mapping)]


def _find_latest(
    reactions: list[dict[str, Any]], kind: str
) -> dict[str, Any] | None:
    for reaction in reversed(reactions):
        if reaction.get("kind") == kind:
            return reaction
    return None


__all__ = [
    "INJECTED_INPUT_KEYS",
    "EffectorHandler",
    "config",
    "declared_inputs",
    "find_reaction",
    "find_output_reaction",
    "input_values",
    "Result",
    "build_result",
    "load_manifest",
    "conduction",
    "output",
    "output_reactions",
    "run_manifest_effector",
    "speak",
    "upstream_reactions",
    "write_result",
]
