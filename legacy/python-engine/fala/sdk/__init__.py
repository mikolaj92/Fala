from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

EffectorHandler = Callable[[dict[str, Any]], dict[str, Any]]


def load_manifest(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    source_env = env or os.environ
    manifest_path = source_env.get("FALA_EFFECTOR_MANIFEST")
    if not manifest_path:
        raise RuntimeError("FALA_EFFECTOR_MANIFEST is required")
    loaded = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise RuntimeError("FALA_EFFECTOR_MANIFEST must contain a JSON object")
    return loaded


def input_values(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return _dict(manifest.get("input"))


INJECTED_INPUT_KEYS: frozenset[str] = frozenset({"conduction", "upstream_reactions"})
"""The ``input`` keys Fala injects when readying a correlation_path effector.

``conduction`` carries the direct upstream outputs; ``upstream_reactions`` carries the
transitive-ancestor reactions when the correlation_path opts in via
``accumulate_upstream_reactions``. Everything else in ``input`` is authored.
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


def output_reactions(effector_output: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Reactions a completed effector wrote, in emission order.

    The host-side twin of :func:`upstream_reactions`: it reads the ``reactions``
    list of an effector's *output* envelope (as produced by :func:`output`), whereas
    ``upstream_reactions`` reads them from a downstream effector's injected input.
    """
    return _reaction_list(_dict(effector_output).get("reactions"))


def find_output_reaction(
    effector_output: Mapping[str, Any], kind: str
) -> dict[str, Any] | None:
    """Most recent reaction of ``kind`` an effector emitted, or ``None`` if absent.

    Scans :func:`output_reactions` newest-first, mirroring :func:`find_reaction`
    -- when an effector wrote several reactions of the same ``kind`` the last wins.
    """
    return _find_latest(output_reactions(effector_output), kind)


def output_metadata(effector_output: Mapping[str, Any]) -> dict[str, Any]:
    """The opaque ``metadata`` a completed effector attached to its output.

    Fala never interprets this channel; it round-trips whatever an effector passed to
    :func:`output`'s ``metadata=``. Reading it back host-side is the twin of a
    effector reading its own inputs.
    """
    return _dict(_dict(effector_output).get("metadata"))


def config(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return _dict(manifest.get("config"))


def output(
    *,
    values: dict[str, Any] | None = None,
    associations: list[dict[str, Any]] | None = None,
    reactions: list[dict[str, Any]] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "values": values or {},
        "associations": associations or [],
        "reactions": reactions or [],
        "metadata": metadata or {},
    }


def write_result(
    result: Mapping[str, Any],
    *,
    env: Mapping[str, str] | None = None,
) -> Path:
    source_env = env or os.environ
    output_dir = source_env.get("FALA_EFFECTOR_OUTPUT_DIR")
    if not output_dir:
        raise RuntimeError("FALA_EFFECTOR_OUTPUT_DIR is required")
    path = Path(output_dir) / "result.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(dict(result), ensure_ascii=False, indent=2, sort_keys=True),
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
    "load_manifest",
    "conduction",
    "output",
    "output_reactions",
    "output_metadata",
    "run_manifest_effector",
    "upstream_reactions",
    "write_result",
]
