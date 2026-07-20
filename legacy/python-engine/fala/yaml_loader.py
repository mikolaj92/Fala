from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from fala.models import FalaPackageSpec


def load_fala_package_yaml(source: str | Path) -> FalaPackageSpec:
    path = Path(source)
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Fala package YAML must contain an object: {path}")
    data = _resolve_fala_package_relative_paths(data, base_dir=path.parent)
    return fala_package_from_mapping(data)


def fala_package_from_mapping(
    data: dict[str, Any],
) -> FalaPackageSpec:
    raw = dict(data)
    return FalaPackageSpec.model_validate(raw)


def _resolve_fala_package_relative_paths(
    data: dict[str, Any],
    *,
    base_dir: Path,
) -> dict[str, Any]:
    resolved = dict(data)
    correlation_paths: list[dict[str, Any]] = []
    for item in data.get("correlation_paths") or []:
        correlation_path = dict(item)
        effectors: list[dict[str, Any]] = []
        for effector_item in correlation_path.get("effectors") or []:
            effector = dict(effector_item)
            adapter = dict(effector.get("adapter") or {})
            cwd = adapter.get("cwd")
            if cwd and not Path(str(cwd)).is_absolute():
                adapter["cwd"] = str((base_dir / str(cwd)).resolve())
            effector["adapter"] = adapter
            effectors.append(effector)
        correlation_path["effectors"] = effectors
        correlation_paths.append(correlation_path)
    resolved["correlation_paths"] = correlation_paths
    return resolved
