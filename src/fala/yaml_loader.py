from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from fala.models import CarrierWorkflowPackageSpec


def load_carrier_workflow_package_yaml(source: str | Path) -> CarrierWorkflowPackageSpec:
    path = Path(source)
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Carrier workflow package YAML must contain an object: {path}")
    data = _resolve_carrier_package_relative_paths(data, base_dir=path.parent)
    return carrier_workflow_package_from_mapping(data)


def carrier_workflow_package_from_mapping(
    data: dict[str, Any],
) -> CarrierWorkflowPackageSpec:
    raw = dict(data)
    return CarrierWorkflowPackageSpec.model_validate(raw)


def _resolve_carrier_package_relative_paths(
    data: dict[str, Any],
    *,
    base_dir: Path,
) -> dict[str, Any]:
    resolved = dict(data)
    flows: list[dict[str, Any]] = []
    for item in data.get("flows") or []:
        flow = dict(item)
        steps: list[dict[str, Any]] = []
        for step_item in flow.get("steps") or []:
            step = dict(step_item)
            adapter = dict(step.get("adapter") or {})
            cwd = adapter.get("cwd")
            if cwd and not Path(str(cwd)).is_absolute():
                adapter["cwd"] = str((base_dir / str(cwd)).resolve())
            step["adapter"] = adapter
            steps.append(step)
        flow["steps"] = steps
        flows.append(flow)
    resolved["flows"] = flows
    return resolved
