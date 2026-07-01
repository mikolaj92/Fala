from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from fala.models import PipelineSpec, WorkflowPackageSpec


def load_pipeline_yaml(source: str | Path) -> PipelineSpec:
    path = Path(source)
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Pipeline YAML must contain an object: {path}")
    data = _resolve_relative_paths(data, base_dir=path.parent)
    return pipeline_from_mapping(data)


def load_workflow_package_yaml(source: str | Path) -> WorkflowPackageSpec:
    path = Path(source)
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Workflow package YAML must contain an object: {path}")
    data = _resolve_package_relative_paths(data, base_dir=path.parent)
    return workflow_package_from_mapping(data)


def pipeline_from_mapping(data: dict[str, Any]) -> PipelineSpec:
    raw = dict(data)
    if "id" not in raw and "pipeline" in raw:
        raw["id"] = raw.pop("pipeline")
    return PipelineSpec.model_validate(raw)


def workflow_package_from_mapping(data: dict[str, Any]) -> WorkflowPackageSpec:
    raw = dict(data)
    if "id" not in raw and "package" in raw:
        raw["id"] = raw.pop("package")
    _move_alias(
        raw,
        alias="carrier_types",
        canonical="document_types",
        context="Workflow package",
    )
    _move_alias(
        raw,
        alias="carrier_relations",
        canonical="document_relations",
        context="Workflow package",
    )
    raw["document_relations"] = [
        _normalize_document_relation_mapping(item)
        for item in raw.get("document_relations") or []
    ]
    raw["capabilities"] = [
        _normalize_capability_mapping(item) for item in raw.get("capabilities") or []
    ]
    raw["workers"] = [_normalize_worker_mapping(item) for item in raw.get("workers") or []]
    return WorkflowPackageSpec.model_validate(raw)


def _move_alias(
    data: dict[str, Any],
    *,
    alias: str,
    canonical: str,
    context: str,
) -> None:
    if alias not in data:
        return
    if canonical in data:
        raise ValueError(f"{context} cannot define both {alias!r} and {canonical!r}")
    data[canonical] = data.pop(alias)


def _normalize_document_relation_mapping(item: Any) -> Any:
    if not isinstance(item, dict):
        return item
    relation = dict(item)
    _move_alias(
        relation,
        alias="source_carrier_types",
        canonical="source_document_types",
        context=f"Document relation {relation.get('id', '<unknown>')!r}",
    )
    _move_alias(
        relation,
        alias="target_carrier_types",
        canonical="target_document_types",
        context=f"Document relation {relation.get('id', '<unknown>')!r}",
    )
    return relation


def _normalize_capability_mapping(item: Any) -> Any:
    if not isinstance(item, dict):
        return item
    capability = dict(item)
    _move_alias(
        capability,
        alias="accepts_carrier_types",
        canonical="accepts_document_types",
        context=f"Capability {capability.get('id', '<unknown>')!r}",
    )
    _move_alias(
        capability,
        alias="emits_carrier_types",
        canonical="emits_document_types",
        context=f"Capability {capability.get('id', '<unknown>')!r}",
    )
    return capability


def _resolve_relative_paths(data: dict[str, Any], *, base_dir: Path) -> dict[str, Any]:
    resolved = dict(data)
    steps: list[dict[str, Any]] = []
    for item in data.get("steps") or []:
        step = dict(item)
        adapter = dict(step.get("adapter") or {})
        cwd = adapter.get("cwd")
        if cwd and not Path(str(cwd)).is_absolute():
            adapter["cwd"] = str((base_dir / str(cwd)).resolve())
        step["adapter"] = adapter
        steps.append(step)
    resolved["steps"] = steps
    return resolved


def _resolve_package_relative_paths(data: dict[str, Any], *, base_dir: Path) -> dict[str, Any]:
    resolved = dict(data)
    workers: list[dict[str, Any]] = []
    for item in data.get("workers") or []:
        worker = _normalize_worker_mapping(item)
        cwd = worker.get("cwd")
        if cwd and not Path(str(cwd)).is_absolute():
            worker["cwd"] = str((base_dir / str(cwd)).resolve())
        workers.append(worker)
    resolved["workers"] = workers
    return resolved


def _normalize_worker_mapping(item: Any) -> dict[str, Any]:
    if not isinstance(item, dict):
        raise ValueError("Workflow package workers must contain objects")
    worker = dict(item)
    if "pipeline_id" not in worker and "pipeline" in worker:
        worker["pipeline_id"] = worker.pop("pipeline")
    if "process_id" not in worker and "process" in worker:
        worker["process_id"] = worker.pop("process")
    return worker
