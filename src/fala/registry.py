from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import yaml

from fala.errors import FalaConfigurationError
from fala.models import (
    PipelineSpec,
    WorkflowPackageSpec,
    WorkflowWorkerSpec,
)
from fala.schema_validation import validate_json_value
from fala.yaml_loader import (
    load_pipeline_yaml,
    load_workflow_package_yaml,
)


PACKAGE_MANIFEST_NAMES = {
    "process-runtime-package.yaml",
    "process-runtime-package.yml",
}


class PipelineRegistryError(FalaConfigurationError):
    pass


class PipelineRegistry:
    def __init__(self, pipelines: Iterable[PipelineSpec] = ()) -> None:
        self._pipelines: dict[str, PipelineSpec] = {}
        self._packages: dict[str, WorkflowPackageSpec] = {}
        self._pipeline_sources: dict[str, str] = {}
        self._package_sources: dict[str, str] = {}
        self._pipeline_packages: dict[str, str] = {}
        for pipeline in pipelines:
            self.add(pipeline)

    @classmethod
    def from_directory(cls, directory: str | Path) -> "PipelineRegistry":
        root = Path(directory)
        registry = cls()
        if not root.exists():
            return registry

        for manifest_path in _package_manifest_paths(root):
            package = load_workflow_package_yaml(manifest_path)
            registry.add_package(package, source=manifest_path)
            for pipeline_ref in package.pipelines:
                pipeline_path = (manifest_path.parent / pipeline_ref).resolve()
                registry.add(
                    load_pipeline_yaml(pipeline_path),
                    source=pipeline_path,
                    package_id=package.id,
                )
            registry.validate_package_workers(package.id)

        for path in _loose_pipeline_paths(root):
            registry.add(load_pipeline_yaml(path), source=path)

        return registry

    def add(
        self,
        pipeline: PipelineSpec,
        *,
        source: str | Path | None = None,
        package_id: str | None = None,
    ) -> None:
        if pipeline.id in self._pipelines:
            raise PipelineRegistryError(f"Duplicate pipeline id {pipeline.id!r}")
        if package_id is not None and package_id not in self._packages:
            raise PipelineRegistryError(f"Unknown workflow package id {package_id!r}")
        self._pipelines[pipeline.id] = pipeline
        if source is not None:
            self._pipeline_sources[pipeline.id] = str(Path(source))
        if package_id is not None:
            self._pipeline_packages[pipeline.id] = package_id

    def add_package(
        self,
        package: WorkflowPackageSpec,
        *,
        source: str | Path | None = None,
    ) -> None:
        if package.id in self._packages:
            raise PipelineRegistryError(f"Duplicate workflow package id {package.id!r}")
        self._packages[package.id] = package
        if source is not None:
            self._package_sources[package.id] = str(Path(source))

    def validate_package_workers(self, package_id: str) -> None:
        package = self.package(package_id)
        package_pipeline_ids = set(self.package_pipeline_ids(package_id))
        capabilities = {
            capability.id: capability for capability in package.capabilities
        }
        for pipeline_id in package_pipeline_ids:
            pipeline = self.get(pipeline_id)
            steps_by_id = {step.id: step for step in pipeline.steps}
            for step in pipeline.steps:
                if step.capability is None:
                    continue
                capability = capabilities.get(step.capability)
                if capability is None:
                    raise PipelineRegistryError(
                        f"Workflow package {package_id!r} pipeline {pipeline.id!r} "
                        f"process {step.id!r} references unknown capability "
                        f"{step.capability!r}"
                    )
                try:
                    validate_json_value(
                        step.config,
                        capability.config_schema,
                        label=(
                            f"Workflow package {package_id!r} pipeline "
                            f"{pipeline.id!r} process {step.id!r} config for "
                            f"capability {step.capability!r} config_schema"
                        ),
                    )
                except ValueError as exc:
                    raise PipelineRegistryError(str(exc)) from exc
                if (
                    not step.needs
                    and package.document_types
                    and not capability.accepts_document_types
                ):
                    raise PipelineRegistryError(
                        f"Workflow package {package_id!r} pipeline {pipeline.id!r} "
                        f"root process {step.id!r} capability {step.capability!r} "
                        "does not accept any document type"
                    )
                needed_artifact_kinds: set[str] = set()
                for need in step.needs:
                    need_step = steps_by_id.get(need)
                    if need_step is None or need_step.capability is None:
                        continue
                    need_capability = capabilities.get(need_step.capability)
                    if need_capability is not None:
                        needed_artifact_kinds.update(
                            need_capability.emits_artifact_kinds
                        )
                accepted_artifact_kinds = set(capability.accepts_artifact_kinds)
                if (
                    accepted_artifact_kinds
                    and needed_artifact_kinds
                    and not accepted_artifact_kinds.intersection(needed_artifact_kinds)
                ):
                    emitted = ", ".join(sorted(needed_artifact_kinds))
                    accepted = ", ".join(sorted(accepted_artifact_kinds))
                    raise PipelineRegistryError(
                        f"Workflow package {package_id!r} pipeline {pipeline.id!r} "
                        f"process {step.id!r} capability {step.capability!r} "
                        "does not accept artifacts emitted by its needs "
                        f"(emitted: {emitted}; accepted: {accepted})"
                    )
        for worker in package.workers:
            if worker.pipeline_id not in package_pipeline_ids:
                raise PipelineRegistryError(
                    f"Workflow package {package_id!r} worker {worker.id!r} "
                    f"references pipeline {worker.pipeline_id!r} outside the package"
                )
            pipeline = self.get(worker.pipeline_id)
            matching_steps = [
                step
                for step in pipeline.steps
                if step.adapter.kind == worker.adapter_kind
                and (worker.process_id is None or step.id == worker.process_id)
            ]
            if matching_steps:
                continue
            if worker.process_id is None:
                raise PipelineRegistryError(
                    f"Workflow package {package_id!r} worker {worker.id!r} "
                    f"does not match any {worker.adapter_kind!r} process in "
                    f"pipeline {worker.pipeline_id!r}"
                )
            known_process_ids = {step.id for step in pipeline.steps}
            if worker.process_id not in known_process_ids:
                raise PipelineRegistryError(
                    f"Workflow package {package_id!r} worker {worker.id!r} "
                    f"references unknown process {worker.process_id!r} in "
                    f"pipeline {worker.pipeline_id!r}"
                )
            raise PipelineRegistryError(
                f"Workflow package {package_id!r} worker {worker.id!r} "
                f"references process {worker.process_id!r} with adapter kind "
                f"other than {worker.adapter_kind!r}"
            )

    def get(self, pipeline_id: str) -> PipelineSpec:
        try:
            return self._pipelines[pipeline_id]
        except KeyError as exc:
            raise PipelineRegistryError(f"Unknown pipeline id {pipeline_id!r}") from exc

    def all(self) -> list[PipelineSpec]:
        return list(self._pipelines.values())

    def packages(self) -> list[WorkflowPackageSpec]:
        return list(self._packages.values())

    def package(self, package_id: str) -> WorkflowPackageSpec:
        try:
            return self._packages[package_id]
        except KeyError as exc:
            raise PipelineRegistryError(f"Unknown workflow package id {package_id!r}") from exc

    def package_pipeline_ids(self, package_id: str) -> list[str]:
        self.package(package_id)
        return [
            pipeline_id
            for pipeline_id, item_package_id in self._pipeline_packages.items()
            if item_package_id == package_id
        ]

    def package_worker(
        self,
        worker_id: str,
        *,
        package_id: str | None = None,
    ) -> WorkflowWorkerSpec:
        packages = (
            [self.package(package_id)]
            if package_id is not None
            else self.packages()
        )
        matches = [
            worker
            for package in packages
            for worker in package.workers
            if worker.id == worker_id
        ]
        if not matches:
            scope = f" in workflow package {package_id!r}" if package_id else ""
            raise PipelineRegistryError(
                f"Unknown workflow package worker id {worker_id!r}{scope}"
            )
        if len(matches) > 1:
            raise PipelineRegistryError(
                f"Workflow package worker id {worker_id!r} is ambiguous; "
                "pass --package-id"
            )
        return matches[0]

    def pipeline_source(self, pipeline_id: str) -> str | None:
        return self._pipeline_sources.get(pipeline_id)

    def package_source(self, package_id: str) -> str | None:
        self.package(package_id)
        return self._package_sources.get(package_id)

    def pipeline_package_id(self, pipeline_id: str) -> str | None:
        return self._pipeline_packages.get(pipeline_id)

    def pipeline_contract(self, pipeline_id: str) -> dict[str, Any]:
        pipeline = self.get(pipeline_id)
        package_id = self.pipeline_package_id(pipeline.id)
        package = self.package(package_id) if package_id is not None else None
        capabilities = (
            {capability.id: capability for capability in package.capabilities}
            if package is not None
            else {}
        )
        document_types = (
            {document_type.id: document_type for document_type in package.document_types}
            if package is not None
            else {}
        )
        document_relations = (
            {
                relation.id: relation
                for relation in package.document_relations
            }
            if package is not None
            else {}
        )
        operation_types = (
            {operation.id: operation for operation in package.operation_types}
            if package is not None
            else {}
        )
        artifact_kinds = (
            {artifact_kind.id: artifact_kind for artifact_kind in package.artifact_kinds}
            if package is not None
            else {}
        )
        steps_by_id = {step.id: step for step in pipeline.steps}
        root_steps = [step for step in pipeline.steps if not step.needs]
        accepted_document_type_ids = sorted(
            {
                document_type
                for step in root_steps
                if step.capability is not None
                for document_type in (
                    capabilities.get(step.capability).accepts_document_types
                    if capabilities.get(step.capability) is not None
                    else []
                )
            }
        )
        return {
            "pipeline": pipeline.model_dump(mode="json"),
            "pipeline_id": pipeline.id,
            "package_id": package_id,
            "source": self.pipeline_source(pipeline.id),
            "typed": package is not None,
            "document_types": [
                _document_type_contract(document_types[document_type_id])
                for document_type_id in accepted_document_type_ids
                if document_type_id in document_types
            ],
            "document_relations": [
                relation.model_dump(mode="json")
                for relation in document_relations.values()
            ],
            "operation_types": [
                operation.model_dump(mode="json")
                for operation in operation_types.values()
            ],
            "artifact_kinds": [
                artifact_kind.model_dump(mode="json")
                for artifact_kind in artifact_kinds.values()
            ],
            "steps": [
                _step_contract(
                    step=step,
                    capabilities=capabilities,
                    artifact_kinds=artifact_kinds,
                    document_types=document_types,
                    steps_by_id=steps_by_id,
                )
                for step in pipeline.steps
            ],
            "combines": [combine.model_dump(mode="json") for combine in pipeline.combines],
            "reduces": [reduce.model_dump(mode="json") for reduce in pipeline.reduces],
            "workers": [
                worker.model_dump(mode="json")
                for worker in (package.workers if package is not None else [])
                if worker.pipeline_id == pipeline.id
            ],
        }


def _package_manifest_paths(root: Path) -> list[Path]:
    return sorted(
        path
        for name in PACKAGE_MANIFEST_NAMES
        for path in root.rglob(name)
        if path.is_file()
    )


def _loose_pipeline_paths(root: Path) -> list[Path]:
    return sorted(
        path
        for path in [*root.glob("*.yaml"), *root.glob("*.yml")]
        if path.name not in PACKAGE_MANIFEST_NAMES
        and _looks_like_loose_pipeline_yaml(path)
    )


def _looks_like_loose_pipeline_yaml(path: Path) -> bool:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return isinstance(data, dict) and (
        "pipeline" in data
        or ("id" in data and "steps" in data)
    )


def _step_contract(
    *,
    step,
    capabilities: dict[str, Any],
    artifact_kinds: dict[str, Any],
    document_types: dict[str, Any],
    steps_by_id: dict[str, Any],
) -> dict[str, Any]:
    capability = capabilities.get(step.capability) if step.capability else None
    accepted_document_type_ids = (
        capability.accepts_document_types if capability is not None else []
    )
    accepted_artifact_kind_ids = (
        capability.accepts_artifact_kinds if capability is not None else []
    )
    emitted_artifact_kind_ids = (
        capability.emits_artifact_kinds if capability is not None else []
    )
    emitted_document_type_ids = (
        capability.emits_document_types if capability is not None else []
    )
    needed_artifact_kind_ids = sorted(
        {
            artifact_kind
            for need in step.needs
            for artifact_kind in _step_emitted_artifact_kinds(
                steps_by_id.get(need),
                capabilities=capabilities,
            )
        }
    )
    return {
        "id": step.id,
        "title": step.title,
        "description": step.description,
        "tags": step.tags,
        "needs": step.needs,
        "adapter_kind": step.adapter.kind,
        "priority": step.priority,
        "max_concurrency": step.max_concurrency,
        "resource_pool": step.resource_pool,
        "resources": step.resources.model_dump(mode="json"),
        "wait_for_children": (
            step.wait_for_children.model_dump(mode="json")
            if step.wait_for_children is not None
            else None
        ),
        "config": step.config,
        "capability": capability.model_dump(mode="json") if capability else None,
        "input_document_types": [
            _document_type_contract(document_types[document_type_id])
            for document_type_id in accepted_document_type_ids
            if document_type_id in document_types
        ],
        "input_artifact_kinds": [
            _artifact_kind_contract(artifact_kinds[artifact_kind_id])
            for artifact_kind_id in accepted_artifact_kind_ids
            if artifact_kind_id in artifact_kinds
        ],
        "needed_artifact_kinds": [
            _artifact_kind_contract(artifact_kinds[artifact_kind_id])
            for artifact_kind_id in needed_artifact_kind_ids
            if artifact_kind_id in artifact_kinds
        ],
        "emitted_document_types": [
            _document_type_contract(document_types[document_type_id])
            for document_type_id in emitted_document_type_ids
            if document_type_id in document_types
        ],
        "emitted_artifact_kinds": [
            _artifact_kind_contract(artifact_kinds[artifact_kind_id])
            for artifact_kind_id in emitted_artifact_kind_ids
            if artifact_kind_id in artifact_kinds
        ],
        "config_schema": capability.config_schema if capability else {},
        "output_schema": capability.output_schema if capability else {},
    }


def _step_emitted_artifact_kinds(step, *, capabilities: dict[str, Any]) -> list[str]:
    if step is None or step.capability is None:
        return []
    capability = capabilities.get(step.capability)
    return list(capability.emits_artifact_kinds) if capability is not None else []


def _document_type_contract(document_type) -> dict[str, Any]:
    return document_type.model_dump(mode="json")


def _artifact_kind_contract(artifact_kind) -> dict[str, Any]:
    return artifact_kind.model_dump(mode="json")
