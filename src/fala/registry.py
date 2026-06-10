from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from fala.models import (
    PipelineSpec,
    WorkflowPackageSpec,
    WorkflowWorkerSpec,
)
from fala.yaml_loader import (
    load_pipeline_yaml,
    load_workflow_package_yaml,
)


PACKAGE_MANIFEST_NAMES = {
    "process-runtime-package.yaml",
    "process-runtime-package.yml",
}


class PipelineRegistryError(RuntimeError):
    pass


class PipelineRegistry:
    def __init__(self, pipelines: Iterable[PipelineSpec] = ()) -> None:
        self._pipelines: dict[str, PipelineSpec] = {}
        self._packages: dict[str, WorkflowPackageSpec] = {}
        self._pipeline_sources: dict[str, str] = {}
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
            registry.add_package(package)
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

    def add_package(self, package: WorkflowPackageSpec) -> None:
        if package.id in self._packages:
            raise PipelineRegistryError(f"Duplicate workflow package id {package.id!r}")
        self._packages[package.id] = package

    def validate_package_workers(self, package_id: str) -> None:
        package = self.package(package_id)
        package_pipeline_ids = set(self.package_pipeline_ids(package_id))
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

    def pipeline_package_id(self, pipeline_id: str) -> str | None:
        return self._pipeline_packages.get(pipeline_id)


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
    )
