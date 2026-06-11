from __future__ import annotations

import hashlib
import csv
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal
from urllib.parse import unquote, urlparse

import yaml
from pydantic import BaseModel, ConfigDict, Field

from fala.models import RuntimeDocumentInput, RuntimeId, RuntimeRunInput
from fala.registry import PipelineRegistry
from fala.service import RuntimeService
from fala.store import InMemoryStateStore


class PackageFileDigest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str
    sha256: str
    size_bytes: int = Field(ge=0)


class PipelineRelease(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_id: RuntimeId
    version: str
    source: str | None = None
    model_sha256: str
    contract_sha256: str
    file: PackageFileDigest | None = None


class WorkflowPackageRelease(BaseModel):
    model_config = ConfigDict(extra="forbid")

    package_id: RuntimeId
    version: str
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    source: str | None = None
    model_sha256: str
    manifest_file: PackageFileDigest | None = None
    contract_sha256: str
    document_type_ids: list[str] = Field(default_factory=list)
    document_relation_ids: list[str] = Field(default_factory=list)
    operation_type_ids: list[str] = Field(default_factory=list)
    artifact_kind_ids: list[str] = Field(default_factory=list)
    capability_ids: list[str] = Field(default_factory=list)
    secret_ids: list[str] = Field(default_factory=list)
    pipeline_ids: list[str] = Field(default_factory=list)
    worker_ids: list[str] = Field(default_factory=list)
    pipelines: list[PipelineRelease] = Field(default_factory=list)


class WorkflowRegistryIndex(BaseModel):
    model_config = ConfigDict(extra="forbid")

    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    package_count: int = Field(ge=0)
    pipeline_count: int = Field(ge=0)
    packages: list[WorkflowPackageRelease] = Field(default_factory=list)


class WorkflowPackageReadinessIssue(BaseModel):
    model_config = ConfigDict(extra="forbid")

    severity: Literal["error", "warning", "info"]
    code: RuntimeId
    message: str
    package_id: RuntimeId
    pipeline_id: RuntimeId | None = None
    process_id: RuntimeId | None = None
    worker_id: RuntimeId | None = None
    document_type_id: RuntimeId | None = None
    capability_id: RuntimeId | None = None
    data: dict[str, Any] = Field(default_factory=dict)


class WorkflowPackageReadiness(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool
    package_id: RuntimeId
    title: str | None = None
    version: str
    source: str | None = None
    pipeline_ids: list[RuntimeId] = Field(default_factory=list)
    worker_ids: list[RuntimeId] = Field(default_factory=list)
    document_type_ids: list[RuntimeId] = Field(default_factory=list)
    document_relation_ids: list[RuntimeId] = Field(default_factory=list)
    operation_type_ids: list[RuntimeId] = Field(default_factory=list)
    artifact_kind_ids: list[RuntimeId] = Field(default_factory=list)
    capability_ids: list[RuntimeId] = Field(default_factory=list)
    routeable_document_type_ids: list[RuntimeId] = Field(default_factory=list)
    unrouteable_document_type_ids: list[RuntimeId] = Field(default_factory=list)
    emitted_document_type_ids: list[RuntimeId] = Field(default_factory=list)
    queue_process_count: int = Field(default=0, ge=0)
    covered_queue_process_count: int = Field(default=0, ge=0)
    missing_worker_process_ids: list[str] = Field(default_factory=list)
    invalid_worker_command_count: int = Field(default=0, ge=0)
    invalid_worker_ids: list[RuntimeId] = Field(default_factory=list)
    sample_files: dict[str, bool] = Field(default_factory=dict)
    error_count: int = Field(default=0, ge=0)
    warning_count: int = Field(default=0, ge=0)
    info_count: int = Field(default=0, ge=0)
    issues: list[WorkflowPackageReadinessIssue] = Field(default_factory=list)


class WorkflowReadinessReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    ok: bool
    package_count: int = Field(ge=0)
    error_count: int = Field(default=0, ge=0)
    warning_count: int = Field(default=0, ge=0)
    info_count: int = Field(default=0, ge=0)
    packages: list[WorkflowPackageReadiness] = Field(default_factory=list)


def build_workflow_registry_index(
    registry: PipelineRegistry,
    *,
    package_id: str | None = None,
) -> WorkflowRegistryIndex:
    packages = (
        [registry.package(package_id)]
        if package_id is not None
        else sorted(registry.packages(), key=lambda item: item.id)
    )
    releases = [
        _package_release(registry, package.id)
        for package in packages
    ]
    return WorkflowRegistryIndex(
        package_count=len(releases),
        pipeline_count=sum(len(release.pipelines) for release in releases),
        packages=releases,
    )


def build_workflow_readiness_report(
    registry: PipelineRegistry,
    *,
    package_id: str | None = None,
) -> WorkflowReadinessReport:
    packages = (
        [registry.package(package_id)]
        if package_id is not None
        else sorted(registry.packages(), key=lambda item: item.id)
    )
    readiness = [
        _package_readiness(registry, package.id)
        for package in packages
    ]
    error_count = sum(item.error_count for item in readiness)
    warning_count = sum(item.warning_count for item in readiness)
    info_count = sum(item.info_count for item in readiness)
    return WorkflowReadinessReport(
        ok=error_count == 0,
        package_count=len(readiness),
        error_count=error_count,
        warning_count=warning_count,
        info_count=info_count,
        packages=readiness,
    )


def package_release(
    registry: PipelineRegistry,
    package_id: str,
) -> WorkflowPackageRelease:
    return _package_release(registry, package_id)


def _package_release(
    registry: PipelineRegistry,
    package_id: str,
) -> WorkflowPackageRelease:
    package = registry.package(package_id)
    pipeline_ids = sorted(registry.package_pipeline_ids(package.id))
    pipelines = [
        _pipeline_release(registry, pipeline_id)
        for pipeline_id in pipeline_ids
    ]
    manifest_source = registry.package_source(package.id)
    contract = {
        "package": package.model_dump(mode="json"),
        "pipelines": [pipeline.model_dump(mode="json") for pipeline in pipelines],
    }
    return WorkflowPackageRelease(
        package_id=package.id,
        version=package.version,
        title=package.title,
        description=package.description,
        tags=list(package.tags),
        source=manifest_source,
        model_sha256=_model_sha256(package.model_dump(mode="json")),
        manifest_file=_file_digest(manifest_source),
        contract_sha256=_model_sha256(contract),
        document_type_ids=sorted(item.id for item in package.document_types),
        document_relation_ids=sorted(
            item.id for item in package.document_relations
        ),
        operation_type_ids=sorted(item.id for item in package.operation_types),
        artifact_kind_ids=sorted(item.id for item in package.artifact_kinds),
        capability_ids=sorted(item.id for item in package.capabilities),
        secret_ids=sorted(secret.id for secret in package.secrets),
        pipeline_ids=pipeline_ids,
        worker_ids=sorted(worker.id for worker in package.workers),
        pipelines=pipelines,
    )


def package_readiness(
    registry: PipelineRegistry,
    package_id: str,
) -> WorkflowPackageReadiness:
    return _package_readiness(registry, package_id)


def _package_readiness(
    registry: PipelineRegistry,
    package_id: str,
) -> WorkflowPackageReadiness:
    package = registry.package(package_id)
    package_source = registry.package_source(package.id)
    package_root = Path(package_source).parent if package_source is not None else None
    pipeline_ids = sorted(registry.package_pipeline_ids(package.id))
    capabilities = {item.id: item for item in package.capabilities}
    issues: list[WorkflowPackageReadinessIssue] = []

    if not package.document_types:
        issues.append(
            _readiness_issue(
                "error",
                "missing_document_types",
                "Package declares no document types; auto-routing and input validation cannot bootstrap document work.",
                package_id=package.id,
            )
        )
    if not package.capabilities:
        issues.append(
            _readiness_issue(
                "error",
                "missing_capabilities",
                "Package declares no capabilities; pipeline steps cannot be typed as reusable work.",
                package_id=package.id,
            )
        )
    if not pipeline_ids:
        issues.append(
            _readiness_issue(
                "error",
                "missing_pipelines",
                "Package references no loadable pipelines.",
                package_id=package.id,
            )
        )

    routeable_document_types: set[str] = set()
    queue_process_count = 0
    covered_queue_processes: set[str] = set()
    missing_worker_processes: set[str] = set()
    used_capabilities: set[str] = set()
    used_artifact_kinds: set[str] = set()
    emitted_document_types: set[str] = set()
    worker_secret_ids = {
        secret_id
        for worker in package.workers
        for secret_id in worker.secrets
    }
    invalid_worker_ids: set[str] = set()

    for worker in package.workers:
        command_issue = _worker_command_issue(
            worker.command,
            cwd=worker.cwd or (str(package_root) if package_root is not None else None),
        )
        if command_issue is not None:
            invalid_worker_ids.add(worker.id)
            issues.append(
                _readiness_issue(
                    "warning",
                    "worker_command_unavailable",
                    (
                        f"Package worker {worker.id!r} command is not runnable: "
                        f"{command_issue}"
                    ),
                    package_id=package.id,
                    pipeline_id=worker.pipeline_id,
                    process_id=worker.process_id,
                    worker_id=worker.id,
                    data={
                        "command": worker.command,
                        "cwd": worker.cwd,
                        "reason": command_issue,
                    },
                )
            )

    for pipeline_id in pipeline_ids:
        try:
            pipeline = registry.get(pipeline_id)
        except Exception as exc:
            issues.append(
                _readiness_issue(
                    "error",
                    "pipeline_not_loadable",
                    f"Pipeline {pipeline_id!r} cannot be loaded: {exc}",
                    package_id=package.id,
                    pipeline_id=pipeline_id,
                )
            )
            continue

        steps_by_id = {step.id: step for step in pipeline.steps}
        if not pipeline.steps:
            issues.append(
                _readiness_issue(
                    "error",
                    "pipeline_has_no_steps",
                    f"Pipeline {pipeline.id!r} has no process steps.",
                    package_id=package.id,
                    pipeline_id=pipeline.id,
                )
            )
            continue

        root_steps = [step for step in pipeline.steps if not step.needs]
        if not root_steps:
            issues.append(
                _readiness_issue(
                    "error",
                    "pipeline_has_no_root_steps",
                    f"Pipeline {pipeline.id!r} has no root process steps.",
                    package_id=package.id,
                    pipeline_id=pipeline.id,
                )
            )

        for step in pipeline.steps:
            adapter_kind = getattr(step.adapter.kind, "value", step.adapter.kind)
            if step.capability is None:
                issues.append(
                    _readiness_issue(
                        "warning",
                        "step_missing_capability",
                        (
                            f"Process {step.id!r} is not bound to a package "
                            "capability; reuse and contract checks are weaker."
                        ),
                        package_id=package.id,
                        pipeline_id=pipeline.id,
                        process_id=step.id,
                    )
                )
            else:
                used_capabilities.add(step.capability)
                capability = capabilities.get(step.capability)
                if capability is not None:
                    used_artifact_kinds.update(capability.emits_artifact_kinds)
                    emitted_document_types.update(capability.emits_document_types)
                    needed_artifact_kinds = _step_needed_artifact_kinds(
                        step,
                        steps_by_id=steps_by_id,
                        capabilities=capabilities,
                    )
                    missing_artifact_kinds = sorted(
                        needed_artifact_kinds
                        - set(capability.accepts_artifact_kinds)
                    )
                    if missing_artifact_kinds:
                        needed = ", ".join(sorted(needed_artifact_kinds))
                        accepted = (
                            ", ".join(sorted(capability.accepts_artifact_kinds))
                            or "none"
                        )
                        missing = ", ".join(missing_artifact_kinds)
                        issues.append(
                            _readiness_issue(
                                "warning",
                                "capability_missing_needed_artifact_kinds",
                                (
                                    f"Process {pipeline.id!r}/{step.id!r} needs "
                                    "artifacts that capability "
                                    f"{capability.id!r} does not accept "
                                    f"(needed: {needed}; accepted: {accepted}; "
                                    f"missing: {missing})."
                                ),
                                package_id=package.id,
                                pipeline_id=pipeline.id,
                                process_id=step.id,
                                capability_id=capability.id,
                                data={
                                    "needs": list(step.needs),
                                    "needed_artifact_kinds": sorted(
                                        needed_artifact_kinds
                                    ),
                                    "accepted_artifact_kinds": sorted(
                                        capability.accepts_artifact_kinds
                                    ),
                                    "missing_artifact_kinds": missing_artifact_kinds,
                                },
                            )
                        )
                    if not step.needs:
                        routeable_document_types.update(
                            capability.accepts_document_types
                        )

            if adapter_kind == "queue":
                queue_process_count += 1
                if _queue_step_has_worker(
                    package.workers,
                    pipeline_id=pipeline.id,
                    process_id=step.id,
                    capability_id=step.capability,
                ):
                    covered_queue_processes.add(f"{pipeline.id}/{step.id}")
                else:
                    missing_worker_processes.add(f"{pipeline.id}/{step.id}")
                    issues.append(
                        _readiness_issue(
                            "warning",
                            "queue_step_without_package_worker",
                            (
                                f"Queue process {pipeline.id!r}/{step.id!r} has "
                                "no declared package worker coverage."
                            ),
                            package_id=package.id,
                            pipeline_id=pipeline.id,
                            process_id=step.id,
                            capability_id=step.capability,
                        )
                    )

    document_type_ids = sorted(item.id for item in package.document_types)
    document_relation_ids = sorted(item.id for item in package.document_relations)
    operation_type_ids = sorted(item.id for item in package.operation_types)
    unrouteable_document_types = sorted(
        set(document_type_ids) - routeable_document_types
    )
    for document_type_id in unrouteable_document_types:
        issues.append(
            _readiness_issue(
                "warning",
                "document_type_not_routeable",
                (
                    f"Document type {document_type_id!r} is not accepted by any "
                    "root process capability."
                ),
                package_id=package.id,
                document_type_id=document_type_id,
            )
        )

    for capability in package.capabilities:
        if capability.id not in used_capabilities:
            issues.append(
                _readiness_issue(
                    "info",
                    "capability_unused",
                    f"Capability {capability.id!r} is not used by package pipelines.",
                    package_id=package.id,
                    capability_id=capability.id,
                )
            )
        if package.operation_types and capability.operation_type is None:
            issues.append(
                _readiness_issue(
                    "info",
                    "capability_has_no_operation_type",
                    (
                        f"Capability {capability.id!r} is not mapped to a "
                        "package operation type."
                    ),
                    package_id=package.id,
                    capability_id=capability.id,
                )
            )
        if (
            not capability.emits_document_types
            and not capability.emits_artifact_kinds
            and not capability.emits_streams
            and not capability.output_schema
        ):
            issues.append(
                _readiness_issue(
                    "info",
                    "capability_has_no_typed_output",
                    (
                        f"Capability {capability.id!r} has no emitted artifact "
                        "kinds, document types, streams, or output schema."
                    ),
                    package_id=package.id,
                    capability_id=capability.id,
                )
            )
        for stream in capability.emits_streams:
            if stream.max_buffered_chunks is not None and not stream.consumers:
                issues.append(
                    _readiness_issue(
                        "warning",
                        "stream_backpressure_without_declared_consumers",
                        (
                            f"Capability {capability.id!r} stream "
                            f"{stream.stream_id!r} has max_buffered_chunks but "
                            "declares no expected consumers."
                        ),
                        package_id=package.id,
                        capability_id=capability.id,
                        data={
                            "stream_id": stream.stream_id,
                            "max_buffered_chunks": stream.max_buffered_chunks,
                        },
                    )
                )

    artifact_kind_ids = sorted(item.id for item in package.artifact_kinds)
    for artifact_kind_id in sorted(set(artifact_kind_ids) - used_artifact_kinds):
        issues.append(
            _readiness_issue(
                "info",
                "artifact_kind_unused",
                f"Artifact kind {artifact_kind_id!r} is not emitted by used capabilities.",
                package_id=package.id,
                data={"artifact_kind_id": artifact_kind_id},
            )
        )

    for secret in package.secrets:
        if secret.id not in worker_secret_ids:
            issues.append(
                _readiness_issue(
                    "info",
                    "secret_unused",
                    f"Secret {secret.id!r} is not referenced by any package worker.",
                    package_id=package.id,
                    data={"secret_id": secret.id},
                )
            )

    source = package_source
    sample_files = _package_sample_files(source)
    if source is None:
        issues.append(
            _readiness_issue(
                "info",
                "package_source_unknown",
                "Package source path is unknown; sample bootstrap files cannot be checked.",
                package_id=package.id,
            )
        )
    elif sample_files:
        sample_root = Path(source).parent
        if not sample_files.get("run_input_example", False):
            issues.append(
                _readiness_issue(
                    "warning",
                    "sample_run_input_missing",
                    "Package directory has no run-input.example.yaml sample.",
                    package_id=package.id,
                )
            )
        else:
            run_input_ok, run_input_error = _validate_sample_run_input(
                registry,
                sample_root / "run-input.example.yaml",
            )
            sample_files["run_input_example_valid"] = run_input_ok
            if not run_input_ok:
                issues.append(
                    _readiness_issue(
                        "warning",
                        "sample_run_input_invalid",
                        (
                            "Package run-input.example.yaml does not pass "
                            f"runtime validation: {run_input_error}"
                        ),
                        package_id=package.id,
                        data={"error": run_input_error},
                    )
                )
        if sample_files.get("source_list_example", False):
            if not sample_files.get("source_list_local_sources_present", False):
                issues.append(
                    _readiness_issue(
                        "warning",
                        "sample_source_files_missing",
                        (
                            "Package source-list.example.csv references local sample "
                            "sources that are missing or not readable."
                        ),
                        package_id=package.id,
                    )
                )
            source_list_ok, source_list_error = _validate_sample_source_list(
                registry,
                sample_root / "source-list.example.csv",
                pipeline_ids=pipeline_ids,
                package_id=package.id,
            )
            sample_files["source_list_example_valid"] = source_list_ok
            if not source_list_ok:
                issues.append(
                    _readiness_issue(
                        "warning",
                        "sample_source_list_invalid",
                        (
                            "Package source-list.example.csv cannot be compiled "
                            f"into a valid RuntimeRunInput: {source_list_error}"
                        ),
                        package_id=package.id,
                        data={"error": source_list_error},
                    )
                )

    error_count = sum(1 for issue in issues if issue.severity == "error")
    warning_count = sum(1 for issue in issues if issue.severity == "warning")
    info_count = sum(1 for issue in issues if issue.severity == "info")
    return WorkflowPackageReadiness(
        ok=error_count == 0,
        package_id=package.id,
        title=package.title,
        version=package.version,
        source=source,
        pipeline_ids=pipeline_ids,
        worker_ids=sorted(worker.id for worker in package.workers),
        document_type_ids=document_type_ids,
        document_relation_ids=document_relation_ids,
        operation_type_ids=operation_type_ids,
        artifact_kind_ids=artifact_kind_ids,
        capability_ids=sorted(capability.id for capability in package.capabilities),
        routeable_document_type_ids=sorted(
            set(document_type_ids) & routeable_document_types
        ),
        unrouteable_document_type_ids=unrouteable_document_types,
        emitted_document_type_ids=sorted(
            set(document_type_ids) & emitted_document_types
        ),
        queue_process_count=queue_process_count,
        covered_queue_process_count=len(covered_queue_processes),
        missing_worker_process_ids=sorted(missing_worker_processes),
        invalid_worker_command_count=len(invalid_worker_ids),
        invalid_worker_ids=sorted(invalid_worker_ids),
        sample_files=sample_files,
        error_count=error_count,
        warning_count=warning_count,
        info_count=info_count,
        issues=issues,
    )


def _pipeline_release(
    registry: PipelineRegistry,
    pipeline_id: str,
) -> PipelineRelease:
    pipeline = registry.get(pipeline_id)
    source = registry.pipeline_source(pipeline_id)
    contract = registry.pipeline_contract(pipeline_id)
    return PipelineRelease(
        pipeline_id=pipeline.id,
        version=pipeline.version,
        source=source,
        model_sha256=_model_sha256(pipeline.model_dump(mode="json")),
        contract_sha256=_model_sha256(contract),
        file=_file_digest(source),
    )


def _file_digest(source: str | None) -> PackageFileDigest | None:
    if source is None:
        return None
    path = Path(source)
    if not path.is_file():
        return None
    data = path.read_bytes()
    return PackageFileDigest(
        path=str(path),
        sha256=hashlib.sha256(data).hexdigest(),
        size_bytes=len(data),
    )


def _readiness_issue(
    severity: Literal["error", "warning", "info"],
    code: str,
    message: str,
    *,
    package_id: str,
    pipeline_id: str | None = None,
    process_id: str | None = None,
    worker_id: str | None = None,
    document_type_id: str | None = None,
    capability_id: str | None = None,
    data: dict[str, Any] | None = None,
) -> WorkflowPackageReadinessIssue:
    return WorkflowPackageReadinessIssue(
        severity=severity,
        code=code,
        message=message,
        package_id=package_id,
        pipeline_id=pipeline_id,
        process_id=process_id,
        worker_id=worker_id,
        document_type_id=document_type_id,
        capability_id=capability_id,
        data=data or {},
    )


def _queue_step_has_worker(
    workers: list[Any],
    *,
    pipeline_id: str,
    process_id: str,
    capability_id: str | None,
) -> bool:
    for worker in workers:
        if worker.pipeline_id != pipeline_id:
            continue
        if worker.process_id is not None:
            if worker.process_id == process_id:
                return True
            continue
        if capability_id is not None and capability_id in worker.capabilities:
            return True
    return False


def _step_needed_artifact_kinds(
    step: Any,
    *,
    steps_by_id: dict[str, Any],
    capabilities: dict[str, Any],
) -> set[str]:
    artifact_kinds: set[str] = set()
    for need in step.needs:
        need_step = steps_by_id.get(need)
        if need_step is None or need_step.capability is None:
            continue
        need_capability = capabilities.get(need_step.capability)
        if need_capability is not None:
            artifact_kinds.update(need_capability.emits_artifact_kinds)
    return artifact_kinds


def _worker_command_issue(command: list[str], *, cwd: str | None = None) -> str | None:
    executable = str(command[0]) if command else ""
    if not executable:
        return "missing executable"

    if "/" not in executable and "\\" not in executable:
        if shutil.which(executable):
            return _command_file_argument_issue(command, cwd=cwd)
        return f"executable {executable!r} not found on PATH"

    path = _resolve_command_path(executable, cwd=cwd)
    if not path.exists():
        return f"executable path does not exist: {path}"
    if not path.is_file():
        return f"executable path is not a file: {path}"
    if not os.access(path, os.X_OK):
        return f"executable path is not executable: {path}"
    return _command_file_argument_issue(command, cwd=cwd)


def _command_file_argument_issue(
    command: list[str],
    *,
    cwd: str | None = None,
) -> str | None:
    if not command:
        return None
    if Path(command[0]).name.lower() not in _SCRIPT_LAUNCHERS:
        return None
    for arg in command[1:]:
        if not _looks_like_script_path(arg):
            continue
        path = _resolve_command_path(arg, cwd=cwd)
        if not path.exists():
            return f"command file path does not exist: {path}"
        if not path.is_file():
            return f"command file path is not a file: {path}"
    return None


_SCRIPT_LAUNCHERS = {
    "bash",
    "node",
    "python",
    "python3",
    "ruby",
    "sh",
}


def _looks_like_script_path(value: str) -> bool:
    if not value or value.startswith("-"):
        return False
    if value in {"run", "uv", "python", "python3"}:
        return False
    path = Path(value)
    return "/" in value or "\\" in value or bool(path.suffix)


def _resolve_command_path(value: str, *, cwd: str | None = None) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    base = Path(cwd).expanduser() if cwd else Path.cwd()
    return (base / path).resolve()


def _package_sample_files(source: str | None) -> dict[str, bool]:
    if source is None:
        return {}
    root = Path(source).parent
    source_list = root / "source-list.example.csv"
    return {
        "run_input_example": (root / "run-input.example.yaml").is_file(),
        "run_input_example_valid": False,
        "source_list_example": source_list.is_file(),
        "source_list_example_valid": False,
        "source_list_local_sources_present": _source_list_local_sources_present(
            source_list
        ),
        "readme_scaffold": (root / "README.scaffold.md").is_file(),
        "makefile": (root / "Makefile").is_file(),
        "contracts_dir": (root / "contracts").is_dir(),
    }


def _validate_sample_run_input(
    registry: PipelineRegistry,
    path: Path,
) -> tuple[bool, str | None]:
    try:
        run_input = _load_sample_run_input(path)
        RuntimeService(
            registry=registry,
            store=InMemoryStateStore(),
        ).preview_runtime_run_input(run_input)
    except Exception as exc:
        return False, str(exc)
    return True, None


def _validate_sample_source_list(
    registry: PipelineRegistry,
    path: Path,
    *,
    pipeline_ids: list[str],
    package_id: str,
) -> tuple[bool, str | None]:
    try:
        run_input = _sample_source_list_run_input(
            path,
            pipeline_id=pipeline_ids[0] if len(pipeline_ids) == 1 else None,
            run_id=f"run_{package_id}_source_list_sample",
        )
        RuntimeService(
            registry=registry,
            store=InMemoryStateStore(),
        ).preview_runtime_run_input(run_input)
    except Exception as exc:
        return False, str(exc)
    return True, None


def _load_sample_run_input(path: Path) -> RuntimeRunInput:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"RuntimeRunInput manifest must contain an object: {path}")
    return RuntimeRunInput.model_validate(data)


def _sample_source_list_run_input(
    source_list: Path,
    *,
    pipeline_id: str | None,
    run_id: str,
) -> RuntimeRunInput:
    if not source_list.is_file():
        raise ValueError(f"Source list does not exist: {source_list}")
    delimiter = "\t" if source_list.suffix.lower() == ".tsv" else ","
    documents: list[RuntimeDocumentInput] = []
    with source_list.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=delimiter)
        if not reader.fieldnames:
            raise ValueError(f"Source list has no header row: {source_list}")
        for row_number, row in enumerate(reader, start=2):
            row = {str(key or "").strip(): (value or "") for key, value in row.items()}
            documents.append(
                _sample_source_list_document_input(
                    row=row,
                    row_number=row_number,
                    source_list=source_list,
                )
            )
    if not documents:
        raise ValueError(f"Source list has no document rows: {source_list}")
    return RuntimeRunInput(
        run_id=run_id,
        pipeline_id=pipeline_id,
        documents=documents,
    )


def _sample_source_list_document_input(
    *,
    row: dict[str, str],
    row_number: int,
    source_list: Path,
) -> RuntimeDocumentInput:
    source_uri = row.get("source_uri", "").strip()
    source_path = (row.get("path") or row.get("source_path") or "").strip()
    if not source_uri and source_path:
        local_path = Path(source_path).expanduser()
        if not local_path.is_absolute():
            local_path = source_list.parent / local_path
        source_uri = local_path.resolve().as_uri()
    if not source_uri:
        raise ValueError(
            f"Source list row {row_number} requires source_uri, path, or source_path"
        )
    document_id = (row.get("document_id") or "").strip() or Path(
        unquote(urlparse(source_uri).path)
    ).name or f"row_{row_number}"
    values = {
        key.removeprefix("value."): _parse_sample_source_list_cell(value)
        for key, value in row.items()
        if key.startswith("value.") and value != ""
    }
    metadata = {
        key.removeprefix("metadata."): _parse_sample_source_list_cell(value)
        for key, value in row.items()
        if key.startswith("metadata.") and value != ""
    }
    source_sha256 = (row.get("source_sha256") or row.get("sha256") or "").strip()
    if source_sha256:
        metadata["source_sha256"] = source_sha256.removeprefix("sha256:")
    return RuntimeDocumentInput(
        document_id=document_id,
        pipeline_id=(row.get("pipeline_id") or row.get("pipeline") or "").strip()
        or None,
        title=(row.get("title") or "").strip() or document_id,
        document_type=(row.get("document_type") or "").strip() or None,
        relation=(row.get("relation") or "").strip() or None,
        parent_document_id=(row.get("parent_document_id") or "").strip()
        or None,
        parent_process_id=(row.get("parent_process_id") or "").strip()
        or None,
        media_type=(row.get("media_type") or "").strip() or None,
        source_uri=source_uri,
        values=values,
        metadata={
            **metadata,
            "source_list": str(source_list),
            "source_list_row": row_number,
        },
    )


def _parse_sample_source_list_cell(value: str) -> Any:
    text = value.strip()
    if not text:
        return ""
    try:
        parsed = yaml.safe_load(text)
    except yaml.YAMLError:
        return value
    return text if parsed is None and text.lower() not in {"null", "~"} else parsed


def _source_list_local_sources_present(source_list: Path) -> bool:
    if not source_list.is_file():
        return False
    delimiter = "\t" if source_list.suffix.lower() == ".tsv" else ","
    seen_source = False
    try:
        with source_list.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter=delimiter)
            if not reader.fieldnames:
                return False
            for row in reader:
                source_path = (
                    (row.get("path") or row.get("source_path") or "").strip()
                )
                source_uri = (row.get("source_uri") or "").strip()
                local_path: Path | None = None
                if source_path:
                    local_path = Path(source_path).expanduser()
                    if not local_path.is_absolute():
                        local_path = source_list.parent / local_path
                    seen_source = True
                elif source_uri:
                    seen_source = True
                    local_path = _local_path_from_file_uri(source_uri)
                if local_path is not None and not local_path.is_file():
                    return False
    except OSError:
        return False
    return seen_source


def _local_path_from_file_uri(source_uri: str) -> Path | None:
    parsed = urlparse(source_uri)
    if parsed.scheme != "file":
        return None
    if parsed.netloc not in ("", "localhost"):
        return None
    return Path(unquote(parsed.path))


def _model_sha256(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
