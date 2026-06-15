from __future__ import annotations

import importlib
import sys
from collections.abc import Iterable
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from fala.registry import PipelineRegistry
from fala.sdk import StepContract


class ContractLintError(RuntimeError):
    pass


def load_step_contract_refs(
    refs: Iterable[str],
    *,
    python_paths: Iterable[str | Path] = (),
) -> list[StepContract]:
    for path in reversed([str(Path(item).expanduser()) for item in python_paths]):
        if path and path not in sys.path:
            sys.path.insert(0, path)
    contracts: list[StepContract] = []
    for ref in refs:
        contracts.extend(_load_step_contract_ref(ref))
    return contracts


def lint_step_contracts(
    registry: PipelineRegistry,
    *,
    pipeline_id: str,
    contracts: Iterable[StepContract],
    require_all_steps: bool = True,
) -> dict[str, Any]:
    pipeline = registry.get(pipeline_id)
    package_id = registry.pipeline_package_id(pipeline.id)
    package = registry.package(package_id) if package_id is not None else None
    capabilities = (
        {capability.id: capability for capability in package.capabilities}
        if package is not None
        else {}
    )
    artifact_kind_ids = (
        {artifact_kind.id for artifact_kind in package.artifact_kinds}
        if package is not None
        else set()
    )
    steps_by_id = {step.id: step for step in pipeline.steps}
    contracts_by_process_id: dict[str, StepContract] = {}
    issues: list[dict[str, Any]] = []

    for contract in contracts:
        if contract.process_id in contracts_by_process_id:
            issues.append(
                _issue(
                    "duplicate_contract",
                    contract.process_id,
                    f"Multiple StepContract objects declare process {contract.process_id!r}",
                )
            )
            continue
        contracts_by_process_id[contract.process_id] = contract

    if require_all_steps:
        for step_id in steps_by_id:
            if step_id not in contracts_by_process_id:
                issues.append(
                    _issue(
                        "missing_contract",
                        step_id,
                        f"Pipeline step {step_id!r} has no StepContract",
                    )
                )

    for process_id, contract in contracts_by_process_id.items():
        step = steps_by_id.get(process_id)
        if step is None:
            issues.append(
                _issue(
                    "unknown_process",
                    process_id,
                    f"StepContract process {process_id!r} is not a step in pipeline {pipeline_id!r}",
                )
            )
            continue

        capability = capabilities.get(step.capability) if step.capability else None
        expected_needs = set(step.needs)
        contract_need_processes = {need.process_id for need in contract.needs.values()}
        for missing_need in sorted(expected_needs - contract_need_processes):
            issues.append(
                _issue(
                    "missing_contract_need",
                    process_id,
                    f"Step {process_id!r} needs process {missing_need!r}, but its StepContract does not read from it",
                )
            )
        for extra_need in sorted(contract_need_processes - expected_needs):
            issues.append(
                _issue(
                    "undeclared_pipeline_need",
                    process_id,
                    f"StepContract for {process_id!r} reads from {extra_need!r}, but the pipeline step does not declare that need",
                    need_process_id=extra_need,
                )
            )

        accepted_artifact_kinds = (
            set(capability.accepts_artifact_kinds) if capability is not None else set()
        )
        for need_name, need in sorted(contract.needs.items()):
            need_step = steps_by_id.get(need.process_id)
            if need_step is None:
                issues.append(
                    _issue(
                        "unknown_need_process",
                        process_id,
                        f"Need {need_name!r} references unknown process {need.process_id!r}",
                        need=need_name,
                        need_process_id=need.process_id,
                    )
                )
                continue
            need_capability = (
                capabilities.get(need_step.capability)
                if need_step.capability is not None
                else None
            )
            emitted_by_need = (
                set(need_capability.emits_artifact_kinds)
                if need_capability is not None
                else set()
            )
            if emitted_by_need and need.artifact_kind not in emitted_by_need:
                issues.append(
                    _issue(
                        "need_artifact_not_emitted",
                        process_id,
                        f"Need {need_name!r} expects artifact kind {need.artifact_kind!r}, but process {need.process_id!r} does not emit it",
                        need=need_name,
                        need_process_id=need.process_id,
                        artifact_kind=need.artifact_kind,
                        emitted_artifact_kinds=sorted(emitted_by_need),
                    )
                )
            if accepted_artifact_kinds and need.artifact_kind not in accepted_artifact_kinds:
                issues.append(
                    _issue(
                        "need_artifact_not_accepted",
                        process_id,
                        f"Need {need_name!r} uses artifact kind {need.artifact_kind!r}, but capability {step.capability!r} does not accept it",
                        need=need_name,
                        artifact_kind=need.artifact_kind,
                        accepted_artifact_kinds=sorted(accepted_artifact_kinds),
                    )
                )

        output_artifact_kinds = {
            output.artifact_kind for output in contract.outputs.values()
        }
        if artifact_kind_ids:
            for artifact_kind in sorted(output_artifact_kinds - artifact_kind_ids):
                issues.append(
                    _issue(
                        "unknown_output_artifact_kind",
                        process_id,
                        f"Output artifact kind {artifact_kind!r} is not declared by the workflow package",
                        artifact_kind=artifact_kind,
                    )
                )
        emitted_artifact_kinds = (
            set(capability.emits_artifact_kinds) if capability is not None else set()
        )
        for artifact_kind in sorted(output_artifact_kinds - emitted_artifact_kinds):
            issues.append(
                _issue(
                    "output_artifact_not_emitted",
                    process_id,
                    f"StepContract output artifact kind {artifact_kind!r} is not emitted by capability {step.capability!r}",
                    artifact_kind=artifact_kind,
                    emitted_artifact_kinds=sorted(emitted_artifact_kinds),
                )
            )
        for artifact_kind in sorted(emitted_artifact_kinds - output_artifact_kinds):
            issues.append(
                _issue(
                    "missing_contract_output",
                    process_id,
                    f"Capability {step.capability!r} emits artifact kind {artifact_kind!r}, but the StepContract has no matching output",
                    artifact_kind=artifact_kind,
                )
            )

    return {
        "ok": not issues,
        "pipeline_id": pipeline.id,
        "package_id": package_id,
        "require_all_steps": require_all_steps,
        "step_count": len(pipeline.steps),
        "contract_count": len(contracts_by_process_id),
        "issue_count": len(issues),
        "issues": issues,
        "contracts": [
            step_contract_summary(contract)
            for contract in contracts_by_process_id.values()
        ],
    }


def step_contract_summary(contract: StepContract) -> dict[str, Any]:
    return {
        "process_id": contract.process_id,
        "manifest_filename": contract.manifest_filename,
        "needs": {
            name: _dataclass_dict(need)
            for name, need in sorted(contract.needs.items())
        },
        "outputs": {
            name: _dataclass_dict(output)
            for name, output in sorted(contract.outputs.items())
        },
    }


def _load_step_contract_ref(ref: str) -> list[StepContract]:
    module_name, _, attr_name = ref.partition(":")
    if not module_name.strip():
        raise ContractLintError("Step contract reference must include a module")
    attr_name = attr_name or "CONTRACT"
    module = importlib.import_module(module_name)
    value = getattr(module, attr_name)
    if isinstance(value, StepContract):
        return [value]
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, dict)):
        contracts = list(value)
        if all(isinstance(item, StepContract) for item in contracts):
            return contracts
    raise ContractLintError(
        f"{ref!r} must resolve to StepContract or iterable of StepContract"
    )


def _issue(
    code: str,
    process_id: str,
    message: str,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "severity": "error",
        "code": code,
        "process_id": process_id,
        "message": message,
        **extra,
    }


def _dataclass_dict(value: Any) -> dict[str, Any]:
    if is_dataclass(value):
        return asdict(value)
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    return dict(value)


__all__ = [
    "ContractLintError",
    "lint_step_contracts",
    "load_step_contract_refs",
    "step_contract_summary",
]
