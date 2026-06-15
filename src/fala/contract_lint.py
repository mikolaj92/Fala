from __future__ import annotations

import importlib
import keyword
import sys
from collections.abc import Iterable
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from fala.registry import PipelineRegistry
from fala.sdk import StepContract


class ContractLintError(RuntimeError):
    pass


CONTRACT_DISCOVERY_ATTRS = ("STEP_CONTRACTS", "CONTRACTS", "CONTRACT")
CONTRACT_DISCOVERY_FILENAMES = ("contracts.py",)
CONTRACT_DISCOVERY_SUFFIXES = ("_contracts.py",)
CONTRACT_DISCOVERY_SKIP_DIRS = {
    ".git",
    ".hg",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".tox",
    ".venv",
    "__pycache__",
    "build",
    "dist",
    "node_modules",
    "site-packages",
}


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


def discover_step_contracts(
    registry: PipelineRegistry | None = None,
    *,
    pipeline_id: str | None = None,
    package_id: str | None = None,
    python_paths: Iterable[str | Path] = (),
    roots: Iterable[str | Path] = (),
) -> dict[str, Any]:
    search_roots, import_roots = _contract_discovery_roots(
        registry,
        pipeline_id=pipeline_id,
        package_id=package_id,
        python_paths=python_paths,
        roots=roots,
    )
    for path in reversed([str(item) for item in import_roots]):
        if path and path not in sys.path:
            sys.path.insert(0, path)

    contracts: list[StepContract] = []
    refs: list[str] = []
    errors: list[dict[str, Any]] = []
    seen_modules: set[str] = set()
    for file_path in _candidate_contract_files(search_roots):
        module_name = _module_name_for_file(file_path, import_roots)
        if module_name is None or module_name in seen_modules:
            continue
        seen_modules.add(module_name)
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:
            errors.append(
                {
                    "file": str(file_path),
                    "module": module_name,
                    "error": str(exc),
                }
            )
            continue
        for attr_name in CONTRACT_DISCOVERY_ATTRS:
            if not hasattr(module, attr_name):
                continue
            ref = f"{module_name}:{attr_name}"
            try:
                loaded = _coerce_step_contracts(getattr(module, attr_name), ref)
            except ContractLintError as exc:
                errors.append(
                    {
                        "file": str(file_path),
                        "module": module_name,
                        "attribute": attr_name,
                        "error": str(exc),
                    }
                )
                continue
            if loaded:
                contracts.extend(loaded)
                refs.append(ref)
                break

    return {
        "contracts": contracts,
        "refs": refs,
        "errors": errors,
        "python_paths": [str(path) for path in import_roots],
        "roots": [str(path) for path in search_roots],
    }


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
    return _coerce_step_contracts(value, ref)


def _coerce_step_contracts(value: Any, ref: str) -> list[StepContract]:
    if isinstance(value, StepContract):
        return [value]
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, dict)):
        contracts = list(value)
        if all(isinstance(item, StepContract) for item in contracts):
            return contracts
    raise ContractLintError(
        f"{ref!r} must resolve to StepContract or iterable of StepContract"
    )


def _contract_discovery_roots(
    registry: PipelineRegistry | None,
    *,
    pipeline_id: str | None,
    package_id: str | None,
    python_paths: Iterable[str | Path],
    roots: Iterable[str | Path],
) -> tuple[list[Path], list[Path]]:
    search_roots: list[Path] = []
    import_roots: list[Path] = []

    def add_import_root(value: str | Path | None) -> None:
        if value is None:
            return
        path = Path(value).expanduser().resolve()
        if path not in import_roots:
            import_roots.append(path)
        if path.exists() and path not in search_roots:
            search_roots.append(path)

    def add_search_root(value: str | Path | None) -> None:
        if value is None:
            return
        path = Path(value).expanduser().resolve()
        if path.exists() and path not in search_roots:
            search_roots.append(path)
        src = path / "src"
        if src.is_dir():
            add_import_root(src)

    for path in python_paths:
        add_import_root(path)
    for root in roots:
        add_search_root(root)

    if registry is not None:
        package_ids: set[str] = set()
        pipeline_ids: set[str] = set()
        if pipeline_id:
            pipeline_ids.add(pipeline_id)
            found_package_id = registry.pipeline_package_id(pipeline_id)
            if found_package_id:
                package_ids.add(found_package_id)
        if package_id:
            package_ids.add(package_id)
            pipeline_ids.update(registry.package_pipeline_ids(package_id))

        for found_package_id in sorted(package_ids):
            package_source = registry.package_source(found_package_id)
            if package_source:
                add_search_root(Path(package_source).parent)
            try:
                package = registry.package(found_package_id)
            except Exception:
                package = None
            if package is not None:
                package_root = Path(package_source).parent if package_source else None
                for worker in package.workers:
                    cwd = _resolve_command_cwd(
                        getattr(worker, "cwd", None),
                        base=package_root,
                    )
                    for path in _python_paths_from_command(worker.command, cwd=cwd):
                        add_import_root(path)

        for found_pipeline_id in sorted(pipeline_ids):
            pipeline_source = registry.pipeline_source(found_pipeline_id)
            pipeline_root = Path(pipeline_source).parent if pipeline_source else None
            if pipeline_root is not None:
                add_search_root(pipeline_root)
            try:
                pipeline = registry.get(found_pipeline_id)
            except Exception:
                continue
            for step in pipeline.steps:
                adapter = step.adapter
                cwd = _resolve_command_cwd(getattr(adapter, "cwd", None), base=pipeline_root)
                for path in _python_paths_from_command(
                    getattr(adapter, "command", None) or [],
                    cwd=cwd,
                ):
                    add_import_root(path)

    if not import_roots:
        add_import_root(Path.cwd())
    if not search_roots:
        search_roots.extend(import_roots)
    return search_roots, import_roots


def _python_paths_from_command(command: list[str], *, cwd: Path | None) -> list[Path]:
    paths: list[Path] = []
    for index, token in enumerate(command):
        if token != "--project" or index + 1 >= len(command):
            continue
        project_arg = command[index + 1]
        if not project_arg or project_arg.startswith("-"):
            continue
        project_path = Path(project_arg).expanduser()
        if not project_path.is_absolute():
            project_path = (cwd or Path.cwd()) / project_path
        project_path = project_path.resolve()
        src_path = project_path / "src"
        paths.append(src_path if src_path.is_dir() else project_path)
    return paths


def _resolve_command_cwd(value: str | None, *, base: Path | None) -> Path | None:
    if not value:
        return base
    path = Path(value).expanduser()
    if not path.is_absolute() and base is not None:
        path = base / path
    return path.resolve()


def _candidate_contract_files(search_roots: list[Path]) -> list[Path]:
    candidates: list[Path] = []
    seen: set[Path] = set()
    for root in search_roots:
        if root.is_file():
            paths = [root]
        elif root.is_dir():
            paths = list(root.rglob("*.py"))
        else:
            paths = []
        for path in paths:
            resolved = path.resolve()
            if resolved in seen or _skip_contract_file(resolved):
                continue
            name = resolved.name
            if name in CONTRACT_DISCOVERY_FILENAMES or any(
                name.endswith(suffix) for suffix in CONTRACT_DISCOVERY_SUFFIXES
            ):
                candidates.append(resolved)
                seen.add(resolved)
    return sorted(candidates)


def _skip_contract_file(path: Path) -> bool:
    return any(part in CONTRACT_DISCOVERY_SKIP_DIRS for part in path.parts)


def _module_name_for_file(path: Path, import_roots: list[Path]) -> str | None:
    for root in sorted(import_roots, key=lambda item: len(str(item)), reverse=True):
        try:
            relative = path.relative_to(root)
        except ValueError:
            continue
        if relative.name == "__init__.py":
            parts = relative.parent.parts
        else:
            parts = (*relative.parent.parts, relative.stem)
        if not parts or not all(_valid_module_part(part) for part in parts):
            return None
        return ".".join(parts)
    return None


def _valid_module_part(value: str) -> bool:
    return value.isidentifier() and not keyword.iskeyword(value)


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
    "discover_step_contracts",
    "lint_step_contracts",
    "load_step_contract_refs",
    "step_contract_summary",
]
