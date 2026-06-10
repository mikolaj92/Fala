from __future__ import annotations

import argparse
import asyncio
import json
import os
import shlex
import shutil
import sys
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from fala.adapters import AdapterRegistry
from fala.models import (
    ArtifactRef,
    CombinedProjection,
    CombineSpec,
    PipelineSpec,
    ProcessActionInput,
    ProcessEvent,
    ProcessEventPage,
    ProcessExecutionContext,
    ProcessOutput,
    ProcessSpec,
    ProcessStatus,
    RuntimeState,
    WorkflowPackageSpec,
    WorkflowWorkerSpec,
)
from fala.registry import PipelineRegistry
from fala.scheduler import PipelineScheduler
from fala.sqlite_store import SQLiteStateStore
from fala.state import build_runtime_document_state, build_runtime_state

ADAPTER_KIND_CHOICES = ("subprocess", "http", "queue")
CONTRACT_MODELS: dict[str, type[BaseModel]] = {
    "pipeline": PipelineSpec,
    "process-context": ProcessExecutionContext,
    "process-output": ProcessOutput,
    "process-action": ProcessActionInput,
    "artifact": ArtifactRef,
    "event": ProcessEvent,
    "event-page": ProcessEventPage,
    "runtime-state": RuntimeState,
    "workflow-package": WorkflowPackageSpec,
}


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        payload = asyncio.run(_run(args))
    except Exception as exc:
        if _should_emit_json_error(args):
            print(json.dumps({"ok": False, "error": str(exc)}, indent=2, sort_keys=True))
        else:
            print(f"error: {exc}")
        return 1

    if payload is not None:
        print(json.dumps(payload, indent=2, sort_keys=True))
        if isinstance(payload, dict) and payload.get("ok") is False:
            return 1
    return 0


def _should_emit_json_error(args: argparse.Namespace) -> bool:
    return bool(
        getattr(args, "json", False)
        or getattr(args, "command", None)
        in {"schema", "validate-output", "validate-context"}
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="process-runtime")
    parser.add_argument(
        "--pipeline-dir",
        default=None,
        help="Directory with *.yaml pipeline definitions.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate", help="Validate pipeline YAML files.")
    validate.add_argument("--json", action="store_true", help="Emit JSON result.")
    validate.add_argument(
        "--check-commands",
        action="store_true",
        help="Also verify subprocess executables are available in this environment.",
    )

    schema = subparsers.add_parser("schema", help="Emit JSON Schema for a runtime contract model.")
    schema.add_argument("model", choices=sorted(CONTRACT_MODELS))

    validate_output = subparsers.add_parser(
        "validate-output",
        help="Validate ProcessOutput JSON from a file or stdin.",
    )
    validate_output.add_argument("--file", default="-", help="JSON file path, or '-' for stdin.")

    validate_context = subparsers.add_parser(
        "validate-context",
        help="Validate ProcessExecutionContext JSON from a file or stdin.",
    )
    validate_context.add_argument("--file", default="-", help="JSON file path, or '-' for stdin.")

    scaffold = subparsers.add_parser(
        "scaffold",
        help="Create a workflow package with one SDK-backed program per step.",
    )
    scaffold.add_argument("--output-dir", required=True, help="Directory to create.")
    scaffold.add_argument("--package-id", required=True, help="Workflow package id.")
    scaffold.add_argument("--pipeline-id", required=True, help="Pipeline id.")
    scaffold.add_argument(
        "--steps",
        required=True,
        help="Comma-separated process ids. Generates one program per id.",
    )
    scaffold.add_argument(
        "--adapter-kind",
        choices=("subprocess", "queue"),
        default="subprocess",
        help="Adapter kind to write in generated pipeline YAML.",
    )
    scaffold.add_argument("--title", default=None, help="Optional package and pipeline title.")

    subparsers.add_parser("list", help="List pipeline ids.")

    worker_commands = subparsers.add_parser(
        "worker-commands",
        help="Render process-runtime-worker commands declared by workflow packages.",
    )
    worker_commands.add_argument("--base-url", required=True, help="Control plane base URL.")
    worker_commands.add_argument("--run-id", required=True)
    worker_commands.add_argument("--package-id", default=None)

    describe = subparsers.add_parser("describe", help="Describe one pipeline.")
    describe.add_argument("pipeline_id")

    init = subparsers.add_parser("init-document", help="Initialize document graph in SQLite.")
    init.add_argument("--db", required=True, help="SQLite runtime DB path.")
    init.add_argument("--pipeline", required=True, help="Pipeline id.")
    init.add_argument("--run-id", required=True)
    init.add_argument("--document-id", required=True)
    init.add_argument(
        "--value",
        action="append",
        default=[],
        help="Initial value as key=value. Repeatable.",
    )
    init.add_argument(
        "--artifact",
        action="append",
        default=[],
        help="Artifact as kind=uri. Repeatable.",
    )

    claim = subparsers.add_parser("claim", help="Claim next ready process from SQLite.")
    claim.add_argument("--db", required=True, help="SQLite runtime DB path.")
    claim.add_argument("--pipeline", required=True, help="Pipeline id.")
    claim.add_argument("--run-id", required=True)
    claim.add_argument("--worker-id", default=None)
    claim.add_argument("--process-id", default=None)
    claim.add_argument("--adapter-kind", choices=ADAPTER_KIND_CHOICES, default=None)
    claim.add_argument("--lease-seconds", type=float, default=300.0)

    work = subparsers.add_parser(
        "work-once",
        help="Claim one ready SQLite process, run its adapter, and persist output.",
    )
    work.add_argument("--db", required=True, help="SQLite runtime DB path.")
    work.add_argument("--pipeline", required=True, help="Pipeline id.")
    work.add_argument("--run-id", required=True)
    work.add_argument("--worker-id", required=True)
    work.add_argument("--process-id", default=None)
    work.add_argument("--adapter-kind", choices=ADAPTER_KIND_CHOICES, default=None)
    work.add_argument("--lease-seconds", type=float, default=300.0)

    run = subparsers.add_parser(
        "run-until-idle",
        help="Run ready SQLite processes until no matching claim remains.",
    )
    run.add_argument("--db", required=True, help="SQLite runtime DB path.")
    run.add_argument("--pipeline", required=True, help="Pipeline id.")
    run.add_argument("--run-id", required=True)
    run.add_argument("--worker-id", required=True)
    run.add_argument("--process-id", default=None)
    run.add_argument("--adapter-kind", choices=ADAPTER_KIND_CHOICES, default=None)
    run.add_argument("--lease-seconds", type=float, default=300.0)
    run.add_argument("--max-steps", type=int, default=1000)
    run.add_argument(
        "--include-events",
        action="store_true",
        help="Include full process event log in returned state.",
    )

    status = subparsers.add_parser("status", help="Show SQLite runtime state for a run.")
    status.add_argument("--db", required=True, help="SQLite runtime DB path.")
    status.add_argument("--run-id", required=True)
    status.add_argument(
        "--include-events",
        action="store_true",
        help="Include full process event log in returned state.",
    )

    return parser


async def _run(args: argparse.Namespace) -> dict[str, Any] | None:
    if args.command == "schema":
        model = CONTRACT_MODELS[args.model]
        return {
            "ok": True,
            "model": args.model,
            "schema": model.model_json_schema(),
        }

    if args.command == "validate-output":
        output = _validate_json_contract(ProcessOutput, args.file)
        return {
            "ok": True,
            "model": "process-output",
            "artifact_count": len(output.artifacts),
            "value_keys": sorted(output.values),
            "metadata_keys": sorted(output.metadata),
        }

    if args.command == "validate-context":
        context = _validate_json_contract(ProcessExecutionContext, args.file)
        return {
            "ok": True,
            "model": "process-context",
            "pipeline_id": context.pipeline_id,
            "run_id": context.run_id,
            "document_id": context.document_id,
            "process_id": context.process_id,
            "attempt": context.attempt,
            "artifact_count": len(context.input.artifacts),
            "initial_keys": sorted(
                (context.input.values.get("initial") or {}).keys()
                if isinstance(context.input.values.get("initial"), dict)
                else []
            ),
            "needs": sorted(
                (context.input.values.get("needs") or {}).keys()
                if isinstance(context.input.values.get("needs"), dict)
                else []
            ),
        }

    if args.command == "scaffold":
        return _scaffold_workflow_package(
            output_dir=Path(args.output_dir),
            package_id=args.package_id,
            pipeline_id=args.pipeline_id,
            steps=_parse_scaffold_steps(args.steps),
            adapter_kind=args.adapter_kind,
            title=args.title,
        )

    registry = PipelineRegistry.from_directory(_pipeline_dir(args))

    if args.command == "validate":
        pipelines = registry.all()
        if not pipelines:
            raise ValueError(f"No pipelines found in {_pipeline_dir(args)}")
        command_issues = (
            [
                *_validate_subprocess_commands(pipelines),
                *_validate_package_worker_commands(registry.packages()),
            ]
            if args.check_commands
            else []
        )
        payload = {
            "ok": not command_issues,
            "package_count": len(registry.packages()),
            "packages": [_package_summary(package) for package in registry.packages()],
            "pipeline_count": len(pipelines),
            "pipelines": [
                _pipeline_summary(pipeline, registry=registry)
                for pipeline in pipelines
            ],
        }
        if args.check_commands:
            payload["command_issues"] = command_issues
        if command_issues and not args.json:
            _print_plain_validation(payload)
            raise ValueError(
                f"{len(command_issues)} subprocess command(s) unavailable"
            )
        return payload if args.json else _print_plain_validation(payload)

    if args.command == "list":
        return {
            "packages": [_package_summary(package) for package in registry.packages()],
            "pipelines": [
                _pipeline_summary(pipeline, registry=registry)
                for pipeline in registry.all()
            ],
        }

    if args.command == "worker-commands":
        packages = (
            [registry.package(args.package_id)]
            if args.package_id is not None
            else registry.packages()
        )
        return {
            "ok": True,
            "pipeline_dir": str(_pipeline_dir(args)),
            "base_url": args.base_url,
            "run_id": args.run_id,
            "workers": [
                _worker_command_summary(
                    package_id=package.id,
                    worker=worker,
                    pipeline_dir=_pipeline_dir(args),
                    base_url=args.base_url,
                    run_id=args.run_id,
                )
                for package in packages
                for worker in package.workers
            ],
        }

    if args.command == "describe":
        pipeline = registry.get(args.pipeline_id)
        return {"pipeline": pipeline.model_dump(mode="json")}

    if args.command == "init-document":
        pipeline = registry.get(args.pipeline)
        store = SQLiteStateStore(args.db)
        scheduler = PipelineScheduler(pipeline, store)
        schedule = await scheduler.initialize_document(
            run_id=args.run_id,
            document_id=args.document_id,
            values=_parse_values(args.value),
            artifacts=_parse_artifacts(args.artifact),
        )
        return {"ok": True, "schedule": schedule.model_dump(mode="json")}

    if args.command == "claim":
        pipeline = registry.get(args.pipeline)
        store = SQLiteStateStore(args.db)
        scheduler = PipelineScheduler(pipeline, store)
        document_ids = await store.list_documents(run_id=args.run_id)
        claim = await scheduler.claim_next(
            run_id=args.run_id,
            document_ids=document_ids,
            worker_id=args.worker_id,
            process_id=args.process_id,
            adapter_kind=args.adapter_kind,
            lease_seconds=args.lease_seconds,
        )
        return {
            "ok": True,
            "claim": claim.model_dump(mode="json") if claim else None,
        }

    if args.command == "work-once":
        pipeline = registry.get(args.pipeline)
        store = SQLiteStateStore(args.db)
        return await _work_once(
            store=store,
            pipeline=pipeline,
            run_id=args.run_id,
            worker_id=args.worker_id,
            process_id=args.process_id,
            adapter_kind=args.adapter_kind,
            lease_seconds=args.lease_seconds,
        )

    if args.command == "run-until-idle":
        if args.max_steps < 1:
            raise ValueError("--max-steps must be greater than zero")
        pipeline = registry.get(args.pipeline)
        store = SQLiteStateStore(args.db)
        steps: list[dict[str, Any]] = []
        idle = False
        limit_reached = False
        for _ in range(args.max_steps):
            result = await _work_once(
                store=store,
                pipeline=pipeline,
                run_id=args.run_id,
                worker_id=args.worker_id,
                process_id=args.process_id,
                adapter_kind=args.adapter_kind,
                lease_seconds=args.lease_seconds,
            )
            if not result["completed"]:
                idle = True
                break
            steps.append(result)
        else:
            limit_reached = True

        return {
            "ok": True,
            "idle": idle,
            "limit_reached": limit_reached,
            "completed_count": len(steps),
            "steps": [
                {
                    "document_id": item["claim"]["document_id"],
                    "process_id": item["claim"]["process"]["id"],
                    "attempt": item["claim"]["attempt"],
                    "output_keys": sorted(item["output"]["values"]),
                    "refreshed_projection_ids": [
                        projection["id"]
                        for projection in item["refreshed_projections"]
                    ],
                }
                for item in steps
            ],
            "state": await _runtime_state(
                store=store,
                run_id=args.run_id,
                pipeline=pipeline,
                registry=registry,
                include_events=args.include_events,
            ),
        }

    if args.command == "status":
        store = SQLiteStateStore(args.db)
        return {
            "ok": True,
            "state": await _runtime_state(
                store=store,
                run_id=args.run_id,
                registry=registry,
                include_events=args.include_events,
            ),
        }

    raise ValueError(f"Unknown command: {args.command}")


async def _work_once(
    *,
    store: SQLiteStateStore,
    pipeline: PipelineSpec,
    run_id: str,
    worker_id: str,
    process_id: str | None = None,
    adapter_kind: str | None = None,
    lease_seconds: float = 300.0,
) -> dict[str, Any]:
    scheduler = PipelineScheduler(pipeline, store)
    document_ids = await store.list_documents(run_id=run_id)
    claim = await scheduler.claim_next(
        run_id=run_id,
        document_ids=document_ids,
        worker_id=worker_id,
        process_id=process_id,
        adapter_kind=adapter_kind,
        lease_seconds=lease_seconds,
    )
    if claim is None:
        return {"ok": True, "claim": None, "completed": False}

    step = _step_by_id(pipeline, claim.process.id)
    try:
        await store.append_event(
            ProcessEvent(
                run_id=claim.run_id,
                document_id=claim.document_id,
                process_id=claim.process.id,
                type="process.started",
                status=ProcessStatus.running,
                data={"worker_id": worker_id, "attempt": claim.attempt},
            )
        )
        output = await AdapterRegistry.default().run(
            step,
            claim.context,
            event_sink=store.append_event,
        )
    except Exception as exc:
        failure = await scheduler.record_process_failure(
            run_id=claim.run_id,
            document_id=claim.document_id,
            process_id=claim.process.id,
            data={"worker_id": worker_id, "error": str(exc)},
        )
        return {
            "ok": True,
            "completed": False,
            "claim": claim.model_dump(mode="json"),
            "error": str(exc),
            "failure": failure.model_dump(mode="json"),
            "schedule": failure.schedule.model_dump(mode="json"),
        }

    await store.put_output(
        run_id=claim.run_id,
        document_id=claim.document_id,
        process_id=claim.process.id,
        output=output,
    )
    await store.set_status(
        run_id=claim.run_id,
        document_id=claim.document_id,
        process_id=claim.process.id,
        status=ProcessStatus.completed,
    )
    await store.clear_claim(
        run_id=claim.run_id,
        document_id=claim.document_id,
        process_id=claim.process.id,
    )
    await store.append_event(
        ProcessEvent(
            run_id=claim.run_id,
            document_id=claim.document_id,
            process_id=claim.process.id,
            type="process.output",
            status=ProcessStatus.completed,
            data={
                "worker_id": worker_id,
                "artifact_count": len(output.artifacts),
                "value_keys": sorted(output.values),
            },
        )
    )
    refreshed = await _refresh_projections_for_process(
        store=store,
        pipeline=pipeline,
        run_id=claim.run_id,
        document_id=claim.document_id,
        process_id=claim.process.id,
    )
    schedule = await scheduler.schedule_ready(
        run_id=claim.run_id,
        document_id=claim.document_id,
    )
    return {
        "ok": True,
        "completed": True,
        "claim": claim.model_dump(mode="json"),
        "output": output.model_dump(mode="json"),
        "refreshed_projections": [
            projection.model_dump(mode="json")
            for projection in refreshed
        ],
        "schedule": schedule.model_dump(mode="json"),
    }


async def _runtime_state(
    *,
    store: SQLiteStateStore,
    run_id: str,
    pipeline: PipelineSpec | None = None,
    registry: PipelineRegistry | None = None,
    include_events: bool = False,
) -> dict[str, Any]:
    documents: list[dict[str, Any]] = []
    for document_id in await store.list_documents(run_id=run_id):
        pipeline_id = await store.get_document_pipeline_id(
            run_id=run_id,
            document_id=document_id,
        )
        statuses = await store.list_statuses(
            run_id=run_id,
            document_id=document_id,
        )
        claims = await store.list_claims(
            run_id=run_id,
            document_id=document_id,
        )
        outputs = await store.list_outputs(
            run_id=run_id,
            document_id=document_id,
        )
        projections = await store.list_projections(
            run_id=run_id,
            document_id=document_id,
        )
        events = (
            await store.list_events(run_id=run_id, document_id=document_id)
            if include_events
            else []
        )
        event_count = await store.count_events(run_id=run_id, document_id=document_id)
        document_pipeline = pipeline if pipeline and pipeline.id == pipeline_id else None
        if document_pipeline is None and registry is not None and pipeline_id:
            try:
                document_pipeline = registry.get(pipeline_id)
            except Exception:
                document_pipeline = None
        documents.append(
            build_runtime_document_state(
                document_id=document_id,
                pipeline_id=pipeline_id,
                pipeline=document_pipeline,
                statuses=statuses,
                claims=claims,
                outputs=outputs,
                projections=projections,
                events=events,
                event_count=event_count,
            )
        )
    return build_runtime_state(run_id=run_id, documents=documents).model_dump(mode="json")


def _pipeline_dir(args: argparse.Namespace) -> Path:
    if args.pipeline_dir:
        return Path(args.pipeline_dir)
    cwd = Path.cwd()
    candidates = [
        cwd / "control-plane" / "examples" / "pipelines",
        cwd / "examples" / "pipelines",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def _pipeline_summary(
    pipeline,
    *,
    registry: PipelineRegistry | None = None,
) -> dict[str, Any]:
    summary = {
        "id": pipeline.id,
        "title": pipeline.title,
        "description": pipeline.description,
        "tags": pipeline.tags,
        "version": pipeline.version,
        "input_values": pipeline.input_values,
        "steps": [
            {
                "id": step.id,
                "title": step.title,
                "description": step.description,
                "tags": step.tags,
                "adapter_kind": step.adapter.kind,
                "needs": step.needs,
            }
            for step in pipeline.steps
        ],
        "combines": [combine.id for combine in pipeline.combines],
    }
    if registry is not None:
        package_id = registry.pipeline_package_id(pipeline.id)
        source = registry.pipeline_source(pipeline.id)
        if package_id is not None:
            summary["package_id"] = package_id
        if source is not None:
            summary["source"] = source
    return summary


def _package_summary(package: WorkflowPackageSpec) -> dict[str, Any]:
    return {
        "id": package.id,
        "title": package.title,
        "description": package.description,
        "tags": package.tags,
        "version": package.version,
        "pipelines": package.pipelines,
        "workers": [
            {
                "id": worker.id,
                "title": worker.title,
                "description": worker.description,
                "tags": worker.tags,
                "pipeline_id": worker.pipeline_id,
                "process_id": worker.process_id,
                "adapter_kind": worker.adapter_kind,
                "command": worker.command,
                "cwd": worker.cwd,
                "env": worker.env,
                "timeout_seconds": worker.timeout_seconds,
            }
            for worker in package.workers
        ],
    }


def _worker_command_summary(
    *,
    package_id: str,
    worker: WorkflowWorkerSpec,
    pipeline_dir: Path,
    base_url: str,
    run_id: str,
) -> dict[str, Any]:
    argv = [
        "process-runtime-worker",
        "--pipeline-dir",
        str(pipeline_dir),
        "--base-url",
        base_url,
        "--run-id",
        run_id,
        "--package-id",
        package_id,
        "--package-worker",
        worker.id,
    ]
    return {
        "package_id": package_id,
        "worker_id": worker.id,
        "pipeline_id": worker.pipeline_id,
        "process_id": worker.process_id,
        "adapter_kind": worker.adapter_kind,
        "argv": argv,
        "shell": " ".join(shlex.quote(part) for part in argv),
    }


def _validate_subprocess_commands(pipelines: list[PipelineSpec]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for pipeline in pipelines:
        for step in pipeline.steps:
            if step.adapter.kind != "subprocess":
                continue
            command = step.adapter.command or []
            reason = _subprocess_command_issue(command, cwd=step.adapter.cwd)
            if reason is None:
                continue
            issues.append(
                {
                    "pipeline_id": pipeline.id,
                    "process_id": step.id,
                    "command": command,
                    "reason": reason,
                }
            )
    return issues


def _validate_package_worker_commands(
    packages: list[WorkflowPackageSpec],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for package in packages:
        for worker in package.workers:
            command = worker.command
            reason = _subprocess_command_issue(command, cwd=worker.cwd)
            if reason is None:
                continue
            issues.append(
                {
                    "package_id": package.id,
                    "worker_id": worker.id,
                    "pipeline_id": worker.pipeline_id,
                    "process_id": worker.process_id,
                    "command": command,
                    "reason": reason,
                }
            )
    return issues


def _subprocess_command_issue(command: list[str], *, cwd: str | None = None) -> str | None:
    executable = str(command[0]) if command else ""
    if not executable:
        return "missing executable"

    has_path_separator = "/" in executable or "\\" in executable
    if not has_path_separator:
        if shutil.which(executable):
            return _command_file_argument_issue(command, cwd=cwd)
        return f"executable {executable!r} not found on PATH"

    issue = _existing_executable_path_issue(executable, cwd=cwd)
    if issue is not None:
        return issue
    return _command_file_argument_issue(command, cwd=cwd)


def _existing_executable_path_issue(executable: str, *, cwd: str | None = None) -> str | None:
    path = _resolve_command_path(executable, cwd=cwd)
    if not path.exists():
        return f"executable path does not exist: {path}"
    if not path.is_file():
        return f"executable path is not a file: {path}"
    if not os.access(path, os.X_OK):
        return f"executable path is not executable: {path}"
    return None


def _command_file_argument_issue(command: list[str], *, cwd: str | None = None) -> str | None:
    if not command:
        return None
    executable_name = Path(command[0]).name.lower()
    if executable_name not in _SCRIPT_LAUNCHERS:
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
    if not path.is_absolute():
        base = Path(cwd).expanduser() if cwd else Path.cwd()
        path = base / path
    return path.resolve()


def _validate_json_contract(model: type[BaseModel], source: str) -> BaseModel:
    raw = _read_json_source(source)
    try:
        return model.model_validate(raw)
    except Exception as exc:
        raise ValueError(f"{model.__name__} validation failed: {exc}") from exc


def _read_json_source(source: str) -> Any:
    text = sys.stdin.read() if source == "-" else Path(source).read_text(encoding="utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {source!r}: {exc}") from exc


def _scaffold_workflow_package(
    *,
    output_dir: Path,
    package_id: str,
    pipeline_id: str,
    steps: list[str],
    adapter_kind: str,
    title: str | None = None,
) -> dict[str, Any]:
    package = WorkflowPackageSpec(
        id=package_id,
        title=title or _title_from_id(package_id),
        version="1",
        pipelines=[f"{pipeline_id}.yaml"],
        workers=[
            WorkflowWorkerSpec(
                id=f"{step_id}_worker",
                title=f"{_title_from_id(step_id)} worker",
                pipeline_id=pipeline_id,
                process_id=step_id,
                command=["python", f"steps/{step_id}.py"],
                cwd=".",
            )
            for step_id in steps
        ]
        if adapter_kind == "queue"
        else [],
    )
    pipeline = PipelineSpec(
        id=pipeline_id,
        title=title or _title_from_id(pipeline_id),
        steps=[
            ProcessSpec(
                id=step_id,
                needs=[steps[index - 1]] if index > 0 else [],
                adapter=_scaffold_adapter(package_id, step_id, adapter_kind),
            )
            for index, step_id in enumerate(steps)
        ],
        combines=[
            CombineSpec(
                id="workflow_result",
                needs=steps,
                emit_partial=True,
            )
        ],
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    steps_dir = output_dir / "steps"
    steps_dir.mkdir(parents=True, exist_ok=True)

    created = [
        _write_new_file(
            output_dir / "process-runtime-package.yaml",
            _package_yaml(package),
        ),
        _write_new_file(
            output_dir / f"{pipeline_id}.yaml",
            _pipeline_yaml(pipeline),
        ),
    ]
    for step_id in steps:
        created.append(
            _write_new_file(
                steps_dir / f"{step_id}.py",
                _step_program_source(step_id),
            )
        )

    return {
        "ok": True,
        "package_id": package_id,
        "pipeline_id": pipeline_id,
        "adapter_kind": adapter_kind,
        "step_ids": steps,
        "created": [str(path) for path in created],
    }


def _scaffold_adapter(package_id: str, step_id: str, adapter_kind: str) -> dict[str, Any]:
    if adapter_kind == "subprocess":
        return {
            "kind": "subprocess",
            "command": ["python", f"steps/{step_id}.py"],
            "cwd": ".",
        }
    if adapter_kind == "queue":
        return {
            "kind": "queue",
            "queue": f"{package_id}.{step_id}",
        }
    raise ValueError(f"Unsupported scaffold adapter kind: {adapter_kind}")


def _parse_scaffold_steps(value: str) -> list[str]:
    steps = [part.strip() for part in value.split(",") if part.strip()]
    if not steps:
        raise ValueError("--steps must include at least one process id")
    if len(set(steps)) != len(steps):
        raise ValueError("--steps must not contain duplicate process ids")
    for step in steps:
        ProcessSpec(id=step, adapter={"kind": "queue", "queue": "validate.id"})
    return steps


def _write_new_file(path: Path, text: str) -> Path:
    if path.exists():
        raise ValueError(f"Refusing to overwrite existing file: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _package_yaml(package: WorkflowPackageSpec) -> str:
    text = (
        f"package: {package.id}\n"
        f"title: {_yaml_string(package.title or _title_from_id(package.id))}\n"
        f"version: {_yaml_string(package.version)}\n"
        "pipelines:\n"
        + "".join(f"  - {pipeline}\n" for pipeline in package.pipelines)
    )
    if package.workers:
        text += "workers:\n"
        for worker in package.workers:
            text += (
                f"  - id: {worker.id}\n"
                f"    title: {_yaml_string(worker.title or _title_from_id(worker.id))}\n"
                f"    pipeline: {worker.pipeline_id}\n"
            )
            if worker.process_id:
                text += f"    process: {worker.process_id}\n"
            text += (
                f"    adapter_kind: {worker.adapter_kind}\n"
                f"    command: {_json_list(worker.command)}\n"
            )
            if worker.cwd is not None:
                text += f"    cwd: {_yaml_string(worker.cwd)}\n"
            if worker.env:
                text += "    env:\n"
                for key, value in sorted(worker.env.items()):
                    text += f"      {key}: {_yaml_string(value)}\n"
            if worker.timeout_seconds is not None:
                text += f"    timeout_seconds: {worker.timeout_seconds}\n"
    return text


def _pipeline_yaml(pipeline: PipelineSpec) -> str:
    lines = [
        f"pipeline: {pipeline.id}",
        f"title: {_yaml_string(pipeline.title or _title_from_id(pipeline.id))}",
        "steps:",
    ]
    for step in pipeline.steps:
        lines.extend(
            [
                f"  - id: {step.id}",
            ]
        )
        if step.needs:
            lines.append(
                "    needs: ["
                + ", ".join(_yaml_string(need) for need in step.needs)
                + "]"
            )
        lines.extend(
            [
                "    adapter:",
                f"      kind: {step.adapter.kind}",
            ]
        )
        if step.adapter.kind == "subprocess":
            lines.extend(
                [
                    f"      command: [\"python\", \"steps/{step.id}.py\"]",
                    "      cwd: \".\"",
                ]
            )
        elif step.adapter.kind == "queue":
            lines.append(f"      queue: {_yaml_string(step.adapter.queue or '')}")
    lines.extend(
        [
            "combines:",
            "  - id: workflow_result",
            "    mode: latest",
            "    emit_partial: true",
            "    needs: ["
            + ", ".join(_yaml_string(step.id) for step in pipeline.steps)
            + "]",
        ]
    )
    return "\n".join(lines) + "\n"


def _step_program_source(step_id: str) -> str:
    return f'''from __future__ import annotations

from fala.sdk import emit_event, output, run_stdio

PROCESS_ID = {step_id!r}


def run(context):
    emit_event(
        "process.progress",
        status="running",
        data={{"process_id": PROCESS_ID, "stage": "started"}},
    )
    return output(
        values={{
            "status": "ok",
            "process_id": PROCESS_ID,
            "document_id": context.get("document_id"),
        }}
    )


if __name__ == "__main__":
    raise SystemExit(run_stdio(run))
'''


def _title_from_id(value: str) -> str:
    return value.replace("_", " ").replace("-", " ").strip().title() or value


def _yaml_string(value: str) -> str:
    return json.dumps(value)


def _json_list(values: list[str]) -> str:
    return "[" + ", ".join(_yaml_string(value) for value in values) + "]"


def _step_by_id(pipeline: PipelineSpec, process_id: str) -> ProcessSpec:
    for step in pipeline.steps:
        if step.id == process_id:
            return step
    raise ValueError(f"Unknown process id {process_id!r} for pipeline {pipeline.id!r}")


async def _refresh_projections_for_process(
    *,
    store: SQLiteStateStore,
    pipeline: PipelineSpec,
    run_id: str,
    document_id: str,
    process_id: str,
) -> list[CombinedProjection]:
    refreshed: list[CombinedProjection] = []
    for combine in pipeline.combines:
        if process_id not in combine.needs:
            continue

        latest = await store.get_outputs(
            run_id=run_id,
            document_id=document_id,
            process_ids=combine.needs,
        )
        complete = set(latest) == set(combine.needs)
        if not complete and not combine.emit_partial:
            continue

        projection = CombinedProjection(
            id=combine.id,
            run_id=run_id,
            document_id=document_id,
            complete=complete,
            latest=latest,
        )
        await store.put_projection(projection)
        await store.append_event(
            ProcessEvent(
                run_id=run_id,
                document_id=document_id,
                process_id=None,
                type="projection.updated",
                data={
                    "projection_id": combine.id,
                    "complete": complete,
                    "process_ids": sorted(latest),
                },
            )
        )
        refreshed.append(projection)
    return refreshed


def _print_plain_validation(payload: dict[str, Any]) -> None:
    print(
        f"ok: {payload['pipeline_count']} pipeline(s), "
        f"{payload.get('package_count', 0)} package(s)"
    )
    for pipeline in payload["pipelines"]:
        print(
            f"- {pipeline['id']} v{pipeline['version']}: "
            f"{len(pipeline['steps'])} step(s), {len(pipeline['combines'])} combine(s)"
        )
    return None


def _parse_values(items: list[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for item in items:
        key, sep, value = item.partition("=")
        if not sep or not key:
            raise ValueError(f"Invalid --value {item!r}; expected key=value")
        values[key] = value
    return values


def _parse_artifacts(items: list[str]) -> list[ArtifactRef]:
    artifacts: list[ArtifactRef] = []
    for item in items:
        kind, sep, uri = item.partition("=")
        if not sep or not kind or not uri:
            raise ValueError(f"Invalid --artifact {item!r}; expected kind=uri")
        artifacts.append(ArtifactRef(kind=kind, uri=uri))
    return artifacts


if __name__ == "__main__":
    raise SystemExit(main())
