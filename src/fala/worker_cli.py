from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any

from fala.adapters import AdapterRegistry, ExternalCommandAdapter
from fala.client import ProcessRuntimeClient
from fala.models import ResourceSpec
from fala.registry import PipelineRegistry
from fala.worker import AdapterProcessRuntimeWorker

ADAPTER_KIND_CHOICES = ("subprocess", "http", "queue")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        payload = asyncio.run(_run(args))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2, sort_keys=True))
        return 1
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="process-runtime-worker")
    parser.add_argument(
        "--pipeline-dir",
        default=None,
        help="Directory with workflow package manifests.",
    )
    parser.add_argument(
        "--package-id",
        default=None,
        help="Workflow package id for --package-worker disambiguation.",
    )
    parser.add_argument(
        "--package-worker",
        default=None,
        help="Worker id declared in process-runtime-package.yaml.",
    )
    parser.add_argument("--base-url", required=True, help="Control plane base URL.")
    parser.add_argument(
        "--api-key",
        default=None,
        help="Fala API key. Defaults to FALA_API_KEY.",
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--pipeline", default=None, help="Pipeline id.")
    parser.add_argument("--worker-id", default=None)
    parser.add_argument("--process-id", default=None)
    parser.add_argument("--adapter-kind", choices=ADAPTER_KIND_CHOICES, default=None)
    parser.add_argument(
        "--capability",
        action="append",
        default=[],
        help="Capability id this worker can claim. Repeatable.",
    )
    parser.add_argument(
        "--command",
        nargs=argparse.REMAINDER,
        default=None,
        help=(
            "Worker-local command for queue adapter claims. "
            "Must be the final option. Receives ProcessExecutionContext JSON on stdin "
            "and returns ProcessOutput JSON on stdout."
        ),
    )
    parser.add_argument("--cwd", default=None, help="Working directory for --command.")
    parser.add_argument(
        "--env",
        action="append",
        default=[],
        help="Environment override for --command as KEY=VALUE. Repeatable.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=None,
        help="Timeout for --command execution.",
    )
    parser.add_argument("--lease-seconds", type=float, default=300.0)
    parser.add_argument(
        "--error-kind",
        default="worker_error",
        help=(
            "Error kind sent to the runtime when this worker command fails. "
            "Pipeline retry policy can mark kinds retryable or terminal."
        ),
    )
    _add_resource_args(parser)
    parser.add_argument("--max-steps", type=int, default=1000)
    parser.add_argument("--idle-sleep", type=float, default=2.0)
    parser.add_argument(
        "--forever",
        action="store_true",
        help="Keep polling when no matching process is queued.",
    )
    parser.add_argument(
        "--max-idle-polls",
        type=int,
        default=1,
        help="Stop after this many idle polls unless --forever is set.",
    )
    return parser


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    if args.max_steps < 1:
        raise ValueError("--max-steps must be greater than zero")
    if args.max_idle_polls < 1:
        raise ValueError("--max-idle-polls must be greater than zero")
    if args.idle_sleep < 0:
        raise ValueError("--idle-sleep must be non-negative")
    resolved = _resolve_worker_config(args)
    command = resolved["command"]
    adapter_kind = resolved["adapter_kind"]
    if command and adapter_kind != "queue":
        raise ValueError("--command is only supported with --adapter-kind queue")
    if adapter_kind == "queue" and not command:
        raise ValueError("--adapter-kind queue requires --command or --package-worker")

    async with ProcessRuntimeClient(
        args.base_url,
        api_key=args.api_key or os.environ.get("FALA_API_KEY"),
    ) as client:
        adapters = AdapterRegistry.default()
        if command:
            adapters.register(
                adapter_kind,
                ExternalCommandAdapter(
                    command=command or [],
                    cwd=resolved["cwd"],
                    env=resolved["env"],
                    timeout_seconds=resolved["timeout_seconds"],
                ),
            )
        worker = AdapterProcessRuntimeWorker(
            client=client,
            pipeline_id=resolved["pipeline"],
            worker_id=resolved["worker_id"],
            adapter_kind=adapter_kind,
            capabilities=resolved["capabilities"],
            resources=resolved["resources"],
            adapters=adapters,
            lease_seconds=args.lease_seconds,
            error_kind=args.error_kind,
        )
        steps: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        idle_polls = 0

        while len(steps) < args.max_steps:
            result = await worker.run_once(
                run_id=args.run_id,
                process_id=resolved["process_id"],
            )
            if result.claimed is None:
                idle_polls += 1
                if not args.forever and idle_polls >= args.max_idle_polls:
                    break
                await asyncio.sleep(args.idle_sleep)
                continue

            idle_polls = 0
            claimed = result.claimed
            item = {
                "document_id": claimed.document_id,
                "process_id": claimed.process.id,
                "capability": claimed.process.capability,
                "attempt": claimed.attempt,
                "outcome": result.outcome.value,
                "completed": result.completed,
                "error": result.error,
                "error_kind": result.error_kind,
            }
            steps.append(item)
            if result.error:
                errors.append(item)
                break

        return {
            "ok": not errors,
            "run_id": args.run_id,
            "pipeline_id": resolved["pipeline"],
            "worker_id": resolved["worker_id"],
            "adapter_kind": adapter_kind,
            "capabilities": resolved["capabilities"],
            "resources": resolved["resources"].model_dump(mode="json"),
            "process_id": resolved["process_id"],
            "package_worker": args.package_worker,
            "error_kind": args.error_kind,
            "completed_count": sum(1 for item in steps if item["completed"]),
            "error_count": len(errors),
            "idle_polls": idle_polls,
            "steps": steps,
        }


def _resolve_worker_config(args: argparse.Namespace) -> dict[str, Any]:
    command = _parse_command(args.command)
    env = _parse_env(args.env)
    resource_overrides = _parse_resource_overrides(args)
    if args.package_worker is None:
        pipeline = args.pipeline
        worker_id = args.worker_id
        adapter_kind = args.adapter_kind
        if not pipeline:
            raise ValueError("--pipeline is required unless --package-worker is used")
        if not worker_id:
            raise ValueError("--worker-id is required unless --package-worker is used")
        if not adapter_kind:
            raise ValueError("--adapter-kind is required unless --package-worker is used")
        return {
            "pipeline": pipeline,
            "worker_id": worker_id,
            "process_id": args.process_id,
            "adapter_kind": adapter_kind,
            "capabilities": list(args.capability),
            "resources": resource_overrides,
            "command": command,
            "cwd": args.cwd,
            "env": env,
            "timeout_seconds": args.timeout_seconds,
        }

    if command:
        raise ValueError("--command cannot be combined with --package-worker")
    registry = PipelineRegistry.from_directory(_pipeline_dir(args))
    worker = registry.package_worker(args.package_worker, package_id=args.package_id)

    if args.pipeline is not None and args.pipeline != worker.pipeline_id:
        raise ValueError(
            f"--pipeline {args.pipeline!r} conflicts with package worker "
            f"pipeline {worker.pipeline_id!r}"
        )
    if args.process_id is not None and args.process_id != worker.process_id:
        raise ValueError(
            f"--process-id {args.process_id!r} conflicts with package worker "
            f"process {worker.process_id!r}"
        )
    if args.adapter_kind is not None and args.adapter_kind != worker.adapter_kind:
        raise ValueError(
            f"--adapter-kind {args.adapter_kind!r} conflicts with package worker "
            f"adapter_kind {worker.adapter_kind!r}"
        )
    capabilities = list(args.capability or worker.capabilities)
    if args.capability and worker.capabilities:
        unknown = sorted(set(args.capability) - set(worker.capabilities))
        if unknown:
            raise ValueError(
                f"--capability {unknown[0]!r} is not declared by package worker "
                f"{worker.id!r}"
            )

    resources = _merge_resources(worker.resources, resource_overrides)
    return {
        "pipeline": worker.pipeline_id,
        "worker_id": args.worker_id or worker.id,
        "process_id": worker.process_id,
        "adapter_kind": worker.adapter_kind,
        "capabilities": capabilities,
        "resources": resources,
        "command": worker.command,
        "cwd": args.cwd if args.cwd is not None else worker.cwd,
        "env": {**worker.env, **env},
        "timeout_seconds": (
            args.timeout_seconds
            if args.timeout_seconds is not None
            else worker.timeout_seconds
        ),
    }


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


def _add_resource_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--cpu-cores", type=float, default=None)
    parser.add_argument("--memory-mb", type=int, default=None)
    parser.add_argument("--disk-mb", type=int, default=None)
    parser.add_argument("--gpu-count", type=int, default=None)
    parser.add_argument(
        "--resource-label",
        action="append",
        default=[],
        help="Resource label provided by this worker. Repeatable.",
    )
    parser.add_argument(
        "--resource-unit",
        action="append",
        default=[],
        help="Named resource capacity as KEY=VALUE. Repeatable.",
    )


def _parse_resource_overrides(args: argparse.Namespace) -> ResourceSpec:
    return ResourceSpec(
        cpu_cores=args.cpu_cores,
        memory_mb=args.memory_mb,
        disk_mb=args.disk_mb,
        gpu_count=args.gpu_count,
        labels=list(args.resource_label),
        units=_parse_resource_units(args.resource_unit),
    )


def _parse_resource_units(values: list[str]) -> dict[str, float]:
    units: dict[str, float] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"--resource-unit must be KEY=VALUE, got {value!r}")
        key, item = value.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"--resource-unit key cannot be empty: {value!r}")
        try:
            units[key] = float(item)
        except ValueError as exc:
            raise ValueError(
                f"--resource-unit value must be numeric for {key!r}: {item!r}"
            ) from exc
    return units


def _merge_resources(base: ResourceSpec, override: ResourceSpec) -> ResourceSpec:
    labels = list(dict.fromkeys([*base.labels, *override.labels]))
    return ResourceSpec(
        cpu_cores=override.cpu_cores if override.cpu_cores is not None else base.cpu_cores,
        memory_mb=override.memory_mb if override.memory_mb is not None else base.memory_mb,
        disk_mb=override.disk_mb if override.disk_mb is not None else base.disk_mb,
        gpu_count=override.gpu_count if override.gpu_count is not None else base.gpu_count,
        labels=labels,
        units={**base.units, **override.units},
    )


def _parse_command(value: list[str] | None) -> list[str] | None:
    command = list(value or [])
    if command and command[0] == "--":
        command = command[1:]
    return command or None


def _parse_env(values: list[str]) -> dict[str, str]:
    env: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"--env must be KEY=VALUE, got {value!r}")
        key, item = value.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"--env key cannot be empty: {value!r}")
        env[key] = item
    return env


if __name__ == "__main__":
    raise SystemExit(main())
