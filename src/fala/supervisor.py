from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from fala.models import ResourceSpec, WorkerSandboxSpec, WorkflowSecretSpec
from fala.registry import PipelineRegistry

RestartPolicy = Literal["never", "on-failure", "always"]
WorkerStatus = Literal["pending", "running", "restarting", "exited", "failed", "stopped"]


class SupervisedWorkerSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    package_id: str | None = None
    pipeline_id: str | None = None
    process_id: str | None = None
    capabilities: list[str] = Field(default_factory=list)
    argv: list[str] = Field(min_length=1)
    cwd: str | None = None
    env: dict[str, str] = Field(default_factory=dict)
    resources: ResourceSpec = Field(default_factory=ResourceSpec)
    secrets: list[WorkflowSecretSpec] = Field(default_factory=list)
    sandbox: WorkerSandboxSpec = Field(default_factory=WorkerSandboxSpec)


class SupervisedWorkerState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    package_id: str | None = None
    pipeline_id: str | None = None
    process_id: str | None = None
    capabilities: list[str] = Field(default_factory=list)
    argv: list[str] = Field(default_factory=list)
    status: WorkerStatus = "pending"
    pid: int | None = None
    starts: int = Field(default=0, ge=0)
    restarts: int = Field(default=0, ge=0)
    exit_code: int | None = None
    started_at: datetime | None = None
    exited_at: datetime | None = None
    error: str | None = None


class SupervisorResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool
    worker_count: int = Field(ge=0)
    restart_policy: RestartPolicy
    max_restarts: int = Field(ge=0)
    workers: list[SupervisedWorkerState]


def build_package_worker_specs(
    *,
    registry: PipelineRegistry,
    pipeline_dir: str | Path,
    base_url: str,
    run_id: str,
    package_id: str | None = None,
    worker_ids: list[str] | None = None,
    worker_executable: str = "process-runtime-worker",
    worker_forever: bool = True,
    lease_seconds: float = 300.0,
    idle_sleep: float = 2.0,
    max_steps: int = 1000,
    max_idle_polls: int = 1,
) -> list[SupervisedWorkerSpec]:
    requested_workers = set(worker_ids or [])
    packages = [registry.package(package_id)] if package_id is not None else registry.packages()
    specs: list[SupervisedWorkerSpec] = []
    for package in packages:
        secret_by_id = {secret.id: secret for secret in package.secrets}
        for worker in package.workers:
            if requested_workers and worker.id not in requested_workers:
                continue
            argv = [
                worker_executable,
                "--pipeline-dir",
                str(pipeline_dir),
                "--base-url",
                base_url,
                "--run-id",
                run_id,
                "--package-id",
                package.id,
                "--package-worker",
                worker.id,
                "--lease-seconds",
                str(lease_seconds),
                "--idle-sleep",
                str(idle_sleep),
                "--max-steps",
                str(max_steps),
                "--max-idle-polls",
                str(max_idle_polls),
            ]
            if worker_forever:
                argv.append("--forever")
            specs.append(
                SupervisedWorkerSpec(
                    id=worker.id,
                    package_id=package.id,
                    pipeline_id=worker.pipeline_id,
                    process_id=worker.process_id,
                    capabilities=worker.capabilities,
                    argv=argv,
                    cwd=worker.cwd,
                    env=worker.env,
                    resources=worker.resources,
                    secrets=[secret_by_id[secret_id] for secret_id in worker.secrets],
                    sandbox=worker.sandbox,
                )
            )
    unknown_workers = requested_workers - {spec.id for spec in specs}
    if unknown_workers:
        raise ValueError(f"Unknown package worker id(s): {', '.join(sorted(unknown_workers))}")
    return specs


class ProcessSupervisor:
    def __init__(
        self,
        workers: list[SupervisedWorkerSpec],
        *,
        restart_policy: RestartPolicy = "on-failure",
        max_restarts: int = 5,
        restart_delay_seconds: float = 1.0,
        stop_timeout_seconds: float = 10.0,
    ) -> None:
        if max_restarts < 0:
            raise ValueError("max_restarts must be non-negative")
        if restart_delay_seconds < 0:
            raise ValueError("restart_delay_seconds must be non-negative")
        if stop_timeout_seconds <= 0:
            raise ValueError("stop_timeout_seconds must be greater than zero")
        self.workers = workers
        self.restart_policy = restart_policy
        self.max_restarts = max_restarts
        self.restart_delay_seconds = restart_delay_seconds
        self.stop_timeout_seconds = stop_timeout_seconds
        self._stop = asyncio.Event()
        self._processes: dict[str, asyncio.subprocess.Process] = {}
        self._states = {
            worker.id: SupervisedWorkerState(
                id=worker.id,
                package_id=worker.package_id,
                pipeline_id=worker.pipeline_id,
                process_id=worker.process_id,
                capabilities=worker.capabilities,
                argv=worker.argv,
            )
            for worker in workers
        }

    async def run(self, *, max_runtime_seconds: float | None = None) -> SupervisorResult:
        tasks = [
            asyncio.create_task(self._run_worker(worker), name=f"fala-supervisor:{worker.id}")
            for worker in self.workers
        ]
        try:
            if max_runtime_seconds is not None:
                if max_runtime_seconds < 0:
                    raise ValueError("max_runtime_seconds must be non-negative")
                await asyncio.sleep(max_runtime_seconds)
                self._stop.set()
                await self._terminate_running()
            if tasks:
                await asyncio.gather(*tasks)
        except BaseException:
            self._stop.set()
            await self._terminate_running()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        return self.result()

    def result(self) -> SupervisorResult:
        workers = list(self._states.values())
        return SupervisorResult(
            ok=all(
                worker.status == "stopped"
                or (worker.status == "exited" and worker.exit_code == 0)
                for worker in workers
            ),
            worker_count=len(workers),
            restart_policy=self.restart_policy,
            max_restarts=self.max_restarts,
            workers=workers,
        )

    async def _run_worker(self, worker: SupervisedWorkerSpec) -> None:
        state = self._states[worker.id]
        while not self._stop.is_set():
            try:
                process = await asyncio.create_subprocess_exec(
                    *worker.argv,
                    cwd=worker.cwd,
                    env={**os.environ, **worker.env} if worker.env else None,
                )
            except Exception as exc:
                state.status = "failed"
                state.error = str(exc)
                state.exited_at = _now()
                return

            self._processes[worker.id] = process
            state.status = "running"
            state.pid = process.pid
            state.starts += 1
            state.exit_code = None
            state.started_at = _now()
            state.exited_at = None
            state.error = None

            exit_code = await process.wait()
            self._processes.pop(worker.id, None)
            state.pid = None
            state.exit_code = exit_code
            state.exited_at = _now()

            if self._stop.is_set():
                state.status = "stopped"
                return
            state.status = "exited" if exit_code == 0 else "failed"
            if not self._should_restart(exit_code=exit_code, restarts=state.restarts):
                return
            state.restarts += 1
            state.status = "restarting"
            await asyncio.sleep(self.restart_delay_seconds)

    def _should_restart(self, *, exit_code: int, restarts: int) -> bool:
        if restarts >= self.max_restarts:
            return False
        if self.restart_policy == "always":
            return True
        if self.restart_policy == "on-failure":
            return exit_code != 0
        return False

    async def _terminate_running(self) -> None:
        processes = list(self._processes.items())
        for _worker_id, process in processes:
            if process.returncode is None:
                process.terminate()
        if not processes:
            return
        try:
            await asyncio.wait_for(
                asyncio.gather(
                    *(process.wait() for _worker_id, process in processes),
                    return_exceptions=True,
                ),
                timeout=self.stop_timeout_seconds,
            )
        except asyncio.TimeoutError:
            for _worker_id, process in processes:
                if process.returncode is None:
                    process.kill()
            await asyncio.gather(
                *(process.wait() for _worker_id, process in processes),
                return_exceptions=True,
            )


def _now() -> datetime:
    return datetime.now(timezone.utc)
