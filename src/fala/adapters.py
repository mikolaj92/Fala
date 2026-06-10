from __future__ import annotations

import asyncio
import inspect
import json
import os
import re
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

import httpx
from fala.sdk import PROCESS_RUNTIME_EVENT_PREFIX

from fala.models import (
    AdapterSpec,
    ProcessEvent,
    ProcessExecutionContext,
    ProcessOutput,
    ProcessSpec,
    ProcessStatus,
)

SUBPROCESS_EVENT_PREFIX = PROCESS_RUNTIME_EVENT_PREFIX
EventSink = Callable[[ProcessEvent], Any]


class ProcessAdapterError(RuntimeError):
    pass


class ProcessAdapter(Protocol):
    async def run(
        self,
        spec: ProcessSpec,
        context: ProcessExecutionContext,
        *,
        event_sink: EventSink | None = None,
    ) -> ProcessOutput:
        ...


class SubprocessAdapter:
    async def run(
        self,
        spec: ProcessSpec,
        context: ProcessExecutionContext,
        *,
        event_sink: EventSink | None = None,
    ) -> ProcessOutput:
        command = spec.adapter.command
        if not command:
            raise ProcessAdapterError(f"Process {spec.id!r} missing subprocess command")

        env = _subprocess_env(spec, context)
        payload = context.model_dump_json().encode("utf-8")
        started = time.monotonic()
        proc = await asyncio.create_subprocess_exec(
            *command,
            cwd=spec.adapter.cwd,
            env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            stdout, stderr_text, event_count = await asyncio.wait_for(
                _communicate_with_event_stream(
                    proc,
                    payload=payload,
                    context=context,
                    event_sink=event_sink,
                ),
                timeout=spec.adapter.timeout_seconds or spec.timeout_seconds,
            )
        except asyncio.TimeoutError as exc:
            proc.kill()
            await proc.wait()
            raise ProcessAdapterError(f"Process {spec.id!r} subprocess timed out") from exc

        if proc.returncode != 0:
            message = stderr.decode("utf-8", errors="replace").strip()
            raise ProcessAdapterError(
                f"Process {spec.id!r} subprocess failed with exit {proc.returncode}: {message}"
            )

        text = stdout.decode("utf-8", errors="replace")
        decoded = _decode_subprocess_json(text, spec.id)
        output = _normalize_output(decoded, spec.id)

        runtime_metadata: dict[str, Any] = {
            "adapter_kind": "subprocess",
            "duration_seconds": round(time.monotonic() - started, 6),
            "exit_code": proc.returncode,
        }
        if event_count:
            runtime_metadata["event_count"] = event_count
        if stderr_text:
            runtime_metadata["stderr_tail"] = stderr_text[-4000:]
        return _with_runtime_metadata(output, runtime_metadata)


class HTTPProcessAdapter:
    async def run(
        self,
        spec: ProcessSpec,
        context: ProcessExecutionContext,
        *,
        event_sink: EventSink | None = None,
    ) -> ProcessOutput:
        url = spec.adapter.url
        if not url:
            raise ProcessAdapterError(f"Process {spec.id!r} missing http adapter url")

        timeout = spec.adapter.timeout_seconds or spec.timeout_seconds
        started = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(url, json=context.model_dump(mode="json"))
                response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise ProcessAdapterError(
                f"Process {spec.id!r} http adapter failed with status "
                f"{exc.response.status_code}: {exc.response.text}"
            ) from exc
        except httpx.HTTPError as exc:
            raise ProcessAdapterError(f"Process {spec.id!r} http adapter failed: {exc}") from exc

        try:
            decoded = response.json()
        except ValueError as exc:
            raise ProcessAdapterError(
                f"Process {spec.id!r} http adapter returned non-JSON response"
            ) from exc
        return _with_runtime_metadata(
            _normalize_output(decoded, spec.id),
            {
                "adapter_kind": "http",
                "duration_seconds": round(time.monotonic() - started, 6),
                "http_status": response.status_code,
            },
        )


class QueueProcessAdapter:
    """Broker-agnostic slot for queue-backed processes.

    Queue steps are executed by external workers that claim and write output
    through the process-runtime API. The control plane never calls business
    handlers in-process for queue steps.
    """

    async def run(
        self,
        spec: ProcessSpec,
        context: ProcessExecutionContext,
        *,
        event_sink: EventSink | None = None,
    ) -> ProcessOutput:
        queue = spec.adapter.queue
        if not queue:
            raise ProcessAdapterError(f"Process {spec.id!r} missing queue adapter queue")
        raise ProcessAdapterError(
            f"Process {spec.id!r} queue {queue!r} cannot run in-process; "
            "use process-runtime-worker --adapter-kind queue --command or an external worker"
        )


class ExternalCommandAdapter:
    """Run a claimed process through a worker-local command.

    This is useful for queue-backed processes: the control plane owns the claim
    and observability, while a remote worker process owns execution.
    """

    def __init__(
        self,
        *,
        command: list[str],
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_seconds: float | None = None,
    ) -> None:
        if not command:
            raise ValueError("External command adapter requires a command")
        self.command = list(command)
        self.cwd = cwd
        self.env = dict(env or {})
        self.timeout_seconds = timeout_seconds
        self._subprocess = SubprocessAdapter()

    async def run(
        self,
        spec: ProcessSpec,
        context: ProcessExecutionContext,
        *,
        event_sink: EventSink | None = None,
    ) -> ProcessOutput:
        claimed_adapter_kind = spec.adapter.kind
        command_spec = spec.model_copy(
            update={
                "adapter": AdapterSpec(
                    kind="subprocess",
                    command=self.command,
                    cwd=self.cwd,
                    env=self.env,
                    timeout_seconds=(
                        self.timeout_seconds
                        or spec.adapter.timeout_seconds
                        or spec.timeout_seconds
                    ),
                )
            }
        )
        output = await self._subprocess.run(
            command_spec,
            context,
            event_sink=event_sink,
        )
        runtime_metadata = dict(output.metadata.get("process_runtime") or {})
        runtime_metadata.update(
            {
                "execution_adapter_kind": "external_command",
                "claimed_adapter_kind": claimed_adapter_kind,
            }
        )
        metadata = dict(output.metadata)
        metadata["process_runtime"] = runtime_metadata
        return output.model_copy(update={"metadata": metadata})


class AdapterRegistry:
    def __init__(self, adapters: dict[str, ProcessAdapter] | None = None) -> None:
        self._adapters = dict(adapters or {})

    @classmethod
    def default(cls) -> "AdapterRegistry":
        return cls(
            {
                "subprocess": SubprocessAdapter(),
                "http": HTTPProcessAdapter(),
                "queue": QueueProcessAdapter(),
            }
        )

    def register(self, kind: str, adapter: ProcessAdapter) -> None:
        self._adapters[kind] = adapter

    def adapter(self, kind: str) -> ProcessAdapter:
        try:
            return self._adapters[kind]
        except KeyError as exc:
            raise ProcessAdapterError(f"No process adapter registered for kind {kind!r}") from exc

    async def run(
        self,
        spec: ProcessSpec,
        context: ProcessExecutionContext,
        *,
        event_sink: EventSink | None = None,
    ) -> ProcessOutput:
        return await self.adapter(spec.adapter.kind).run(
            spec,
            context,
            event_sink=event_sink,
        )


async def _communicate_with_event_stream(
    proc: asyncio.subprocess.Process,
    *,
    payload: bytes,
    context: ProcessExecutionContext,
    event_sink: EventSink | None,
) -> tuple[bytes, str, int]:
    stdout_task = asyncio.create_task(_read_stdout(proc))
    stderr_task = asyncio.create_task(
        _read_stderr_events(proc, context=context, event_sink=event_sink)
    )
    write_task = asyncio.create_task(_write_stdin(proc, payload))

    try:
        await proc.wait()
        await write_task
        stdout = await stdout_task
        stderr_text, event_count = await stderr_task
        return stdout, stderr_text, event_count
    except BaseException:
        for task in (stdout_task, stderr_task, write_task):
            task.cancel()
        await asyncio.gather(stdout_task, stderr_task, write_task, return_exceptions=True)
        raise


async def _write_stdin(proc: asyncio.subprocess.Process, payload: bytes) -> None:
    if proc.stdin is None:
        return
    try:
        proc.stdin.write(payload)
        await proc.stdin.drain()
    except (BrokenPipeError, ConnectionResetError):
        return
    finally:
        proc.stdin.close()


async def _read_stdout(proc: asyncio.subprocess.Process) -> bytes:
    if proc.stdout is None:
        return b""
    return await proc.stdout.read()


async def _read_stderr_events(
    proc: asyncio.subprocess.Process,
    *,
    context: ProcessExecutionContext,
    event_sink: EventSink | None,
) -> tuple[str, int]:
    if proc.stderr is None:
        return "", 0

    stderr_lines: list[str] = []
    event_count = 0
    while line := await proc.stderr.readline():
        text = line.decode("utf-8", errors="replace").rstrip("\r\n")
        event = _event_from_stderr_line(text, context)
        if event is None:
            stderr_lines.append(text)
            continue
        event_count += 1
        if event_sink is not None:
            result = event_sink(event)
            if inspect.isawaitable(result):
                await result
    return "\n".join(line for line in stderr_lines if line).strip(), event_count


def _event_from_stderr_line(
    text: str,
    context: ProcessExecutionContext,
) -> ProcessEvent | None:
    if not text.startswith(SUBPROCESS_EVENT_PREFIX):
        return None
    raw = text[len(SUBPROCESS_EVENT_PREFIX):].strip()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    event_type = payload.get("type")
    if not isinstance(event_type, str) or not event_type:
        return None
    data = payload.get("data") or {}
    if not isinstance(data, dict):
        data = {"value": data}
    status = payload.get("status")
    try:
        parsed_status = ProcessStatus(status) if status else None
    except ValueError:
        return None
    return ProcessEvent(
        run_id=context.run_id,
        document_id=context.document_id,
        process_id=context.process_id,
        type=event_type,
        status=parsed_status,
        data=data,
    )


def _subprocess_env(spec: ProcessSpec, context: ProcessExecutionContext) -> dict[str, str]:
    env = os.environ.copy()
    env.update(spec.adapter.env)
    env.update(_runtime_env(spec, context, env))
    return env


def _runtime_env(
    spec: ProcessSpec,
    context: ProcessExecutionContext,
    env: dict[str, str],
) -> dict[str, str]:
    artifact_dir = _process_artifact_dir(spec, context, env)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    return {
        "PROCESS_RUNTIME_PIPELINE_ID": context.pipeline_id,
        "PROCESS_RUNTIME_RUN_ID": context.run_id,
        "PROCESS_RUNTIME_DOCUMENT_ID": context.document_id,
        "PROCESS_RUNTIME_PROCESS_ID": context.process_id,
        "PROCESS_RUNTIME_ATTEMPT": str(context.attempt),
        "PROCESS_RUNTIME_ARTIFACT_DIR": str(artifact_dir),
    }


def _process_artifact_dir(
    spec: ProcessSpec,
    context: ProcessExecutionContext,
    env: dict[str, str],
) -> Path:
    configured_root = (
        env.get("PROCESS_RUNTIME_ARTIFACT_ROOT")
        or ".flow-runs/process-artifacts"
    )
    root = Path(configured_root).expanduser()
    if not root.is_absolute():
        base = Path(spec.adapter.cwd).expanduser() if spec.adapter.cwd else Path.cwd()
        root = base / root
    return (
        root
        / _slug(context.run_id)
        / _slug(context.document_id)
        / _slug(context.process_id)
        / f"attempt-{context.attempt}"
    ).resolve()


def _slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._") or "item"


def _normalize_output(value: Any, process_id: str) -> ProcessOutput:
    if isinstance(value, ProcessOutput):
        return value
    if isinstance(value, dict):
        return ProcessOutput.model_validate(value)
    raise ProcessAdapterError(
        f"Process {process_id!r} returned unsupported output type {type(value).__name__}"
    )


def _with_runtime_metadata(output: ProcessOutput, runtime_metadata: dict[str, Any]) -> ProcessOutput:
    metadata = dict(output.metadata)
    existing = metadata.get("process_runtime")
    if isinstance(existing, dict):
        merged = {**existing, **runtime_metadata}
    else:
        merged = dict(runtime_metadata)
    metadata["process_runtime"] = merged
    return output.model_copy(update={"metadata": metadata})


def _decode_subprocess_json(text: str, process_id: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped[0] not in "[{":
            continue
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            continue

    raise ProcessAdapterError(
        f"Process {process_id!r} subprocess returned non-JSON stdout: {text!r}"
    )

