from __future__ import annotations

import asyncio
import inspect
import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass, field
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol

from fala.errors import FalaAdapterError
from fala.models import CarrierAdapterSpec


@dataclass(frozen=True)
class StepRunRequest:
    run_id: str
    process_id: str
    adapter: CarrierAdapterSpec
    carrier_id: str | None = None
    input: dict[str, Any] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)
    work_dir: Path | None = None


@dataclass(frozen=True)
class StepRunResult:
    output: dict[str, Any] = field(default_factory=dict)
    stdout: str = ""
    stderr: str = ""
    returncode: int | None = None
    waiting: bool = False
    gate_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class StepAdapter(Protocol):
    async def run(self, request: StepRunRequest) -> StepRunResult: ...


class PythonFunctionStepAdapter:
    async def run(self, request: StepRunRequest) -> StepRunResult:
        if request.adapter.kind != "python_function":
            raise FalaAdapterError("python_function adapter received wrong adapter kind")
        if not request.adapter.ref:
            raise FalaAdapterError("python_function adapter requires ref")

        function = _load_ref(request.adapter.ref)
        result = function(request)
        if inspect.isawaitable(result):
            result = await result
        return _coerce_result(result)


class SubprocessStepAdapter:
    async def run(self, request: StepRunRequest) -> StepRunResult:
        if request.adapter.kind != "subprocess":
            raise FalaAdapterError("subprocess adapter received wrong adapter kind")
        if not request.adapter.command:
            raise FalaAdapterError("subprocess adapter requires command")

        if request.work_dir is None:
            with tempfile.TemporaryDirectory(prefix="fala-step-") as tmp:
                return await self._run_in_dir(request, Path(tmp))
        return await self._run_in_dir(request, request.work_dir)

    async def _run_in_dir(self, request: StepRunRequest, root: Path) -> StepRunResult:
        input_dir = root / "input"
        output_dir = root / "output"
        input_dir.mkdir(parents=True, exist_ok=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = input_dir / "manifest.json"
        result_path = output_dir / "result.json"
        manifest_path.write_text(
            json.dumps(_manifest(request), indent=2, sort_keys=True),
            encoding="utf-8",
        )
        adapter_env = _resolve_adapter_env(request.adapter.env)
        redacted_values = {
            value for value in adapter_env.values() if value
        }

        env = {
            **os.environ,
            **adapter_env,
            "FALA_STEP_INPUT_DIR": str(input_dir),
            "FALA_STEP_OUTPUT_DIR": str(output_dir),
            "FALA_STEP_MANIFEST": str(manifest_path),
        }
        process = await asyncio.create_subprocess_exec(
            *request.adapter.command,
            cwd=request.adapter.cwd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=request.adapter.timeout_seconds,
            )
        except TimeoutError as exc:
            process.kill()
            await process.wait()
            raise FalaAdapterError("subprocess adapter timed out") from exc

        stdout_text = stdout.decode("utf-8", errors="replace")
        stderr_text = stderr.decode("utf-8", errors="replace")
        stdout_text = _redact(stdout_text, redacted_values)
        stderr_text = _redact(stderr_text, redacted_values)
        if process.returncode != 0:
            raise FalaAdapterError(
                f"subprocess adapter failed with exit {process.returncode}: "
                f"{stderr_text.strip()}"
            )
        if not result_path.exists():
            raise FalaAdapterError("subprocess adapter did not write output/result.json")
        output = _load_output_result(result_path)
        return StepRunResult(
            output=output,
            stdout=stdout_text,
            stderr=stderr_text,
            returncode=process.returncode,
        )


class ManualGateStepAdapter:
    async def run(self, request: StepRunRequest) -> StepRunResult:
        if request.adapter.kind != "manual_gate":
            raise FalaAdapterError("manual_gate adapter received wrong adapter kind")
        return StepRunResult(
            waiting=True,
            gate_id=f"gate:{request.run_id}:{request.process_id}",
            output={"status": "waiting"},
        )


class FalaRuntimeStepAdapter:
    async def run(self, request: StepRunRequest) -> StepRunResult:
        if request.adapter.kind != "fala_runtime":
            raise FalaAdapterError("fala_runtime adapter received wrong adapter kind")
        if not request.adapter.runtime_ref:
            raise FalaAdapterError("fala_runtime adapter requires runtime_ref")
        return StepRunResult(
            waiting=True,
            output={
                "runtime_ref": request.adapter.runtime_ref,
                "status": "submitted",
            },
        )


def create_step_adapter(kind: str) -> StepAdapter:
    if kind == "python_function":
        return PythonFunctionStepAdapter()
    if kind == "subprocess":
        return SubprocessStepAdapter()
    if kind == "manual_gate":
        return ManualGateStepAdapter()
    if kind == "fala_runtime":
        return FalaRuntimeStepAdapter()
    raise FalaAdapterError(f"unknown step adapter kind: {kind!r}")


def _load_ref(ref: str) -> Any:
    module_name, separator, attr_name = ref.partition(":")
    if not separator:
        module_name, separator, attr_name = ref.rpartition(".")
    if not module_name or not attr_name:
        raise FalaAdapterError(f"invalid python_function ref: {ref!r}")
    try:
        return getattr(import_module(module_name), attr_name)
    except (ImportError, AttributeError) as exc:
        raise FalaAdapterError(f"cannot load python_function ref: {ref!r}") from exc


def _coerce_result(value: Any) -> StepRunResult:
    if isinstance(value, StepRunResult):
        return value
    if isinstance(value, Mapping):
        return StepRunResult(output=dict(value))
    raise FalaAdapterError("python_function adapter must return dict or StepRunResult")


def _manifest(request: StepRunRequest) -> dict[str, Any]:
    adapter = request.adapter.model_dump(mode="json")
    if adapter.get("env"):
        adapter["env"] = {
            key: _redacted_env_value(value)
            for key, value in adapter["env"].items()
        }
    return {
        "run_id": request.run_id,
        "process_id": request.process_id,
        "carrier_id": request.carrier_id,
        "input": request.input,
        "config": request.config,
        "adapter": adapter,
    }


def _resolve_adapter_env(env: Mapping[str, str]) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for key, value in env.items():
        ref = _env_ref_name(value)
        if ref is None:
            resolved[key] = value
            continue
        if ref not in os.environ:
            raise FalaAdapterError(f"adapter env references missing variable: {ref}")
        resolved[key] = os.environ[ref]
    return resolved


def _redacted_env_value(value: str) -> str:
    ref = _env_ref_name(value)
    if ref is not None:
        return f"${{env:{ref}}}"
    return "<redacted>"


def _env_ref_name(value: str) -> str | None:
    if value.startswith("${env:") and value.endswith("}"):
        name = value[6:-1]
        if not name:
            raise FalaAdapterError("adapter env reference cannot be empty")
        return name
    return None


def _redact(text: str, secrets: set[str]) -> str:
    redacted = text
    for secret in sorted(secrets, key=len, reverse=True):
        redacted = redacted.replace(secret, "<redacted>")
    return redacted


def _load_output_result(path: Path) -> dict[str, Any]:
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise FalaAdapterError("subprocess output/result.json is not valid JSON") from exc
    if not isinstance(loaded, dict):
        raise FalaAdapterError("subprocess output/result.json must contain an object")
    return loaded


__all__ = [
    "FalaRuntimeStepAdapter",
    "ManualGateStepAdapter",
    "PythonFunctionStepAdapter",
    "StepAdapter",
    "StepRunRequest",
    "StepRunResult",
    "SubprocessStepAdapter",
    "create_step_adapter",
]
