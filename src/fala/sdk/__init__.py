from __future__ import annotations

import json
import os
import re
import sys
import hashlib
import subprocess
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TextIO
from urllib.parse import unquote, urlparse

PROCESS_RUNTIME_EVENT_PREFIX = "PROCESS_RUNTIME_EVENT "
FALA_ARTIFACT_SCHEME = "fala-artifact"
StepHandler = Callable[[dict[str, Any]], dict[str, Any]]
StepContextHandler = Callable[["StepContext"], dict[str, Any]]


class ArtifactNotFoundError(FileNotFoundError):
    """Raised when a required process-runtime artifact is absent."""


class ArtifactReadError(RuntimeError):
    """Raised when an artifact exists but cannot be read as requested."""


@dataclass(frozen=True)
class JsonNeed:
    process_id: str
    artifact_kind: str
    required: bool = True


@dataclass(frozen=True)
class JsonArtifact:
    artifact_kind: str
    filename: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StepContract:
    process_id: str
    needs: dict[str, JsonNeed] = field(default_factory=dict)
    outputs: dict[str, JsonArtifact] = field(default_factory=dict)
    manifest_filename: str = "step_run_manifest.json"


class _NeedReader:
    def __init__(self, step_context: "StepContext") -> None:
        self._ctx = step_context

    def json(self, name: str, *, default: Any | None = None) -> Any:
        need = self._ctx.contract.needs[name]
        if not need.required and default is None:
            default = None
        return read_needed_json(
            needs(self._ctx.raw_context),
            need.process_id,
            need.artifact_kind,
            default=default,
        )

    def text(self, name: str) -> str:
        need = self._ctx.contract.needs[name]
        return read_needed_text(
            needs(self._ctx.raw_context),
            need.process_id,
            need.artifact_kind,
        )

    def path(self, name: str) -> Path | None:
        need = self._ctx.contract.needs[name]
        path = path_from_output(
            needs(self._ctx.raw_context).get(need.process_id, {}),
            need.artifact_kind,
        )
        if path is None and need.required:
            raise ArtifactNotFoundError(
                f"Missing required artifact {need.artifact_kind!r} from process "
                f"{need.process_id!r}"
            )
        return path


class StepContext:
    def __init__(self, contract: StepContract, context: dict[str, Any]) -> None:
        self.contract = contract
        self.raw_context = context
        self.needs = _NeedReader(self)

    @property
    def root(self) -> Path:
        return artifact_root(self.raw_context, self.contract.process_id)

    @property
    def initial(self) -> dict[str, Any]:
        return initial(self.raw_context)

    def write_json_artifact(
        self,
        output_name: str,
        payload: Any,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        spec = self.contract.outputs[output_name]
        path = write_json(self.root / spec.filename, payload)
        return artifact(
            spec.artifact_kind,
            path,
            {**spec.metadata, **(metadata or {})},
        )

    def complete(
        self,
        *,
        values: dict[str, Any] | None = None,
        artifacts: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        refs = [
            self.write_json_artifact(name, payload)
            for name, payload in (artifacts or {}).items()
        ]
        return output(values=values or {}, artifacts=refs, metadata=metadata)

    def skip(self, reason: str) -> dict[str, Any]:
        return skipped(self.raw_context, self.contract.process_id, reason)


@dataclass(frozen=True)
class CarrierWorkerContext:
    run_id: str
    carrier_id: str
    carrier_type: str
    payload: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    process_id: str | None = None
    attempt: int = 1

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "CarrierWorkerContext":
        return cls(
            run_id=str(payload["run_id"]),
            carrier_id=str(payload["carrier_id"]),
            carrier_type=str(payload["carrier_type"]),
            payload=dict(payload.get("payload") or {}),
            metadata=dict(payload.get("metadata") or {}),
            process_id=(
                str(payload["process_id"]) if payload.get("process_id") is not None else None
            ),
            attempt=int(payload.get("attempt") or 1),
        )

    def to_payload(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "run_id": self.run_id,
            "carrier_id": self.carrier_id,
            "carrier_type": self.carrier_type,
            "payload": dict(self.payload),
            "metadata": dict(self.metadata),
            "attempt": self.attempt,
        }
        if self.process_id is not None:
            result["process_id"] = self.process_id
        return result


def build_carrier_env(
    context: CarrierWorkerContext,
    *,
    base_env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    env = dict(base_env or {})
    env.update(
        {
            "FALA_RUN_ID": context.run_id,
            "FALA_CARRIER_ID": context.carrier_id,
            "FALA_CARRIER_TYPE": context.carrier_type,
            "FALA_ATTEMPT": str(context.attempt),
        }
    )
    if context.process_id is not None:
        env["FALA_PROCESS_ID"] = context.process_id
    return env


def carrier_output(
    *,
    payload: dict[str, Any] | None = None,
    observations: list[dict[str, Any]] | None = None,
    artifacts: list[dict[str, Any]] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "payload": payload or {},
        "observations": observations or [],
        "artifacts": artifacts or [],
        "metadata": metadata or {},
    }


def run_stdio(handler: StepHandler) -> int:
    """Run one process-runtime step over stdin/stdout JSON."""
    try:
        context = json.loads(sys.stdin.read() or "{}")
        output_value = handler(context)
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(json.dumps(output_value, ensure_ascii=False))
    return 0


def run_step(contract: StepContract, handler: StepContextHandler) -> int:
    """Run a contracted step over stdin/stdout JSON and write a replay manifest."""

    def wrapped(context: dict[str, Any]) -> dict[str, Any]:
        step_context = StepContext(contract, context)
        try:
            output_value = _coerce_process_output_dict(handler(step_context))
        except Exception as exc:
            _write_step_manifest(step_context, error=str(exc))
            raise
        _write_step_manifest(step_context, output_value=output_value)
        return output_value

    return run_stdio(wrapped)


def emit_event(
    event_type: str,
    *,
    status: str | None = None,
    data: dict[str, Any] | None = None,
    stream: TextIO | None = None,
) -> None:
    payload: dict[str, Any] = {"type": event_type}
    if status is not None:
        payload["status"] = status
    if data is not None:
        payload["data"] = data
    print(
        f"{PROCESS_RUNTIME_EVENT_PREFIX}{json.dumps(payload, ensure_ascii=False)}",
        file=stream or sys.stderr,
        flush=True,
    )


def input_values(context: dict[str, Any]) -> dict[str, Any]:
    return dict((context.get("input") or {}).get("values") or {})


def initial(context: dict[str, Any]) -> dict[str, Any]:
    return dict(input_values(context).get("initial") or {})


def needs(context: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return dict(input_values(context).get("needs") or {})


def input_artifacts(context: dict[str, Any]) -> list[dict[str, Any]]:
    return list((context.get("input") or {}).get("artifacts") or [])


def latest_input_artifact(
    context: dict[str, Any],
    kind: str,
    *,
    required: bool = True,
) -> dict[str, Any] | None:
    for artifact_value in reversed(input_artifacts(context)):
        artifact_dict = _artifact_to_dict(artifact_value)
        if artifact_dict.get("kind") == kind:
            return artifact_dict
    if required:
        raise ArtifactNotFoundError(f"Missing required input artifact kind {kind!r}")
    return None


def require_artifact_path(context: dict[str, Any], kind: str) -> Path:
    artifact_value = latest_input_artifact(context, kind, required=True)
    path = path_from_artifact(artifact_value)
    if path is None:
        raise ArtifactNotFoundError(f"Input artifact kind {kind!r} has no readable URI")
    if not path.exists():
        raise ArtifactNotFoundError(f"Input artifact kind {kind!r} path does not exist: {path}")
    if not path.is_file():
        raise ArtifactNotFoundError(f"Input artifact kind {kind!r} path is not a file: {path}")
    return path


def read_json_artifact(context: dict[str, Any], kind: str) -> Any:
    path = require_artifact_path(context, kind)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        raise
    except Exception as exc:
        raise ArtifactReadError(f"Cannot read JSON artifact {kind!r} from {path}") from exc


def write_json_artifact(
    context: dict[str, Any],
    process_id: str,
    kind: str,
    payload: Any,
    *,
    filename: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    path = write_json(artifact_root(context, process_id) / filename, payload)
    return artifact(kind, path, metadata)


def build_step_env(
    *,
    process_artifact_root: str | Path | None = None,
    artifact_store_root: str | Path | None = None,
    artifact_store: str | None = None,
    artifact_cache_root: str | Path | None = None,
    base_env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    env = dict(base_env or {})
    if process_artifact_root is not None:
        env["PROCESS_RUNTIME_ARTIFACT_ROOT"] = str(Path(process_artifact_root).expanduser())
    if artifact_store_root is not None:
        env["FALA_ARTIFACT_STORE_ROOT"] = str(Path(artifact_store_root).expanduser())
    if artifact_store is not None:
        env["FALA_ARTIFACT_STORE"] = artifact_store
    if artifact_cache_root is not None:
        env["FALA_ARTIFACT_CACHE_ROOT"] = str(Path(artifact_cache_root).expanduser())
    return env


def artifact_root(context: dict[str, Any], process_id: str) -> Path:
    runtime_dir = os.environ.get("PROCESS_RUNTIME_ARTIFACT_DIR")
    if runtime_dir:
        root = Path(runtime_dir).expanduser()
        root.mkdir(parents=True, exist_ok=True)
        return root

    base = Path(os.environ.get("PROCESS_RUNTIME_ARTIFACT_ROOT", ".flow-runs/process-artifacts"))
    root = (
        base
        / slug(str(context.get("run_id") or "run"))
        / slug(str(context.get("document_id") or "document"))
        / process_id
    )
    root.mkdir(parents=True, exist_ok=True)
    return root


def artifact(kind: str, path: Path, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    resolved = path.resolve()
    merged_metadata = dict(metadata or {})
    if resolved.exists() and resolved.is_file():
        digest = hashlib.sha256()
        size = 0
        with resolved.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                size += len(chunk)
                digest.update(chunk)
        merged_metadata.setdefault("sha256", digest.hexdigest())
        merged_metadata.setdefault("size_bytes", size)
        merged_metadata.setdefault("filename", resolved.name)
    return {
        "kind": kind,
        "uri": resolved.as_uri(),
        "metadata": merged_metadata,
    }


def replay_step_manifest(
    manifest_path: str | Path,
    command: Sequence[str],
    *,
    cwd: str | Path | None = None,
    env: Mapping[str, str] | None = None,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    manifest_file = Path(manifest_path).expanduser()
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    context = manifest.get("context")
    if not isinstance(context, dict):
        raise ValueError(f"Step manifest {manifest_file} does not contain a context object")
    if not command:
        raise ValueError("replay_step_manifest requires a command")
    completed = subprocess.run(
        list(command),
        input=json.dumps(context),
        text=True,
        capture_output=True,
        cwd=str(cwd) if cwd is not None else None,
        env={**os.environ, **dict(env or {})},
        timeout=timeout_seconds,
        check=False,
    )
    parsed_output: Any | None = None
    if completed.stdout.strip():
        try:
            parsed_output = json.loads(completed.stdout)
        except json.JSONDecodeError:
            parsed_output = None
    return {
        "ok": completed.returncode == 0,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "output": parsed_output,
        "manifest": {
            "path": str(manifest_file),
            "run_id": manifest.get("run_id"),
            "document_id": manifest.get("document_id"),
            "process_id": manifest.get("process_id"),
            "attempt": manifest.get("attempt"),
        },
    }


def output(
    *,
    values: dict[str, Any],
    artifacts: list[dict[str, Any]] | None = None,
    metadata: dict[str, Any] | None = None,
    output_documents: list[dict[str, Any]] | None = None,
    stream_chunks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "values": values,
        "artifacts": artifacts or [],
        "metadata": metadata or {},
        "output_documents": output_documents or [],
        "stream_chunks": stream_chunks or [],
    }


def output_document(
    *,
    document_type: str,
    media_type: str | None = None,
    uri: str | None = None,
    artifact_id: str | None = None,
    relation: str = "derived",
    title: str | None = None,
    values: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    id: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "document_type": document_type,
        "relation": relation,
        "values": values or {},
        "metadata": metadata or {},
    }
    if id is not None:
        payload["id"] = id
    if title is not None:
        payload["title"] = title
    if media_type is not None:
        payload["media_type"] = media_type
    if uri is not None:
        payload["uri"] = uri
    if artifact_id is not None:
        payload["artifact_id"] = artifact_id
    return payload


def stream_chunk(
    *,
    stream_id: str = "main",
    values: dict[str, Any] | None = None,
    artifacts: list[dict[str, Any]] | None = None,
    metadata: dict[str, Any] | None = None,
    kind: str | None = None,
    sequence: int | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "stream_id": stream_id,
        "values": values or {},
        "artifacts": artifacts or [],
        "metadata": metadata or {},
    }
    if kind is not None:
        payload["kind"] = kind
    if sequence is not None:
        payload["sequence"] = sequence
    return payload


def skipped(context: dict[str, Any], process_id: str, reason: str) -> dict[str, Any]:
    path = write_json(artifact_root(context, process_id) / "empty.json", [])
    return output(
        values={"status": "skipped", "reason": reason},
        artifacts=[artifact("empty", path)],
    )


def path_from_output(output_value: dict[str, Any], kind: str) -> Path | None:
    for artifact_value in output_value.get("artifacts") or []:
        if artifact_value.get("kind") == kind:
            return path_from_artifact(artifact_value)
    values = output_value.get("values") if isinstance(output_value.get("values"), dict) else output_value
    value = values.get(f"{kind}_path") if isinstance(values, dict) else None
    return optional_path(str(value or ""))


def read_needed_text(needs_value: dict[str, dict[str, Any]], process_id: str, kind: str) -> str:
    path = path_from_output(needs_value.get(process_id, {}), kind)
    if not path or not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def read_needed_json(
    needs_value: dict[str, dict[str, Any]],
    process_id: str,
    kind: str,
    *,
    default: Any | None = None,
) -> Any:
    path = path_from_output(needs_value.get(process_id, {}), kind)
    if not path or not path.exists():
        return {} if default is None else default
    return json.loads(path.read_text(encoding="utf-8"))


def write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def write_json(path: Path, value: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def path_from_uri(uri: str) -> Path | None:
    if not uri:
        return None
    parsed = urlparse(uri)
    if parsed.scheme == FALA_ARTIFACT_SCHEME:
        return _path_from_fala_artifact_uri(parsed)
    if parsed.scheme == "file":
        return Path(unquote(parsed.path))
    if not parsed.scheme:
        return Path(uri).expanduser()
    return None


def optional_path(value: str) -> Path | None:
    if not value:
        return None
    return path_from_uri(value) or Path(value).expanduser()


def path_from_artifact(artifact_value: Any) -> Path | None:
    if hasattr(artifact_value, "model_dump"):
        artifact_value = artifact_value.model_dump(mode="json")
    if not isinstance(artifact_value, dict):
        return None
    return path_from_uri(str(artifact_value.get("uri") or ""))


def _path_from_fala_artifact_uri(parsed) -> Path | None:
    if parsed.netloc != "sha256":
        return None
    digest = parsed.path.strip("/").lower()
    if not _valid_sha256_digest(digest):
        return None

    store_target = os.environ.get("FALA_ARTIFACT_STORE")
    if store_target:
        return _path_from_fala_artifact_store(digest, store_target)

    return _local_fala_artifact_path(
        digest,
        os.environ.get("FALA_ARTIFACT_STORE_ROOT") or ".flow-runs/artifact-store",
    )


def _path_from_fala_artifact_store(digest: str, store_target: str) -> Path | None:
    store = urlparse(store_target)
    if store.scheme == "s3":
        return _path_from_s3_artifact_store(digest, store)
    if store.scheme == "file":
        return _local_fala_artifact_path(digest, unquote(store.path))
    if store.scheme in {"", None}:
        return _local_fala_artifact_path(digest, store_target)
    return None


def _local_fala_artifact_path(digest: str, root_value: str | Path) -> Path | None:
    root = Path(root_value).expanduser().resolve()
    path = root / "blobs" / "sha256" / digest[:2] / digest
    resolved = path.resolve()
    try:
        resolved.relative_to(root)
    except ValueError:
        return None
    return resolved


def _path_from_s3_artifact_store(digest: str, store) -> Path | None:
    if not store.netloc:
        return None
    cache_root = os.environ.get("FALA_ARTIFACT_CACHE_ROOT")
    if not cache_root:
        cache_root = os.environ.get("PROCESS_RUNTIME_ARTIFACT_CACHE_ROOT")
    if not cache_root:
        cache_root = str(Path(tempfile.gettempdir()) / "fala-artifact-cache")
    target = _local_fala_artifact_path(digest, cache_root)
    if target is None:
        return None
    if target.exists():
        return target
    key = _s3_blob_key(store.path, digest)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(".tmp")
    try:
        client = _s3_client()
        with temp.open("wb") as handle:
            client.download_fileobj(store.netloc, key, handle)
        os.replace(temp, target)
    except Exception:
        temp.unlink(missing_ok=True)
        return None
    return target


def _s3_blob_key(prefix_path: str, digest: str) -> str:
    prefix = unquote(prefix_path).strip("/")
    parts = [prefix, "blobs", "sha256", digest[:2], digest]
    return "/".join(part.strip("/") for part in parts if part.strip("/"))


def _s3_client():
    try:
        import boto3
    except ImportError:
        return _MissingS3Client()
    return boto3.client("s3")


class _MissingS3Client:
    def download_fileobj(self, *_args, **_kwargs) -> None:
        raise RuntimeError("boto3 is required to resolve s3:// artifact stores")


def _valid_sha256_digest(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._") or "item"


def _coerce_process_output_dict(value: Any) -> dict[str, Any]:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    if not isinstance(value, dict):
        raise TypeError(f"Step handler returned unsupported output type: {type(value).__name__}")
    value.setdefault("values", {})
    value.setdefault("artifacts", [])
    value.setdefault("metadata", {})
    value.setdefault("output_documents", [])
    value.setdefault("stream_chunks", [])
    return value


def _write_step_manifest(
    step_context: StepContext,
    *,
    output_value: dict[str, Any] | None = None,
    error: str | None = None,
) -> Path:
    manifest = _step_manifest(step_context, output_value=output_value, error=error)
    path = write_json(step_context.root / step_context.contract.manifest_filename, manifest)
    if output_value is not None:
        metadata = output_value.setdefault("metadata", {})
        metadata["step_run_manifest"] = artifact("step_run_manifest", path)
    return path


def _step_manifest(
    step_context: StepContext,
    *,
    output_value: dict[str, Any] | None,
    error: str | None,
) -> dict[str, Any]:
    context = step_context.raw_context
    return {
        "schema": "fala.step_run_manifest.v1",
        "pipeline_id": context.get("pipeline_id"),
        "run_id": context.get("run_id"),
        "document_id": context.get("document_id"),
        "process_id": context.get("process_id") or step_context.contract.process_id,
        "contract_process_id": step_context.contract.process_id,
        "attempt": context.get("attempt"),
        "capability": context.get("capability"),
        "context": context,
        "inputs": [
            _artifact_summary(artifact_value)
            for artifact_value in input_artifacts(context)
        ],
        "needs": {
            name: {
                "process_id": spec.process_id,
                "artifact_kind": spec.artifact_kind,
                "artifact": _artifact_summary(
                    _artifact_from_needed_output(context, spec.process_id, spec.artifact_kind)
                ),
            }
            for name, spec in step_context.contract.needs.items()
        },
        "outputs": [
            _artifact_summary(artifact_value)
            for artifact_value in ((output_value or {}).get("artifacts") or [])
        ],
        "output_values": (output_value or {}).get("values") or {},
        "error": error,
    }


def _artifact_from_needed_output(
    context: dict[str, Any],
    process_id: str,
    kind: str,
) -> dict[str, Any] | None:
    output_value = needs(context).get(process_id, {})
    for artifact_value in output_value.get("artifacts") or []:
        artifact_dict = _artifact_to_dict(artifact_value)
        if artifact_dict.get("kind") == kind:
            return artifact_dict
    return None


def _artifact_summary(artifact_value: Any) -> dict[str, Any] | None:
    artifact_dict = _artifact_to_dict(artifact_value)
    if not artifact_dict:
        return None
    path = path_from_artifact(artifact_dict)
    metadata = artifact_dict.get("metadata") if isinstance(artifact_dict.get("metadata"), dict) else {}
    sha256 = metadata.get("sha256")
    size_bytes = metadata.get("size_bytes")
    if path is not None and path.exists() and path.is_file() and (not sha256 or size_bytes is None):
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                size += len(chunk)
                digest.update(chunk)
        sha256 = sha256 or digest.hexdigest()
        size_bytes = size if size_bytes is None else size_bytes
    return {
        "id": artifact_dict.get("id"),
        "kind": artifact_dict.get("kind"),
        "uri": artifact_dict.get("uri"),
        "path": str(path) if path is not None else None,
        "sha256": sha256,
        "size_bytes": size_bytes,
        "metadata": metadata,
    }


def _artifact_to_dict(artifact_value: Any) -> dict[str, Any]:
    if artifact_value is None:
        return {}
    if hasattr(artifact_value, "model_dump"):
        artifact_value = artifact_value.model_dump(mode="json")
    return dict(artifact_value) if isinstance(artifact_value, dict) else {}


__all__ = [
    "PROCESS_RUNTIME_EVENT_PREFIX",
    "ArtifactNotFoundError",
    "ArtifactReadError",
    "JsonArtifact",
    "JsonNeed",
    "StepContract",
    "StepContext",
    "StepHandler",
    "artifact",
    "artifact_root",
    "build_step_env",
    "emit_event",
    "initial",
    "input_artifacts",
    "input_values",
    "latest_input_artifact",
    "needs",
    "optional_path",
    "output",
    "path_from_output",
    "path_from_uri",
    "read_needed_json",
    "read_needed_text",
    "read_json_artifact",
    "replay_step_manifest",
    "require_artifact_path",
    "run_stdio",
    "run_step",
    "skipped",
    "slug",
    "stream_chunk",
    "write_json_artifact",
    "write_json",
    "write_text",
]
