from __future__ import annotations

import json
import os
import re
import sys
import hashlib
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any, TextIO
from urllib.parse import unquote, urlparse

PROCESS_RUNTIME_EVENT_PREFIX = "PROCESS_RUNTIME_EVENT "
FALA_ARTIFACT_SCHEME = "fala-artifact"
StepHandler = Callable[[dict[str, Any]], dict[str, Any]]


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


__all__ = [
    "PROCESS_RUNTIME_EVENT_PREFIX",
    "StepHandler",
    "artifact",
    "artifact_root",
    "emit_event",
    "initial",
    "input_artifacts",
    "input_values",
    "needs",
    "optional_path",
    "output",
    "path_from_output",
    "path_from_uri",
    "read_needed_json",
    "read_needed_text",
    "run_stdio",
    "skipped",
    "slug",
    "stream_chunk",
    "write_json",
    "write_text",
]
