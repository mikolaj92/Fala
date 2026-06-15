from __future__ import annotations

import hashlib
import json
import mimetypes
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from pydantic import BaseModel, ConfigDict, Field


EVIDENCE_PACK_SCHEMA_VERSION = "fala-evidence-pack-v1"


class ArtifactManifestEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str | None = None
    kind: str
    uri: str | None = None
    path: str | None = None
    exists: bool = False
    is_file: bool = False
    size_bytes: int | None = None
    sha256: str | None = None
    media_type: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ArtifactManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    artifacts: list[ArtifactManifestEntry] = Field(default_factory=list)


class EvidencePack(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = EVIDENCE_PACK_SCHEMA_VERSION
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    workflow_id: str | None = None
    run_id: str | None = None
    status: str = "unknown"
    summary: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[ArtifactManifestEntry] = Field(default_factory=list)
    gates: list[dict[str, Any]] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


def artifact_entry(
    value: str | Path | dict[str, Any] | Any,
    *,
    base_dir: str | Path | None = None,
    compute_sha256: bool = True,
) -> ArtifactManifestEntry:
    raw = _coerce_artifact_mapping(value)
    uri = _string_or_none(raw.get("uri"))
    path_value = _string_or_none(raw.get("path")) or _path_from_uri(uri)
    path = _resolve_path(path_value, base_dir=base_dir)
    exists = bool(path and path.exists())
    is_file = bool(path and path.is_file())
    size_bytes = path.stat().st_size if is_file and path is not None else None
    media_type = _string_or_none(raw.get("media_type"))
    if media_type is None and path is not None:
        media_type = mimetypes.guess_type(path.name)[0]
    sha256 = _sha256_file(path) if compute_sha256 and is_file and path is not None else None
    return ArtifactManifestEntry(
        id=_string_or_none(raw.get("id")),
        kind=str(raw.get("kind") or _kind_from_path(path) or "artifact"),
        uri=uri,
        path=str(path) if path is not None else path_value,
        exists=exists,
        is_file=is_file,
        size_bytes=size_bytes,
        sha256=sha256,
        media_type=media_type,
        metadata=dict(raw.get("metadata") or {}),
    )


def build_artifact_manifest(
    artifacts: list[str | Path | dict[str, Any] | Any],
    *,
    base_dir: str | Path | None = None,
    compute_sha256: bool = True,
) -> ArtifactManifest:
    return ArtifactManifest(
        artifacts=[
            artifact_entry(item, base_dir=base_dir, compute_sha256=compute_sha256)
            for item in artifacts
        ]
    )


def build_evidence_pack(
    *,
    workflow_id: str | None = None,
    run_id: str | None = None,
    artifacts: list[str | Path | dict[str, Any] | Any] | None = None,
    gate_results: list[dict[str, Any] | Any] | None = None,
    metadata: dict[str, Any] | None = None,
    base_dir: str | Path | None = None,
    status: str | None = None,
) -> EvidencePack:
    entries = build_artifact_manifest(artifacts or [], base_dir=base_dir).artifacts
    gates = [_model_dump_jsonable(item) for item in gate_results or []]
    passed = sum(1 for item in gates if item.get("status") == "passed")
    failed = sum(1 for item in gates if item.get("status") == "failed")
    skipped = sum(1 for item in gates if item.get("status") == "skipped")
    pack_status = status or ("passed" if failed == 0 else "failed")
    return EvidencePack(
        workflow_id=workflow_id,
        run_id=run_id,
        status=pack_status,
        summary={
            "artifact_count": len(entries),
            "gate_count": len(gates),
            "gates_passed": passed,
            "gates_failed": failed,
            "gates_skipped": skipped,
        },
        artifacts=entries,
        gates=gates,
        metadata=dict(metadata or {}),
    )


def write_evidence_pack(pack: EvidencePack, path: str | Path) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(pack.model_dump(mode="json"), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def _coerce_artifact_mapping(value: str | Path | dict[str, Any] | Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, Path):
        return {"kind": _kind_from_path(value), "path": str(value)}
    if isinstance(value, str):
        parsed = urlparse(value)
        if parsed.scheme:
            return {"kind": Path(parsed.path).suffix.lstrip(".") or "artifact", "uri": value}
        return {"kind": _kind_from_path(Path(value)), "path": value}
    if hasattr(value, "model_dump"):
        return dict(value.model_dump(mode="json"))
    raise TypeError(f"Unsupported artifact manifest value: {type(value).__name__}")


def _resolve_path(value: str | None, *, base_dir: str | Path | None) -> Path | None:
    if not value:
        return None
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return (Path(base_dir) / path).resolve() if base_dir is not None else path


def _path_from_uri(uri: str | None) -> str | None:
    if not uri:
        return None
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        return unquote(parsed.path)
    if parsed.scheme == "":
        return uri
    return None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _kind_from_path(path: Path | None) -> str | None:
    if path is None:
        return None
    suffix = path.suffix.lower().lstrip(".")
    return suffix or "artifact"


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _model_dump_jsonable(value: dict[str, Any] | Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if hasattr(value, "model_dump"):
        return dict(value.model_dump(mode="json"))
    raise TypeError(f"Unsupported evidence gate result: {type(value).__name__}")
