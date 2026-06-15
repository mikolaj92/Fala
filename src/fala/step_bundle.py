from __future__ import annotations

import hashlib
import json
import os
import shlex
import tarfile
from copy import deepcopy
from pathlib import Path, PurePosixPath
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse

from fala.sdk import path_from_uri, slug

STEP_BUNDLE_SCHEMA = "fala.step_replay_bundle.v1"


def write_step_replay_bundle(
    manifest_path: str | Path,
    *,
    output: str | Path,
    command: Sequence[str],
    cwd: str | Path | None = None,
    env: Mapping[str, str] | None = None,
    bundle_name: str | None = None,
) -> dict[str, Any]:
    manifest_file = Path(manifest_path).expanduser().resolve()
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    selected_bundle_name = bundle_name or _step_bundle_name(manifest, manifest_file)
    artifacts, uri_map = _bundle_artifacts(manifest)
    replay_manifest = _rewrite_manifest_artifact_uris(manifest, uri_map)
    bundle_manifest = {
        "schema": STEP_BUNDLE_SCHEMA,
        "bundle_name": selected_bundle_name,
        "source_manifest": str(manifest_file),
        "replay_manifest": "step_run_manifest.bundle.json",
        "original_manifest": "step_run_manifest.json",
        "replay_script": "replay.py",
        "pipeline_id": manifest.get("pipeline_id"),
        "run_id": manifest.get("run_id"),
        "document_id": manifest.get("document_id"),
        "process_id": manifest.get("process_id"),
        "command": list(command),
        "cwd": str(Path(cwd).expanduser().resolve()) if cwd is not None else None,
        "env": dict(env or {}),
        "artifacts": artifacts,
        "artifact_count": len(artifacts),
    }
    generated_files = {
        "bundle-manifest.json": _json_bytes(bundle_manifest),
        "step_run_manifest.json": _json_bytes(manifest),
        "step_run_manifest.bundle.json": _json_bytes(replay_manifest),
        "replay.py": _replay_script_bytes(),
    }

    output_path = Path(output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(output_path, "w:gz") as archive:
        for relative_path, content in generated_files.items():
            _add_bytes(
                archive,
                f"{selected_bundle_name}/{relative_path}",
                content,
                executable=relative_path == "replay.py",
            )
        for artifact in artifacts:
            source = Path(artifact["source_path"])
            archive.add(
                source,
                arcname=f"{selected_bundle_name}/{artifact['path']}",
                recursive=False,
            )

    return {
        "ok": True,
        "output": str(output_path),
        "bundle_name": selected_bundle_name,
        "source_manifest": str(manifest_file),
        "replay_manifest": "step_run_manifest.bundle.json",
        "artifact_count": len(artifacts),
        "file_count": len(generated_files) + len(artifacts),
        "generated_file_count": len(generated_files),
        "replay_command": " ".join(shlex.quote(item) for item in ["python", "replay.py"]),
    }


def verify_step_replay_bundle(bundle: str | Path) -> dict[str, Any]:
    bundle_path = Path(bundle).expanduser().resolve()
    errors: list[dict[str, Any]] = []
    try:
        archive = tarfile.open(bundle_path, "r:gz")
    except Exception as exc:
        return {
            "ok": False,
            "archive": str(bundle_path),
            "error_count": 1,
            "errors": [
                {
                    "code": "bundle_open_failed",
                    "message": str(exc),
                }
            ],
        }

    with archive:
        members = archive.getmembers()
        safe_names: set[str] = set()
        bundle_name: str | None = None
        manifest_member: tarfile.TarInfo | None = None
        for member in members:
            if member.islnk() or member.issym():
                errors.append(
                    {
                        "code": "bundle_link_entry",
                        "message": "Step replay bundle must not contain link entries.",
                        "path": member.name,
                    }
                )
                continue
            safe_name = _safe_bundle_member_name(member.name)
            if safe_name is None:
                errors.append(
                    {
                        "code": "bundle_unsafe_path",
                        "message": "Bundle member path is unsafe.",
                        "path": member.name,
                    }
                )
                continue
            safe_names.add(str(safe_name))
            parts = safe_name.parts
            if len(parts) < 2:
                errors.append(
                    {
                        "code": "bundle_missing_prefix",
                        "message": "Bundle members must be under one top-level directory.",
                        "path": member.name,
                    }
                )
                continue
            if bundle_name is None:
                bundle_name = parts[0]
            elif bundle_name != parts[0]:
                errors.append(
                    {
                        "code": "bundle_multiple_prefixes",
                        "message": "Bundle members must share one top-level directory.",
                        "path": member.name,
                    }
                )
            if safe_name.name == "bundle-manifest.json":
                manifest_member = member

        manifest: dict[str, Any] = {}
        if manifest_member is None:
            errors.append(
                {
                    "code": "bundle_manifest_missing",
                    "message": "Bundle must contain bundle-manifest.json.",
                }
            )
        else:
            try:
                extracted = archive.extractfile(manifest_member)
                if extracted is None:
                    raise ValueError("bundle-manifest.json is not readable")
                manifest = json.loads(extracted.read().decode("utf-8"))
            except Exception as exc:
                errors.append(
                    {
                        "code": "bundle_manifest_invalid",
                        "message": str(exc),
                    }
                )

        if manifest:
            expected_schema = manifest.get("schema")
            if expected_schema != STEP_BUNDLE_SCHEMA:
                errors.append(
                    {
                        "code": "bundle_schema_mismatch",
                        "message": "Bundle manifest has an unsupported schema.",
                        "schema": expected_schema,
                    }
                )
            required_files = {
                "bundle-manifest.json",
                str(manifest.get("replay_manifest") or "step_run_manifest.bundle.json"),
                str(manifest.get("original_manifest") or "step_run_manifest.json"),
                str(manifest.get("replay_script") or "replay.py"),
            }
            prefix = str(manifest.get("bundle_name") or bundle_name or "").strip("/")
            for relative_path in required_files:
                if prefix and f"{prefix}/{relative_path}" not in safe_names:
                    errors.append(
                        {
                            "code": "bundle_required_file_missing",
                            "message": "Bundle required file is missing.",
                            "path": relative_path,
                        }
                    )
            for artifact in manifest.get("artifacts") or []:
                if not isinstance(artifact, dict):
                    continue
                _verify_artifact_member(
                    archive,
                    prefix=prefix,
                    artifact=artifact,
                    safe_names=safe_names,
                    errors=errors,
                )

    return {
        "ok": not errors,
        "archive": str(bundle_path),
        "bundle_name": bundle_name,
        "checked_file_count": len(safe_names),
        "artifact_count": len(manifest.get("artifacts") or []) if manifest else 0,
        "error_count": len(errors),
        "errors": errors,
    }


def _bundle_artifacts(manifest: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, str]]:
    artifacts: list[dict[str, Any]] = []
    uri_map: dict[str, str] = {}
    path_map: dict[Path, str] = {}
    used_names: set[str] = set()
    for ref in _iter_artifact_refs(manifest):
        uri = str(ref.get("uri") or "").strip()
        if not uri:
            continue
        path = path_from_uri(uri)
        if path is None or not path.exists() or not path.is_file():
            continue
        resolved = path.resolve()
        relative_path = path_map.get(resolved)
        if relative_path is None:
            digest, size_bytes = _file_digest(resolved)
            relative_path = f"artifacts/{_artifact_bundle_filename(ref, resolved, digest, used_names)}"
            path_map[resolved] = relative_path
            artifacts.append(
                {
                    "kind": ref.get("kind"),
                    "uri": uri,
                    "source_path": str(resolved),
                    "path": relative_path,
                    "sha256": digest,
                    "size_bytes": size_bytes,
                }
            )
        uri_map[uri] = relative_path
    return artifacts, uri_map


def _iter_artifact_refs(value: Any) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []

    def visit(item: Any) -> None:
        if isinstance(item, dict):
            uri = item.get("uri")
            if isinstance(uri, str) and ("kind" in item or "metadata" in item):
                refs.append(item)
            for child in item.values():
                visit(child)
        elif isinstance(item, list):
            for child in item:
                visit(child)

    visit(value)
    return refs


def _rewrite_manifest_artifact_uris(
    manifest: dict[str, Any],
    uri_map: Mapping[str, str],
) -> dict[str, Any]:
    rewritten = deepcopy(manifest)

    def visit(item: Any) -> Any:
        if isinstance(item, dict):
            uri = item.get("uri")
            mapped = uri_map.get(uri) if isinstance(uri, str) else None
            updated = {key: visit(value) for key, value in item.items()}
            if mapped is not None:
                updated["uri"] = mapped
                if "path" in updated:
                    updated["path"] = mapped
            return updated
        if isinstance(item, list):
            return [visit(child) for child in item]
        return item

    return visit(rewritten)


def _verify_artifact_member(
    archive: tarfile.TarFile,
    *,
    prefix: str,
    artifact: dict[str, Any],
    safe_names: set[str],
    errors: list[dict[str, Any]],
) -> None:
    path = str(artifact.get("path") or "").strip("/")
    if not path:
        errors.append(
            {
                "code": "bundle_artifact_path_missing",
                "message": "Artifact entry has no path.",
            }
        )
        return
    member_name = f"{prefix}/{path}" if prefix else path
    if member_name not in safe_names:
        errors.append(
            {
                "code": "bundle_artifact_missing",
                "message": "Artifact file declared by bundle manifest is missing.",
                "path": path,
            }
        )
        return
    try:
        extracted = archive.extractfile(member_name)
        if extracted is None:
            raise ValueError("artifact member is not readable")
        digest = hashlib.sha256()
        size = 0
        for chunk in iter(lambda: extracted.read(1024 * 1024), b""):
            size += len(chunk)
            digest.update(chunk)
    except Exception as exc:
        errors.append(
            {
                "code": "bundle_artifact_unreadable",
                "message": str(exc),
                "path": path,
            }
        )
        return
    expected_sha = artifact.get("sha256")
    expected_size = artifact.get("size_bytes")
    if expected_sha and digest.hexdigest() != expected_sha:
        errors.append(
            {
                "code": "bundle_artifact_sha256_mismatch",
                "message": "Artifact checksum does not match bundle manifest.",
                "path": path,
            }
        )
    if expected_size is not None and size != expected_size:
        errors.append(
            {
                "code": "bundle_artifact_size_mismatch",
                "message": "Artifact size does not match bundle manifest.",
                "path": path,
            }
        )


def _artifact_bundle_filename(
    ref: dict[str, Any],
    path: Path,
    digest: str,
    used_names: set[str],
) -> str:
    kind = slug(str(ref.get("kind") or "artifact"))
    suffix = path.suffix if path.suffix else ".bin"
    base = f"{kind}-{digest[:12]}{suffix}"
    name = base
    index = 2
    while name in used_names:
        name = f"{kind}-{digest[:12]}-{index}{suffix}"
        index += 1
    used_names.add(name)
    return name


def _file_digest(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            size += len(chunk)
            digest.update(chunk)
    return digest.hexdigest(), size


def _add_bytes(
    archive: tarfile.TarFile,
    name: str,
    content: bytes,
    *,
    executable: bool = False,
) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(content)
    info.mode = 0o755 if executable else 0o644
    info.mtime = int(os.path.getmtime(__file__))
    import io

    archive.addfile(info, io.BytesIO(content))


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )


def _replay_script_bytes() -> bytes:
    script = r'''#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse

from fala.sdk import replay_step_manifest


def main() -> int:
    root = Path(__file__).resolve().parent
    bundle = json.loads((root / "bundle-manifest.json").read_text(encoding="utf-8"))
    manifest = json.loads((root / bundle["replay_manifest"]).read_text(encoding="utf-8"))
    manifest = absolutize_uris(manifest, root)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False) as handle:
        json.dump(manifest, handle)
        temp_manifest = handle.name
    try:
        result = replay_step_manifest(
            temp_manifest,
            bundle["command"],
            cwd=bundle.get("cwd"),
            env=bundle.get("env") or {},
        )
    finally:
        Path(temp_manifest).unlink(missing_ok=True)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result.get("ok") else 1


def absolutize_uris(value, root: Path):
    if isinstance(value, dict):
        updated = {key: absolutize_uris(child, root) for key, child in value.items()}
        uri = updated.get("uri")
        if isinstance(uri, str) and not urlparse(uri).scheme:
            updated["uri"] = (root / uri).resolve().as_uri()
            if "path" in updated:
                updated["path"] = str((root / uri).resolve())
        return updated
    if isinstance(value, list):
        return [absolutize_uris(item, root) for item in value]
    return value


if __name__ == "__main__":
    raise SystemExit(main())
'''
    return script.encode("utf-8")


def _safe_bundle_member_name(name: str) -> PurePosixPath | None:
    parsed = urlparse(name)
    if parsed.scheme or parsed.netloc:
        return None
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts:
        return None
    return path


def _step_bundle_name(manifest: dict[str, Any], manifest_file: Path) -> str:
    run_id = slug(str(manifest.get("run_id") or "run"))
    process_id = slug(str(manifest.get("process_id") or manifest_file.stem))
    return f"{run_id}-{process_id}-replay"


__all__ = [
    "STEP_BUNDLE_SCHEMA",
    "verify_step_replay_bundle",
    "write_step_replay_bundle",
]
