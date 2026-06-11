from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, BinaryIO, Iterable, Protocol
from urllib.parse import unquote, urlparse

from fala.models import ArtifactRef

FALA_ARTIFACT_SCHEME = "fala-artifact"


@dataclass(frozen=True)
class ArtifactBlobInfo:
    digest: str
    location: str
    size_bytes: int

    def __iter__(self):
        yield self.digest
        yield self.location
        yield self.size_bytes


class ArtifactStore(Protocol):
    @property
    def location(self) -> str:
        ...

    def put_file(
        self,
        *,
        kind: str,
        path: str | Path,
        artifact_id: str | None = None,
        metadata: dict | None = None,
    ) -> ArtifactRef:
        ...

    def put_fileobj(
        self,
        *,
        kind: str,
        fileobj: BinaryIO,
        filename: str,
        artifact_id: str | None = None,
        metadata: dict | None = None,
    ) -> ArtifactRef:
        ...

    def open(self, artifact: ArtifactRef) -> BinaryIO:
        ...

    def resolve(self, artifact: ArtifactRef) -> Path:
        ...

    def list_blobs(self) -> list[ArtifactBlobInfo]:
        ...

    def delete_blobs(self, digests: Iterable[str]) -> list[str]:
        ...


class S3Client(Protocol):
    def put_object(self, **kwargs: Any) -> Any:
        ...

    def get_object(self, **kwargs: Any) -> Any:
        ...

    def head_object(self, **kwargs: Any) -> Any:
        ...

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        ...

    def delete_objects(self, **kwargs: Any) -> Any:
        ...


class FileArtifactStore:
    def __init__(self, root: str | Path) -> None:
        self.root = _resolve_root(root)
        self.root.mkdir(parents=True, exist_ok=True)

    @property
    def location(self) -> str:
        return str(self.root)

    def put_file(
        self,
        *,
        kind: str,
        path: str | Path,
        artifact_id: str | None = None,
        metadata: dict | None = None,
    ) -> ArtifactRef:
        source = Path(path).expanduser().resolve()
        if not source.exists() or not source.is_file():
            raise FileNotFoundError(f"Artifact source file not found: {source}")
        digest, size_bytes = _sha256_file(source)
        target = self._blob_path(digest)
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            temp = target.with_suffix(".tmp")
            shutil.copyfile(source, temp)
            os.replace(temp, target)
        merged_metadata = dict(metadata or {})
        merged_metadata.update(
            {
                "sha256": digest,
                "size_bytes": size_bytes,
                "filename": source.name,
                "storage": {
                    "backend": "file",
                    "content_addressed": True,
                },
            }
        )
        return ArtifactRef(
            id=artifact_id or f"artifact_{digest[:12]}",
            kind=kind,
            uri=f"{FALA_ARTIFACT_SCHEME}://sha256/{digest}",
            metadata=merged_metadata,
        )

    def put_fileobj(
        self,
        *,
        kind: str,
        fileobj: BinaryIO,
        filename: str,
        artifact_id: str | None = None,
        metadata: dict | None = None,
    ) -> ArtifactRef:
        temp_dir = self.root / "tmp"
        temp_dir.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        size_bytes = 0
        temp = temp_dir / f"upload-{os.getpid()}-{id(fileobj)}.tmp"
        with temp.open("wb") as handle:
            while True:
                chunk = fileobj.read(1024 * 1024)
                if not chunk:
                    break
                size_bytes += len(chunk)
                digest.update(chunk)
                handle.write(chunk)
        hex_digest = digest.hexdigest()
        target = self._blob_path(hex_digest)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            temp.unlink(missing_ok=True)
        else:
            os.replace(temp, target)
        merged_metadata = dict(metadata or {})
        merged_metadata.update(
            {
                "sha256": hex_digest,
                "size_bytes": size_bytes,
                "filename": filename,
                "storage": {
                    "backend": "file",
                    "content_addressed": True,
                },
            }
        )
        return ArtifactRef(
            id=artifact_id or f"artifact_{hex_digest[:12]}",
            kind=kind,
            uri=f"{FALA_ARTIFACT_SCHEME}://sha256/{hex_digest}",
            metadata=merged_metadata,
        )

    def resolve(self, artifact: ArtifactRef) -> Path:
        parsed = urlparse(artifact.uri)
        if parsed.scheme != FALA_ARTIFACT_SCHEME:
            raise ValueError("Not a Fala artifact URI")
        if parsed.netloc != "sha256":
            raise ValueError(f"Unsupported artifact digest algorithm: {parsed.netloc!r}")
        digest = parsed.path.strip("/")
        if not digest or any(char not in "0123456789abcdef" for char in digest.lower()):
            raise ValueError("Invalid artifact digest")
        path = self._blob_path(digest.lower()).resolve()
        root = self.root.resolve()
        if not _is_relative_to(path, root):
            raise PermissionError("Artifact path escapes artifact store root")
        if not path.exists() or not path.is_file():
            raise FileNotFoundError("Stored artifact blob not found")
        return path

    def open(self, artifact: ArtifactRef) -> BinaryIO:
        return self.resolve(artifact).open("rb")

    def list_blobs(self) -> list[ArtifactBlobInfo]:
        blob_root = self.root / "blobs" / "sha256"
        if not blob_root.exists():
            return []
        blobs: list[ArtifactBlobInfo] = []
        for path in sorted(blob_root.glob("*/*")):
            if not path.is_file():
                continue
            digest = path.name.lower()
            if len(digest) != 64 or any(
                char not in "0123456789abcdef" for char in digest
            ):
                continue
            blobs.append(
                ArtifactBlobInfo(
                    digest=digest,
                    location=str(path.resolve()),
                    size_bytes=path.stat().st_size,
                )
            )
        return blobs

    def delete_blobs(self, digests: Iterable[str]) -> list[str]:
        deleted: list[str] = []
        for digest in sorted({item.lower() for item in digests}):
            path = self._blob_path(digest).resolve()
            root = self.root.resolve()
            if not _is_relative_to(path, root):
                raise PermissionError("Artifact path escapes artifact store root")
            if not path.exists():
                continue
            if not path.is_file():
                raise FileNotFoundError("Stored artifact blob is not a file")
            path.unlink()
            deleted.append(digest)
            _remove_empty_parent_dirs(path.parent, stop_at=root / "blobs" / "sha256")
        return deleted

    def _blob_path(self, digest: str) -> Path:
        return self.root / "blobs" / "sha256" / digest[:2] / digest


class MemoryArtifactStore:
    def __init__(self, location: str = "memory://fala-artifacts") -> None:
        self._location = location
        self._blobs: dict[str, bytes] = {}
        self._materialized_root = Path(tempfile.mkdtemp(prefix="fala-artifacts-"))

    @property
    def location(self) -> str:
        return self._location

    def put_file(
        self,
        *,
        kind: str,
        path: str | Path,
        artifact_id: str | None = None,
        metadata: dict | None = None,
    ) -> ArtifactRef:
        source = Path(path).expanduser().resolve()
        if not source.exists() or not source.is_file():
            raise FileNotFoundError(f"Artifact source file not found: {source}")
        return self._put_bytes(
            kind=kind,
            data=source.read_bytes(),
            filename=source.name,
            artifact_id=artifact_id,
            metadata=metadata,
        )

    def put_fileobj(
        self,
        *,
        kind: str,
        fileobj: BinaryIO,
        filename: str,
        artifact_id: str | None = None,
        metadata: dict | None = None,
    ) -> ArtifactRef:
        chunks: list[bytes] = []
        while True:
            chunk = fileobj.read(1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        return self._put_bytes(
            kind=kind,
            data=b"".join(chunks),
            filename=filename,
            artifact_id=artifact_id,
            metadata=metadata,
        )

    def open(self, artifact: ArtifactRef) -> BinaryIO:
        digest = _digest_from_ref(artifact)
        try:
            return BytesIO(self._blobs[digest])
        except KeyError as exc:
            raise FileNotFoundError("Stored artifact blob not found") from exc

    def resolve(self, artifact: ArtifactRef) -> Path:
        digest = _digest_from_ref(artifact)
        try:
            data = self._blobs[digest]
        except KeyError as exc:
            raise FileNotFoundError("Stored artifact blob not found") from exc
        target = self._blob_path(digest)
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            target.write_bytes(data)
        return target

    def list_blobs(self) -> list[ArtifactBlobInfo]:
        return [
            ArtifactBlobInfo(
                digest=digest,
                location=f"{self.location}/sha256/{digest}",
                size_bytes=len(data),
            )
            for digest, data in sorted(self._blobs.items())
        ]

    def delete_blobs(self, digests: Iterable[str]) -> list[str]:
        deleted: list[str] = []
        for digest in sorted({item.lower() for item in digests}):
            if digest not in self._blobs:
                continue
            del self._blobs[digest]
            self._blob_path(digest).unlink(missing_ok=True)
            deleted.append(digest)
        return deleted

    def _put_bytes(
        self,
        *,
        kind: str,
        data: bytes,
        filename: str,
        artifact_id: str | None,
        metadata: dict | None,
    ) -> ArtifactRef:
        digest = hashlib.sha256(data).hexdigest()
        self._blobs.setdefault(digest, data)
        merged_metadata = dict(metadata or {})
        merged_metadata.update(
            {
                "sha256": digest,
                "size_bytes": len(data),
                "filename": filename,
                "storage": {
                    "backend": "memory",
                    "content_addressed": True,
                },
            }
        )
        return ArtifactRef(
            id=artifact_id or f"artifact_{digest[:12]}",
            kind=kind,
            uri=f"{FALA_ARTIFACT_SCHEME}://sha256/{digest}",
            metadata=merged_metadata,
        )

    def _blob_path(self, digest: str) -> Path:
        return self._materialized_root / "blobs" / "sha256" / digest[:2] / digest


class S3ArtifactStore:
    """Content-addressed artifact store backed by S3-compatible object storage."""

    def __init__(
        self,
        target: str,
        *,
        client: S3Client | None = None,
        materialized_root: str | Path | None = None,
    ) -> None:
        parsed = urlparse(target)
        if parsed.scheme != "s3":
            raise ValueError("S3 artifact store target must use s3://")
        if not parsed.netloc:
            raise ValueError("S3 artifact store target must include a bucket")
        self.bucket = parsed.netloc
        self.prefix = unquote(parsed.path).strip("/")
        self._location = _s3_location(self.bucket, self.prefix)
        self._client = client or _default_s3_client()
        self._materialized_root = _resolve_root(
            materialized_root
            or Path(tempfile.mkdtemp(prefix="fala-s3-artifacts-"))
        )

    @property
    def location(self) -> str:
        return self._location

    def put_file(
        self,
        *,
        kind: str,
        path: str | Path,
        artifact_id: str | None = None,
        metadata: dict | None = None,
    ) -> ArtifactRef:
        source = Path(path).expanduser().resolve()
        if not source.exists() or not source.is_file():
            raise FileNotFoundError(f"Artifact source file not found: {source}")
        digest, size_bytes = _sha256_file(source)
        key = self._blob_key(digest)
        if not self._object_exists(key):
            with source.open("rb") as handle:
                self._client.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=handle,
                    Metadata={"sha256": digest},
                )
        return self._artifact_ref(
            kind=kind,
            digest=digest,
            size_bytes=size_bytes,
            filename=source.name,
            artifact_id=artifact_id,
            metadata=metadata,
            key=key,
        )

    def put_fileobj(
        self,
        *,
        kind: str,
        fileobj: BinaryIO,
        filename: str,
        artifact_id: str | None = None,
        metadata: dict | None = None,
    ) -> ArtifactRef:
        data = fileobj.read()
        digest = hashlib.sha256(data).hexdigest()
        key = self._blob_key(digest)
        if not self._object_exists(key):
            self._client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=data,
                Metadata={"sha256": digest},
            )
        return self._artifact_ref(
            kind=kind,
            digest=digest,
            size_bytes=len(data),
            filename=filename,
            artifact_id=artifact_id,
            metadata=metadata,
            key=key,
        )

    def open(self, artifact: ArtifactRef) -> BinaryIO:
        digest = _digest_from_ref(artifact)
        try:
            response = self._client.get_object(
                Bucket=self.bucket,
                Key=self._blob_key(digest),
            )
        except Exception as exc:
            raise FileNotFoundError("Stored artifact blob not found") from exc
        body = response["Body"]
        data = body.read() if hasattr(body, "read") else body
        return BytesIO(data)

    def resolve(self, artifact: ArtifactRef) -> Path:
        digest = _digest_from_ref(artifact)
        target = self._blob_path(digest)
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            with self.open(artifact) as handle:
                target.write_bytes(handle.read())
        return target

    def list_blobs(self) -> list[ArtifactBlobInfo]:
        prefix = self._blob_prefix()
        blobs: list[ArtifactBlobInfo] = []
        token: str | None = None
        while True:
            kwargs: dict[str, Any] = {
                "Bucket": self.bucket,
                "Prefix": prefix,
            }
            if token:
                kwargs["ContinuationToken"] = token
            response = self._client.list_objects_v2(**kwargs)
            for item in response.get("Contents", []) or []:
                key = str(item.get("Key") or "")
                digest = Path(key).name.lower()
                if len(digest) != 64 or any(
                    char not in "0123456789abcdef" for char in digest
                ):
                    continue
                blobs.append(
                    ArtifactBlobInfo(
                        digest=digest,
                        location=f"s3://{self.bucket}/{key}",
                        size_bytes=int(item.get("Size") or 0),
                    )
                )
            if not response.get("IsTruncated"):
                break
            token = response.get("NextContinuationToken")
            if not token:
                break
        return sorted(blobs, key=lambda item: item.digest)

    def delete_blobs(self, digests: Iterable[str]) -> list[str]:
        normalized = [
            digest.lower()
            for digest in sorted(set(digests))
            if _valid_sha256_digest(digest)
        ]
        deleted: list[str] = []
        for chunk in _chunks(normalized, 1000):
            if not chunk:
                continue
            objects = [{"Key": self._blob_key(digest)} for digest in chunk]
            self._client.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": objects, "Quiet": True},
            )
            deleted.extend(chunk)
            for digest in chunk:
                self._blob_path(digest).unlink(missing_ok=True)
        return deleted

    def _artifact_ref(
        self,
        *,
        kind: str,
        digest: str,
        size_bytes: int,
        filename: str,
        artifact_id: str | None,
        metadata: dict | None,
        key: str,
    ) -> ArtifactRef:
        merged_metadata = dict(metadata or {})
        merged_metadata.update(
            {
                "sha256": digest,
                "size_bytes": size_bytes,
                "filename": filename,
                "storage": {
                    "backend": "s3",
                    "bucket": self.bucket,
                    "key": key,
                    "content_addressed": True,
                },
            }
        )
        return ArtifactRef(
            id=artifact_id or f"artifact_{digest[:12]}",
            kind=kind,
            uri=f"{FALA_ARTIFACT_SCHEME}://sha256/{digest}",
            metadata=merged_metadata,
        )

    def _object_exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self.bucket, Key=key)
        except Exception:
            return False
        return True

    def _blob_prefix(self) -> str:
        parts = [self.prefix, "blobs", "sha256"]
        return "/".join(part.strip("/") for part in parts if part.strip("/"))

    def _blob_key(self, digest: str) -> str:
        digest = digest.lower()
        return f"{self._blob_prefix()}/{digest[:2]}/{digest}"

    def _blob_path(self, digest: str) -> Path:
        return self._materialized_root / "blobs" / "sha256" / digest[:2] / digest


def create_artifact_store(target: str | Path | None = None) -> ArtifactStore:
    target_value = os.fspath(
        target
        or os.environ.get("FALA_ARTIFACT_STORE")
        or os.environ.get("FALA_ARTIFACT_STORE_ROOT")
        or Path.cwd() / ".flow-runs" / "artifact-store"
    )
    parsed = urlparse(target_value)
    if parsed.scheme == "memory":
        return MemoryArtifactStore(target_value)
    if parsed.scheme == "s3":
        return S3ArtifactStore(target_value)
    if parsed.scheme == "file":
        return FileArtifactStore(unquote(parsed.path))
    if not parsed.scheme:
        return FileArtifactStore(target_value)
    raise ValueError(f"Unsupported Fala artifact store target: {target_value}")


def is_fala_artifact_uri(uri: str) -> bool:
    return urlparse(uri).scheme == FALA_ARTIFACT_SCHEME


def digest_from_fala_artifact_uri(uri: str) -> str | None:
    parsed = urlparse(uri)
    if parsed.scheme != FALA_ARTIFACT_SCHEME or parsed.netloc != "sha256":
        return None
    digest = parsed.path.strip("/").lower()
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        return None
    return digest


def _digest_from_ref(artifact: ArtifactRef) -> str:
    digest = digest_from_fala_artifact_uri(artifact.uri)
    if digest is None:
        raise ValueError("Not a supported Fala artifact URI")
    return digest


def local_path_from_uri(uri: str) -> Path | None:
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        return Path(unquote(parsed.path)).expanduser().resolve()
    if not parsed.scheme:
        return Path(uri).expanduser().resolve()
    return None


def _sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            size += len(chunk)
            digest.update(chunk)
    return digest.hexdigest(), size


def _resolve_root(value: str | Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return path.resolve()


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _remove_empty_parent_dirs(path: Path, *, stop_at: Path) -> None:
    stop_at = stop_at.resolve()
    current = path.resolve()
    while _is_relative_to(current, stop_at) and current != stop_at:
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent


def _default_s3_client() -> S3Client:
    try:
        import boto3
    except ImportError as exc:  # pragma: no cover - optional dependency guard
        raise RuntimeError(
            "S3 artifact stores require boto3. Install fala[s3] or provide "
            "an explicit S3-compatible client."
        ) from exc
    return boto3.client("s3")


def _s3_location(bucket: str, prefix: str) -> str:
    return f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}"


def _valid_sha256_digest(value: str) -> bool:
    digest = value.lower()
    return len(digest) == 64 and all(char in "0123456789abcdef" for char in digest)


def _chunks(items: list[str], size: int) -> Iterable[list[str]]:
    for index in range(0, len(items), size):
        yield items[index:index + size]
