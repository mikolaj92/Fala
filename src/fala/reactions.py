from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, BinaryIO, Iterable, Protocol
from urllib.parse import unquote, urlparse

from fala.models import ReactionRef

FALA_REACTION_SCHEME = "fala-reaction"


@dataclass(frozen=True)
class ReactionBlobInfo:
    digest: str
    location: str
    size_bytes: int

    def __iter__(self):
        yield self.digest
        yield self.location
        yield self.size_bytes


class ReactionStore(Protocol):
    @property
    def location(self) -> str:
        ...

    def put_file(
        self,
        *,
        kind: str,
        path: str | Path,
        reaction_id: str | None = None,
        metadata: dict | None = None,
    ) -> ReactionRef:
        ...

    def put_fileobj(
        self,
        *,
        kind: str,
        fileobj: BinaryIO,
        filename: str,
        reaction_id: str | None = None,
        metadata: dict | None = None,
    ) -> ReactionRef:
        ...

    def open(self, reaction: ReactionRef) -> BinaryIO:
        ...

    def resolve(self, reaction: ReactionRef) -> Path:
        ...

    def list_blobs(self) -> list[ReactionBlobInfo]:
        ...

    def delete_blobs(self, digests: Iterable[str]) -> list[str]:
        ...


class FileReactionStore:
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
        reaction_id: str | None = None,
        metadata: dict | None = None,
    ) -> ReactionRef:
        source = Path(path).expanduser().resolve()
        if not source.exists() or not source.is_file():
            raise FileNotFoundError(f"Reaction source file not found: {source}")
        digest, size_bytes = _sha256_file(source)
        target = self._blob_path(digest)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            _ensure_blob_digest(target, digest)
        else:
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
        return ReactionRef(
            id=reaction_id or f"reaction_{digest[:12]}",
            kind=kind,
            uri=f"{FALA_REACTION_SCHEME}://sha256/{digest}",
            metadata=merged_metadata,
        )

    def put_fileobj(
        self,
        *,
        kind: str,
        fileobj: BinaryIO,
        filename: str,
        reaction_id: str | None = None,
        metadata: dict | None = None,
    ) -> ReactionRef:
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
            _ensure_blob_digest(target, hex_digest)
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
        return ReactionRef(
            id=reaction_id or f"reaction_{hex_digest[:12]}",
            kind=kind,
            uri=f"{FALA_REACTION_SCHEME}://sha256/{hex_digest}",
            metadata=merged_metadata,
        )

    def resolve(self, reaction: ReactionRef) -> Path:
        parsed = urlparse(reaction.uri)
        if parsed.scheme != FALA_REACTION_SCHEME:
            raise ValueError("Not a Fala reaction URI")
        if parsed.netloc != "sha256":
            raise ValueError(f"Unsupported reaction digest algorithm: {parsed.netloc!r}")
        digest = parsed.path.strip("/")
        if not digest or any(char not in "0123456789abcdef" for char in digest.lower()):
            raise ValueError("Invalid reaction digest")
        path = self._blob_path(digest.lower()).resolve()
        root = self.root.resolve()
        if not _is_relative_to(path, root):
            raise PermissionError("Reaction path escapes reaction store root")
        if not path.exists() or not path.is_file():
            raise FileNotFoundError("Stored reaction blob not found")
        return path

    def open(self, reaction: ReactionRef) -> BinaryIO:
        return self.resolve(reaction).open("rb")

    def list_blobs(self) -> list[ReactionBlobInfo]:
        blob_root = self.root / "blobs" / "sha256"
        if not blob_root.exists():
            return []
        blobs: list[ReactionBlobInfo] = []
        for path in sorted(blob_root.glob("*/*")):
            if not path.is_file():
                continue
            digest = path.name.lower()
            if len(digest) != 64 or any(
                char not in "0123456789abcdef" for char in digest
            ):
                continue
            blobs.append(
                ReactionBlobInfo(
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
                raise PermissionError("Reaction path escapes reaction store root")
            if not path.exists():
                continue
            if not path.is_file():
                raise FileNotFoundError("Stored reaction blob is not a file")
            path.unlink()
            deleted.append(digest)
            _remove_empty_parent_dirs(path.parent, stop_at=root / "blobs" / "sha256")
        return deleted

    def _blob_path(self, digest: str) -> Path:
        return self.root / "blobs" / "sha256" / digest[:2] / digest


class MemoryReactionStore:
    def __init__(self, location: str = "memory://fala-reactions") -> None:
        self._location = location
        self._blobs: dict[str, bytes] = {}
        self._materialized_root = Path(tempfile.mkdtemp(prefix="fala-reactions-"))

    @property
    def location(self) -> str:
        return self._location

    def put_file(
        self,
        *,
        kind: str,
        path: str | Path,
        reaction_id: str | None = None,
        metadata: dict | None = None,
    ) -> ReactionRef:
        source = Path(path).expanduser().resolve()
        if not source.exists() or not source.is_file():
            raise FileNotFoundError(f"Reaction source file not found: {source}")
        return self._put_bytes(
            kind=kind,
            data=source.read_bytes(),
            filename=source.name,
            reaction_id=reaction_id,
            metadata=metadata,
        )

    def put_fileobj(
        self,
        *,
        kind: str,
        fileobj: BinaryIO,
        filename: str,
        reaction_id: str | None = None,
        metadata: dict | None = None,
    ) -> ReactionRef:
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
            reaction_id=reaction_id,
            metadata=metadata,
        )

    def open(self, reaction: ReactionRef) -> BinaryIO:
        digest = _digest_from_ref(reaction)
        try:
            return BytesIO(self._blobs[digest])
        except KeyError as exc:
            raise FileNotFoundError("Stored reaction blob not found") from exc

    def resolve(self, reaction: ReactionRef) -> Path:
        digest = _digest_from_ref(reaction)
        try:
            data = self._blobs[digest]
        except KeyError as exc:
            raise FileNotFoundError("Stored reaction blob not found") from exc
        target = self._blob_path(digest)
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            target.write_bytes(data)
        return target

    def list_blobs(self) -> list[ReactionBlobInfo]:
        return [
            ReactionBlobInfo(
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
        reaction_id: str | None,
        metadata: dict | None,
    ) -> ReactionRef:
        digest = sha256_digest(data)
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
        return ReactionRef(
            id=reaction_id or f"reaction_{digest[:12]}",
            kind=kind,
            uri=f"{FALA_REACTION_SCHEME}://sha256/{digest}",
            metadata=merged_metadata,
        )

    def _blob_path(self, digest: str) -> Path:
        return self._materialized_root / "blobs" / "sha256" / digest[:2] / digest


def create_reaction_store(target: str | Path | None = None) -> ReactionStore:
    target_value = os.fspath(
        target
        or os.environ.get("FALA_REACTION_STORE")
        or os.environ.get("FALA_REACTION_STORE_ROOT")
        or Path.cwd() / ".correlation-path-runs" / "reaction-store"
    )
    parsed = urlparse(target_value)
    if parsed.scheme == "memory":
        return MemoryReactionStore(target_value)
    if parsed.scheme == "file":
        return FileReactionStore(unquote(parsed.path))
    if not parsed.scheme:
        return FileReactionStore(target_value)
    raise ValueError(f"Unsupported Fala reaction store target: {target_value}")


def is_fala_reaction_uri(uri: str) -> bool:
    return urlparse(uri).scheme == FALA_REACTION_SCHEME


def digest_from_fala_reaction_uri(uri: str) -> str | None:
    parsed = urlparse(uri)
    if parsed.scheme != FALA_REACTION_SCHEME or parsed.netloc != "sha256":
        return None
    digest = parsed.path.strip("/").lower()
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        return None
    return digest


def _digest_from_ref(reaction: ReactionRef) -> str:
    digest = digest_from_fala_reaction_uri(reaction.uri)
    if digest is None:
        raise ValueError("Not a supported Fala reaction URI")
    return digest


def local_path_from_uri(uri: str) -> Path | None:
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        return Path(unquote(parsed.path)).expanduser().resolve()
    if not parsed.scheme:
        return Path(uri).expanduser().resolve()
    return None


def resolve_reaction_local_path(
    ref: ReactionRef | Mapping[str, Any], store_root: str | Path
) -> Path | None:
    """Resolve a reaction ref to a local file path, or ``None`` if unresolvable.

    Tries the content-addressed store rooted at ``store_root`` first; a ref that
    is not a stored blob -- a bare ``file:`` URI or filesystem path -- falls back
    to the URI's own local path via :func:`local_path_from_uri`. Returns ``None``
    for a structurally invalid ref or any non-local URI scheme, so a host-side
    reader can treat "unresolvable" uniformly without re-deriving the blob layout
    (:meth:`FileReactionStore.resolve`) or the reaction URI grammar. ``ref`` may
    be an :class:`ReactionRef` or the raw mapping an effector emitted.
    """
    if isinstance(ref, ReactionRef):
        reaction = ref
    else:
        try:
            reaction = ReactionRef.model_validate(dict(ref))
        except ValueError:
            return None
    try:
        return FileReactionStore(store_root).resolve(reaction)
    except (ValueError, FileNotFoundError, PermissionError):
        pass
    return local_path_from_uri(reaction.uri)


def _sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            size += len(chunk)
            digest.update(chunk)
    return digest.hexdigest(), size


def content_address_file(path: Path) -> str:
    """Return the sha256 hex content address of a file's bytes.

    This is the same digest Fala stamps on every stored reaction
    (``ReactionRef.metadata['sha256']`` and the ``fala-reaction://sha256/<digest>``
    URI). A caller that must content-address a file *before* it enters a reaction
    store -- and so has no ref to read the digest from -- should use this instead
    of re-implementing the hash, keeping its value byte-identical to the store's.
    Stream the file so arbitrarily large inputs stay bounded in memory.
    """
    return _sha256_file(path)[0]


def sha256_digest(data: bytes) -> str:
    """Return the sha256 hex content address of in-memory bytes.

    In-memory counterpart to :func:`content_address_file`; the single source of
    truth for the digest :class:`MemoryReactionStore` (and, for a fully-buffered
    blob, :class:`FileReactionStore`) compute over the same bytes.
    """
    return hashlib.sha256(data).hexdigest()


def content_address_json(payload: Any) -> str:
    """sha256 hex content address of a JSON-able payload.

    Canonical form: ``json.dumps(payload, sort_keys=True, ensure_ascii=False,
    separators=(",", ":"))`` encoded UTF-8 — key order and whitespace never
    affect the digest. Non-serializable payloads raise ``TypeError`` (fail
    closed). Completes the content-address family: :func:`content_address_file`
    (file), :func:`sha256_digest` (bytes), :func:`content_address_json`
    (structured payload).
    """
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return sha256_digest(canonical.encode("utf-8"))


def _ensure_blob_digest(path: Path, expected_digest: str) -> None:
    digest, _size = _sha256_file(path)
    if digest != expected_digest:
        raise ValueError("Stored reaction blob digest mismatch")


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
