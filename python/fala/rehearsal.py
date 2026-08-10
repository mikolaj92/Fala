"""Public, domain-blind rehearsal of journal retention on a safe snapshot."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import stat
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

from fala.host import maintain_journal


def _identity(path: Path) -> dict[str, Any]:
    try:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags)
    except FileNotFoundError:
        return {"present": False}
    try:
        first = os.fstat(fd)
        if not stat.S_ISREG(first.st_mode):
            raise RuntimeError(f"fala rehearsal: not a regular file: {path}")
        digest = hashlib.sha256()
        while True:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk: break
            digest.update(chunk)
        last = os.fstat(fd)
        if (first.st_dev, first.st_ino, first.st_size, first.st_mtime_ns) != (last.st_dev, last.st_ino, last.st_size, last.st_mtime_ns):
            raise RuntimeError(f"fala rehearsal: file changed while observed: {path}")
        return {"present": True, "device": first.st_dev, "inode": first.st_ino,
                "size_bytes": first.st_size, "mtime_ns": first.st_mtime_ns,
                "sha256": digest.hexdigest()}
    finally:
        os.close(fd)


def _source_set(source: Path) -> dict[str, dict[str, Any]]:
    result = {name: _identity(path) for name, path in (
        ("main", source), ("wal", Path(str(source) + "-wal")), ("shm", Path(str(source) + "-shm")))}
    # SQLite readers legitimately maintain transient lock bytes and mtime in SHM;
    # its durable identity contract is presence/device/inode, not content.
    if result["shm"].get("present"):
        result["shm"] = {key: result["shm"][key] for key in ("present", "device", "inode")}
    return result


def _write_exclusive(path: Path, data: bytes, mode: int = 0o600) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, mode)
    try:
        view = memoryview(data)
        while view:
            view = view[os.write(fd, view):]
        os.fsync(fd)
    finally:
        os.close(fd)


def _policy(value: Mapping[str, Any]) -> tuple[float, int, int]:
    if not isinstance(value, Mapping):
        raise TypeError("fala rehearsal: policy must be a mapping")
    unknown = set(value) - {"older_than_days", "keep_last", "safety_margin_bytes"}
    if unknown: raise ValueError(f"fala rehearsal: unknown policy keys: {sorted(unknown)!r}")
    age = value.get("older_than_days")
    keep = value.get("keep_last", -1)
    margin = value.get("safety_margin_bytes", 16 * 1024 * 1024)
    if isinstance(age, bool) or not isinstance(age, (int, float)) or not __import__("math").isfinite(float(age)) or age < 0:
        raise ValueError("fala rehearsal: older_than_days must be finite and non-negative")
    if isinstance(keep, bool) or not isinstance(keep, int) or keep < -1:
        raise ValueError("fala rehearsal: keep_last must be -1 or non-negative")
    if isinstance(margin, bool) or not isinstance(margin, int) or margin < 0:
        raise ValueError("fala rehearsal: safety_margin_bytes must be non-negative")
    return float(age), keep, margin


def rehearse_journal_retention(source: str | Path, destination: str | Path, policy: Mapping[str, Any]) -> dict[str, Any]:
    """Snapshot and rehearse retention without mutating the live journal.

    The output directory is created exclusively at mode 0700. Failures retain
    ``.incomplete``. The source's main/WAL/SHM identities must remain stable
    across backup, validation, and the one dry-run maintenance call.
    """
    age, keep, margin = _policy(policy)
    requested_source = Path(source).expanduser().absolute()
    try:
        source_lstat = os.lstat(requested_source)
    except OSError as exc:
        raise FileNotFoundError(f"fala rehearsal: source unavailable: {requested_source}") from exc
    if stat.S_ISLNK(source_lstat.st_mode) or not stat.S_ISREG(source_lstat.st_mode):
        raise ValueError("fala rehearsal: source must be a non-symlink regular file")
    source_path = requested_source
    parent = Path(destination).expanduser().parent.resolve(strict=True)
    output = parent / Path(destination).name
    os.mkdir(output, 0o700)
    os.chmod(output, 0o700)
    marker = output / ".incomplete"
    _write_exclusive(marker, b"Fala retention rehearsal incomplete\n")
    snapshot = output / "journal.snapshot.sqlite"
    try:
        before = _source_set(source_path)
        footprint = sum(x.get("size_bytes", 0) for x in before.values())
        free_pre = shutil.disk_usage(output).free
        required = footprint + margin
        if free_pre < required:
            raise RuntimeError(f"fala rehearsal: insufficient pre-backup space ({free_pre} < {required})")
        # Reserve the pathname exclusively before SQLite opens it. The private
        # directory prevents an untrusted path replacement between these steps.
        snap_fd = os.open(snapshot, os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
        snap_stat = os.fstat(snap_fd); os.close(snap_fd)
        uri = f"file:{quote(str(source_path), safe='/')}?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=30.0) as live:
            live.execute("PRAGMA query_only=ON")
            live.execute("BEGIN")
            live.execute("SELECT count(*) FROM sqlite_master").fetchone()  # pin live+committed WAL
            with sqlite3.connect(snapshot) as copy:
                live.backup(copy)
        current_snap = os.stat(snapshot, follow_symlinks=False)
        if (current_snap.st_dev, current_snap.st_ino) != (snap_stat.st_dev, snap_stat.st_ino):
            raise RuntimeError("fala rehearsal: snapshot path identity changed")
        if before["main"].get("device") == current_snap.st_dev and before["main"].get("inode") == current_snap.st_ino:
            raise RuntimeError("fala rehearsal: snapshot aliases source")
        os.chmod(snapshot, 0o600)
        with sqlite3.connect(f"file:{quote(str(snapshot), safe='/')}?mode=ro", uri=True) as check:
            integrity = [row[0] for row in check.execute("PRAGMA integrity_check")]
            foreign_keys = [list(row) for row in check.execute("PRAGMA foreign_key_check")]
        if integrity != ["ok"]: raise RuntimeError(f"fala rehearsal: snapshot integrity check failed: {integrity!r}")
        if foreign_keys: raise RuntimeError(f"fala rehearsal: snapshot foreign-key check failed: {foreign_keys!r}")
        free_post = shutil.disk_usage(output).free
        if free_post < margin:
            raise RuntimeError(f"fala rehearsal: insufficient post-backup space ({free_post} < {margin})")
        # Exactly one policy evaluation, always against the snapshot.
        snapshot_before = _identity(snapshot)
        dry_plan = maintain_journal(snapshot, older_than_days=age, keep_last=keep, vacuum=False, dry_run=True)
        snapshot_after = _identity(snapshot)
        with sqlite3.connect(f"file:{quote(str(snapshot), safe='/')}?mode=ro", uri=True) as check:
            post_integrity = [row[0] for row in check.execute("PRAGMA integrity_check")]
            post_foreign_keys = [list(row) for row in check.execute("PRAGMA foreign_key_check")]
        if post_integrity != ["ok"] or post_foreign_keys:
            raise RuntimeError("fala rehearsal: post-plan snapshot validation failed")
        if snapshot_before != snapshot_after:
            raise RuntimeError("fala rehearsal: dry-run mutated the rehearsal snapshot")
        after = _source_set(source_path)
        if before != after: raise RuntimeError("fala rehearsal: source main/WAL/SHM identity changed")
        result = {"schema_version": 1, "created_at": datetime.now(UTC).isoformat(),
                  "source": str(source_path), "snapshot": str(snapshot),
                  "source_before": before, "source_after": after,
                  "source_footprint_bytes": footprint,
                  "space": {"required_pre_bytes": required, "free_pre_bytes": free_pre,
                            "safety_margin_bytes": margin, "free_post_bytes": free_post},
                  "integrity_check": {"pre": integrity, "post": post_integrity},
                  "foreign_key_check": {"pre": foreign_keys, "post": post_foreign_keys},
                  "snapshot_before": snapshot_before, "snapshot_after": snapshot_after,
                  "policy": {"older_than_days": age, "keep_last": keep},
                  "dry_run": dry_plan, "read_only_rehearsal": True,
                  "authority": "fala.rehearse_journal_retention"}
        encoded = (json.dumps(result, ensure_ascii=False, allow_nan=False, indent=2, sort_keys=True) + "\n").encode()
        _write_exclusive(output / "retention-plan.json", encoded)
        marker.unlink()
        directory_fd = os.open(output, os.O_RDONLY)
        try: os.fsync(directory_fd)
        finally: os.close(directory_fd)
        result["plan_path"] = str(output / "retention-plan.json")
        return result
    except BaseException:
        raise

__all__ = ["rehearse_journal_retention"]
