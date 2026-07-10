"""File-backed cache entries with source-scoped invalidation and stats.

A cache directory holds one ``<key>.json`` file per entry plus a
``stats.json`` counter file. The entry format is caller-owned: entries are
stored as opaque text, and invalidation asks the caller -- via
``read_source_id`` -- which source an existing entry belongs to, so storing a
fresh entry drops the stale entries of the same source without Fala knowing
the entry schema.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic import BaseModel


class CacheStats(BaseModel):
    hits: int = 0
    misses: int = 0
    stores: int = 0
    invalidations: int = 0


def entry_path(cache_dir: str | Path, key: str) -> Path:
    return Path(cache_dir) / f"{key}.json"


def store_entry(
    cache_dir: str | Path,
    *,
    key: str,
    payload: str,
    source_id: str,
    read_source_id: Callable[[str], str],
) -> Path:
    """Write one entry, invalidating stale entries of the same source.

    ``read_source_id`` maps an existing entry's text to its source identity;
    entries matching ``source_id`` stored under a different key are removed
    and counted as invalidations. An entry it cannot parse propagates -- a
    cache directory holding foreign files is a caller error, not something to
    guess over.
    """
    root = Path(cache_dir)
    root.mkdir(parents=True, exist_ok=True)
    invalidated = _invalidate_stale_entries(root, source_id, key, read_source_id)
    if invalidated:
        _update_stats(root, invalidations=invalidated)

    path = entry_path(root, key)
    path.write_text(payload, encoding="utf-8")
    _update_stats(root, stores=1)
    return path


def record_cache_hit(cache_dir: str | Path) -> None:
    _update_stats(cache_dir, hits=1)


def record_cache_miss(cache_dir: str | Path) -> None:
    _update_stats(cache_dir, misses=1)


def export_cache_stats(cache_dir: str | Path) -> dict[str, Any]:
    root = Path(cache_dir)
    stats = _load_stats(root)
    entries = (
        sorted(path for path in root.glob("*.json") if path.name != "stats.json")
        if root.exists()
        else []
    )
    total_bytes = sum(path.stat().st_size for path in entries)
    return {
        "hits": stats.hits,
        "misses": stats.misses,
        "stores": stats.stores,
        "invalidations": stats.invalidations,
        "entry_count": len(entries),
        "total_size_bytes": total_bytes,
        "hit_rate": _hit_rate(stats.hits, stats.misses),
    }


def render_cache_stats_json(cache_dir: str | Path) -> str:
    return json.dumps(export_cache_stats(cache_dir), indent=2, sort_keys=True)


def warm_cache_summary(processed: int, cache_dir: str | Path) -> dict[str, Any]:
    payload = export_cache_stats(cache_dir)
    payload["processed_impulses"] = processed
    return payload


def render_warm_cache_summary(processed: int, cache_dir: str | Path) -> str:
    return json.dumps(warm_cache_summary(processed, cache_dir), indent=2, sort_keys=True)


def _stats_path(cache_dir: str | Path) -> Path:
    return Path(cache_dir) / "stats.json"


def _load_stats(cache_dir: str | Path) -> CacheStats:
    path = _stats_path(cache_dir)
    if not path.exists():
        return CacheStats()
    return CacheStats.model_validate_json(path.read_text(encoding="utf-8"))


def _update_stats(
    cache_dir: str | Path,
    *,
    hits: int = 0,
    misses: int = 0,
    stores: int = 0,
    invalidations: int = 0,
) -> None:
    root = Path(cache_dir)
    root.mkdir(parents=True, exist_ok=True)
    stats = _load_stats(root)
    updated = stats.model_copy(
        update={
            "hits": stats.hits + hits,
            "misses": stats.misses + misses,
            "stores": stats.stores + stores,
            "invalidations": stats.invalidations + invalidations,
        }
    )
    _stats_path(root).write_text(updated.model_dump_json(indent=2), encoding="utf-8")


def _invalidate_stale_entries(
    cache_dir: Path,
    source_id: str,
    keep_key: str,
    read_source_id: Callable[[str], str],
) -> int:
    removed = 0
    for path in sorted(cache_dir.glob("*.json")):
        if path.name == "stats.json" or path.stem == keep_key:
            continue
        if read_source_id(path.read_text(encoding="utf-8")) == source_id:
            path.unlink()
            removed += 1
    return removed


def _hit_rate(hits: int, misses: int) -> float:
    total = hits + misses
    if total == 0:
        return 0.0
    return round(hits / total, 4)
