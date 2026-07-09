from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from fala.cache import (
    CacheStats,
    entry_path,
    export_cache_stats,
    record_cache_hit,
    record_cache_miss,
    render_cache_stats_json,
    render_warm_cache_summary,
    store_entry,
    warm_cache_summary,
)


def _read_source_id(text: str) -> str:
    return json.loads(text)["source"]


def _payload(source: str, value: int) -> str:
    return json.dumps({"source": source, "value": value})


def _store(cache_dir: Path, key: str, source: str, value: int) -> Path:
    return store_entry(
        cache_dir,
        key=key,
        payload=_payload(source, value),
        source_id=source,
        read_source_id=_read_source_id,
    )


class CacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.cache_dir = Path(self._tmp.name) / "cache"

    def test_store_entry_writes_payload_and_counts_store(self) -> None:
        path = _store(self.cache_dir, "k1", "a.docx", 1)
        self.assertEqual(path, entry_path(self.cache_dir, "k1"))
        self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["value"], 1)
        stats = export_cache_stats(self.cache_dir)
        self.assertEqual(stats["stores"], 1)
        self.assertEqual(stats["invalidations"], 0)
        self.assertEqual(stats["entry_count"], 1)

    def test_store_entry_invalidates_stale_entries_of_same_source(self) -> None:
        _store(self.cache_dir, "old", "a.docx", 1)
        _store(self.cache_dir, "other", "b.docx", 2)
        _store(self.cache_dir, "new", "a.docx", 3)
        self.assertFalse(entry_path(self.cache_dir, "old").exists())
        self.assertTrue(entry_path(self.cache_dir, "other").exists())
        self.assertTrue(entry_path(self.cache_dir, "new").exists())
        stats = export_cache_stats(self.cache_dir)
        self.assertEqual(stats["stores"], 3)
        self.assertEqual(stats["invalidations"], 1)
        self.assertEqual(stats["entry_count"], 2)

    def test_restore_under_same_key_does_not_invalidate_itself(self) -> None:
        _store(self.cache_dir, "k1", "a.docx", 1)
        path = _store(self.cache_dir, "k1", "a.docx", 2)
        self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["value"], 2)
        stats = export_cache_stats(self.cache_dir)
        self.assertEqual(stats["stores"], 2)
        self.assertEqual(stats["invalidations"], 0)
        self.assertEqual(stats["entry_count"], 1)

    def test_hit_miss_counters_and_hit_rate(self) -> None:
        record_cache_hit(self.cache_dir)
        record_cache_hit(self.cache_dir)
        record_cache_miss(self.cache_dir)
        stats = export_cache_stats(self.cache_dir)
        self.assertEqual(stats["hits"], 2)
        self.assertEqual(stats["misses"], 1)
        self.assertEqual(stats["hit_rate"], round(2 / 3, 4))
        self.assertEqual(stats["entry_count"], 0)

    def test_export_stats_on_missing_dir(self) -> None:
        stats = export_cache_stats(self.cache_dir)
        self.assertEqual(
            stats,
            {
                "hits": 0,
                "misses": 0,
                "stores": 0,
                "invalidations": 0,
                "entry_count": 0,
                "total_size_bytes": 0,
                "hit_rate": 0.0,
            },
        )

    def test_render_and_warm_summary(self) -> None:
        record_cache_miss(self.cache_dir)
        rendered = json.loads(render_cache_stats_json(self.cache_dir))
        self.assertEqual(rendered["misses"], 1)
        summary = warm_cache_summary(4, self.cache_dir)
        self.assertEqual(summary["processed_documents"], 4)
        rendered_summary = json.loads(render_warm_cache_summary(4, self.cache_dir))
        self.assertEqual(rendered_summary["processed_documents"], 4)

    def test_stats_file_round_trips_through_model(self) -> None:
        record_cache_hit(self.cache_dir)
        stats = CacheStats.model_validate_json(
            (self.cache_dir / "stats.json").read_text(encoding="utf-8")
        )
        self.assertEqual(stats.hits, 1)


if __name__ == "__main__":
    unittest.main()
