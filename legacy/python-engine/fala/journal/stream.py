"""Stream composition helpers for nested multi-Fala journal batches.

v1 multi-Fala still uses bridge deliveries for semantic merge (see
EVENT_STREAM_CORE.md). These helpers only attach composition metadata and
build an export-oriented ``stream.merged`` envelope for debugging / pipe
handoff — they do not remap child events into the parent run.
"""

from __future__ import annotations

from typing import Any

from fala.journal.types import JournalBatch

STREAM_MERGED_KIND = "stream.merged"
STREAM_MERGED_VERSION = 1


def nest_child_batch(
    child: JournalBatch,
    *,
    parent_stream_id: str | None,
    parent_process_id: str | None,
    stream_id: str | None = None,
) -> JournalBatch:
    """Return a copy of ``child`` tagged as a subtree of a parent stream/process."""
    return child.model_copy(
        update={
            "stream_id": stream_id if stream_id is not None else child.stream_id,
            "parent_stream_id": parent_stream_id,
            "parent_process_id": parent_process_id,
        }
    )


def nest_child_batches(
    children: list[JournalBatch],
    *,
    parent_stream_id: str | None,
    parent_process_id: str | None,
    stream_id: str | None = None,
) -> list[JournalBatch]:
    return [
        nest_child_batch(
            batch,
            parent_stream_id=parent_stream_id,
            parent_process_id=parent_process_id,
            stream_id=stream_id,
        )
        for batch in children
    ]


def stream_merged_envelope(
    parent_batches: list[JournalBatch],
    child_batches: list[JournalBatch],
    *,
    parent_process_id: str,
    parent_stream_id: str | None = None,
    child_stream_id: str | None = None,
) -> dict[str, Any]:
    """Build a ``stream.merged`` export document (debug / handoff, not a Journal unit)."""
    resolved_parent_stream = parent_stream_id
    if resolved_parent_stream is None and parent_batches:
        resolved_parent_stream = parent_batches[0].stream_id

    nested = nest_child_batches(
        child_batches,
        parent_stream_id=resolved_parent_stream,
        parent_process_id=parent_process_id,
        stream_id=child_stream_id,
    )
    return {
        "v": STREAM_MERGED_VERSION,
        "kind": STREAM_MERGED_KIND,
        "parent_process_id": parent_process_id,
        "parent_stream_id": resolved_parent_stream,
        "parent": [batch.model_dump(mode="json") for batch in parent_batches],
        "children": [batch.model_dump(mode="json") for batch in nested],
    }


def flatten_stream_merged(envelope: dict[str, Any]) -> list[JournalBatch]:
    """Flatten a ``stream.merged`` envelope back into parent then nested child batches."""
    if envelope.get("kind") != STREAM_MERGED_KIND:
        raise ValueError(
            f"Expected kind {STREAM_MERGED_KIND!r}, got {envelope.get('kind')!r}"
        )
    parent = [
        JournalBatch.model_validate(item) for item in envelope.get("parent") or []
    ]
    children = [
        JournalBatch.model_validate(item) for item in envelope.get("children") or []
    ]
    return parent + children


__all__ = [
    "STREAM_MERGED_KIND",
    "STREAM_MERGED_VERSION",
    "flatten_stream_merged",
    "nest_child_batch",
    "nest_child_batches",
    "stream_merged_envelope",
]
