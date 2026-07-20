"""Optional projection rebuild ops (not Essential Fala).

Lightweight put/get/list remain on NativeDomainStore; rebuild is ops surface.
"""

from std.collections import List
from fala.sqlite import SQLiteError
from fala.domain import Projection
from fala.journal import EventInput
from fala.domain_store import NativeDomainStore, ProjectionRebuildResult


def rebuild_projection(
    mut store: NativeDomainStore, run_id: String, name: String, updated_at: String = ""
) raises SQLiteError -> Projection:
    return store.rebuild_projection(run_id, name, updated_at)


def rebuild_projections(
    mut store: NativeDomainStore, run_id: String, names: List[String], updated_at: String = ""
) raises SQLiteError -> List[Projection]:
    return store.rebuild_projections(run_id, names, updated_at)


def rebuild_projections_with_command(
    mut store: NativeDomainStore,
    run_id: String,
    names: List[String],
    command_id: String,
    command_type: String,
    idempotency_key: String,
    created_at: String,
    updated_at: String = "",
    events: List[EventInput] = List[EventInput](),
    actor: String = "",
    correlation_id: String = "",
    causation_id: String = "",
) raises SQLiteError -> ProjectionRebuildResult:
    return store.rebuild_projections_with_command(
        run_id, names, command_id, command_type, idempotency_key, created_at,
        updated_at, events, actor, correlation_id, causation_id,
    )
