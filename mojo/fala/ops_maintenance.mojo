"""Optional journal ops: retention, maintenance, reaction GC (not Essential Fala).

Canonical import surface for operators. Bodies delegate to NativeDomainStore.
"""

from std.collections import List
from fala.sqlite import SQLiteError
from fala.domain_store import (
    NativeDomainStore,
    RunDeleteCounts,
    RunRetentionCandidate,
    RunRetentionItem,
    RunRetentionPlan,
    ReactionGarbageCollectionPlan,
    JournalMaintenancePlan,
)


def delete_run(mut store: NativeDomainStore, run_id: String) raises SQLiteError -> RunDeleteCounts:
    return store.delete_run(run_id)


def run_retention(
    mut store: NativeDomainStore,
    before: String,
    statuses: List[String] = List[String](),
    dry_run: Bool = True,
    keep_run_ids: List[String] = List[String](),
) raises SQLiteError -> RunRetentionPlan:
    return store.run_retention(before, statuses, dry_run, keep_run_ids)


def maintain_journal(
    mut store: NativeDomainStore,
    older_than_days: Float64,
    keep_last: Int = -1,
    vacuum: Bool = True,
    dry_run: Bool = True,
    reaction_root: String = "",
) raises SQLiteError -> JournalMaintenancePlan:
    return store.maintain_journal(older_than_days, keep_last, vacuum, dry_run, reaction_root)


def collect_reaction_garbage(
    mut store: NativeDomainStore,
    reaction_root: String,
    run_id: String = "",
    dry_run: Bool = True,
) raises SQLiteError -> ReactionGarbageCollectionPlan:
    return store.collect_reaction_garbage(reaction_root, run_id, dry_run)
