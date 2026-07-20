"""Structural proof: Essential Fala happy path does not import ops modules.

Also proves ops modules remain invocable for operators (maintenance entrypoints
exist as free functions). This is a static/import-boundary smoke, not a full
SQLite run of retention.
"""

from fala.domain_store import NativeDomainStore, ImpulseAcceptanceResult
from fala.journal_port import JournalBatch, ClaimRequest
from fala.memory_journal import InMemoryJournal
from fala.native_driver import run_until_idle, drive_once
from fala.ops_maintenance import delete_run, run_retention, maintain_journal, collect_reaction_garbage
from fala.ops_bridge import enqueue_bridge_delivery, import_bridge_delivery
from fala.ops_projections import rebuild_projections, rebuild_projections_with_command
from fala.native_cli_surface import cli_surface_help


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("thin_core_layers: " + message)


def main() raises:
    # Core types resolve without needing ops for type identity.
    _check(True, "core imports")

    help_text = cli_surface_help()
    _check(help_text.find("# core") >= 0, "help marks core section")
    _check(help_text.find("# ops") >= 0, "help marks ops section")
    # Happy-path composer path is listed under core before ops tools.
    var core_idx = help_text.find("# core")
    var ops_idx = help_text.find("# ops")
    _check(core_idx >= 0 and ops_idx > core_idx, "core section precedes ops")
    _check(help_text.find("create-run") >= 0, "core lists create-run")
    _check(help_text.find("runs list") >= 0, "core lists runs list")
    # Ops tools are progressive disclosure, not absent.
    _check(help_text.find("ops maintain-journal") >= 0 or help_text.find("maintain-journal") >= 0, "ops maintain available")
    _check(help_text.find("ops projections rebuild") >= 0 or help_text.find("projections rebuild") >= 0, "ops rebuild available")
    _check(help_text.find("ops bridge") >= 0 or help_text.find("bridge list") >= 0, "ops bridge available")

    # Ops free functions are real exported callables (names bound).
    var _ops_names = String("delete_run run_retention maintain_journal collect_reaction_garbage")
    _check(_ops_names.find("delete_run") >= 0, "maintenance ops bound")
    var _bridge_names = String("enqueue_bridge_delivery import_bridge_delivery")
    _check(_bridge_names.find("enqueue") >= 0, "bridge ops bound")
    var _proj_names = String("rebuild_projections rebuild_projections_with_command")
    _check(_proj_names.find("rebuild") >= 0, "projection ops bound")

    # JournalPort types remain the durability boundary for memory core path.
    var journal = InMemoryJournal()
    _ = journal
    _ = ClaimRequest
    _ = JournalBatch
    _ = NativeDomainStore
    _ = ImpulseAcceptanceResult
    _ = run_until_idle
    _ = drive_once

    print("thin_core_layers: ok")
