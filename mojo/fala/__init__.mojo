"""Native Fala runtime package."""

from .native_process_host import (
    ProcessHost, ProcessHostError, start as start_native_process,
    native_process_host_available,
    PROCESS_OK, PROCESS_INVALID_ARGUMENT, PROCESS_SYSTEM_ERROR,
    PROCESS_TIMED_OUT, PROCESS_CANCELLED,
    PROCESS_RUNNING, PROCESS_EXITED, PROCESS_SIGNALED,
    PROCESS_STATUS_TIMED_OUT, PROCESS_STATUS_CANCELLED, PROCESS_STATUS_ERROR,
)
from .status import ProcessStatus, RunStatus, can_transition_process, can_transition_run
from .json import JsonValue, parse_json, serialize_json, canonical_json_text, quote_json_string
from .adapters import (
    AdapterKind, AdapterError, NativeFunctionRegistry, AdapterSpec,
    EffectorRequest, EffectorResult, SubprocessBoundary,
    resolve_environment, redact_environment, adapter_manifest_json,
    adapter_result_json, adapter_spec_json, adapter_spec_from_json,
    execute_native_function, execute_subprocess,
)
from .native_package import (
    PackageManifestError, PackageEffector, PackageCorrelationPath,
    PackageManifest, load_package_json, serialize_package_json, serialize_correlation_path_json,
    validate_package_json_text,
)
from .toml import parse_toml_value, parse_toml_json
from .package import NativePackage, load_fala_package_json, load_package_toml, load_fala_package_toml
from .graph_tools import graph_expand, graph_validate, graph_fingerprint, graph_diff
from .explain import explain_run
from .graph_rehearsal import rehearse_graph
from .effect_protocol import EffectIntent, EffectObservation, EffectDecision, record_effect_intent, reconcile_effect
from .durable_subprocess import wait_durable_subprocess
from .execution_metadata import validate_usage_json, provenance_json, aggregate_usage
from .context_policy import ResolvedContext, resolve_context
from .journal import (
    RunRow, RunRecord, CommandRow, CommandResult, EventRow, ProcessRow,
    NativeJournal,
)
from .runs import RunLifecycleRecord, RunLifecycle
from .processes import (
    PROCESS_SCHEMA_VERSION, ProcessRecord, process_is_claimable,
    ready_processes, claim_process, actor_can_transition,
    transition_process, retry_is_eligible, retry_backoff_seconds,
    retry_process, expire_process,
)
from .correlation import (
    EffectorNode, ConductionEdge, Readiness, CorrelationGraph,
    effector_ids, conduction_edges, validate_graph, topological_order,
    readiness, CorrelationInputField, CorrelationEffectorSpec,
    CorrelationPathSpec, CorrelationProcessPlan, CorrelationInstantiationPlan,
    CorrelationExecutionState, CorrelationConductionValue, CorrelationBlocked,
    CorrelationAdvancePlan, CorrelationWaitDiagnostic,
    validate_correlation_inputs,
    validate_correlation_input_json, instantiate_correlation_path_plan,
    instantiate_correlation_path, project_conduction,
    advance_correlation_states, replay_safe_advance,
)
from .models import WaitDiagnosticIssue, WaitGraphDiagnostic
from .models_native import (
    RunStatus as NativeRunStatus, ProcessStatus as NativeProcessStatus,
    HomeostatStatus as NativeHomeostatStatus,
    Run as NativeRun, Impulse as NativeImpulse,
    ImpulseType as NativeImpulseType, ImpulseRelation as NativeImpulseRelation,
    RuntimeCommand as NativeRuntimeCommand, RuntimeEvent as NativeRuntimeEvent,
    Association as NativeAssociation, Reaction as NativeReaction,
    Process as NativeProcess, Homeostat as NativeHomeostat,
    Projection as NativeProjection, RuntimeRef as NativeRuntimeRef,
    RuntimeBudget as NativeRuntimeBudget,
)
from .correlation_persistence import (
    CorrelationPersistenceError, CorrelationPersistenceResult,
    validate_correlation_persistence_plan, refresh_correlation_readiness,
    persist_correlation_plan,
)
from .validation import (
    is_valid_runtime_id, validate_runtime_id, validate_positive_number,
    validate_optional_positive_number, validate_known_fields,
    validate_unique_values, validate_unique_ids, validate_known_references,
    validate_no_self_reference, validate_adapter_boundary, validate_acyclic,
)
from .errors import ValidationError
from .reactions import (
    FALA_REACTION_SCHEME, ReactionBlob, sha256_bytes, content_address_json,
    is_fala_reaction_uri, digest_from_fala_reaction_uri,
    reaction_digest_or_empty, put_bytes, resolve_uri, FileReactionStore,
)
from .schema import (
    SCHEMA_VERSION, SCHEMA_SQL, table_names, initialize_schema,
    SchemaStatus, schema_status, migrate_schema, initialize_native_schema,
)
from .schema_contract import ensure_host_journal
from .host_journal import (
    upsert_run_metadata, transition_run, upsert_process,
    complete_waiting_process, record_process_start, record_process_finish,
)
from .domain import (
    Impulse, ImpulseType, ImpulseRelation, Association, Reaction, Homeostat,
    Projection, RuntimeRef, RunRef, EventRef, RuntimeBudget,
    BridgeDelivery
)
from .runtime_policy import (
    RuntimePolicyError, DelegationEnvelope,
    merge_runtime_budgets, budget_allows_request, parse_runtime_budget_json,
)
from .native_driver import (
    DriverResult, AdapterBinding, AllRunDriverResult,
    RunFinalizationResult, RunUntilIdleResult, RunBoundaryResult,
    DelegationCloseResult,
    persist_adapter_binding, persist_adapter_bindings, load_adapter_bindings,
    drive_once, drive_until_idle, drive_ready_batch, drive_bound_queue, drive_all_runs,
    run_until_idle, diagnose_waits, diagnose_wait_graph,
    resume_homeostat, cancel_homeostat, expire_homeostat, reopen_homeostat, rearm_homeostat, transition_homeostat_terminal,
    finalize_run, observe_run_boundary, close_delegations,
    advance_after_terminal, drive_correlation_once, drive_correlation_until_idle,
)
from .correlation_advance import CorrelationAdvanceError, CorrelationAdvanceResult, CorrelationReactionMarker, advance_correlation, validate_correlation_advance_plan
from .correlation_runtime import CorrelationRuntimeResult, run_correlation_path
from .domain_store import NativeDomainStore, ImpulseAcceptanceResult, HomeostatTransitionResult
# Ops layers (optional — not Essential Fala merge-gate):
from .ops_maintenance import (
    RunDeleteCounts, RunRetentionPlan, ReactionGarbageCollectionPlan, JournalMaintenancePlan,
    delete_run, delete_terminal_run, run_retention, maintain_journal, collect_reaction_garbage,
)
from .ops_bridge import (
    BridgeEnqueueResult, enqueue_bridge_delivery, import_bridge_delivery, import_inbox_delivery,
    deliver_bridge_delivery, claim_bridge_delivery, transition_bridge_delivery,
)
from .ops_projections import (
    ProjectionRebuildResult, rebuild_projection, rebuild_projections, rebuild_projections_with_command,
)
from .reaction_effects import (
    ReactionEffectResult, LocalReactionEffectResult,
    accumulate_reaction_effects, persist_reaction_effects,
    materialize_local_reaction_effect,
)
from .bridge_transport import BridgeTransportResult, deliver_local_bridge, deliver_local_bridge_delivery
from .native_cli_surface import cli_surface_help, dispatch_native_command, dispatch_command
from .migration import MigrationReport, legacy_to_native_json, migrate_package_json

# Event-stream core — memory journal path without SQLite.
from .journal_port import (
    StateFact, CommandRecord, EventRecord, CommandUnit, JournalBatch,
    AppendResult, ClaimRequest, ClaimResult, leading_command, leading_idempotency_key,
)
from .memory_journal import InMemoryJournal
from .memory_runtime import MemoryRuntime, ProcessExtra, RunRow as MemoryRunRow
from .memory_driver import MemoryDriver
from .open_journal import open_journal_kind, open_memory_runtime, open_memory_driver, OpenedJournal
from .jsonl_journal import JsonlJournal
from .tee_journal import TeeJournal
from .sqlite_journal_port import SqliteJournalPort
from .domain_packs.signals import (
    SIGNALS_DOMAIN_PACK_ID,
    SIGNALS_READING,
    SignalReading,
    impulse_from_reading,
    channel_association,
    threshold_homeostat,
    evaluate_essential_variable,
    regulation_decision,
    signal_projection,
)
from .domain_packs.splot import (
    SPLOT_DOMAIN_PACK_ID,
    SPLOT_ARBITRATION_CASE,
    SPLOT_JURISDICTION,
    SPLOT_REVIEW,
    SplotArbitrationCase,
    impulse_from_case,
    case_from_impulse,
    jurisdiction_association,
    review_homeostat,
    case_projection,
    process_semantics_json,
)
from .domain_packs.takt import (
    TAKT_DOMAIN_PACK_ID,
    TAKT_CASCADE_REQUEST,
    TAKT_PLANT_LAYER,
    TAKT_ERROR_SIGNAL,
    TAKT_SAFETY_INTERLOCK,
    TAKT_ACTUATION,
    TaktCascadeRequest,
    impulse_from_cascade,
    cascade_from_impulse,
    plant_layer_association,
    error_signal_association,
    safety_interlock_homeostat,
    cascade_projection,
)
