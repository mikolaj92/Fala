"""Python-free deterministic process driver for the native journal."""

from std.collections import List
from std.collections import Dict
from emberjson import Object, Value, to_string

from fala.adapters import (
    AdapterError,
    AdapterKind,
    AdapterSpec,
    EffectorRequest,
    EffectorResult,
    NativeFunctionRegistry,
    adapter_spec_json,
    adapter_spec_from_json,
    execute_manual_homeostat,
    execute_native_function,
    execute_subprocess,
)
from fala.native_process_host import native_process_host_available
from fala.journal import NativeJournal, ProcessRow
from fala.sqlite import SQLiteError
from fala.status import RunStatus
from fala.correlation import CorrelationWaitDiagnostic, CorrelationInstantiationPlan, CorrelationProcessPlan
from fala.models import WaitDiagnosticIssue, WaitGraphDiagnostic
from fala.correlation_advance import advance_correlation

def _empty_wait_graph() -> WaitGraphDiagnostic:
    return WaitGraphDiagnostic(run_id="", impulse_id="", deadlocked=False, deadlocks=List[List[String]](), wait_edges=Dict[String, List[String]](), blocked=List[WaitDiagnosticIssue](), open_homeostats=List[String](), pending=List[String](), ready=List[String](), running=List[String](), waiting=List[String](), retry_wait=List[String](), succeeded=List[String](), failed=List[String](), cancel_requested=List[String](), cancelled=List[String](), timed_out=List[String](), blocked_process_ids=List[String](), reason="", code="")


struct RunUntilIdleResult(Copyable, Movable):
    """Stable bounded-drive outcome over durable process rows."""
    var ok: Bool
    var ticks: Int
    var stopped_reason: String
    var completed: List[ProcessRow]
    var failed: List[ProcessRow]
    var waiting: List[ProcessRow]
    var deadlocked: Bool
    var deadlocks: List[List[String]]
    var wait_diagnostic: WaitGraphDiagnostic

    def __init__(
        out self,
        ok: Bool = True,
        ticks: Int = 0,
        stopped_reason: String = "idle",
        completed: List[ProcessRow] = List[ProcessRow](),
        failed: List[ProcessRow] = List[ProcessRow](),
        waiting: List[ProcessRow] = List[ProcessRow](),
        deadlocked: Bool = False,
        deadlocks: List[List[String]] = List[List[String]](),
        wait_diagnostic: WaitGraphDiagnostic = _empty_wait_graph(),
    ):
        self.ok = ok
        self.ticks = ticks
        self.stopped_reason = stopped_reason
        self.completed = completed.copy()
        self.failed = failed.copy()
        self.waiting = waiting.copy()
        self.deadlocked = deadlocked
        self.deadlocks = deadlocks.copy()
        self.wait_diagnostic = wait_diagnostic.copy()


struct DriverResult(Copyable, Movable):
    """Result of one tick, or the aggregate of a bounded drive."""

    var idle: Bool
    var completed: Bool
    var waiting: Bool
    var failed: Bool
    var timed_out: Bool
    var ticks: Int
    var process_id: String
    var error: AdapterError
    # Preserve every failed transition, including retry_wait rows, so a
    # bounded drive exposes the same attempt history as the reference.
    var failure_rows: List[ProcessRow]

    def __init__(
        out self,
        idle: Bool = False,
        completed: Bool = False,
        waiting: Bool = False,
        failed: Bool = False,
        timed_out: Bool = False,
        ticks: Int = 0,
        process_id: String = "",
        error: AdapterError = AdapterError(),
        failure_rows: List[ProcessRow] = List[ProcessRow](),
    ):
        self.idle = idle
        self.completed = completed
        self.waiting = waiting
        self.failed = failed
        self.timed_out = timed_out
        self.ticks = ticks
        self.process_id = process_id
        self.error = error.copy()
        self.failure_rows = failure_rows.copy()
struct RunFinalizationResult(Copyable, Movable):
    """Durable run boundary derived from every effector row in the run."""
    var status: String
    var reason: String
    var already_terminal: Bool
    var total_count: Int
    var completed_count: Int
    var failed_count: Int
    var waiting_count: Int
    var incomplete_count: Int

    def __init__(
        out self,
        status: String = "",
        reason: String = "",
        already_terminal: Bool = False,
        total_count: Int = 0,
        completed_count: Int = 0,
        failed_count: Int = 0,
        waiting_count: Int = 0,
        incomplete_count: Int = 0,
    ):
        self.status = status
        self.reason = reason
        self.already_terminal = already_terminal
        self.total_count = total_count
        self.completed_count = completed_count
        self.failed_count = failed_count
        self.waiting_count = waiting_count
        self.incomplete_count = incomplete_count
@fieldwise_init
struct RunBoundaryResult(Copyable, Movable):
    """Durable child-run boundary observed from the target journal."""
    var run_id: String
    var status: String
    var derived_status: String
    var process_status_counts: String
    var event_watermark: Int

@fieldwise_init
struct DelegationCloseResult(Copyable, Movable):
    """Deterministic terminal parent rows closed from a child boundary."""
    var closed: List[ProcessRow]

def _boundary_count(rows: List[ProcessRow], status: String) -> Int:
    var count = 0
    for row in rows:
        if row.status == status: count += 1
    return count

def _boundary_counts_json(rows: List[ProcessRow]) -> String:
    # Process statuses are a closed set; emitting in lexical order keeps the
    # boundary stable without relying on map iteration order.
    var result = "{"
    var first = True
    for status in ["cancel_requested", "cancelled", "failed", "pending", "ready", "retry_wait", "running", "succeeded", "timed_out", "waiting"]:
        var count = _boundary_count(rows, status)
        if count > 0:
            if not first: result += ","
            result += _json_quote(status) + ":" + String(count)
            first = False
    result += "}"
    return result

def observe_run_boundary(mut target: NativeJournal, run_id: String) raises SQLiteError -> RunBoundaryResult:
    """Observe a child run using only its durable run/process/event rows."""
    if run_id == "":
        raise SQLiteError(code=1, message="driver: boundary run_id must not be empty")
    var run = target.get_run_record(run_id)
    var rows = target.list_processes(run_id)
    var derived = run.status
    var run_status = RunStatus(run.status)
    if not run_status.is_terminal():
        var failed = False
        var all_succeeded = len(rows) > 0
        var waiting = False
        for row in rows:
            if row.status == "failed" or row.status == "cancelled" or row.status == "timed_out": failed = True
            if row.status != "succeeded": all_succeeded = False
            if row.status == "waiting": waiting = True
        if failed: derived = "failed"
        elif all_succeeded: derived = "completed"
        elif waiting: derived = "waiting"
    var watermark = 0
    var events = target.list_events(run_id)
    if len(events) > 0: watermark = events[len(events) - 1].sequence
    return RunBoundaryResult(
        run_id=run_id,
        status=run.status,
        derived_status=derived,
        process_status_counts=_boundary_counts_json(rows),
        event_watermark=watermark,
    )

def _boundary_json(boundary: RunBoundaryResult) -> String:
    return "{\"run_id\":" + _json_quote(boundary.run_id) + ",\"status\":" + _json_quote(boundary.status) + ",\"derived_status\":" + _json_quote(boundary.derived_status) + ",\"process_status_counts\":" + boundary.process_status_counts + ",\"event_watermark\":" + String(boundary.event_watermark) + "}"

def _delegation_target(output_json: String) raises -> String:
    var parsed = Value(parse_string=output_json)
    if not parsed.is_object(): return ""
    var root = parsed.object().copy()
    if "delivery_id" not in root or "target_run_id" not in root: return ""
    if not root["delivery_id"].is_string() or not root["target_run_id"].is_string(): return ""
    if root["delivery_id"].string() == "" or root["target_run_id"].string() == "": return ""
    return root["target_run_id"].string()

def close_delegations(
    mut parent: NativeJournal,
    mut target: NativeJournal,
    parent_run_id: String,
    actor: String,
    at: String,
) raises SQLiteError -> List[ProcessRow]:
    """Close parent delegation waits from an explicitly supplied target journal."""
    if parent_run_id == "" or actor == "" or at == "":
        raise SQLiteError(code=1, message="driver: delegation closure requires parent run, actor, and timestamp")
    var closed = List[ProcessRow]()
    var waiting = parent.list_processes(parent_run_id, "waiting", "")
    for process in waiting:
        var target_run_id = ""
        try:
            target_run_id = _delegation_target(process.output_json)
        except err:
            target_run_id = ""
        if target_run_id == "": continue
        var boundary: RunBoundaryResult
        try:
            boundary = observe_run_boundary(target, target_run_id)
        except err:
            # The child may not have been delivered yet; preserve the wait.
            continue
        if not RunStatus(boundary.derived_status).is_terminal(): continue
        var boundary_json = _boundary_json(boundary)
        if boundary.derived_status == "completed":
            var output = process.output_json
            try:
                var output_value = Value(parse_string=output)
                if output_value.is_object():
                    var root = output_value.object().copy()
                    root["run.boundary"] = Value(parse_string=boundary_json)
                    output = to_string(root^)
                else:
                    output = "{\"run.boundary\":" + boundary_json + "}"
            except err:
                output = "{\"run.boundary\":" + boundary_json + "}"
            closed.append(parent.complete_process(parent_run_id, process.id, actor, at, output, "{}"))
        else:
            var message = "delegated run " + target_run_id + " ended " + boundary.derived_status
            var error = "{\"type\":\"DelegatedRunNotCompleted\",\"message\":" + _json_quote(message) + ",\"run.boundary\":" + boundary_json + "}"
            closed.append(parent.fail_process(parent_run_id, process.id, actor, at, error))
    return closed^


def _metadata_config_json(metadata: String) -> String:
    """Extract persisted correlation config, defaulting safely to an object."""
    if metadata == "": return "{}"
    try:
        var parsed = Value(parse_string=metadata)
        if not parsed.is_object(): return "{}"
        for pair in parsed.object().items():
            if pair.key == "__correlation_config":
                if pair.value.is_object(): return to_string(pair.value.copy())
                return "{}"
    except e:
        return "{}"
    return "{}"


def _input_config_json(input_json: String) -> String:
    """Extract authored config envelope, preserving explicit invalid values."""
    if input_json == "": return "{}"
    try:
        var parsed = Value(parse_string=input_json)
        if not parsed.is_object(): return "{}"
        for pair in parsed.object().items():
            if pair.key == "config": return to_string(pair.value.copy())
    except e:
        return "{}"
    return "{}"
def _has_input_config(input_json: String) -> Bool:
    """Detect config key presence, including an explicit empty object."""
    if input_json == "": return False
    try:
        var parsed = Value(parse_string=input_json)
        if not parsed.is_object(): return False
        for pair in parsed.object().items():
            if pair.key == "config": return True
    except e:
        return False
    return False


def _effective_config_json(input_json: String, metadata: String) -> String:
    if _has_input_config(input_json): return _input_config_json(input_json)
    return _metadata_config_json(metadata)

def _request_input_json(input_json: String) -> String:
    """Remove adapter/config envelopes before passing authored input onward."""
    if input_json == "": return "{}"
    try:
        var parsed = Value(parse_string=input_json)
        if not parsed.is_object(): return input_json
        var input = Object(capacity=len(parsed.object()))
        for pair in parsed.object().items():
            if pair.key != "adapter" and pair.key != "config":
                input[pair.key] = pair.value.copy()
        return to_string(input^)
    except e:
        return input_json

def _json_quote(value: String) -> String:
    """JSON-quote a string for adapter error payloads.

    Must walk **codepoints** (not bytes). Error messages carry multi-byte UTF-8
    (Polish legal text, paths). Byte-indexed ``value[byte=i]`` aborts Mojo when
    ``i`` is mid-codepoint (Fala#121 follow-up on durable subprocess failures).
    """
    var result = "\""
    for ch in value.codepoint_slices():
        if ch == "\"": result += "\\\""
        elif ch == "\\": result += "\\\\"
        elif ch == "\n": result += "\\n"
        elif ch == "\r": result += "\\r"
        elif ch == "\t": result += "\\t"
        else: result += ch
    result += "\""
    return result^

def _adapter_error_json(error: AdapterError) -> String:
    return "{\"code\":" + _json_quote(error.code) + ",\"message\":" + _json_quote(error.message) + "}"


def _row_claimable(process: ProcessRow, now: String) -> Bool:
    # Expired running rows are selected even on their final attempt so the
    # driver can durably emit the terminal failure transition.
    if process.status == "running":
        return process.lease_owner != "" and process.lease_expires_at != "" and process.lease_expires_at <= now
    if process.attempt >= process.max_attempts: return False
    if process.status == "ready": return True
    if process.status == "retry_wait": return process.available_at <= now
    return False


def _row_before(a: ProcessRow, b: ProcessRow) -> Bool:
    # Match the journal queue index: priority DESC, then due time, then FIFO.
    if a.priority != b.priority: return a.priority > b.priority
    if a.available_at != b.available_at: return a.available_at < b.available_at
    if a.created_at != b.created_at: return a.created_at < b.created_at
    return a.id < b.id


def _retry_due(process: ProcessRow, now: String) -> String:
    """Make retry work available at the transition timestamp.

    The reference retry_process defaults availability to the transition time,
    so a bounded drive can reclaim a retry on its next tick.
    """
    return now
def _has_process_status(rows: List[ProcessRow], candidate: ProcessRow) -> Bool:
    for row in rows:
        if row.id == candidate.id and row.status == candidate.status and row.attempt == candidate.attempt:
            return True
    return False






def resume_homeostat(mut journal: NativeJournal, run_id: String, homeostat_id: String, process_id: String, actor: String, at: String, output_json: String = "{}") raises SQLiteError -> ProcessRow:
    return journal.transition_homeostat_process(run_id, homeostat_id, process_id, "completed", "succeeded", actor, at, output_json, "{}", "homeostat.complete:" + homeostat_id)

def cancel_homeostat(mut journal: NativeJournal, run_id: String, homeostat_id: String, process_id: String, actor: String, at: String, error_json: String = "{}") raises SQLiteError -> ProcessRow:
    return journal.transition_homeostat_process(run_id, homeostat_id, process_id, "cancelled", "cancelled", actor, at, "{}", error_json, "homeostat.cancel:" + homeostat_id)

def expire_homeostat(mut journal: NativeJournal, run_id: String, homeostat_id: String, process_id: String, actor: String, at: String, error_json: String = "{}") raises SQLiteError -> ProcessRow:
    return journal.transition_homeostat_process(run_id, homeostat_id, process_id, "expired", "timed_out", actor, at, "{}", error_json, "homeostat.expire:" + homeostat_id)
def reopen_homeostat(mut journal: NativeJournal, run_id: String, homeostat_id: String, process_id: String, actor: String, at: String, idempotency_key: String = "") raises SQLiteError -> ProcessRow:
    return journal.reopen_homeostat_process(run_id, homeostat_id, process_id, actor, at, idempotency_key)


def rearm_homeostat(
    mut journal: NativeJournal,
    run_id: String,
    homeostat_id: String,
    process_id: String,
    actor: String,
    at: String,
    idempotency_key: String = "",
) raises SQLiteError -> ProcessRow:
    """Atomically re-arm a terminal homeostat on a waiting run (#68).

    Requires the run to be waiting; reopens the homeostat and resets the
    succeeded/cancelled/timed_out process to waiting under one journal batch.
    """
    return journal.reopen_homeostat_process(
        run_id, homeostat_id, process_id, actor, at, idempotency_key, require_waiting_run=True
    )
def transition_homeostat_terminal(
    mut journal: NativeJournal,
    plan: CorrelationInstantiationPlan,
    run_id: String,
    homeostat_id: String,
    process_id: String,
    homeostat_status: String,
    process_status: String,
    actor: String,
    at: String,
    output_json: String = "{}",
    error_json: String = "{}",
    idempotency_key: String = "",
) raises SQLiteError -> ProcessRow:
    """Atomically finish a homeostat, then reconcile its explicit correlation plan."""
    if run_id != plan.run_id:
        raise SQLiteError(code=1, message="driver: homeostat correlation plan run_id differs from transition run_id")
    var row = journal.transition_homeostat_process(
        run_id, homeostat_id, process_id, homeostat_status, process_status,
        actor, at, output_json, error_json, idempotency_key,
    )
    try:
        _ = advance_after_terminal(journal, plan, row.id)
    except err:
        raise SQLiteError(code=1, message="driver: correlation advancement failed: " + String(err))
    return row^
def _dispatch(request: EffectorRequest, registry: NativeFunctionRegistry) -> EffectorResult:
    # Direct callers must observe the same boundary as durable drive_once.
    var preflight = _preflight_adapter(request.adapter)
    if not preflight.is_ok(): return EffectorResult.failure(preflight)
    if request.adapter.kind == AdapterKind.manual_homeostat():
        return execute_manual_homeostat(request)
    if request.adapter.kind == AdapterKind.subprocess():
        return execute_subprocess(request)
    if request.adapter.kind == AdapterKind.native_function():
        return execute_native_function(request, registry)
    return EffectorResult.failure(AdapterError.invalid("unknown adapter kind: " + request.adapter.kind.value))
def _preflight_adapter(adapter: AdapterSpec) -> AdapterError:
    """Validate an adapter before claiming durable work.

    Unsupported transports must be reported without incrementing attempts or
    acquiring a lease; only the native registry, subprocess host, and manual
    homeostat paths are executable in this runtime.
    """
    var validation = adapter.validate()
    if not validation.is_ok(): return validation.copy()
    if adapter.kind == AdapterKind.subprocess() and not native_process_host_available():
        return AdapterError.subprocess_transport_unavailable()
    if adapter.timeout_seconds > 0.0 and adapter.kind != AdapterKind.subprocess():
        return AdapterError.timeout_unavailable()
    return AdapterError.none()

struct AdapterBinding(Copyable, Movable):
    """Explicit durable process-to-adapter mapping for queue driving."""
    var process_id: String
    var adapter: AdapterSpec
    var run_id: String

    def __init__(out self, process_id: String, adapter: AdapterSpec, run_id: String = ""):
        self.process_id = process_id
        self.adapter = adapter.copy()
        self.run_id = run_id

def _metadata_with_binding(metadata: String, encoded: String) raises -> String:
    var root = Object()
    if metadata != "":
        var parsed = Value(parse_string=metadata)
        if parsed.is_object():
            root = parsed.object().copy()
    root["__adapter_binding"] = Value(parse_string=encoded)
    return to_string(root^)


def _binding_from_metadata(metadata: String, process_id: String, run_id: String) raises SQLiteError -> AdapterBinding:
    try:
        var parsed = Value(parse_string=metadata)
        if not parsed.is_object(): raise Error("adapter binding metadata must be an object")
        var root = parsed.object().copy()
        if "__adapter_binding" not in root: raise Error("adapter binding metadata is absent")
        return AdapterBinding(process_id=process_id, adapter=adapter_spec_from_json(to_string(root["__adapter_binding"].copy())), run_id=run_id)
    except err:
        raise SQLiteError(code=1, message="driver: invalid adapter binding metadata")


def persist_adapter_bindings(mut journal: NativeJournal, bindings: List[AdapterBinding], at: String) raises SQLiteError:
    """Atomically persist a complete explicit adapter-binding set."""
    if len(bindings) == 0 or at == "":
        raise SQLiteError(code=2, message="driver: bindings and timestamp must not be empty")
    var encoded = List[String]()
    for binding in bindings:
        if binding.run_id == "" or binding.process_id == "":
            raise SQLiteError(code=2, message="driver: binding run_id and process_id must not be empty")
        var validation = binding.adapter.validate()
        if not validation.is_ok():
            raise SQLiteError(code=2, message=validation.message)
        try:
            encoded.append(adapter_spec_json(binding.adapter))
        except err:
            raise SQLiteError(code=2, message="driver: encode adapter binding failed")
    journal.db.begin_immediate()
    try:
        for index in range(len(bindings)):
            var binding = bindings[index].copy()
            var row = journal.get_process(binding.run_id, binding.process_id)
            var marker_state = _binding_metadata_state(row.metadata)
            if marker_state < 0:
                raise SQLiteError(code=1, message="driver: existing adapter binding metadata is invalid")
            if marker_state > 0:
                var existing_binding = _binding_from_metadata(row.metadata, binding.process_id, binding.run_id)
                var existing_encoded = ""
                try:
                    existing_encoded = adapter_spec_json(existing_binding.adapter)
                except err:
                    raise SQLiteError(code=1, message="driver: existing adapter binding metadata is invalid")
                if existing_encoded != encoded[index]:
                    raise SQLiteError(code=1, message="driver: adapter binding conflict for process")
                continue
            var metadata = ""
            try:
                metadata = _metadata_with_binding(row.metadata, encoded[index])
            except err:
                raise SQLiteError(code=1, message="driver: process metadata is invalid")
            var stmt = journal.db.query("UPDATE processes SET metadata=?,updated_at=? WHERE run_id=? AND id=?")
            stmt.bind_text(1, metadata)
            stmt.bind_text(2, at)
            stmt.bind_text(3, binding.run_id)
            stmt.bind_text(4, binding.process_id)
            _ = stmt.step()
            stmt.close()
            if journal.db.changes() != 1:
                raise SQLiteError(code=1, message="driver: process not found for adapter binding")
        journal.db.commit()
    except err:
        journal.db.rollback()
        raise SQLiteError(code=1, message="driver: persist adapter bindings failed: " + String(err))


def persist_adapter_binding(mut journal: NativeJournal, binding: AdapterBinding, at: String) raises SQLiteError:
    """Persist one explicit mapping through the atomic batch boundary."""
    var bindings = List[AdapterBinding]()
    bindings.append(binding.copy())
    persist_adapter_bindings(journal, bindings^, at)


def _binding_metadata_state(metadata: String) -> Int:
    """Return 1 for a marker, 0 when absent, and -1 when malformed."""
    try:
        var parsed = Value(parse_string=metadata)
        if parsed.is_object() and "__adapter_binding" in parsed.object(): return 1
        return 0
    except err:
        # Invalid JSON containing the reserved marker must fail closed; truly
        # unmarked malformed metadata remains unrelated to adapter bindings.
        if metadata.find("\"__adapter_binding\"") >= 0: return -1
        return 0

def load_adapter_bindings(mut journal: NativeJournal, run_id: String) raises SQLiteError -> List[AdapterBinding]:
    """Reload explicit mappings from process metadata without inferring adapters."""
    if run_id == "": raise SQLiteError(code=2, message="driver: run_id must not be empty")
    var result = List[AdapterBinding]()
    var stmt = journal.db.query("SELECT id,metadata FROM processes WHERE run_id=? ORDER BY id ASC")
    stmt.bind_text(1, run_id)
    while stmt.step():
        var metadata = stmt.column_text(1)
        var marker_state = _binding_metadata_state(metadata)
        if marker_state < 0:
            stmt.close()
            raise SQLiteError(code=1, message="driver: invalid adapter binding metadata")
        if marker_state > 0:
            result.append(_binding_from_metadata(metadata, stmt.column_text(0), run_id)^)
    stmt.close()
    return result^

def _wait_refs(process: ProcessRow, first: String, second: String, third: String) raises SQLiteError -> List[String]:
    var result = List[String]()
    var sources = List[String]()
    sources.append(process.input_json.copy())
    sources.append(process.metadata.copy())
    for source in sources:
        var parsed: Value
        try:
            parsed = Value(parse_string=source)
        except err:
            raise SQLiteError(code=1, message="driver: invalid persisted wait metadata")
        if not parsed.is_object(): continue
        for key in [first, second, third]:
            if key not in parsed.object(): continue
            try:
                var value = parsed.object()[key].copy()
                if value.is_string():
                    result.append(value.string())
                elif value.is_array():
                    for item in value.array():
                        if item.is_string(): result.append(item.string())
            except err:
                raise SQLiteError(code=1, message="driver: invalid persisted wait metadata")
    var unique = List[String]()
    for item in result:
        var seen = False
        for prior in unique:
            if prior == item: seen = True
        if not seen: unique.append(item)
    return unique^

def _wait_cycle_visit(node: String, edges: Dict[String, List[String]], mut path: List[String], mut visiting: Dict[String, Int], mut visited: Dict[String, Bool], mut cycles: List[List[String]]) raises:
    if node in visiting:
        var start = visiting[node]
        var cycle = List[String]()
        for index in range(start, len(path)): cycle.append(path[index])
        var duplicate = False
        for prior in cycles:
            if len(prior) != len(cycle): continue
            var same = True
            for item in cycle:
                var found = False
                for other in prior:
                    if item == other: found = True
                if not found: same = False
            if same: duplicate = True
        if not duplicate: cycles.append(cycle^)
        return
    if node in visited: return
    visiting[node] = len(path)
    path.append(node)
    if node in edges:
        for dependency in edges[node]:
            if dependency in edges: _wait_cycle_visit(dependency, edges, path, visiting, visited, cycles)
    _ = path.pop()
    visiting.pop(node)
    visited[node] = True

def _wait_cycles(edges: Dict[String, List[String]]) raises -> List[List[String]]:
    var nodes = List[String]()
    for pair in edges.items(): nodes.append(pair.key)
    var i = 1
    while i < len(nodes):
        var key = nodes[i].copy(); var j = i
        while j > 0 and nodes[j - 1] > key:
            nodes[j] = nodes[j - 1].copy(); j -= 1
        nodes[j] = key^; i += 1
    var path = List[String](); var visiting = Dict[String, Int](); var visited = Dict[String, Bool](); var cycles = List[List[String]]()
    for node in nodes: _wait_cycle_visit(node, edges, path, visiting, visited, cycles)
    return cycles^

def _wait_bucket(values: Dict[String, List[String]], status: String) raises -> List[String]:
    if status == "pending": return values["pending"].copy()
    if status == "ready": return values["ready"].copy()
    if status == "running": return values["running"].copy()
    if status == "waiting": return values["waiting"].copy()
    if status == "retry_wait": return values["retry_wait"].copy()
    if status == "succeeded": return values["succeeded"].copy()
    if status == "failed": return values["failed"].copy()
    if status == "cancel_requested": return values["cancel_requested"].copy()
    if status == "cancelled": return values["cancelled"].copy()
    return values["timed_out"].copy()

def _sort_wait_ids(mut values: List[String]):
    var i = 1
    while i < len(values):
        var key = values[i].copy()
        var j = i
        while j > 0 and values[j - 1] > key:
            values[j] = values[j - 1].copy()
            j -= 1
        values[j] = key^
        i += 1

def _wait_status(values: Dict[String, String], key: String) -> String:
    try:
        return values[key]
    except err:
        return ""

def diagnose_wait_graph(mut journal: NativeJournal, run_id: String, impulse_id: String = "") raises SQLiteError -> WaitGraphDiagnostic:
    if run_id == "": raise SQLiteError(code=2, message="driver: run_id must not be empty")
    var rows = journal.list_processes(run_id, impulse_id=impulse_id)
    var row_index = 1
    while row_index < len(rows):
        var row_key = rows[row_index].copy()
        var row_pos = row_index
        while row_pos > 0 and rows[row_pos - 1].id > row_key.id:
            rows[row_pos] = rows[row_pos - 1].copy()
            row_pos -= 1
        rows[row_pos] = row_key^
        row_index += 1
    var process_statuses = Dict[String, String]()
    var pending = List[String](); var ready = List[String](); var running = List[String]()
    var waiting = List[String](); var retry_wait = List[String](); var succeeded = List[String]()
    var failed = List[String](); var cancel_requested = List[String](); var cancelled = List[String](); var timed_out = List[String]()
    for row in rows:
        process_statuses[row.id] = row.status
        if row.status == "pending": pending.append(row.id)
        elif row.status == "ready": ready.append(row.id)
        elif row.status == "running": running.append(row.id)
        elif row.status == "waiting": waiting.append(row.id)
        elif row.status == "retry_wait": retry_wait.append(row.id)
        elif row.status == "succeeded": succeeded.append(row.id)
        elif row.status == "failed": failed.append(row.id)
        elif row.status == "cancel_requested": cancel_requested.append(row.id)
        elif row.status == "cancelled": cancelled.append(row.id)
        elif row.status == "timed_out": timed_out.append(row.id)
    _sort_wait_ids(pending); _sort_wait_ids(ready); _sort_wait_ids(running); _sort_wait_ids(waiting); _sort_wait_ids(retry_wait)
    _sort_wait_ids(succeeded); _sort_wait_ids(failed); _sort_wait_ids(cancel_requested); _sort_wait_ids(cancelled); _sort_wait_ids(timed_out)
    var homeostat_statuses = Dict[String, String]()
    var homeostat_stmt = journal.db.query("SELECT id,status FROM homeostats WHERE run_id=?" + (" AND impulse_id=?" if impulse_id != "" else "") + " ORDER BY id ASC")
    homeostat_stmt.bind_text(1, run_id)
    if impulse_id != "": homeostat_stmt.bind_text(2, impulse_id)
    var open_homeostats = List[String]()
    while homeostat_stmt.step():
        var homeostat_id = homeostat_stmt.column_text(0)
        var homeostat_status = homeostat_stmt.column_text(1)
        homeostat_statuses[homeostat_id] = homeostat_status
        if homeostat_status == "open": open_homeostats.append(homeostat_id)
    homeostat_stmt.close()
    _sort_wait_ids(open_homeostats)
    var wait_edges = Dict[String, List[String]]()
    var blocked = List[WaitDiagnosticIssue]()
    for row in rows:
        if row.status == "retry_wait":
            var data = "{\"available_at\":" + _json_quote_driver(row.available_at) + "}"
            blocked.append(WaitDiagnosticIssue(process_id=row.id, status=row.status, reason="retry_wait", blocked_by=List[String](), dependency_statuses=Dict[String, String](), data=data))
            continue
        if row.status != "waiting": continue
        var process_dependencies = _wait_refs(row, "wait_for_processes", "wait_for_process_ids", "blocked_by_processes")
        var homeostat_dependencies = _wait_refs(row, "wait_for_homeostats", "wait_for_homeostat_ids", "blocked_by_homeostats")
        var blocked_by = List[String](); var dependency_statuses = Dict[String, String](); var process_edges = List[String]()
        for dependency_id in process_dependencies:
            var dependency_status = _wait_status(process_statuses, dependency_id)
            dependency_statuses[dependency_id] = dependency_status
            if dependency_status == "" or dependency_status != "succeeded":
                blocked_by.append(dependency_id); process_edges.append(dependency_id)
        for homeostat_id in homeostat_dependencies:
            var key = "homeostat:" + homeostat_id
            var status = _wait_status(homeostat_statuses, homeostat_id)
            dependency_statuses[key] = status
            if status == "" or status != "completed": blocked_by.append(key)
        if len(process_edges) > 0: wait_edges[row.id] = process_edges^
        var issue_reason = "waiting_without_known_blocker" if len(blocked_by) == 0 else "waiting"
        blocked.append(WaitDiagnosticIssue(process_id=row.id, status=row.status, reason=issue_reason, blocked_by=blocked_by^, dependency_statuses=dependency_statuses^, data="{}"))
    var deadlocks: List[List[String]]
    try:
        deadlocks = _wait_cycles(wait_edges)
    except err:
        raise SQLiteError(code=1, message="driver: wait graph unavailable")
    var blocked_process_ids = List[String]()
    for issue in blocked: blocked_process_ids.append(issue.process_id)
    var reason = ""; var code = ""
    var has_actual_blocker = False
    for issue in blocked:
        if issue.reason == "waiting": has_actual_blocker = True
    if len(deadlocks) > 0: reason = "feedback_cycle_wait"; code = "feedback_cycle_wait"
    elif len(blocked) > 0 and has_actual_blocker: reason = "waiting"; code = "waiting"
    elif len(blocked) > 0: reason = "waiting_without_known_blocker"; code = "waiting_without_known_blocker"
    return WaitGraphDiagnostic(run_id=run_id, impulse_id=impulse_id, deadlocked=len(deadlocks) > 0, deadlocks=deadlocks^, wait_edges=wait_edges^, blocked=blocked^, open_homeostats=open_homeostats^, pending=pending^, ready=ready^, running=running^, waiting=waiting^, retry_wait=retry_wait^, succeeded=succeeded^, failed=failed^, cancel_requested=cancel_requested^, cancelled=cancelled^, timed_out=timed_out^, blocked_process_ids=blocked_process_ids^, reason=reason, code=code)

def _json_quote_driver(value: String) -> String:
    var result = "\""
    for ch in value.codepoint_slices():
        if ch == "\"": result += "\\\""
        elif ch == "\\": result += "\\\\"
        elif ch == "\n": result += "\\n"
        elif ch == "\r": result += "\\r"
        elif ch == "\t": result += "\\t"
        else: result += ch
    return result + "\""

def _persisted_correlation_wait(mut journal: NativeJournal, run_id: String, impulse_id: String = "") raises -> CorrelationWaitDiagnostic:
    """Read a correlation wait marker persisted on a pending process row."""
    var rows = journal.list_processes(run_id, impulse_id=impulse_id)
    for row in rows:
        if row.status != "pending": continue
        var metadata = Value(parse_string=row.metadata)
        if not metadata.is_object() or "__correlation_wait_diagnostic" not in metadata.object(): continue
        var marker = metadata.object()["__correlation_wait_diagnostic"].copy()
        if not marker.is_object(): continue
        if "blocked_process_ids" not in marker.object() or not marker.object()["blocked_process_ids"].is_array(): continue
        var ids = List[String]()
        var valid = True
        for value in marker.object()["blocked_process_ids"].array():
            if not value.is_string():
                valid = False
                break
            ids.append(value.string())
        if not valid: continue
        var deadlocked = False
        var reason = ""
        var code = ""
        if "deadlocked" in marker.object() and marker.object()["deadlocked"].is_bool(): deadlocked = marker.object()["deadlocked"].bool()
        if "reason" in marker.object() and marker.object()["reason"].is_string(): reason = marker.object()["reason"].string()
        if "code" in marker.object() and marker.object()["code"].is_string(): code = marker.object()["code"].string()
        if code == "": continue
        return CorrelationWaitDiagnostic(ids^, deadlocked, reason, code)
    return CorrelationWaitDiagnostic(List[String](), False, "", "")

def diagnose_waits(mut journal: NativeJournal, run_id: String, impulse_id: String = "") raises -> CorrelationWaitDiagnostic:
    """Legacy compact diagnosis retained for bounded driver APIs."""
    var graph = diagnose_wait_graph(journal, run_id, impulse_id)
    var persisted = _persisted_correlation_wait(journal, run_id, impulse_id)
    if persisted.code != "": return persisted^
    return CorrelationWaitDiagnostic(graph.blocked_process_ids^, graph.deadlocked, graph.reason, graph.code)


def maintain_process(
    mut journal: NativeJournal,
    process: ProcessRow,
    worker_id: String,
    now: String,
    retry_available_at: String = "",
    error_json: String = "",
) raises SQLiteError -> ProcessRow:
    """Resolve an expired lease without executing the effector.

    Expired work with attempts remaining is returned to retry_wait; exhausted
    work is terminally failed.  The lease owner is the only actor allowed to
    perform the transition, matching the journal's ownership checks.
    """
    if worker_id == "":
        raise SQLiteError(code=1, message="driver: worker_id must not be empty")
    var current = journal.get_process(process.run_id, process.id)
    if current.status != process.status or current.attempt != process.attempt or current.lease_owner != process.lease_owner or current.lease_expires_at != process.lease_expires_at:
        raise SQLiteError(code=1, message="driver: process lease changed concurrently")
    if current.status != "running":
        return current.copy()
    if current.lease_owner == "":
        raise SQLiteError(code=1, message="driver: running process has no lease owner")
    if current.lease_owner != worker_id:
        raise SQLiteError(code=1, message="driver: lease is held by another worker")
    if current.lease_expires_at == "":
        raise SQLiteError(code=1, message="driver: running process has no lease expiry")
    if current.lease_expires_at > now:
        return current.copy()
    var failure = error_json if error_json != "" else "{\"code\":\"lease_expired\",\"message\":\"process lease expired\"}"
    if current.attempt >= current.max_attempts:
        return journal.fail_process(current.run_id, current.id, worker_id, now, failure)
    var due = retry_available_at if retry_available_at != "" else now
    if due < now:
        raise SQLiteError(code=1, message="driver: retry availability must not precede transition timestamp")
    return journal.retry_process(current.run_id, current.id, worker_id, now, due, failure)



def _driver_json_quoted(value: String) -> String:
    var escaped = String()
    for ch in value.codepoint_slices():
        if ch == "\\": escaped += "\\\\"
        elif ch == "\"": escaped += "\\\""
        elif ch == "\n": escaped += "\\n"
        elif ch == "\r": escaped += "\\r"
        elif ch == "\t": escaped += "\\t"
        else: escaped += ch
    return "\"" + escaped + "\""
def _success_output_json(result: EffectorResult) -> String:
    """Persist effector output while reserving adapter telemetry separately."""
    try:
        var output = Value(parse_string=result.output_json)
        if not output.is_object(): return result.output_json
        var object = output.object().copy()
        var adapter_value = Value(parse_string="{\"returncode\":" + String(result.returncode) + ",\"stdout\":" + _driver_json_quoted(result.stdout) + ",\"stderr\":" + _driver_json_quoted(result.stderr) + "}")
        if result.metadata_json != "":
            var metadata = Value(parse_string=result.metadata_json)
            if metadata.is_object():
                var adapter_object = adapter_value.object().copy()
                adapter_object["metadata"] = metadata.copy()
                adapter_value = Value(parse_string=to_string(adapter_object^))
        object["adapter"] = adapter_value.copy()
        return to_string(object^)
    except e:
        return result.output_json
def drive_once(
    mut journal: NativeJournal,
    process: ProcessRow,
    adapter: AdapterSpec,
    worker_id: String,
    now: String,
    lease_expires_at: String,
    registry: NativeFunctionRegistry,
) raises SQLiteError -> DriverResult:
    """Claim one supplied process and dispatch it without unsupported transports."""
    var preflight = _preflight_adapter(adapter)
    if worker_id == "":
        raise SQLiteError(code=1, message="driver: worker_id must not be empty")
    var claimed = journal.claim_process(
        process.run_id, process.id, worker_id, now, lease_expires_at
    )
    if not preflight.is_ok():
        # Claim before failing so unsupported work cannot remain ready/pending
        # while run finalization reports waiting.  No effector output is made.
        var error_json = _adapter_error_json(preflight)
        var stored = journal.fail_process(
            claimed.run_id, claimed.id, worker_id, now, error_json
        )
        var failure_rows = List[ProcessRow]()
        failure_rows.append(stored^)
        return DriverResult(
            failed=True,
            ticks=1,
            process_id=claimed.id,
            error=preflight,
            failure_rows=failure_rows^,
        )
    var request = EffectorRequest(
        process_id=claimed.id,
        adapter=adapter,
        impulse_id=claimed.impulse_id,
        input_json=_request_input_json(claimed.input_json),
        config_json=_effective_config_json(claimed.input_json, claimed.metadata),
        work_dir="",
    )
    var result = _dispatch(request, registry)
    if result.waiting:
        var parked = journal.park_homeostat_process(
            claimed.run_id,
            result.homeostat_id,
            claimed.id,
            worker_id,
            now,
            result.output_json,
            result.metadata_json,
            "homeostat.open:" + result.homeostat_id,
        )
        _ = parked
        return DriverResult(waiting=True, ticks=1, process_id=claimed.id, error=result.error)
    if result.success and result.error.is_ok():
        _ = journal.complete_process(
            claimed.run_id,
            claimed.id,
            worker_id,
            now,
            _success_output_json(result),
            "{}",
        )
        return DriverResult(completed=True, ticks=1, process_id=claimed.id)

    var failure = result.error.copy()
    if failure.is_ok():
        failure = AdapterError("adapter_failed", "effector returned success=false without an error")
    var timed_out = (
        failure.code == "timeout" or failure.code == "adapter_timeout"
    )
    var error_json = _adapter_error_json(failure)
    var stored: ProcessRow
    # Retry transitions are immediately claimable at the transition timestamp,
    # matching reference retry_process and bounded-drive semantics.
    var retry_due = _retry_due(claimed, now)
    if timed_out and claimed.attempt < claimed.max_attempts:
        stored = journal.retry_process(
            claimed.run_id, claimed.id, worker_id, now, retry_due, error_json
        )
    elif timed_out:
        stored = journal.timeout_process(
            claimed.run_id, claimed.id, worker_id, now, error_json
        )
    elif claimed.attempt < claimed.max_attempts:
        stored = journal.retry_process(
            claimed.run_id, claimed.id, worker_id, now, retry_due, error_json
        )
    else:
        stored = journal.fail_process(
            claimed.run_id, claimed.id, worker_id, now, error_json
        )
    var failure_rows = List[ProcessRow]()
    failure_rows.append(stored^)
    return DriverResult(
        failed=not timed_out,
        timed_out=timed_out,
        ticks=1,
        process_id=claimed.id,
        error=failure,
        failure_rows=failure_rows^,
    )


def _supplied_process_index(processes: List[ProcessRow], run_id: String, process_id: String) -> Int:
    """Find the caller-supplied adapter mapping for one durable row."""
    var index = 0
    while index < len(processes):
        if processes[index].run_id == run_id and processes[index].id == process_id:
            return index
        index += 1
    return -1
def _validate_supplied_processes(processes: List[ProcessRow]) raises SQLiteError:
    """Reject ambiguous process mappings before selecting a run or row."""
    if len(processes) == 0: return
    var run_id = processes[0].run_id
    var index = 1
    while index < len(processes):
        if processes[index].run_id != run_id:
            raise SQLiteError(code=1, message="driver: all processes must target one run")
        var prior = 0
        while prior < index:
            if processes[prior].run_id == run_id and processes[prior].id == processes[index].id:
                raise SQLiteError(code=1, message="driver: duplicate process id")
            prior += 1
        index += 1


def drive_until_idle(
    mut journal: NativeJournal,
    processes: List[ProcessRow],
    adapters: List[AdapterSpec],
    worker_id: String,
    now: String,
    lease_expires_at: String,
    max_ticks: Int,
    registry: NativeFunctionRegistry,
    claims_per_round: Int = 1,
) raises SQLiteError -> DriverResult:
    """Drive supplied adapters against durable rows until idle or bounded.

    Process rows are reloaded for the supplied run before every tick.  A
    durable row is executable only when its id is present in the caller's
    process/adapter mapping; rows discovered by the reload remain queued.

    ``claims_per_round`` (default 1) is how many ready processes may be claimed
    and executed in one outer loop iteration. Values > 1 enable multi-claim
    composition inside a single journal (still one lease owner; not a fleet).
    """
    if max_ticks < 1:
        raise SQLiteError(code=1, message="driver: max_ticks must be greater than zero")
    if claims_per_round < 1:
        raise SQLiteError(code=1, message="driver: claims_per_round must be greater than zero")
    if worker_id == "":
        raise SQLiteError(code=1, message="driver: worker_id must not be empty")
    if now == "":
        raise SQLiteError(code=1, message="driver: now must not be empty")
    if lease_expires_at == "" or lease_expires_at <= now:
        raise SQLiteError(code=1, message="driver: lease must expire after now")
    if len(processes) != len(adapters):
        raise SQLiteError(code=1, message="driver: process and adapter lists differ in length")
    _validate_supplied_processes(processes)
    var aggregate = DriverResult(idle=True)
    var run_id = processes[0].run_id if len(processes) != 0 else ""
    while aggregate.ticks < max_ticks:
        var claimed_this_round = 0
        while claimed_this_round < claims_per_round and aggregate.ticks < max_ticks:
            var durable = List[ProcessRow]()
            if run_id != "": durable = journal.list_processes(run_id)
            var best = -1
            var best_supplied = -1
            var index = 0
            while index < len(durable):
                var supplied = _supplied_process_index(processes, run_id, durable[index].id)
                if supplied >= 0 and _row_claimable(durable[index], now):
                    if best < 0 or _row_before(durable[index], durable[best]):
                        best = index
                        best_supplied = supplied
                index += 1
            if best < 0: break
            var candidate = durable[best].copy()
            if candidate.status == "running" and candidate.lease_owner != "" and candidate.lease_expires_at <= now:
                # Resolve the old owner's lease first.  This preserves one attempt
                # per claim and emits the journal transition before re-claiming.
                var maintained = maintain_process(
                    journal, candidate, candidate.lease_owner, now, now,
                    "{\"code\":\"lease_expired\",\"message\":\"process lease expired\"}",
                )
                if maintained.status == "failed":
                    aggregate.failed = True
                    aggregate.ticks += 1
                    aggregate.process_id = candidate.id
                    claimed_this_round += 1
                    continue
                candidate = journal.get_process(candidate.run_id, candidate.id)
            var one = drive_once(
                journal, candidate, adapters[best_supplied], worker_id, now, lease_expires_at, registry
            )
            if one.ticks == 0 and not one.error.is_ok():
                aggregate.failed = True
                aggregate.idle = False
                aggregate.process_id = candidate.id
                aggregate.error = one.error.copy()
                return aggregate^
            aggregate.ticks += one.ticks
            claimed_this_round += 1
            for failure_row in one.failure_rows:
                aggregate.failure_rows.append(failure_row.copy())
            aggregate.completed = aggregate.completed or one.completed
            aggregate.waiting = aggregate.waiting or one.waiting
            aggregate.failed = aggregate.failed or one.failed
            aggregate.timed_out = aggregate.timed_out or one.timed_out
            aggregate.process_id = one.process_id
            if not one.error.is_ok(): aggregate.error = one.error.copy()
        if claimed_this_round == 0: break
    if aggregate.ticks != 0: aggregate.idle = aggregate.ticks < max_ticks
    return aggregate^


def drive_ready_batch(
    mut journal: NativeJournal,
    processes: List[ProcessRow],
    adapters: List[AdapterSpec],
    worker_id: String,
    now: String,
    lease_expires_at: String,
    max_claims: Int,
    registry: NativeFunctionRegistry,
) raises SQLiteError -> DriverResult:
    """Claim and drive up to ``max_claims`` ready processes in one batch.

    First-class multi-claim entry for composition; equivalent to one round of
    ``drive_until_idle(..., claims_per_round=max_claims, max_ticks=max_claims)``.
    """
    if max_claims < 1:
        raise SQLiteError(code=1, message="driver: max_claims must be greater than zero")
    return drive_until_idle(
        journal, processes, adapters, worker_id, now, lease_expires_at,
        max_claims, registry, claims_per_round=max_claims,
    )

def run_until_idle(
    mut journal: NativeJournal,
    processes: List[ProcessRow],
    adapters: List[AdapterSpec],
    worker_id: String,
    now: String,
    lease_expires_at: String,
    max_ticks: Int,
    registry: NativeFunctionRegistry,
    stop: Bool = False,
) raises SQLiteError -> RunUntilIdleResult:
    """Drive a run and return durable rows with a deterministic stop reason."""
    if worker_id == "":
        raise SQLiteError(code=1, message="driver: worker_id must not be empty")
    if now == "":
        raise SQLiteError(code=1, message="driver: now must not be empty")
    if lease_expires_at == "" or lease_expires_at <= now:
        raise SQLiteError(code=1, message="driver: lease must expire after now")
    if max_ticks < 1:
        raise SQLiteError(code=1, message="driver: max_ticks must be greater than zero")
    if len(processes) != len(adapters):
        raise SQLiteError(code=1, message="driver: process and adapter lists differ in length")
    _validate_supplied_processes(processes)
    var run_id = processes[0].run_id if len(processes) > 0 else ""
    if run_id != "":
        var existing_run = journal.get_run_record(run_id)
        if RunStatus(existing_run.status).is_terminal():
            return RunUntilIdleResult(ok=True, ticks=0, stopped_reason="already_terminal", completed=List[ProcessRow](), failed=List[ProcessRow](), waiting=List[ProcessRow](), deadlocked=False, deadlocks=List[List[String]](), wait_diagnostic=_empty_wait_graph())
    var aggregate = DriverResult(idle=True)
    var reason = "idle"
    if stop:
        reason = "stopped"
    else:
        aggregate = drive_until_idle(journal, processes, adapters, worker_id, now, lease_expires_at, max_ticks, registry)
        if aggregate.ticks >= max_ticks: reason = "max_ticks"
        elif not aggregate.error.is_ok(): reason = "failed"
        else: reason = "idle"
    var final_rows = List[ProcessRow]()
    if run_id != "": final_rows = journal.list_processes(run_id)
    var final_all_terminal = len(final_rows) > 0
    for row in final_rows:
        if row.status != "succeeded" and row.status != "failed" and row.status != "cancelled" and row.status != "timed_out": final_all_terminal = False
    var completed = List[ProcessRow]()
    var failed = List[ProcessRow]()
    var waiting = List[ProcessRow]()
    for historical in aggregate.failure_rows:
        if not _has_process_status(failed, historical): failed.append(historical.copy())
    for row in final_rows:
        if row.status == "succeeded": completed.append(row.copy())
        elif row.status == "failed" or row.status == "cancelled" or row.status == "timed_out" or row.status == "retry_wait":
            if not _has_process_status(failed, row): failed.append(row.copy())
        elif row.status == "waiting": waiting.append(row.copy())
    var graph_diagnostic = _empty_wait_graph()
    if run_id != "": graph_diagnostic = diagnose_wait_graph(journal, run_id)
    if final_all_terminal: reason = "idle"
    elif aggregate.ticks == 0 and not aggregate.error.is_ok(): reason = "failed"
    var all_succeeded = len(final_rows) > 0 and len(completed) == len(final_rows)
    if all_succeeded: reason = "idle"
    if graph_diagnostic.deadlocked and reason == "idle": reason = "stopped"
    var ok = not graph_diagnostic.deadlocked and (reason == "idle" or reason == "already_terminal" or reason == "stopped")
    return RunUntilIdleResult(
        ok=ok,
        ticks=aggregate.ticks,
        stopped_reason=reason,
        completed=completed^,
        failed=failed^,
        waiting=waiting^,
        deadlocked=graph_diagnostic.deadlocked,
        deadlocks=graph_diagnostic.deadlocks.copy(),
        wait_diagnostic=graph_diagnostic^,
    )
def drive_bound_queue(
    mut journal: NativeJournal,
    bindings: List[AdapterBinding],
    worker_id: String,
    now: String,
    lease_expires_at: String,
    max_ticks: Int,
    registry: NativeFunctionRegistry,
    run_id: String = "",
) raises SQLiteError -> DriverResult:
    """Drive claimable durable rows using only explicit id-to-adapter bindings.

    Rows are reloaded from SQLite on entry, so callers need not retain stale
    ProcessRow values.  A binding without a run id uses the supplied run_id;
    mixed-run bindings are rejected rather than scanning an unintended run.
    """
    if max_ticks < 1:
        raise SQLiteError(code=1, message="driver: max_ticks must be greater than zero")
    if worker_id == "":
        raise SQLiteError(code=1, message="driver: worker_id must not be empty")
    if now == "":
        raise SQLiteError(code=1, message="driver: now must not be empty")
    if lease_expires_at == "" or lease_expires_at <= now:
        raise SQLiteError(code=1, message="driver: lease must expire after now")
    var effective_bindings = bindings.copy()
    if len(effective_bindings) == 0:
        if run_id == "": return DriverResult(idle=True)
        effective_bindings = load_adapter_bindings(journal, run_id)
        if len(effective_bindings) == 0: return DriverResult(idle=True)
    var effective_run = run_id
    if effective_run == "": effective_run = effective_bindings[0].run_id
    if effective_run == "":
        raise SQLiteError(code=1, message="driver: run_id must not be empty")
    var rows = List[ProcessRow]()
    var adapters = List[AdapterSpec]()
    var index = 0
    while index < len(effective_bindings):
        var binding = effective_bindings[index].copy()
        if binding.run_id != "" and binding.run_id != effective_run:
            raise SQLiteError(code=1, message="driver: all adapter bindings must target one run")
        rows.append(journal.get_process(effective_run, binding.process_id))
        adapters.append(binding.adapter.copy())
        index += 1
    return drive_until_idle(journal, rows, adapters, worker_id, now, lease_expires_at, max_ticks, registry)
struct AllRunDriverResult(Copyable, Movable):
    """Durable report for an explicit multi-run queue drive.

    Rows are executable only when a caller-supplied binding matches both run
    and process id.  Missing and unsupported mappings are reported and never
    claimed.  `ticks` counts effector claims and terminal lease closures.
    """
    var idle: Bool
    var stopped: Bool
    var bounded: Bool
    var completed: Bool
    var waiting: Bool
    var failed: Bool
    var timed_out: Bool
    var ticks: Int
    var runs_scanned: Int
    var rows_scanned: Int
    var expired_leases: Int
    var missing_mappings: Int
    var unsupported_mappings: Int
    var process_id: String
    var error: AdapterError

    def __init__(
        out self,
        idle: Bool = True,
        stopped: Bool = False,
        bounded: Bool = False,
        completed: Bool = False,
        waiting: Bool = False,
        failed: Bool = False,
        timed_out: Bool = False,
        ticks: Int = 0,
        runs_scanned: Int = 0,
        rows_scanned: Int = 0,
        expired_leases: Int = 0,
        missing_mappings: Int = 0,
        unsupported_mappings: Int = 0,
        process_id: String = "",
        error: AdapterError = AdapterError(),
    ):
        self.idle = idle
        self.stopped = stopped
        self.bounded = bounded
        self.completed = completed
        self.waiting = waiting
        self.failed = failed
        self.timed_out = timed_out
        self.ticks = ticks
        self.runs_scanned = runs_scanned
        self.rows_scanned = rows_scanned
        self.expired_leases = expired_leases
        self.missing_mappings = missing_mappings
        self.unsupported_mappings = unsupported_mappings
        self.process_id = process_id
        self.error = error.copy()


def _binding_index(bindings: List[AdapterBinding], run_id: String, process_id: String) -> Int:
    var index = 0
    while index < len(bindings):
        if bindings[index].run_id == run_id and bindings[index].process_id == process_id:
            return index
        index += 1
    return -1


def _binding_run_seen(bindings: List[AdapterBinding], index: Int) -> Bool:
    var prior = 0
    while prior < index:
        if bindings[prior].run_id == bindings[index].run_id: return True
        prior += 1
    return False


def drive_all_runs(
    mut journal: NativeJournal,
    bindings: List[AdapterBinding],
    worker_id: String,
    now: String,
    lease_expires_at: String,
    registry: NativeFunctionRegistry,
    max_ticks: Int = 0,
    stop: Bool = False,
) raises SQLiteError -> AllRunDriverResult:
    """Scan and drive all runs represented by explicit adapter bindings.

    The scanner never derives an adapter from process_type or capabilities.
    It first closes expired leases (including exhausted final attempts), then
    selects the globally earliest row by priority DESC, available_at ASC,
    created_at ASC, and id ASC.  A zero max_ticks means unbounded; `stop`
    provides an immediate, side-effect-free stop control.
    """
    if worker_id == "":
        raise SQLiteError(code=1, message="driver: worker_id must not be empty")
    if now == "":
        raise SQLiteError(code=1, message="driver: now must not be empty")
    if max_ticks < 0:
        raise SQLiteError(code=1, message="driver: max_ticks must be non-negative")
    var report = AllRunDriverResult(stopped=stop, bounded=max_ticks > 0)
    if stop or len(bindings) == 0: return report^
    var first = 0
    while first < len(bindings):
        if bindings[first].run_id == "":
            raise SQLiteError(code=1, message="driver: all-run bindings require an explicit run_id")
        first += 1
    # Duplicate process mappings must agree byte-for-byte; never infer which
    # adapter wins when callers provide conflicting explicit bindings.
    var binding_check = 0
    while binding_check < len(bindings):
        var duplicate_check = binding_check + 1
        while duplicate_check < len(bindings):
            if bindings[duplicate_check].run_id == bindings[binding_check].run_id and bindings[duplicate_check].process_id == bindings[binding_check].process_id:
                var left_json = ""
                var right_json = ""
                try:
                    left_json = adapter_spec_json(bindings[binding_check].adapter)
                except err:
                    raise SQLiteError(code=1, message="driver: invalid adapter binding")
                try:
                    right_json = adapter_spec_json(bindings[duplicate_check].adapter)
                except err:
                    raise SQLiteError(code=1, message="driver: invalid adapter binding")
                if left_json != right_json:
                    raise SQLiteError(code=1, message="driver: conflicting adapter bindings for process")
            duplicate_check += 1
        binding_check += 1

    while True:
        if stop:
            report.stopped = True
            break
        if max_ticks > 0 and report.ticks >= max_ticks:
            report.bounded = True
            break

        # Resolve every expired lease before looking for a candidate.  This
        # also durably closes exhausted final attempts with no adapter needed.
        var run_index = 0
        var closure_failed = False
        while run_index < len(bindings):
            if not _binding_run_seen(bindings, run_index):
                var rows = journal.list_processes(bindings[run_index].run_id)
                report.runs_scanned += 1
                var row_index = 0
                while row_index < len(rows):
                    report.rows_scanned += 1
                    var row = rows[row_index].copy()
                    if row.status == "running" and row.lease_owner != "" and row.lease_expires_at != "" and row.lease_expires_at <= now:
                        var maintained = maintain_process(
                            journal, row, row.lease_owner, now, now,
                            "{\"code\":\"lease_expired\",\"message\":\"process lease expired\"}",
                        )
                        report.expired_leases += 1
                        if maintained.status == "failed" or maintained.status == "timed_out":
                            report.failed = report.failed or maintained.status == "failed"
                            report.timed_out = report.timed_out or maintained.status == "timed_out"
                            report.ticks += 1
                            report.process_id = row.id
                            closure_failed = True
                            report.idle = False
                            if max_ticks > 0 and report.ticks >= max_ticks: break
                    row_index += 1
                if max_ticks > 0 and report.ticks >= max_ticks: break
            run_index += 1
        if max_ticks > 0 and report.ticks >= max_ticks: break
        if closure_failed:
            # A terminal closure is already a durable tick; rescan freshly.
            continue

        var best = -1
        var best_binding = -1
        var best_candidate_run = ""
        var best_candidate_id = ""
        run_index = 0
        while run_index < len(bindings):
            if not _binding_run_seen(bindings, run_index):
                var durable = journal.list_processes(bindings[run_index].run_id)
                var row_index = 0
                while row_index < len(durable):
                    var candidate = durable[row_index].copy()
                    var bound = _binding_index(bindings, candidate.run_id, candidate.id)
                    if bound < 0:
                        report.missing_mappings += 1
                    elif _row_claimable(candidate, now):
                        var preflight = _preflight_adapter(bindings[bound].adapter)
                        if not preflight.is_ok():
                            report.unsupported_mappings += 1
                            if report.error.is_ok(): report.error = preflight.copy()
                        var outranks = best < 0
                        if not outranks:
                            var current_best = journal.get_process(best_candidate_run, best_candidate_id)
                            outranks = _row_before(candidate, current_best)
                        if outranks:
                            best = row_index
                            best_binding = bound
                            best_candidate_run = candidate.run_id
                            best_candidate_id = candidate.id
                    row_index += 1
            run_index += 1
        if best < 0: break
        var selected = journal.get_process(best_candidate_run, best_candidate_id)
        var one = drive_once(
            journal, selected, bindings[best_binding].adapter, worker_id,
            now, lease_expires_at, registry,
        )
        if one.ticks == 0 and not one.error.is_ok():
            report.failed = True
            report.idle = False
            report.process_id = one.process_id
            report.error = one.error.copy()
            break
        report.ticks += one.ticks
        report.idle = False
        report.completed = report.completed or one.completed
        report.waiting = report.waiting or one.waiting
        report.failed = report.failed or one.failed
        report.timed_out = report.timed_out or one.timed_out
        report.process_id = one.process_id
        if not one.error.is_ok(): report.error = one.error.copy()
    if report.ticks == 0 and not report.stopped:
        if report.unsupported_mappings > 0:
            # An explicitly claimable row with no native transport is a failed drive, not idle.
            report.failed = True
            report.idle = False
        else:
            report.idle = True
    return report^

def finalize_run(
    mut journal: NativeJournal,
    run_id: String,
    stopped_reason: String,
    max_ticks: Int,
    now: String,
    idempotency_key: String = "",
) raises SQLiteError -> RunFinalizationResult:
    """Derive and persist the run boundary from every durable effector row."""
    if run_id == "" or stopped_reason == "" or now == "":
        raise SQLiteError(code=1, message="driver: run finalization requires run_id, stopped_reason, and timestamp")
    if stopped_reason != "idle" and stopped_reason != "max_ticks" and stopped_reason != "failed" and stopped_reason != "waiting" and stopped_reason != "stopped" and stopped_reason != "already_terminal":
        raise SQLiteError(code=1, message="driver: unknown run finalization reason " + stopped_reason)
    if max_ticks < 0:
        raise SQLiteError(code=1, message="driver: max_ticks must be non-negative")
    var run = journal.get_run_record(run_id)
    var rows = journal.list_processes(run_id)
    var total = len(rows)
    var completed = 0
    var failed = 0
    var waiting = 0
    var incomplete = 0
    for row in rows:
        if row.status == "succeeded":
            completed += 1
        elif row.status == "failed" or row.status == "cancelled" or row.status == "timed_out":
            failed += 1
        elif row.status == "waiting":
            waiting += 1
            incomplete += 1
        else:
            incomplete += 1
    if RunStatus(run.status).is_terminal():
        return RunFinalizationResult(status=run.status, reason="already_terminal", already_terminal=True, total_count=total, completed_count=completed, failed_count=failed, waiting_count=waiting, incomplete_count=incomplete)
    var target = run.status
    var reason = stopped_reason
    if failed > 0:
        target = "failed"
        reason = "effector_failed"
    elif total > 0 and completed == total:
        target = "completed"
        reason = "all_effectors_succeeded"
    elif stopped_reason == "max_ticks" and max_ticks > 0:
        target = "timed_out"
        reason = "max_ticks"
    elif waiting > 0 or incomplete > 0 or total == 0 or stopped_reason == "idle" or stopped_reason == "waiting" or stopped_reason == "stopped" or stopped_reason == "failed":
        target = "waiting"
        reason = "waiting"
    if target != run.status:
        var key = idempotency_key
        if key == "": key = "run.finalize:" + run_id + ":" + target
        _ = journal.transition_run_status(run_id, target, now, key, reason=reason)
    return RunFinalizationResult(status=target, reason=reason, already_terminal=False, total_count=total, completed_count=completed, failed_count=failed, waiting_count=waiting, incomplete_count=incomplete)
def advance_after_terminal(mut journal: NativeJournal, plan: CorrelationInstantiationPlan, process_id: String = "") raises -> Bool:
    """Reconcile correlation readiness after a terminal process transition."""
    if plan.run_id == "":
        raise SQLiteError(code=1, message="driver: correlation plan run_id must not be empty")
    if process_id != "":
        var row = journal.get_process(plan.run_id, process_id)
        if row.status != "succeeded" and row.status != "failed" and row.status != "cancelled" and row.status != "timed_out":
            return False
    _ = advance_correlation(journal, plan)
    return True

def drive_correlation_once(
    mut journal: NativeJournal,
    process: ProcessRow,
    adapter: AdapterSpec,
    worker_id: String,
    now: String,
    lease_expires_at: String,
    registry: NativeFunctionRegistry,
    plan: CorrelationInstantiationPlan,
) raises SQLiteError -> DriverResult:
    """Drive one effector and immediately reconcile downstream readiness."""
    var result = drive_once(journal, process, adapter, worker_id, now, lease_expires_at, registry)
    if result.completed or result.failed or result.timed_out:
        try:
            _ = advance_after_terminal(journal, plan, result.process_id)
        except err:
            raise SQLiteError(code=1, message="driver: correlation advancement failed: " + String(err))
    return result^

def drive_correlation_until_idle(
    mut journal: NativeJournal,
    processes: List[ProcessRow],
    adapters: List[AdapterSpec],
    worker_id: String,
    now: String,
    lease_expires_at: String,
    max_ticks: Int,
    registry: NativeFunctionRegistry,
    plan: CorrelationInstantiationPlan,
) raises SQLiteError -> RunUntilIdleResult:
    """Advance and drive a correlation queue to a deterministic fixed point."""
    if max_ticks < 1:
        raise SQLiteError(code=1, message="driver: max_ticks must be greater than zero")
    if len(processes) != len(adapters):
        raise SQLiteError(code=1, message="driver: process and adapter lists differ in length")
    var ticks = 0
    var last = DriverResult(idle=True)
    while ticks < max_ticks:
        try:
            _ = advance_correlation(journal, plan)
        except err:
            raise SQLiteError(code=1, message="driver: correlation advancement failed: " + String(err))
        var before = journal.list_processes(plan.run_id)
        var current_adapters = List[AdapterSpec]()
        for durable_process in before:
            var adapter_index = _supplied_process_index(processes, plan.run_id, durable_process.id)
            if adapter_index < 0:
                raise SQLiteError(code=1, message="driver: correlation adapter mapping missing for " + durable_process.id)
            current_adapters.append(adapters[adapter_index].copy())
        var result = drive_until_idle(journal, before, current_adapters, worker_id, now, lease_expires_at, 1, registry)
        last = result.copy()
        if result.ticks == 0:
            break
        ticks += result.ticks
        try:
            _ = advance_after_terminal(journal, plan, result.process_id)
        except err:
            raise SQLiteError(code=1, message="driver: correlation advancement failed: " + String(err))
    # Reconcile once more before deriving the run boundary.  External terminal
    # transitions (for example homeostat or delegation closure) may not have a
    # process_id in this drive loop; the existing helper safely replays durable
    # readiness and dead-upstream cancellation without fabricating execution.
    try:
        _ = advance_after_terminal(journal, plan)
    except err:
        raise SQLiteError(code=1, message="driver: correlation advancement failed: " + String(err))
    var final = run_until_idle(journal, processes, adapters, worker_id, now, lease_expires_at, 1, registry, stop=True)
    final.ticks = ticks
    var durable_final = journal.list_processes(plan.run_id)
    var all_succeeded = len(durable_final) > 0
    var has_failure = False
    for row in durable_final:
        if row.status != "succeeded": all_succeeded = False
        if row.status == "failed" or row.status == "cancelled" or row.status == "timed_out": has_failure = True
    if has_failure:
        final.ok = False
        final.stopped_reason = "failed"
    elif all_succeeded:
        final.ok = True
        final.stopped_reason = "idle"
    elif final.deadlocked:
        final.ok = False
        final.stopped_reason = "stopped"
    elif final.stopped_reason == "already_terminal":
        final.ok = True
    elif ticks >= max_ticks:
        final.ok = False
        final.stopped_reason = "max_ticks"
    elif last.ticks == 0 and final.stopped_reason == "idle":
        final.ok = True
        final.stopped_reason = "idle"
    _ = finalize_run(journal, plan.run_id, final.stopped_reason, max_ticks, now)
    return final^
