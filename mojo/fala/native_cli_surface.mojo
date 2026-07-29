"""Python-free native operator CLI helpers."""
from std.collections import List
from fala.journal import RunRow, NativeJournal, EventInput, ProcessRow
from fala.schema import initialize_native_schema, SCHEMA_VERSION, table_names, SchemaStatus, schema_status
from fala.sqlite import Connection, Statement, SQLiteError
from fala.json import parse_json, canonical_json_text
from fala.domain import Impulse, Association, Reaction, RuntimeBudget, BridgeDelivery, EventRef, RuntimeRef, RunRef
from fala.native_driver import diagnose_waits, diagnose_wait_graph, observe_run_boundary
from fala.reactions import FileReactionStore, ReactionBlob
from emberjson import Value, Object, to_string
from std.pathlib import Path, cwd
from fala.domain_store import NativeDomainStore
from fala.ops_maintenance import (
    JournalMaintenancePlan, RunDeleteCounts, RunRetentionPlan, RunRetentionItem,
    ReactionGarbageCollectionPlan, collect_reaction_garbage, maintain_journal,
)
from fala.ops_projections import rebuild_projections
from fala.ops_bridge import import_bridge_delivery, get_outbox_delivery, deliver_bridge_delivery
from std.os import makedirs, remove
from std.ffi import CStringSlice, c_int, external_call
from fala.bridge_transport import deliver_local_bridge
from fala.runs import RunLifecycle, RunLifecycleRecord
def _quote(value: String) -> String:
    var result = "\""
    for ch in value.codepoint_slices():
        if ch == "\"": result += "\\\""
        elif ch == "\\": result += "\\\\"
        elif ch == "\b": result += "\\b"
        elif ch == "\f": result += "\\f"
        elif ch == "\n": result += "\\n"
        elif ch == "\r": result += "\\r"
        elif ch == "\t": result += "\\t"
        elif ch == "\0": result += "\\u0000"
        elif ch == "\x01": result += "\\u0001"
        elif ch == "\x02": result += "\\u0002"
        elif ch == "\x03": result += "\\u0003"
        elif ch == "\x04": result += "\\u0004"
        elif ch == "\x05": result += "\\u0005"
        elif ch == "\x06": result += "\\u0006"
        elif ch == "\a": result += "\\u0007"
        elif ch == "\x0b": result += "\\u000b"
        elif ch == "\x0e": result += "\\u000e"
        elif ch == "\x0f": result += "\\u000f"
        elif ch == "\x10": result += "\\u0010"
        elif ch == "\x11": result += "\\u0011"
        elif ch == "\x12": result += "\\u0012"
        elif ch == "\x13": result += "\\u0013"
        elif ch == "\x14": result += "\\u0014"
        elif ch == "\x15": result += "\\u0015"
        elif ch == "\x16": result += "\\u0016"
        elif ch == "\x17": result += "\\u0017"
        elif ch == "\x18": result += "\\u0018"
        elif ch == "\x19": result += "\\u0019"
        elif ch == "\x1a": result += "\\u001a"
        elif ch == "\x1b": result += "\\u001b"
        elif ch == "\x1c": result += "\\u001c"
        elif ch == "\x1d": result += "\\u001d"
        elif ch == "\x1e": result += "\\u001e"
        elif ch == "\x1f": result += "\\u001f"
        else: result += ch
    result += "\""
    return result


def _safe(path: String) -> Bool:
    if path == "": return False
    for i in range(path.byte_length()):
        var ch = path[byte=i]
        if ch == "\0" or ch == "\n" or ch == "\r": return False
    return True


def _word(command: String, wanted: Int) -> String:
    var current = 0
    var token = ""
    var active = False
    var quote = ""
    var escaped = False
    var json_depth = 0
    var json_string = False
    for ch in command.codepoint_slices():
        if escaped:
            token += ch
            active = True
            escaped = False
            continue
        if ch == "\\" and quote != "'":
            token += "\\"
            escaped = True
            active = True
            continue
        if json_depth > 0:
            token += ch
            active = True
            if ch == "\"": json_string = not json_string
            elif (not json_string and ch == "{") or (not json_string and ch == "["): json_depth += 1
            elif (not json_string and ch == "}") or (not json_string and ch == "]"): json_depth -= 1
            continue
        if quote != "":
            if ch == quote: quote = ""
            else: token += ch
            active = True
            continue
        if ch == "{" or ch == "[":
            token += ch
            active = True
            json_depth = 1
            json_string = False
        elif ch == "'" or ch == "\"":
            quote = String(ch)
            active = True
        elif ch == " " or ch == "\t":
            if active:
                if current == wanted: return token
                current += 1
                token = ""
                active = False
        else:
            token += ch
            active = True
    if escaped: token += "\\"
    if active and current == wanted: return token
    return ""


def _count(command: String) -> Int:
    var result = 0
    var active = False
    var quote = ""
    var escaped = False
    var json_depth = 0
    var json_string = False
    for ch in command.codepoint_slices():
        if escaped:
            escaped = False
            active = True
            continue
        if ch == "\\" and quote != "'":
            escaped = True
            active = True
        elif json_depth > 0:
            active = True
            if ch == "\"": json_string = not json_string
            elif (not json_string and ch == "{") or (not json_string and ch == "["): json_depth += 1
            elif (not json_string and ch == "}") or (not json_string and ch == "]"): json_depth -= 1
        elif quote != "":
            if ch == quote: quote = ""
            active = True
        elif ch == "{" or ch == "[":
            json_depth = 1
            json_string = False
            active = True
        elif ch == "'" or ch == "\"":
            quote = String(ch)
            active = True
        elif ch == " " or ch == "\t":
            if active:
                result += 1
                active = False
        else: active = True
    if active: result += 1
    return result




def _option_base(item: String) -> String:
    var result = ""
    for ch in item.codepoint_slices():
        if ch == "=": return result
        result += ch
    return result


def _flag(command: String, name: String, default: String = "") -> String:
    var index = 0
    var count = _count(command)
    while index < count:
        var item = _word(command, index)
        if item == name:
            if index + 1 < count:
                var next = _word(command, index + 1)
                if not next.startswith("--"): return next
            return default
        var option = _option_base(item)
        if option == name:
            var equals = item.find("=")
            if equals >= 0:
                var value = String(item[byte=equals + 1:])
                if value != "": return value
                return default
        index += 1
    return default
def _flag_alias(command: String, primary: String, alt: String, default: String = "") -> String:
    if _has_option(command, primary): return _flag(command, primary, default)
    return _flag(command, alt, default)


def _known_option(kind: String, item: String) -> Bool:
    var option = _option_base(item)
    if option == "--db": return True
    if kind == "db" and option == "--ensure-schema": return True
    if kind == "doctor" and option == "--ensure-schema": return True
    if kind == "schema": return False
    if kind == "init" and (option == "--db" or option == "--reaction-root"): return True
    if kind == "create" and (option == "--run-id" or option == "--metadata" or option == "--now" or option == "--title" or option == "--idempotency-key" or option == "--package-id" or option == "--package-version" or option == "--package-digest" or option == "--correlation-path-id" or option == "--correlation-path-digest" or option == "--runtime-version" or option == "--backend-version"): return True
    if kind == "transition" and (option == "--run-id" or option == "--now" or option == "--idempotency-key" or option == "--reason"): return True
    if kind == "impulse-create" and (option == "--run-id" or option == "--impulse-id" or option == "--impulse-type" or option == "--payload" or option == "--payload-json" or option == "--metadata" or option == "--metadata-json" or option == "--now" or option == "--idempotency-key" or option == "--actor" or option == "--correlation-id" or option == "--causation-id"): return True
    if kind == "process-schedule" and (option == "--run-id" or option == "--process-id" or option == "--process-type" or option == "--impulse-id" or option == "--input" or option == "--input-json" or option == "--metadata" or option == "--metadata-json" or option == "--priority" or option == "--max-attempts" or option == "--available-at" or option == "--output-schema" or option == "--now" or option == "--idempotency-key" or option == "--actor"): return True
    if kind == "process-transition" and (option == "--run-id" or option == "--process-id" or option == "--actor" or option == "--now" or option == "--error" or option == "--error-json" or option == "--idempotency-key"): return True
    if kind == "association-append" and (option == "--run-id" or option == "--association-id" or option == "--kind" or option == "--impulse-id" or option == "--values" or option == "--values-json" or option == "--metadata" or option == "--metadata-json" or option == "--now" or option == "--idempotency-key" or option == "--actor"): return True
    if kind == "homeostat-open" and (option == "--run-id" or option == "--homeostat-id" or option == "--process-id" or option == "--kind" or option == "--impulse-id" or option == "--values-json" or option == "--actor" or option == "--now" or option == "--output" or option == "--metadata" or option == "--metadata-json" or option == "--idempotency-key"): return True
    if kind == "homeostat-transition" and (option == "--run-id" or option == "--homeostat-id" or option == "--process-id" or option == "--actor" or option == "--now" or option == "--output" or option == "--error" or option == "--error-json" or option == "--metadata" or option == "--metadata-json" or option == "--idempotency-key"): return True
    if kind == "homeostat-domain-open" and (option == "--run-id" or option == "--homeostat-id" or option == "--impulse-id" or option == "--kind" or option == "--values-json" or option == "--metadata-json" or option == "--idempotency-key"): return True
    if kind == "homeostat-domain-transition" and (option == "--run-id" or option == "--homeostat-id" or option == "--value" or option == "--idempotency-key"): return True
    if kind == "run-observe" and (option == "--db" or option == "--run-id"): return True
    if kind == "inspect" and (option == "--run-id" or option == "--impulse-type-id" or option == "--relation-id" or option == "--reaction-id" or option == "--association-id" or option == "--impulse-id" or option == "--process-id" or option == "--command-id"): return True
    if kind == "commands-list" and (option == "--run-id" or option == "--command-type" or option == "--actor" or option == "--limit" or option == "--jsonl"): return True
    if kind == "events-list" and (option == "--run-id" or option == "--impulse-id" or option == "--after-sequence" or option == "--limit" or option == "--event-type" or option == "--process-id" or option == "--command-id" or option == "--actor" or option == "--jsonl"): return True
    if kind == "processes-list" and (option == "--run-id" or option == "--status" or option == "--impulse-id" or option == "--limit" or option == "--jsonl"): return True
    if kind == "impulses-list" and (option == "--run-id" or option == "--impulse-type" or option == "--limit" or option == "--jsonl"): return True
    if kind == "impulse-types-list" and (option == "--run-id" or option == "--limit" or option == "--jsonl"): return True
    if kind == "impulse-relations-list" and (option == "--run-id" or option == "--impulse-id" or option == "--relation-type" or option == "--limit" or option == "--jsonl"): return True
    if kind == "associations-list" and (option == "--run-id" or option == "--impulse-id" or option == "--kind" or option == "--limit" or option == "--jsonl"): return True
    if kind == "reactions-list" and (option == "--run-id" or option == "--impulse-id" or option == "--kind" or option == "--limit" or option == "--jsonl"): return True
    if kind == "homeostats-list" and (option == "--run-id" or option == "--impulse-id" or option == "--status" or option == "--kind" or option == "--limit" or option == "--jsonl"): return True
    if kind == "projections-list" and (option == "--run-id" or option == "--limit" or option == "--jsonl"): return True
    if kind == "bridge-list" and (option == "--run-id" or option == "--status" or option == "--box" or option == "--limit" or option == "--jsonl"): return True
    if kind == "rows" and (option == "--run-id" or option == "--impulse-id" or option == "--impulse-type" or option == "--status" or option == "--event-type" or option == "--process-id" or option == "--command-id" or option == "--command-type" or option == "--actor" or option == "--relation-type" or option == "--kind" or option == "--after-sequence" or option == "--limit" or option == "--box" or option == "--jsonl"): return True
    if kind == "trace" and option == "--run-id": return True
    if kind == "diagnose-waits" and (option == "--run-id" or option == "--impulse-id"): return True
    if kind == "run-list" and (option == "--db" or option == "--run-id" or option == "--status" or option == "--limit" or option == "--jsonl"): return True
    if kind == "event-schema" and (option == "--db" or option == "--run-id" or option == "--max-schema-version"): return True
    if kind == "projection" and (option == "--run-id" or option == "--name" or option == "--now"): return True
    if kind == "maintenance" and (option == "--older-than-days" or option == "--keep-last" or option == "--reaction-root" or option == "--dry-run" or option == "--delete" or option == "--vacuum" or option == "--no-vacuum"): return True
    if kind == "gc" and (option == "--reaction-root" or option == "--run-id" or option == "--older-than" or option == "--dry-run" or option == "--delete"): return True
    if kind == "reaction-record" and (option == "--run-id" or option == "--reaction-root" or option == "--path" or option == "--kind" or option == "--reaction-id" or option == "--impulse-id" or option == "--media-type" or option == "--metadata-json" or option == "--idempotency-key" or option == "--now"): return True
    if kind == "bridge-deliver" and (option == "--run-id" or option == "--delivery-id" or option == "--target-db" or option == "--idempotency-key" or option == "--import-idempotency-key" or option == "--now"): return True
    if kind == "bridge-export" and (option == "--delivery-id" or option == "--out"): return True
    if kind == "bridge-import" and (option == "--file" or option == "--idempotency-key"): return True
    return False


def _validate_status_value(kind: String, value: String) raises SQLiteError:
    if value == "": return
    var valid = False
    if kind == "run-list":
        valid = value == "created" or value == "active" or value == "waiting" or value == "completed" or value == "failed" or value == "cancel_requested" or value == "cancelled" or value == "timed_out"
    elif kind == "processes-list":
        valid = value == "pending" or value == "ready" or value == "running" or value == "waiting" or value == "retry_wait" or value == "succeeded" or value == "failed" or value == "cancel_requested" or value == "cancelled" or value == "timed_out"
    elif kind == "homeostats-list":
        valid = value == "open" or value == "completed" or value == "cancelled" or value == "expired"
    elif kind == "bridge-list":
        valid = value == "pending" or value == "delivered" or value == "imported" or value == "failed"
    if not valid:
        raise SQLiteError(code=2, message="argument_error: invalid value for --status: " + value)
def _has_option(command: String, name: String) -> Bool:
    var index = 0
    while index < _count(command):
        var item = _word(command, index)
        if _option_base(item) == name: return True
        index += 1
    return False
def _bool_option(command: String, name: String) raises SQLiteError -> Bool:
    """Parse a boolean option without consuming a following token."""
    var index = 0
    while index < _count(command):
        var item = _word(command, index)
        if _option_base(item) == name:
            var equals = item.find("=")
            if equals < 0: return True
            var value = String(item[byte=equals + 1:])
            if value == "true": return True
            if value == "false": return False
            raise SQLiteError(code=2, message="argument_error: invalid boolean value for " + name)
        index += 1
    return False



def _limit(command: String) raises SQLiteError -> Int:
    var raw = _flag(command, "--limit", "")
    if raw == "": return -1
    try:
        var parsed = parse_json(raw)
        var value = -1
        if parsed.value.is_int(): value = Int(parsed.value.int())
        elif parsed.value.is_uint(): value = Int(parsed.value.uint())
        else: raise Error("not integer")
        if value < -1: raise Error("negative")
        return value
    except err:
        raise SQLiteError(code=2, message="argument_error: invalid integer value for --limit")


def _after_sequence(command: String) raises SQLiteError -> Int:
    var raw = _flag(command, "--after-sequence", "")
    if raw == "": return -1
    try:
        var parsed = parse_json(raw)
        var value = -1
        if parsed.value.is_int(): value = Int(parsed.value.int())
        elif parsed.value.is_uint(): value = Int(parsed.value.uint())
        else: raise Error("not integer")
        if value < 0: raise Error("negative")
        return value
    except err:
        raise SQLiteError(code=2, message="argument_error: invalid nonnegative integer value for --after-sequence")
 
 
 
 
def _maintenance_number(command: String, name: String, default: Float64 = 0.0) raises SQLiteError -> Float64:
    var raw = _flag(command, name, "")
    if raw == "": return default
    try:
        var parsed = parse_json(raw)
        if parsed.value.is_float(): return parsed.value.float()
        if parsed.value.is_int(): return Float64(parsed.value.int())
        if parsed.value.is_uint(): return Float64(parsed.value.uint())
    except err:
        pass
    raise SQLiteError(code=2, message="argument_error: invalid numeric value for " + name)


def _maintenance_integer(command: String, name: String, default: Int = -1) raises SQLiteError -> Int:
    var raw = _flag(command, name, "")
    if raw == "": return default
    try:
        var parsed = parse_json(raw)
        if parsed.value.is_int(): return Int(parsed.value.int())
        if parsed.value.is_uint(): return Int(parsed.value.uint())
    except err:
        pass
    raise SQLiteError(code=2, message="argument_error: invalid integer value for " + name)


def _validate(command: String, kind: String, positional: Bool = False) raises SQLiteError:
    var index = 0
    var count = _count(command)
    if kind == "run-list" or kind == "run-observe" or kind == "event-schema" or kind == "homeostat-list" or kind == "inspect" or kind == "rows" or kind == "commands-list" or kind == "events-list" or kind == "processes-list" or kind == "impulses-list" or kind == "impulse-types-list" or kind == "impulse-relations-list" or kind == "associations-list" or kind == "reactions-list" or kind == "homeostats-list" or kind == "projections-list" or kind == "bridge-list" or kind == "projection" or kind == "reaction-record" or kind == "bridge-deliver" or kind == "bridge-export" or kind == "bridge-import" or kind == "transition" or kind == "impulse-create" or kind == "process-schedule" or kind == "process-transition" or kind == "association-append" or kind == "homeostat-open" or kind == "homeostat-transition" or kind == "homeostat-domain-open" or kind == "homeostat-domain-transition": index = 2
    elif kind == "doctor" or kind == "trace" or kind == "diagnose-waits": index = 1
    elif kind == "init": index = 1
    elif kind == "create" or kind == "schema" or kind == "maintenance" or kind == "gc": index = 1
    elif kind == "db": index = 2
    var has_positional = False
    var has_db_option = False
    while index < count:
        var item = _word(command, index)
        if item.startswith("--"):
            if not _known_option(kind, item):
                raise SQLiteError(code=2, message="argument_error: unknown argument " + item)
            var option = _option_base(item)
            if option == "--db": has_db_option = True
            var equals = item.find("=")
            if (kind == "run-list" or kind == "rows" or kind == "homeostat-list" or kind == "commands-list" or kind == "events-list" or kind == "processes-list" or kind == "impulses-list" or kind == "impulse-types-list" or kind == "impulse-relations-list" or kind == "associations-list" or kind == "reactions-list" or kind == "homeostats-list" or kind == "projections-list" or kind == "bridge-list") and option == "--jsonl":
                _ = _bool_option(command, "--jsonl")
                index += 1
                continue
            if kind == "gc" and (option == "--dry-run" or option == "--delete"):
                index += 1
                continue
            if (kind == "db" or kind == "doctor") and option == "--ensure-schema":
                index += 1
                continue
            if kind == "maintenance" and (option == "--dry-run" or option == "--delete" or option == "--vacuum" or option == "--no-vacuum"):
                index += 1
                continue
            if equals >= 0:
                if equals + 1 >= item.byte_length():
                    raise SQLiteError(code=2, message="argument_error: missing value for " + option)
                index += 1
                continue
            if index + 1 >= count or _word(command, index + 1).startswith("--"):
                raise SQLiteError(code=2, message="argument_error: missing value for " + option)
            index += 2
        else:
            if not positional or has_positional or has_db_option:
                raise SQLiteError(code=2, message="argument_error: unknown argument " + item)
            has_positional = True
            index += 1
    if kind == "schema" and not has_positional:
        raise SQLiteError(code=2, message="argument_error: schema model is required")
    if kind == "schema":
        var model = _word(command, 1)
        if model != "impulse" and model != "model" and model != "fala-package":
            raise SQLiteError(code=2, message="argument_error: unknown schema model " + model)
    if kind == "reaction-record":
        if _flag(command, "--db", "") == "": raise SQLiteError(code=2, message="argument_error: --db is required")
        if _flag(command, "--run-id", "") == "": raise SQLiteError(code=2, message="argument_error: --run-id is required")
        if _flag(command, "--reaction-root", "") == "": raise SQLiteError(code=2, message="argument_error: --reaction-root is required")
        if _flag(command, "--path", "") == "": raise SQLiteError(code=2, message="argument_error: --path is required")
        if _flag(command, "--kind", "") == "": raise SQLiteError(code=2, message="argument_error: --kind is required")
    if kind == "bridge-deliver":
        if _flag(command, "--db", "") == "": raise SQLiteError(code=2, message="argument_error: --db is required")
        if _flag(command, "--run-id", "") == "": raise SQLiteError(code=2, message="argument_error: --run-id is required")
        if _flag(command, "--delivery-id", "") == "": raise SQLiteError(code=2, message="argument_error: --delivery-id is required")
        if _flag(command, "--target-db", "") == "": raise SQLiteError(code=2, message="argument_error: --target-db is required")
        if _flag(command, "--now", "") == "": raise SQLiteError(code=2, message="argument_error: --now is required")
    if kind == "bridge-export":
        if _flag(command, "--db", "") == "": raise SQLiteError(code=2, message="argument_error: --db is required")
        if _flag(command, "--run-id", "") == "": raise SQLiteError(code=2, message="argument_error: --run-id is required")
    if kind == "homeostat-list" or kind == "homeostats-list":
        if _flag(command, "--run-id", "") == "": raise SQLiteError(code=2, message="argument_error: --run-id is required")
    if kind == "homeostat-domain-open" or kind == "homeostat-domain-transition":
        if _flag(command, "--db", "") == "": raise SQLiteError(code=2, message="argument_error: --db is required")
        if _flag(command, "--run-id", "") == "": raise SQLiteError(code=2, message="argument_error: --run-id is required")
    if kind == "homeostat-domain-open" and _flag(command, "--kind", "") == "":
        raise SQLiteError(code=2, message="argument_error: --kind is required")
    if kind == "homeostat-domain-transition" and _flag(command, "--homeostat-id", "") == "":
        raise SQLiteError(code=2, message="argument_error: --homeostat-id is required")
    if kind == "commands-list" or kind == "events-list" or kind == "processes-list" or kind == "impulses-list" or kind == "impulse-types-list" or kind == "impulse-relations-list" or kind == "associations-list" or kind == "reactions-list" or kind == "homeostats-list" or kind == "projections-list" or kind == "bridge-list":
        if _flag(command, "--run-id", "") == "": raise SQLiteError(code=2, message="argument_error: --run-id is required")
    if kind == "bridge-import":
        if _flag(command, "--db", "") == "": raise SQLiteError(code=2, message="argument_error: --db is required")
        if _flag(command, "--file", "") == "": raise SQLiteError(code=2, message="argument_error: --file is required")
    if kind == "gc":
        if _flag(command, "--db", "") == "": raise SQLiteError(code=2, message="argument_error: --db is required")
        if _flag(command, "--reaction-root", "") == "": raise SQLiteError(code=2, message="argument_error: --reaction-root is required")
    if kind == "rows":
        var first = _word(command, 0)
        var requires_run = first == "runs" or first == "commands" or first == "events" or first == "processes" or first == "bridge" or first == "trace" or first == "diagnose-waits" or first == "impulses" or first == "impulse-types" or first == "impulse-relations" or first == "relations" or first == "associations" or first == "reactions" or first == "homeostats" or first == "projections"
        if requires_run and _flag(command, "--run-id") == "":
            raise SQLiteError(code=2, message="argument_error: --run-id is required")
    if kind == "trace" and _flag(command, "--run-id") == "":
        raise SQLiteError(code=2, message="argument_error: --run-id is required")
    if kind == "diagnose-waits":
        if _flag(command, "--db", "") == "": raise SQLiteError(code=2, message="argument_error: --db is required")
        if _flag(command, "--run-id", "") == "": raise SQLiteError(code=2, message="argument_error: --run-id is required")

    if kind == "projection" and _flag(command, "--run-id", "") == "":
        raise SQLiteError(code=2, message="argument_error: --run-id is required")
    if kind == "create" or kind == "transition" or kind == "impulse-create" or kind == "process-schedule" or kind == "process-transition" or kind == "association-append" or kind == "homeostat-open" or kind == "homeostat-transition" or kind == "projection" or kind == "reaction-record":
        if _flag(command, "--now", "") == "":
            raise SQLiteError(code=2, message="argument_error: --now is required")
    if kind == "run-list" or kind == "processes-list" or kind == "homeostats-list" or kind == "bridge-list":
        _validate_status_value(kind, _flag(command, "--status", ""))
def _positional_path(command: String, start: Int) -> String:
    var count = _count(command)
    var index = start
    while index < count:
        var item = _word(command, index)
        if not item.startswith("--"): return item
        index += 1
    return ""
def _hex_digit(ch: String) -> Int:
    var digits = "0123456789abcdefABCDEF"
    var index = digits.find(ch)
    if index < 0: return -1
    if index < 16: return index
    return index - 6


def _url_decode(value: String) -> String:
    var result = String()
    var index = 0
    while index < value.byte_length():
        var ch = value[byte=index]
        if ch == "%" and index + 2 < value.byte_length():
            var high = _hex_digit(String(value[byte=index + 1]))
            var low = _hex_digit(String(value[byte=index + 2]))
            if high >= 0 and low >= 0:
                result.append(Codepoint(UInt8(high * 16 + low)))
                index += 3
                continue
        result += String(ch)
        index += 1
    return result


def _database_url(value: String) raises SQLiteError -> String:
    var rest = ""
    if value.startswith("sqlite://"):
        rest = String(value[byte=8:])
    elif value.startswith("sqlite3://"):
        rest = String(value[byte=9:])
    else:
        var colon = value.find(":")
        if colon > 0: raise SQLiteError(code=2, message="argument_error: unsupported database URL scheme")
        return value
    var path = ""
    if rest.startswith("/localhost/"):
        path = "/" + String(rest[byte=10:])
    elif rest.startswith("localhost/"):
        path = "/" + String(rest[byte=9:])
    elif rest.startswith("localhost"):
        raise SQLiteError(code=2, message="argument_error: SQLite URL must include a database path")
    elif rest.startswith("//"):
        # sqlite:////tmp/db encodes an absolute path.
        path = String(rest[byte=1:])
    elif rest.startswith("/"):
        # sqlite:///relative.db is relative, matching the reference resolver.
        path = String(rest[byte=1:])
    else:
        raise SQLiteError(code=2, message="argument_error: SQLite URL host must be empty or localhost")
    if path == "": raise SQLiteError(code=2, message="argument_error: SQLite URL must include a database path")
    return _url_decode(path)


def _parent_directory(path: String) -> String:
    var last = -1
    for i in range(path.byte_length()):
        if path[byte=i] == "/": last = i
    if last < 0: return "."
    if last == 0: return "/"
    return String(path[byte=0:last])

def _path(command: String) raises SQLiteError -> String:
    var path = _flag(command, "--db", "")
    var first = _word(command, 0)
    if path == "":
        if first == "db": path = _positional_path(command, 2)
        elif first == "doctor": path = _positional_path(command, 1)
    if path == "" and first != "doctor" and first != "init":
        raise SQLiteError(code=2, message="argument_error: --db is required")
    if path == "": path = ".fala/state.sqlite"
    path = _database_url(path)
    if not _safe(path): raise SQLiteError(code=2, message="unsafe_path: invalid database path")
    return path


def _require_db_value(command: String, kind: String) raises SQLiteError:
    var count = _count(command)
    var index = 0
    if kind == "db": index = 2
    elif kind == "doctor": index = 1
    while index < count:
        var item = _word(command, index)
        if item == "--db":
            if index + 1 >= count or _word(command, index + 1).startswith("--"):
                raise SQLiteError(code=2, message="argument_error: missing value for --db")
            return
        if _option_base(item) == "--db":
            var equals = item.find("=")
            if equals < 0 or equals + 1 >= item.byte_length():
                raise SQLiteError(code=2, message="argument_error: missing value for --db")
            return
        index += 1


def _json(value: String) raises SQLiteError:
    try:
        var parsed = parse_json(value)
        _ = parsed
    except err:
        raise SQLiteError(code=2, message="invalid_json")

def _metadata_value(command: String) raises SQLiteError -> String:
    var values = _repeat_values(command, "--metadata")
    if len(values) == 0: return "{}"
    # Preserve the legacy single JSON-object form. Repeated JSON objects are
    # ambiguous; key=value entries are the repeatable form.
    var object_values = 0
    for value in values:
        try:
            var parsed = parse_json(value)
            if parsed.value.is_object(): object_values += 1
        except err:
            pass
    if object_values > 0:
        if len(values) != 1 or object_values != 1:
            raise SQLiteError(code=2, message="argument_error: repeated --metadata JSON objects are ambiguous")
        try:
            var parsed = parse_json(values[0])
            return canonical_json_text(to_string(parsed.value))
        except err:
            raise SQLiteError(code=2, message="invalid_json: metadata")
    var metadata = Object(capacity=len(values))
    for item in values:
        var equals = item.find("=")
        if equals <= 0:
            raise SQLiteError(code=2, message="argument_error: invalid value " + item + "; expected key=value")
        var key = String(item[byte=0:equals])
        var value = String(item[byte=equals + 1:])
        metadata[key] = Value(value)
    try:
        return canonical_json_text(to_string(Value(metadata^)))
    except err:
        raise SQLiteError(code=2, message="invalid_json: metadata")

def _repeat_values(command: String, name: String) raises SQLiteError -> List[String]:
    var values = List[String]()
    var index = 0
    var count = _count(command)
    while index < count:
        var item = _word(command, index)
        if _option_base(item) == name:
            var equals = item.find("=")
            if equals >= 0:
                if equals + 1 >= item.byte_length(): raise SQLiteError(code=2, message="argument_error: missing value for " + name)
                var value = String(item[byte=equals + 1:])
                values.append(value^)
            else:
                if index + 1 >= count or _word(command, index + 1).startswith("--"):
                    raise SQLiteError(code=2, message="argument_error: missing value for " + name)
                values.append(_word(command, index + 1)^)
                index += 1
        index += 1
    return values^

def _string_array(values: List[String]) -> String:
    var result = "["
    var first = True
    for value in values:
        if not first: result += ","
        first = False
        result += _quote(value)
    result += "]"
    return result

def _init(command: String) raises SQLiteError -> String:
    var db_path = _path(command)
    var reaction_root = _flag(command, "--reaction-root", ".fala/reactions")
    if not _safe(reaction_root):
        raise SQLiteError(code=2, message="unsafe_path: invalid reaction root path")
    try:
        makedirs(Path(_parent_directory(db_path)), exist_ok=True)
        makedirs(Path(reaction_root) / "blobs" / "sha256", exist_ok=True)
        _ = initialize_database(db_path)
    except err:
        raise SQLiteError(code=1, message="init failed: " + String(err))
    return "{\"ok\":true,\"runtime\":\"mojo\",\"db\":" + _quote(db_path) + ",\"reaction_root\":" + _quote(reaction_root) + ",\"schema_version\":" + String(SCHEMA_VERSION) + "}"

def initialize_database(path: String) raises SQLiteError -> String:
    var connection = Connection(path)
    initialize_native_schema(connection)
    connection.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"database\":" + _quote(path) + "}"


def _schema_model() -> String:
    var tables = "["
    var first = True
    for name in table_names():
        if not first: tables += ","
        first = False
        tables += _quote(name)
    tables += "]"
    return "{\"ok\":true,\"runtime\":\"mojo\",\"schema\":{\"version\":" + String(SCHEMA_VERSION) + ",\"tables\":" + tables + "}}"
def _schema_status_json(status: SchemaStatus) -> String:
    var missing = "["
    var first = True
    for name in status.missing_tables:
        if not first: missing += ","
        first = False
        missing += _quote(name)
    missing += "]"
    var process_id = "false"
    if status.runtime_events_has_process_id: process_id = "true"
    var event_schema = "false"
    if status.runtime_events_has_schema_version: event_schema = "true"
    var current = "false"
    if status.is_current(): current = "true"
    return "{\"current_version\":" + String(status.current_version) + ",\"latest_version\":" + String(status.latest_version) + ",\"user_version\":" + String(status.user_version) + ",\"migration_version\":" + String(status.migration_version) + ",\"missing_tables\":" + missing + ",\"runtime_events_has_process_id\":" + process_id + ",\"runtime_events_has_schema_version\":" + event_schema + ",\"current\":" + current + "}"


def _migration_metadata(mut connection: Connection) raises SQLiteError -> String:
    var table = connection.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations'")
    if not table.step():
        table.close()
        return "null"
    table.close()
    var stmt = connection.query("SELECT id,version,name,applied_at FROM schema_migrations WHERE id='runtime_backend'")
    if not stmt.step():
        stmt.close()
        return "null"
    var result = "{\"id\":" + _quote(stmt.column_text(0)) + ",\"version\":" + String(stmt.column_int(1)) + ",\"name\":" + _quote(stmt.column_text(2)) + ",\"applied_at\":" + _quote(stmt.column_text(3)) + "}"
    stmt.close()
    return result


def _status(path: String) raises SQLiteError -> String:
    var connection = Connection(path)
    var status = schema_status(connection)
    var current = "false"
    if status.is_current(): current = "true"
    var user_version = status.user_version
    var status_json = _schema_status_json(status^)
    var tables = 0
    var count = connection.query("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    if count.step(): tables = count.column_int(0)
    var migration = _migration_metadata(connection)
    connection.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"database\":" + _quote(path) + ",\"schema_version\":" + String(user_version) + ",\"expected_version\":" + String(SCHEMA_VERSION) + ",\"table_count\":" + String(tables) + ",\"schema\":" + status_json + ",\"migration\":" + migration + ",\"current\":" + current + "}"


def _vacuum(path: String) raises SQLiteError -> String:
    var connection = Connection(path)
    connection.execute("VACUUM")
    connection.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"database\":" + _quote(path) + ",\"vacuumed\":true}"


def _text(mut stmt: Statement, index: Int) raises SQLiteError -> String:
    if stmt.column_null(index): return String("")
    return stmt.column_text(index)
def _nullable_text_json(mut stmt: Statement, index: Int) raises SQLiteError -> String:
    if stmt.column_null(index): return "null"
    return _quote(stmt.column_text(index))

def _nullable_int_json(mut stmt: Statement, index: Int) raises SQLiteError -> String:
    if stmt.column_null(index): return "null"
    return String(stmt.column_int(index))



def _event_schema_max(command: String) raises SQLiteError -> Int:
    var raw = _flag(command, "--max-schema-version", "1")
    try:
        var parsed = parse_json(raw)
        var value = -1
        if parsed.value.is_int(): value = Int(parsed.value.int())
        elif parsed.value.is_uint(): value = Int(parsed.value.uint())
        else: raise Error("not integer")
        if value < 1: raise Error("non-positive")
        return value
    except err:
        raise SQLiteError(code=2, message="argument_error: --max-schema-version must be greater than zero")


def _events_validate_schema(path: String, command: String) raises SQLiteError -> String:
    var run_id = _flag(command, "--run-id")
    if run_id == "": raise SQLiteError(code=2, message="argument_error: --run-id is required")
    var max_schema_version = _event_schema_max(command)
    var connection = Connection(path)
    initialize_native_schema(connection)
    var stmt = connection.query("SELECT id,sequence,event_type,schema_version FROM runtime_events WHERE run_id=? ORDER BY sequence ASC")
    stmt.bind_text(1, run_id)
    var event_count = 0
    var versions = "{"
    var version_counts = List[Int]()
    var version_values = List[Int]()
    var unsupported = "["
    var unsupported_first = True
    while stmt.step():
        var version = stmt.column_int(3)
        var version_index = -1
        for index in range(len(version_values)):
            if version_values[index] == version: version_index = index
        if version_index < 0:
            version_values.append(version)
            version_counts.append(1)
        else:
            version_counts[version_index] += 1
        if version > max_schema_version:
            if not unsupported_first: unsupported += ","
            unsupported_first = False
            unsupported += "{\"id\":" + _quote(_text(stmt, 0)) + ",\"sequence\":" + String(stmt.column_int(1)) + ",\"event_type\":" + _quote(_text(stmt, 2)) + ",\"schema_version\":" + String(version) + "}"
        event_count += 1
    stmt.close()
    connection.close()
    # Event schema versions are emitted in numeric order like the reference.
    for outer in range(len(version_values)):
        for inner in range(outer + 1, len(version_values)):
            if version_values[inner] < version_values[outer]:
                var swap_version = version_values[outer]
                version_values[outer] = version_values[inner]
                version_values[inner] = swap_version
                var swap_count = version_counts[outer]
                version_counts[outer] = version_counts[inner]
                version_counts[inner] = swap_count
    for index in range(len(version_values)):
        if index > 0: versions += ","
        versions += _quote(String(version_values[index])) + ":" + String(version_counts[index])
    versions += "}"
    unsupported += "]"
    var ok = "true"
    if unsupported != "[]": ok = "false"
    return "{\"ok\":" + ok + ",\"event_count\":" + String(event_count) + ",\"max_schema_version\":" + String(max_schema_version) + ",\"schema_versions\":" + versions + ",\"unsupported_events\":" + unsupported + "}"




def _runs(path: String, status: String, run_id: String = "", limit: Int = -1, jsonl: Bool = False) raises SQLiteError -> String:
    var connection = Connection(path)
    initialize_native_schema(connection)
    var sql = "SELECT id,status,title,package_id,package_version,package_digest,correlation_path_id,correlation_path_digest,runtime_version,backend_version,schema_version,metadata,created_at,updated_at,started_at,finished_at FROM runs"
    var has_where = False
    if status != "": sql += " WHERE status=?"; has_where = True
    if run_id != "":
        if has_where: sql += " AND id=?"
        else: sql += " WHERE id=?"; has_where = True
    sql += " ORDER BY created_at ASC,id ASC"
    if limit >= 0: sql += " LIMIT ?"
    var stmt = connection.query(sql)
    var bind = 1
    if status != "": stmt.bind_text(bind, status); bind += 1
    if run_id != "": stmt.bind_text(bind, run_id); bind += 1
    if limit >= 0: stmt.bind_int(bind, limit)
    var items = "["
    var lines = ""
    var first = True
    var line_first = True
    var row_count = 0
    while stmt.step():
        var row_json = "{\"id\":" + _quote(_text(stmt,0)) + ",\"status\":" + _quote(_text(stmt,1)) + ",\"title\":" + _nullable_text_json(stmt,2) + ",\"package_id\":" + _nullable_text_json(stmt,3) + ",\"package_version\":" + _nullable_text_json(stmt,4) + ",\"package_digest\":" + _nullable_text_json(stmt,5) + ",\"correlation_path_id\":" + _nullable_text_json(stmt,6) + ",\"correlation_path_digest\":" + _nullable_text_json(stmt,7) + ",\"runtime_version\":" + _nullable_text_json(stmt,8) + ",\"backend_version\":" + _nullable_text_json(stmt,9) + ",\"schema_version\":" + String(stmt.column_int(10)) + ",\"metadata\":" + _text(stmt,11) + ",\"created_at\":" + _quote(_text(stmt,12)) + ",\"updated_at\":" + _quote(_text(stmt,13)) + ",\"started_at\":" + _nullable_text_json(stmt,14) + ",\"finished_at\":" + _nullable_text_json(stmt,15) + "}"
        row_count += 1
        if not first: items += ","
        first = False
        items += row_json
        if jsonl:
            if not line_first: lines += "\n"
            line_first = False
            lines += row_json
    items += "]"
    stmt.close()
    connection.close()
    if jsonl: return lines
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"runs\",\"count\":" + String(row_count) + ",\"items\":" + items + "}"

def _command_rows(path: String, command: String) raises SQLiteError -> String:
    var run_id = _flag(command, "--run-id")
    if run_id == "": raise SQLiteError(code=2, message="argument_error: --run-id is required")
    var command_type = _flag(command, "--command-type")
    var actor = _flag(command, "--actor")
    var limit = _limit(command)
    var jsonl = _bool_option(command, "--jsonl")
    var connection = Connection(path)
    initialize_native_schema(connection)
    var sql = "SELECT run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at FROM runtime_commands WHERE run_id=?"
    if command_type != "": sql += " AND command_type=?"
    if actor != "": sql += " AND actor=?"
    sql += " ORDER BY created_at ASC,id ASC"
    if limit >= 0: sql += " LIMIT ?"
    var stmt = connection.query(sql)
    var bind = 1
    stmt.bind_text(bind, run_id); bind += 1
    if command_type != "": stmt.bind_text(bind, command_type); bind += 1
    if actor != "": stmt.bind_text(bind, actor); bind += 1
    if limit >= 0: stmt.bind_int(bind, limit)
    var items = "["
    var lines = ""
    var first = True
    var line_first = True
    var row_count = 0
    while stmt.step():
        var row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"id\":" + _quote(_text(stmt,1)) + ",\"command_type\":" + _quote(_text(stmt,2)) + ",\"idempotency_key\":" + _quote(_text(stmt,3)) + ",\"actor\":" + _nullable_text_json(stmt,4) + ",\"correlation_id\":" + _nullable_text_json(stmt,5) + ",\"causation_id\":" + _nullable_text_json(stmt,6) + ",\"payload\":" + _text(stmt,7) + ",\"created_at\":" + _quote(_text(stmt,8)) + "}"
        if not first: items += ","
        first = False
        items += row_json
        if jsonl:
            if not line_first: lines += "\n"
            line_first = False
            lines += row_json
        row_count += 1
    items += "]"
    stmt.close()
    connection.close()
    if jsonl: return lines
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"commands\",\"count\":" + String(row_count) + ",\"items\":" + items + "}"



def _run_inspect(path: String, run_id: String) raises SQLiteError -> String:
    if run_id == "": raise SQLiteError(code=2, message="argument_error: --run-id is required")
    var connection = Connection(path)
    initialize_native_schema(connection)
    var stmt = connection.query("SELECT id,status,title,package_id,package_version,package_digest,correlation_path_id,correlation_path_digest,runtime_version,backend_version,schema_version,metadata,created_at,updated_at,started_at,finished_at FROM runs WHERE id=?")
    stmt.bind_text(1, run_id)
    if not stmt.step():
        stmt.close()
        connection.close()
        return "{\"ok\":false,\"runtime\":\"mojo\",\"run\":null}"
    var result = "{\"ok\":true,\"runtime\":\"mojo\",\"run\":{"
    result += "\"id\":" + _quote(_text(stmt, 0))
    result += ",\"status\":" + _quote(_text(stmt, 1))
    result += ",\"title\":" + _nullable_text_json(stmt, 2)
    result += ",\"package_id\":" + _nullable_text_json(stmt, 3)
    result += ",\"package_version\":" + _nullable_text_json(stmt, 4)
    result += ",\"package_digest\":" + _nullable_text_json(stmt, 5)
    result += ",\"correlation_path_id\":" + _nullable_text_json(stmt, 6)
    result += ",\"correlation_path_digest\":" + _nullable_text_json(stmt, 7)
    result += ",\"runtime_version\":" + _nullable_text_json(stmt, 8)
    result += ",\"backend_version\":" + _nullable_text_json(stmt, 9)
    result += ",\"schema_version\":" + String(stmt.column_int(10))
    result += ",\"metadata\":" + _text(stmt, 11)
    result += ",\"created_at\":" + _quote(_text(stmt, 12))
    result += ",\"updated_at\":" + _quote(_text(stmt, 13))
    result += ",\"started_at\":" + _nullable_text_json(stmt, 14)
    result += ",\"finished_at\":" + _nullable_text_json(stmt, 15)
    result += "}}"
    stmt.close()
    connection.close()
    return result

def _run_observe(path: String, run_id: String) raises SQLiteError -> String:
    if run_id == "": raise SQLiteError(code=2, message="argument_error: --run-id is required")
    var journal = NativeJournal.open(path)
    journal.initialize()
    var boundary = observe_run_boundary(journal, run_id)
    journal.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"boundary\":{\"run_id\":" + _quote(boundary.run_id) + ",\"status\":" + _quote(boundary.status) + ",\"derived_status\":" + _quote(boundary.derived_status) + ",\"process_status_counts\":" + boundary.process_status_counts + ",\"event_watermark\":" + String(boundary.event_watermark) + "}}"

def _diagnose_waits(path: String, run_id: String, impulse_id: String = "") raises SQLiteError -> String:
    if run_id == "": raise SQLiteError(code=2, message="argument_error: --run-id is required")
    var journal = NativeJournal.open(path)
    journal.initialize()
    var diagnostic = diagnose_wait_graph(journal, run_id, impulse_id)
    var result = "{\"ok\":true,\"runtime\":\"mojo\",\"wait_diagnostics\":" + diagnostic.to_json() + "}"
    journal.close()
    return result

def _impulse_inspect(path: String, run_id: String, impulse_id: String) raises SQLiteError -> String:
    if run_id == "": raise SQLiteError(code=2, message="argument_error: --run-id is required")
    if impulse_id == "": raise SQLiteError(code=2, message="argument_error: --impulse-id is required")
    var connection = Connection(path)
    initialize_native_schema(connection)
    var stmt = connection.query("SELECT id,run_id,impulse_type,payload,metadata,created_at,updated_at FROM impulses WHERE run_id=? AND id=?")
    stmt.bind_text(1, run_id)
    stmt.bind_text(2, impulse_id)
    if not stmt.step():
        stmt.close()
        connection.close()
        return "{\"ok\":false,\"runtime\":\"mojo\",\"impulse\":null}"
    var result = "{\"ok\":true,\"runtime\":\"mojo\",\"impulse\":{"
    result += "\"id\":" + _quote(_text(stmt, 0))
    result += ",\"run_id\":" + _quote(_text(stmt, 1))
    result += ",\"impulse_type\":" + _quote(_text(stmt, 2))
    result += ",\"payload\":" + _text(stmt, 3)
    result += ",\"metadata\":" + _text(stmt, 4)
    result += ",\"created_at\":" + _quote(_text(stmt, 5))
    result += ",\"updated_at\":" + _quote(_text(stmt, 6))
    result += "}}"
    stmt.close()
    connection.close()
    return result

def _command_inspect(path: String, run_id: String, command_id: String) raises SQLiteError -> String:
    if run_id == "": raise SQLiteError(code=2, message="argument_error: --run-id is required")
    if command_id == "": raise SQLiteError(code=2, message="argument_error: --command-id is required")
    var connection = Connection(path)
    initialize_native_schema(connection)
    var stmt = connection.query("SELECT run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at FROM runtime_commands WHERE run_id=? AND id=?")
    stmt.bind_text(1, run_id)
    stmt.bind_text(2, command_id)
    if not stmt.step():
        stmt.close()
        connection.close()
        return "{\"ok\":false,\"runtime\":\"mojo\",\"command\":null}"
    var result = "{\"ok\":true,\"runtime\":\"mojo\",\"command\":{"
    result += "\"run_id\":" + _quote(_text(stmt, 0))
    result += ",\"id\":" + _quote(_text(stmt, 1))
    result += ",\"command_type\":" + _quote(_text(stmt, 2))
    result += ",\"idempotency_key\":" + _quote(_text(stmt, 3))
    result += ",\"actor\":" + _nullable_text_json(stmt, 4)
    result += ",\"correlation_id\":" + _nullable_text_json(stmt, 5)
    result += ",\"causation_id\":" + _nullable_text_json(stmt, 6)
    result += ",\"payload\":" + _text(stmt, 7)
    result += ",\"created_at\":" + _quote(_text(stmt, 8)) + "}}"
    stmt.close()
    connection.close()
    return result

def _process_inspect(path: String, run_id: String, process_id: String) raises SQLiteError -> String:
    if run_id == "": raise SQLiteError(code=2, message="argument_error: --run-id is required")
    if process_id == "": raise SQLiteError(code=2, message="argument_error: --process-id is required")
    var connection = Connection(path)
    initialize_native_schema(connection)
    var stmt = connection.query("SELECT run_id,id,process_type,impulse_id,status,priority,attempt,max_attempts,available_at,lease_owner,lease_expires_at,input_json,output_json,error_json,metadata,created_at,updated_at,started_at,finished_at,output_schema_json FROM processes WHERE run_id=? AND id=?")
    stmt.bind_text(1, run_id)
    stmt.bind_text(2, process_id)
    if not stmt.step():
        stmt.close()
        connection.close()
        return "{\"ok\":false,\"runtime\":\"mojo\",\"process\":null}"
    var result = "{\"ok\":true,\"runtime\":\"mojo\",\"process\":{"
    result += "\"run_id\":" + _quote(_text(stmt, 0))
    result += ",\"id\":" + _quote(_text(stmt, 1))
    result += ",\"process_type\":" + _quote(_text(stmt, 2))
    result += ",\"impulse_id\":" + _nullable_text_json(stmt, 3)
    result += ",\"status\":" + _quote(_text(stmt, 4))
    result += ",\"priority\":" + String(stmt.column_int(5))
    result += ",\"attempt\":" + String(stmt.column_int(6))
    result += ",\"max_attempts\":" + String(stmt.column_int(7))
    result += ",\"available_at\":" + _quote(_text(stmt, 8))
    result += ",\"lease_owner\":" + _nullable_text_json(stmt, 9)
    result += ",\"lease_expires_at\":" + _nullable_text_json(stmt, 10)
    result += ",\"input\":" + _text(stmt, 11)
    result += ",\"output\":" + _text(stmt, 12)
    result += ",\"error\":" + _text(stmt, 13)
    result += ",\"metadata\":" + _text(stmt, 14)
    result += ",\"created_at\":" + _quote(_text(stmt, 15))
    result += ",\"updated_at\":" + _quote(_text(stmt, 16))
    result += ",\"started_at\":" + _nullable_text_json(stmt, 17)
    result += ",\"finished_at\":" + _nullable_text_json(stmt, 18)
    result += ",\"output_schema\":" + _text(stmt, 19)
    result += "}}"
    stmt.close()
    connection.close()
    return result
def _domain_inspect(path: String, resource: String, table: String, run_id: String, row_id: String, id_name: String) raises SQLiteError -> String:
    if row_id == "": raise SQLiteError(code=2, message="argument_error: " + id_name + " is required")
    if run_id == "": raise SQLiteError(code=2, message="argument_error: --run-id is required")
    var connection = Connection(path)
    initialize_native_schema(connection)
    var sql = ""
    if table == "impulse_types": sql = "SELECT id,run_id,title,description,media_types,value_schema_json,metadata,created_at,updated_at FROM impulse_types WHERE run_id=? AND id=?"
    elif table == "impulse_relations": sql = "SELECT id,run_id,relation_type,source_impulse_id,target_impulse_id,metadata,created_at FROM impulse_relations WHERE run_id=? AND id=?"
    elif table == "associations": sql = "SELECT id,run_id,kind,impulse_id,values_json,metadata,created_at FROM associations WHERE run_id=? AND id=?"
    elif table == "reactions": sql = "SELECT id,run_id,kind,uri,impulse_id,media_type,size_bytes,content_hash,metadata,created_at FROM reactions WHERE run_id=? AND id=?"
    else:
        connection.close()
        raise SQLiteError(code=2, message="argument_error: unsupported inspect table")
    var stmt = connection.query(sql)
    stmt.bind_text(1, run_id)
    stmt.bind_text(2, row_id)
    if not stmt.step():
        stmt.close()
        connection.close()
        return "{\"ok\":false,\"runtime\":\"mojo\",\"" + resource + "\":null}"
    var row = ""
    if table == "impulse_types": row = "{\"id\":" + _quote(_text(stmt,0)) + ",\"run_id\":" + _quote(_text(stmt,1)) + ",\"title\":" + _nullable_text_json(stmt,2) + ",\"description\":" + _nullable_text_json(stmt,3) + ",\"media_types\":" + _text(stmt,4) + ",\"value_schema\":" + _text(stmt,5) + ",\"metadata\":" + _text(stmt,6) + ",\"created_at\":" + _quote(_text(stmt,7)) + ",\"updated_at\":" + _quote(_text(stmt,8)) + "}"
    elif table == "impulse_relations": row = "{\"id\":" + _quote(_text(stmt,0)) + ",\"run_id\":" + _quote(_text(stmt,1)) + ",\"relation_type\":" + _quote(_text(stmt,2)) + ",\"source_impulse_id\":" + _quote(_text(stmt,3)) + ",\"target_impulse_id\":" + _quote(_text(stmt,4)) + ",\"metadata\":" + _text(stmt,5) + ",\"created_at\":" + _quote(_text(stmt,6)) + "}"
    elif table == "associations": row = "{\"id\":" + _quote(_text(stmt,0)) + ",\"run_id\":" + _quote(_text(stmt,1)) + ",\"kind\":" + _quote(_text(stmt,2)) + ",\"impulse_id\":" + _nullable_text_json(stmt,3) + ",\"values\":" + _text(stmt,4) + ",\"metadata\":" + _text(stmt,5) + ",\"created_at\":" + _quote(_text(stmt,6)) + "}"
    else: row = "{\"id\":" + _quote(_text(stmt,0)) + ",\"run_id\":" + _quote(_text(stmt,1)) + ",\"kind\":" + _quote(_text(stmt,2)) + ",\"uri\":" + _quote(_text(stmt,3)) + ",\"impulse_id\":" + _nullable_text_json(stmt,4) + ",\"media_type\":" + _nullable_text_json(stmt,5) + ",\"size_bytes\":" + _nullable_int_json(stmt,6) + ",\"content_hash\":" + _nullable_text_json(stmt,7) + ",\"metadata\":" + _text(stmt,8) + ",\"created_at\":" + _quote(_text(stmt,9)) + "}"
    stmt.close()
    connection.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"" + resource + "\":" + row + "}"

def _table(path: String, resource: String, table: String, run_id: String) raises SQLiteError -> String:
    var connection = Connection(path)
    initialize_native_schema(connection)
    var sql = "SELECT COUNT(*) FROM " + table
    if run_id != "": sql += " WHERE run_id=?"
    var stmt = connection.query(sql)
    if run_id != "": stmt.bind_text(1, run_id)
    var count = 0
    while stmt.step(): count = stmt.column_int(0)
    stmt.close()
    connection.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":" + _quote(resource) + ",\"count\":" + String(count) + "}"
def _rows(path: String, resource: String, table: String, command: String) raises SQLiteError -> String:
    var run_id = _flag(command, "--run-id")
    var impulse_id = _flag(command, "--impulse-id")
    var impulse_type = _flag(command, "--impulse-type")
    var status = _flag(command, "--status")
    var event_type = _flag(command, "--event-type")
    var process_id = _flag(command, "--process-id")
    var command_id = _flag(command, "--command-id")
    var command_type = _flag(command, "--command-type")
    var actor = _flag(command, "--actor")
    var relation_type = _flag(command, "--relation-type")
    var kind = _flag(command, "--kind")
    var after_sequence = _after_sequence(command)
    var limit = _limit(command)
    var jsonl = _bool_option(command, "--jsonl")
    var connection = Connection(path)
    initialize_native_schema(connection)
    var sql = ""
    if table == "impulses": sql = "SELECT run_id,id,impulse_type,payload,metadata,created_at,updated_at FROM impulses"
    elif table == "impulse_types": sql = "SELECT run_id,id,title,description,media_types,value_schema_json,metadata,created_at,updated_at FROM impulse_types"
    elif table == "impulse_relations": sql = "SELECT run_id,id,relation_type,source_impulse_id,target_impulse_id,metadata,created_at FROM impulse_relations"
    elif table == "associations": sql = "SELECT run_id,id,kind,impulse_id,values_json,metadata,created_at FROM associations"
    elif table == "reactions": sql = "SELECT run_id,id,kind,uri,impulse_id,media_type,size_bytes,content_hash,metadata,created_at FROM reactions"
    elif table == "homeostats": sql = "SELECT run_id,id,kind,impulse_id,status,values_json,metadata,attempt,max_attempts,created_at,updated_at FROM homeostats"
    elif table == "projections": sql = "SELECT p.id,p.run_id,p.name,p.version,p.data,p.source_event_sequence,p.updated_at,CASE WHEN EXISTS (SELECT 1 FROM runtime_events e WHERE e.run_id=p.run_id AND e.sequence>p.source_event_sequence) THEN 1 ELSE 0 END FROM projections p"
    elif table == "bridge_outbox" or table == "bridge_inbox": sql = "SELECT run_id,id,idempotency_key,status,attempts,source_ref,target_ref,impulse_json,event_ref,pool_id,budget,metadata,created_at,updated_at FROM " + table
    elif table == "runtime_commands": sql = "SELECT run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at FROM runtime_commands"
    elif table == "runtime_events": sql = "SELECT run_id,sequence,id,event_type,schema_version,impulse_id,process_id,command_id,actor,correlation_id,causation_id,payload,created_at FROM runtime_events"
    elif table == "processes": sql = "SELECT run_id,id,process_type,impulse_id,status,priority,attempt,max_attempts,available_at,lease_owner,lease_expires_at,input_json,output_json,error_json,metadata,created_at,updated_at,started_at,finished_at,output_schema_json FROM processes"
    else: raise SQLiteError(code=2, message="argument_error: unsupported row table")
    var where = False
    if run_id != "": sql += " WHERE run_id=?"; where = True
    if impulse_id != "":
        if table == "impulse_relations":
            if where: sql += " AND (source_impulse_id=? OR target_impulse_id=?)"
            else: sql += " WHERE (source_impulse_id=? OR target_impulse_id=?)"; where = True
        elif table == "impulses" or table == "associations" or table == "reactions" or table == "homeostats" or table == "processes":
            if where: sql += " AND impulse_id=?"
            else: sql += " WHERE impulse_id=?"; where = True
    if impulse_type != "" and table == "impulses":
        if where: sql += " AND impulse_type=?"
        else: sql += " WHERE impulse_type=?"; where = True
    if status != "" and (table == "homeostats" or table == "processes" or table == "bridge_outbox" or table == "bridge_inbox"):
        if where: sql += " AND status=?"
        else: sql += " WHERE status=?"; where = True
    if event_type != "" and table == "runtime_events":
        if where: sql += " AND event_type=?"
        else: sql += " WHERE event_type=?"; where = True
    if after_sequence >= 0 and table == "runtime_events":
        if where: sql += " AND sequence>?"
        else: sql += " WHERE sequence>?"; where = True
    if process_id != "" and table == "runtime_events":
        if where: sql += " AND process_id=?"
        else: sql += " WHERE process_id=?"; where = True
    if command_id != "" and table == "runtime_events":
        if where: sql += " AND command_id=?"
        else: sql += " WHERE command_id=?"; where = True
    if command_type != "" and table == "runtime_commands":
        if where: sql += " AND command_type=?"
        else: sql += " WHERE command_type=?"; where = True
    if actor != "" and (table == "runtime_commands" or table == "runtime_events"):
        if where: sql += " AND actor=?"
        else: sql += " WHERE actor=?"; where = True
    if relation_type != "" and table == "impulse_relations":
        if where: sql += " AND relation_type=?"
        else: sql += " WHERE relation_type=?"; where = True
    if kind != "" and (table == "associations" or table == "reactions" or table == "homeostats"):
        if where: sql += " AND kind=?"
        else: sql += " WHERE kind=?"; where = True
    if table == "runtime_events": sql += " ORDER BY sequence ASC"
    elif table == "impulse_types": sql += " ORDER BY id ASC"
    elif table == "projections": sql += " ORDER BY name ASC"
    elif table == "processes": sql += " ORDER BY priority DESC,available_at ASC,created_at ASC,id ASC"
    elif table == "homeostats": sql += " ORDER BY updated_at ASC,id ASC"
    else: sql += " ORDER BY created_at ASC,id ASC"
    if limit >= 0: sql += " LIMIT ?"
    var stmt = connection.query(sql)
    var bind = 1
    if run_id != "": stmt.bind_text(bind, run_id); bind += 1
    if impulse_id != "" and (table == "impulse_relations" or table == "impulses" or table == "associations" or table == "reactions" or table == "homeostats" or table == "processes"):
        stmt.bind_text(bind, impulse_id); bind += 1
        if table == "impulse_relations": stmt.bind_text(bind, impulse_id); bind += 1
    if impulse_type != "" and table == "impulses": stmt.bind_text(bind, impulse_type); bind += 1
    if status != "" and (table == "homeostats" or table == "processes" or table == "bridge_outbox" or table == "bridge_inbox"): stmt.bind_text(bind, status); bind += 1
    if event_type != "" and table == "runtime_events": stmt.bind_text(bind, event_type); bind += 1
    if after_sequence >= 0 and table == "runtime_events": stmt.bind_int(bind, after_sequence); bind += 1
    if process_id != "" and table == "runtime_events": stmt.bind_text(bind, process_id); bind += 1
    if command_id != "" and table == "runtime_events": stmt.bind_text(bind, command_id); bind += 1
    if command_type != "" and table == "runtime_commands": stmt.bind_text(bind, command_type); bind += 1
    if actor != "" and (table == "runtime_commands" or table == "runtime_events"): stmt.bind_text(bind, actor); bind += 1
    if relation_type != "" and table == "impulse_relations": stmt.bind_text(bind, relation_type); bind += 1
    if kind != "" and (table == "associations" or table == "reactions" or table == "homeostats"): stmt.bind_text(bind, kind); bind += 1
    if limit >= 0: stmt.bind_int(bind, limit)
    var items = "["
    var jsonl_items = ""
    var first = True
    var jsonl_first = True
    var row_count = 0
    while stmt.step():
        var row_json = ""
        if table == "impulses": row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"id\":" + _quote(_text(stmt,1)) + ",\"impulse_type\":" + _quote(_text(stmt,2)) + ",\"payload\":" + _text(stmt,3) + ",\"metadata\":" + _text(stmt,4) + ",\"created_at\":" + _quote(_text(stmt,5)) + ",\"updated_at\":" + _quote(_text(stmt,6)) + "}"
        elif table == "impulse_types": row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"id\":" + _quote(_text(stmt,1)) + ",\"title\":" + _nullable_text_json(stmt,2) + ",\"description\":" + _nullable_text_json(stmt,3) + ",\"media_types\":" + _text(stmt,4) + ",\"value_schema\":" + _text(stmt,5) + ",\"metadata\":" + _text(stmt,6) + ",\"created_at\":" + _quote(_text(stmt,7)) + ",\"updated_at\":" + _quote(_text(stmt,8)) + "}"
        elif table == "impulse_relations": row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"id\":" + _quote(_text(stmt,1)) + ",\"relation_type\":" + _quote(_text(stmt,2)) + ",\"source_impulse_id\":" + _quote(_text(stmt,3)) + ",\"target_impulse_id\":" + _quote(_text(stmt,4)) + ",\"metadata\":" + _text(stmt,5) + ",\"created_at\":" + _quote(_text(stmt,6)) + "}"
        elif table == "associations": row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"id\":" + _quote(_text(stmt,1)) + ",\"kind\":" + _quote(_text(stmt,2)) + ",\"impulse_id\":" + _nullable_text_json(stmt,3) + ",\"values\":" + _text(stmt,4) + ",\"metadata\":" + _text(stmt,5) + ",\"created_at\":" + _quote(_text(stmt,6)) + "}"
        elif table == "reactions": row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"id\":" + _quote(_text(stmt,1)) + ",\"kind\":" + _quote(_text(stmt,2)) + ",\"uri\":" + _quote(_text(stmt,3)) + ",\"impulse_id\":" + _nullable_text_json(stmt,4) + ",\"media_type\":" + _nullable_text_json(stmt,5) + ",\"size_bytes\":" + _nullable_int_json(stmt,6) + ",\"content_hash\":" + _nullable_text_json(stmt,7) + ",\"metadata\":" + _text(stmt,8) + ",\"created_at\":" + _quote(_text(stmt,9)) + "}"
        elif table == "homeostats": row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"id\":" + _quote(_text(stmt,1)) + ",\"kind\":" + _quote(_text(stmt,2)) + ",\"impulse_id\":" + _nullable_text_json(stmt,3) + ",\"status\":" + _quote(_text(stmt,4)) + ",\"values\":" + _text(stmt,5) + ",\"metadata\":" + _text(stmt,6) + ",\"attempt\":" + String(stmt.column_int(7)) + ",\"max_attempts\":" + String(stmt.column_int(8)) + ",\"created_at\":" + _quote(_text(stmt,9)) + ",\"updated_at\":" + _quote(_text(stmt,10)) + "}"
        elif table == "projections": row_json = "{\"id\":" + _quote(_text(stmt,0)) + ",\"run_id\":" + _quote(_text(stmt,1)) + ",\"name\":" + _quote(_text(stmt,2)) + ",\"version\":" + String(stmt.column_int(3)) + ",\"data\":" + _text(stmt,4) + ",\"source_event_sequence\":" + String(stmt.column_int(5)) + ",\"updated_at\":" + _quote(_text(stmt,6)) + ",\"stale\":" + ("true" if stmt.column_int(7) != 0 else "false") + "}"
        elif table == "runtime_commands": row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"id\":" + _quote(_text(stmt,1)) + ",\"command_type\":" + _quote(_text(stmt,2)) + ",\"idempotency_key\":" + _quote(_text(stmt,3)) + ",\"actor\":" + _nullable_text_json(stmt,4) + ",\"correlation_id\":" + _nullable_text_json(stmt,5) + ",\"causation_id\":" + _nullable_text_json(stmt,6) + ",\"payload\":" + _text(stmt,7) + ",\"created_at\":" + _quote(_text(stmt,8)) + "}"
        elif table == "runtime_events": row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"sequence\":" + String(stmt.column_int(1)) + ",\"id\":" + _quote(_text(stmt,2)) + ",\"event_type\":" + _quote(_text(stmt,3)) + ",\"schema_version\":" + String(stmt.column_int(4)) + ",\"impulse_id\":" + _nullable_text_json(stmt,5) + ",\"process_id\":" + _nullable_text_json(stmt,6) + ",\"command_id\":" + _nullable_text_json(stmt,7) + ",\"actor\":" + _nullable_text_json(stmt,8) + ",\"correlation_id\":" + _nullable_text_json(stmt,9) + ",\"causation_id\":" + _nullable_text_json(stmt,10) + ",\"payload\":" + _text(stmt,11) + ",\"created_at\":" + _quote(_text(stmt,12)) + "}"
        elif table == "processes": row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"id\":" + _quote(_text(stmt,1)) + ",\"process_type\":" + _quote(_text(stmt,2)) + ",\"impulse_id\":" + _nullable_text_json(stmt,3) + ",\"status\":" + _quote(_text(stmt,4)) + ",\"priority\":" + String(stmt.column_int(5)) + ",\"attempt\":" + String(stmt.column_int(6)) + ",\"max_attempts\":" + String(stmt.column_int(7)) + ",\"available_at\":" + _quote(_text(stmt,8)) + ",\"lease_owner\":" + _nullable_text_json(stmt,9) + ",\"lease_expires_at\":" + _nullable_text_json(stmt,10) + ",\"input\":" + _text(stmt,11) + ",\"output\":" + _text(stmt,12) + ",\"error\":" + _text(stmt,13) + ",\"metadata\":" + _text(stmt,14) + ",\"created_at\":" + _quote(_text(stmt,15)) + ",\"updated_at\":" + _quote(_text(stmt,16)) + ",\"started_at\":" + _nullable_text_json(stmt,17) + ",\"finished_at\":" + _nullable_text_json(stmt,18) + ",\"output_schema\":" + _text(stmt,19) + "}"
        elif table == "bridge_outbox" or table == "bridge_inbox": row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"id\":" + _quote(_text(stmt,1)) + ",\"idempotency_key\":" + _quote(_text(stmt,2)) + ",\"status\":" + _quote(_text(stmt,3)) + ",\"attempts\":" + String(stmt.column_int(4)) + ",\"source_ref\":" + _quote(_text(stmt,5)) + ",\"target_ref\":" + _quote(_text(stmt,6)) + ",\"impulse\":" + _text(stmt,7) + ",\"event_ref\":" + _nullable_text_json(stmt,8) + ",\"pool_id\":" + _nullable_text_json(stmt,9) + ",\"budget\":" + _text(stmt,10) + ",\"metadata\":" + _text(stmt,11) + ",\"created_at\":" + _quote(_text(stmt,12)) + ",\"updated_at\":" + _quote(_text(stmt,13)) + "}"
        if not first: items += ","
        first = False
        items += row_json
        if jsonl:
            if not jsonl_first: jsonl_items += "\n"
            jsonl_first = False
            jsonl_items += row_json
        row_count += 1
    items += "]"
    stmt.close()
    connection.close()
    if jsonl: return jsonl_items
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":" + _quote(resource) + ",\"count\":" + String(row_count) + ",\"items\":" + items + "}"
def _trace_parse(text: String) raises SQLiteError -> Value:
    try:
        var parsed = parse_json(text)
        return parsed.value.copy()
    except err:
        raise SQLiteError(code=1, message="trace: invalid persisted JSON")

def _trace_items(path: String, resource: String, table: String, command: String) raises SQLiteError -> String:
    try:
        var envelope = _trace_parse(_rows(path, resource, table, command))
        if not envelope.is_object(): raise SQLiteError(code=1, message="trace rows envelope is invalid")
        var items_text = _trace_value(envelope, "items")
        var items = _trace_parse(items_text)
        if not items.is_array(): raise SQLiteError(code=1, message="trace rows items are invalid")
        return items_text
    except err:
        raise SQLiteError(code=1, message="trace: unable to read " + resource)

def _trace_value(value: Value, key: String) raises SQLiteError -> String:
    try:
        if value.is_object() and key in value.object():
            var child = value.object()[key].copy()
            return to_string(child^)
        return "null"
    except err:
        raise SQLiteError(code=1, message="trace: invalid row value")

def _trace_run(path: String, run_id: String) raises SQLiteError -> String:
    try:
        var envelope = _trace_parse(_run_inspect(path, run_id))
        if not envelope.is_object(): raise SQLiteError(code=1, message="run envelope is invalid")
        var run_text = _trace_value(envelope, "run")
        var run = _trace_parse(run_text)
        return run_text
    except err:
        raise SQLiteError(code=1, message="trace: unable to read run")

def _trace_timeline(events_json: String) raises SQLiteError -> String:
    try:
        var events = _trace_parse(events_json)
        if not events.is_array(): raise SQLiteError(code=1, message="events must be an array")
        var timeline = "["
        var first = True
        for event in events.array():
            if not first: timeline += ","
            first = False
            timeline += "{\"sequence\":" + _trace_value(event, "sequence")
            timeline += ",\"type\":" + _trace_value(event, "event_type")
            timeline += ",\"impulse_id\":" + _trace_value(event, "impulse_id")
            timeline += ",\"process_id\":" + _trace_value(event, "process_id")
            timeline += ",\"actor\":" + _trace_value(event, "actor")
            timeline += ",\"created_at\":" + _trace_value(event, "created_at") + "}"
        return timeline + "]"
    except err:
        raise SQLiteError(code=1, message="trace: unable to build timeline")
def _trace_count(items_json: String) raises SQLiteError -> Int:
    try:
        var items = _trace_parse(items_json)
        if not items.is_array(): raise SQLiteError(code=1, message="trace items must be an array")
        return len(items.array())
    except err:
        raise SQLiteError(code=1, message="trace: unable to count rows")


def _trace(path: String, command: String) raises SQLiteError -> String:
    var run_id = _flag(command, "--run-id", "")
    if run_id == "": raise SQLiteError(code=2, message="argument_error: --run-id is required")
    var run = _trace_run(path, run_id)
    var events = _trace_items(path, "events", "runtime_events", command)
    var impulses = _trace_items(path, "impulses", "impulses", command)
    var relations = _trace_items(path, "impulse-relations", "impulse_relations", command)
    var associations = _trace_items(path, "associations", "associations", command)
    var reactions = _trace_items(path, "reactions", "reactions", command)
    var processes = _trace_items(path, "processes", "processes", command)
    var homeostats = _trace_items(path, "homeostats", "homeostats", command)
    var projections = _trace_items(path, "projections", "projections", command)
    var timeline = _trace_timeline(events)
    var trace = "{\"run_id\":" + _quote(run_id) + ",\"run\":" + run + ",\"counts\":{"
    trace += "\"reactions\":" + String(_trace_count(reactions))
    trace += ",\"impulse_relations\":" + String(_trace_count(relations))
    trace += ",\"impulses\":" + String(_trace_count(impulses))
    trace += ",\"events\":" + String(_trace_count(events))
    trace += ",\"homeostats\":" + String(_trace_count(homeostats))
    trace += ",\"associations\":" + String(_trace_count(associations))
    trace += ",\"processes\":" + String(_trace_count(processes))
    trace += ",\"projections\":" + String(_trace_count(projections)) + "}"
    trace += ",\"timeline\":" + timeline + ",\"events\":" + events
    trace += ",\"impulses\":" + impulses + ",\"impulse_relations\":" + relations
    trace += ",\"associations\":" + associations + ",\"reactions\":" + reactions
    trace += ",\"processes\":" + processes + ",\"homeostats\":" + homeostats
    trace += ",\"projections\":" + projections + "}"
    return "{\"ok\":true,\"runtime\":\"mojo\",\"trace\":" + trace + "}"
def _integer_option(command: String, name: String, default: Int) raises SQLiteError -> Int:
    var raw = _flag(command, name, "")
    if raw == "": return default
    try:
        var parsed = parse_json(raw)
        if parsed.value.is_int(): return Int(parsed.value.int())
        if parsed.value.is_uint(): return Int(parsed.value.uint())
    except err:
        pass
    raise SQLiteError(code=2, message="argument_error: invalid integer value for " + name)

def _impulse_create(command: String) raises SQLiteError -> String:
    var run_id = _flag(command, "--run-id"); var impulse_id = _flag(command, "--impulse-id"); var impulse_type = _flag(command, "--impulse-type")
    if run_id == "" or impulse_id == "" or impulse_type == "": raise SQLiteError(code=2, message="argument_error: --run-id, --impulse-id, and --impulse-type are required")
    var payload = _flag_alias(command, "--payload", "--payload-json", "{}"); var metadata = _flag_alias(command, "--metadata", "--metadata-json", "{}"); _json(payload); _json(metadata)
    var now = _flag(command, "--now"); var key = _flag(command, "--idempotency-key", "impulse.accept:" + impulse_id)
    var row = Impulse(id=impulse_id, run_id=run_id, impulse_type=impulse_type, payload=payload, metadata=metadata, created_at=now, updated_at=now)
    var store = NativeDomainStore.open(_path(command)); store.initialize(); var accepted = store.accept_impulse(row, key, now, _flag(command, "--actor"), _flag(command, "--correlation-id"), _flag(command, "--causation-id")); store.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"impulse\",\"id\":" + _quote(accepted.impulse.id) + ",\"run_id\":" + _quote(run_id) + ",\"replayed\":" + ("true" if accepted.replayed else "false") + ",\"command_id\":" + _quote(accepted.command.id) + ",\"event_id\":" + _quote(accepted.events[0].id) + "}"

def _process_schedule(command: String) raises SQLiteError -> String:
    var run_id = _flag(command, "--run-id"); var process_id = _flag(command, "--process-id"); var process_type = _flag(command, "--process-type")
    if run_id == "" or process_id == "" or process_type == "": raise SQLiteError(code=2, message="argument_error: --run-id, --process-id, and --process-type are required")
    var input_json = _flag_alias(command, "--input", "--input-json", "{}"); var metadata = _flag_alias(command, "--metadata", "--metadata-json", "{}"); var output_schema = _flag(command, "--output-schema", "{}"); _json(input_json); _json(metadata); _json(output_schema)
    var now = _flag(command, "--now"); var journal = NativeJournal.open(_path(command)); journal.initialize()
    var row = journal.schedule_process(run_id, process_id, process_type, now, input_json, metadata, _flag(command, "--impulse-id"), _integer_option(command, "--priority", 0), _integer_option(command, "--max-attempts", 1), _flag(command, "--available-at", now), output_schema, _flag(command, "--idempotency-key", "process.schedule:" + process_id), _flag(command, "--actor")); journal.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"process\",\"id\":" + _quote(row.id) + ",\"run_id\":" + _quote(row.run_id) + ",\"status\":" + _quote(row.status) + "}"

def _process_transition(command: String, target: String) raises SQLiteError -> String:
    var run_id = _flag(command, "--run-id"); var process_id = _flag(command, "--process-id"); var actor = _flag(command, "--actor", "cli"); var now = _flag(command, "--now")
    if run_id == "" or process_id == "": raise SQLiteError(code=2, message="argument_error: --run-id and --process-id are required")
    var error_json = _flag_alias(command, "--error", "--error-json", "{}"); _json(error_json); var journal = NativeJournal.open(_path(command)); journal.initialize(); var row = ProcessRow(run_id="", id="", process_type="", impulse_id="", status="", priority=0, attempt=0, max_attempts=1, available_at="", lease_owner="", lease_expires_at="", input_json="{}", output_json="{}", error_json="{}", metadata="{}", created_at="", updated_at="", started_at="", finished_at="", output_schema_json="{}")
    if target == "cancel": row = journal.cancel_process(run_id, process_id, actor, now, error_json)
    else: row = journal.timeout_process(run_id, process_id, actor, now, error_json)
    journal.close(); return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"process\",\"id\":" + _quote(row.id) + ",\"run_id\":" + _quote(row.run_id) + ",\"status\":" + _quote(row.status) + "}"

def _association_append(command: String) raises SQLiteError -> String:
    var run_id = _flag(command, "--run-id"); var association_id = _flag(command, "--association-id"); var kind = _flag(command, "--kind")
    if run_id == "" or association_id == "" or kind == "": raise SQLiteError(code=2, message="argument_error: --run-id, --association-id, and --kind are required")
    var values = _flag_alias(command, "--values", "--values-json", "{}"); var metadata = _flag_alias(command, "--metadata", "--metadata-json", "{}"); _json(values); _json(metadata)
    var row = Association(id=association_id, run_id=run_id, kind=kind, impulse_id=_flag(command, "--impulse-id"), values=values, metadata=metadata, created_at=_flag(command, "--now")); var store = NativeDomainStore.open(_path(command)); store.initialize(); store.put_association(row); store.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"association\",\"id\":" + _quote(association_id) + ",\"run_id\":" + _quote(run_id) + "}"
def _reaction_blob(root: String, content: List[UInt8], filename: String, metadata: String) raises SQLiteError -> ReactionBlob:
    try:
        var store = FileReactionStore(root)
        return store.put_bytes_raw(content, filename, metadata)
    except err:
        raise SQLiteError(code=2, message="argument_error: unable to persist reaction: " + String(err))
def _reaction_record(command: String) raises SQLiteError -> String:
    var run_id = _flag(command, "--run-id")
    var reaction_kind = _flag(command, "--kind")
    var input_path = _flag(command, "--path")
    var reaction_root = _flag(command, "--reaction-root")
    if run_id == "" or reaction_kind == "" or input_path == "" or reaction_root == "":
        raise SQLiteError(code=2, message="argument_error: --run-id, --kind, --path, and --reaction-root are required")
    if not _safe(input_path) or not _safe(reaction_root):
        raise SQLiteError(code=2, message="argument_error: invalid reaction path")
    var metadata_raw = _flag(command, "--metadata-json", "{}")
    var metadata = String("")
    try:
        metadata = canonical_json_text(metadata_raw)
        var parsed = parse_json(metadata)
        if not parsed.value.is_object(): raise Error("metadata must be an object")
    except err:
        raise SQLiteError(code=2, message="invalid_json: metadata")
    var source = Path(input_path)
    if not source.exists() or not source.is_file():
        raise SQLiteError(code=2, message="argument_error: reaction path must be a file")
    var content = List[UInt8]()
    try:
        content = source.read_bytes()
    except err:
        raise SQLiteError(code=2, message="argument_error: unable to read reaction path")
    var blob = _reaction_blob(reaction_root, content, source.name(), metadata)
    var metadata_value = Value()
    try:
        metadata_value = Value(parse_string=blob.metadata)
    except err:
        raise SQLiteError(code=1, message="reaction metadata serialization failed")
    if not metadata_value.is_object():
        raise SQLiteError(code=1, message="reaction metadata serialization failed")
    metadata_value.object()["reaction_store"] = Value(reaction_root)
    var persisted_metadata = String("")
    try:
        persisted_metadata = canonical_json_text(to_string(metadata_value))
    except err:
        raise SQLiteError(code=1, message="reaction metadata serialization failed")
    var reaction_id = _flag(command, "--reaction-id", "")
    if reaction_id == "": reaction_id = "reaction:" + blob.digest
    var key = _flag(command, "--idempotency-key", "")
    if key == "": key = "reaction.record:" + reaction_id
    var now = _flag(command, "--now")
    var row = Reaction(id=reaction_id, run_id=run_id, kind=reaction_kind, uri=blob.uri, impulse_id=_flag(command, "--impulse-id"), media_type=_flag(command, "--media-type"), size_bytes=blob.size_bytes, content_hash="sha256:" + blob.digest, metadata=persisted_metadata, created_at=now)
    var events = List[EventInput]()
    events.append(EventInput(id=key + ":event", event_type="reaction.recorded", payload=row.to_json(), created_at=now, impulse_id=row.impulse_id, process_id="", schema_version=1, actor="", correlation_id="", causation_id=""))
    var store = NativeDomainStore.open(_path(command))
    try:
        var submission = store.record_reaction(row, key, "reaction.record", key, now, events)
        var response_row = row.copy()
        if submission.replayed:
            response_row = store.get_reaction(run_id, reaction_id)
        store.close()
        var command_json = "{\"run_id\":" + _quote(submission.command.run_id) + ",\"id\":" + _quote(submission.command.id) + ",\"command_type\":" + _quote(submission.command.command_type) + ",\"idempotency_key\":" + _quote(submission.command.idempotency_key) + ",\"actor\":" + (_quote(submission.command.actor) if submission.command.actor != "" else "null") + ",\"correlation_id\":" + (_quote(submission.command.correlation_id) if submission.command.correlation_id != "" else "null") + ",\"causation_id\":" + (_quote(submission.command.causation_id) if submission.command.causation_id != "" else "null") + ",\"payload\":" + submission.command.payload + ",\"created_at\":" + _quote(submission.command.created_at) + "}"
        var event_json = "null"
        if len(submission.events) > 0:
            var event = submission.events[0].copy()
            event_json = "{\"run_id\":" + _quote(event.run_id) + ",\"sequence\":" + String(event.sequence) + ",\"id\":" + _quote(event.id) + ",\"event_type\":" + _quote(event.event_type) + ",\"schema_version\":" + String(event.schema_version) + ",\"command_id\":" + _quote(event.command_id) + ",\"payload\":" + event.payload + ",\"created_at\":" + _quote(event.created_at) + "}"
        return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"reaction\",\"replayed\":" + ("true" if submission.replayed else "false") + ",\"reaction\":" + response_row.to_json() + ",\"command\":" + command_json + ",\"event\":" + event_json + "}"
    except err:
        try:
            store.close()
        except close_err:
            pass
        raise err^


def _homeostat_transition(command: String, operation: String) raises SQLiteError -> String:
    var run_id = _flag(command, "--run-id"); var homeostat_id = _flag(command, "--homeostat-id"); var process_id = _flag(command, "--process-id"); var actor = _flag(command, "--actor", "cli"); var now = _flag(command, "--now")
    if run_id == "" or homeostat_id == "" or process_id == "": raise SQLiteError(code=2, message="argument_error: --run-id, --homeostat-id, and --process-id are required")
    var output = _flag(command, "--output", "{}"); var error_json = _flag_alias(command, "--error", "--error-json", "{}"); var metadata = _flag_alias(command, "--metadata", "--metadata-json", "{}"); _json(output); _json(error_json); _json(metadata)
    var key = _flag(command, "--idempotency-key", "homeostat." + operation + ":" + homeostat_id)
    if operation == "reopen" and not _has_option(command, "--idempotency-key"): key = ""
    var journal = NativeJournal.open(_path(command)); journal.initialize(); var row = ProcessRow(run_id="", id="", process_type="", impulse_id="", status="", priority=0, attempt=0, max_attempts=1, available_at="", lease_owner="", lease_expires_at="", input_json="{}", output_json="{}", error_json="{}", metadata="{}", created_at="", updated_at="", started_at="", finished_at="", output_schema_json="{}")
    if operation == "open": row = journal.park_homeostat_process(run_id, homeostat_id, process_id, actor, now, output, metadata, key)
    elif operation == "reopen": row = journal.reopen_homeostat_process(run_id, homeostat_id, process_id, actor, now, key)
    else:
        var hs = "expired"; var ps = "timed_out"
        if operation == "complete": hs = "completed"; ps = "succeeded"
        elif operation == "cancel": hs = "cancelled"; ps = "cancelled"
        row = journal.transition_homeostat_process(run_id, homeostat_id, process_id, hs, ps, actor, now, output, error_json, key)
    journal.close(); return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"homeostat\",\"id\":" + _quote(homeostat_id) + ",\"process_id\":" + _quote(row.id) + ",\"status\":" + _quote(row.status) + "}"


def _homeostat_domain_values(command: String) raises SQLiteError -> String:
    var items = _repeat_values(command, "--value")
    var values = Object(capacity=len(items))
    for item in items:
        var equals = item.find("=")
        if equals <= 0:
            raise SQLiteError(code=2, message="Invalid value '" + item + "'; expected key=value")
        var key = String(item[byte=0:equals])
        var value = String(item[byte=equals + 1:])
        values[key] = Value(value)
    try:
        return canonical_json_text(to_string(Value(values^)))
    except err:
        raise SQLiteError(code=2, message="invalid_json")


def _homeostat_domain(command: String, operation: String) raises SQLiteError -> String:
    if operation == "open":
        var values = _flag(command, "--values-json", "{}")
        var metadata = _flag(command, "--metadata-json", "{}")
        try:
            var values_parsed = parse_json(values)
            var metadata_parsed = parse_json(metadata)
            if not values_parsed.value.is_object() or not metadata_parsed.value.is_object():
                raise Error("homeostat JSON must be an object")
            _ = canonical_json_text(to_string(values_parsed.value))
            _ = canonical_json_text(to_string(metadata_parsed.value))
        except err:
            raise SQLiteError(code=2, message="invalid_json")
        if _flag(command, "--homeostat-id", "") == "":
            return _error("native_boundary", "homeostat open requires a native identifier generator")
    else:
        _ = _homeostat_domain_values(command)
    return _error("native_boundary", "homeostat mutations require a native clock source")


def _create(command: String) raises SQLiteError -> String:
    var path = _path(command)

    var run_id = _flag(command, "--run-id")
    if run_id == "": raise SQLiteError(code=2, message="argument_error: --run-id is required; native run-id generation is unavailable")
    var metadata = _metadata_value(command)
    var lifecycle = RunLifecycle.open(path)
    lifecycle.initialize()
    var result = lifecycle.create_result(run_id, _flag(command,"--now"), metadata, _flag(command,"--title"), "created", _flag(command,"--idempotency-key","run.create"), package_id=_flag(command,"--package-id"), package_version=_flag(command,"--package-version"), package_digest=_flag(command,"--package-digest"), correlation_path_id=_flag(command,"--correlation-path-id"), correlation_path_digest=_flag(command,"--correlation-path-digest"), runtime_version=_flag(command,"--runtime-version"), backend_version=_flag(command,"--backend-version"), actor="cli:user")
    lifecycle.close()
    var run = "{\"id\":" + _quote(result.run.id) + ",\"status\":" + _quote(result.run.status) + ",\"title\":" + ("null" if result.run.title == "" else _quote(result.run.title)) + ",\"package_id\":" + ("null" if result.run.package_id == "" else _quote(result.run.package_id)) + ",\"package_version\":" + ("null" if result.run.package_version == "" else _quote(result.run.package_version)) + ",\"package_digest\":" + ("null" if result.run.package_digest == "" else _quote(result.run.package_digest)) + ",\"correlation_path_id\":" + ("null" if result.run.correlation_path_id == "" else _quote(result.run.correlation_path_id)) + ",\"correlation_path_digest\":" + ("null" if result.run.correlation_path_digest == "" else _quote(result.run.correlation_path_digest)) + ",\"runtime_version\":" + ("null" if result.run.runtime_version == "" else _quote(result.run.runtime_version)) + ",\"backend_version\":" + ("null" if result.run.backend_version == "" else _quote(result.run.backend_version)) + ",\"schema_version\":" + String(result.run.schema_version) + ",\"metadata\":" + result.run.metadata + ",\"created_at\":" + _quote(result.run.created_at) + ",\"updated_at\":" + _quote(result.run.updated_at) + ",\"started_at\":" + ("null" if result.run.started_at == "" else _quote(result.run.started_at)) + ",\"finished_at\":" + ("null" if result.run.finished_at == "" else _quote(result.run.finished_at)) + "}"
    var cmd = "{\"run_id\":" + _quote(result.command.run_id) + ",\"id\":" + _quote(result.command.id) + ",\"command_type\":" + _quote(result.command.command_type) + ",\"idempotency_key\":" + _quote(result.command.idempotency_key) + ",\"actor\":" + ("null" if result.command.actor == "" else _quote(result.command.actor)) + ",\"correlation_id\":" + ("null" if result.command.correlation_id == "" else _quote(result.command.correlation_id)) + ",\"causation_id\":" + ("null" if result.command.causation_id == "" else _quote(result.command.causation_id)) + ",\"payload\":" + result.command.payload + ",\"created_at\":" + _quote(result.command.created_at) + "}"
    return "{\"ok\":true,\"run\":" + run + ",\"command\":" + cmd + ",\"replayed\":" + ("true" if result.replayed else "false") + "}"
    
def _count_json(counts: RunDeleteCounts) -> String:
    return "{\"run_id\":" + _quote(counts.run_id) + ",\"bridge_inbox\":" + String(counts.bridge_inbox) + ",\"bridge_outbox\":" + String(counts.bridge_outbox) + ",\"projections\":" + String(counts.projections) + ",\"homeostats\":" + String(counts.homeostats) + ",\"processes\":" + String(counts.processes) + ",\"reactions\":" + String(counts.reactions) + ",\"associations\":" + String(counts.associations) + ",\"impulse_relations\":" + String(counts.impulse_relations) + ",\"impulse_types\":" + String(counts.impulse_types) + ",\"impulses\":" + String(counts.impulses) + ",\"runtime_events\":" + String(counts.runtime_events) + ",\"runtime_commands\":" + String(counts.runtime_commands) + ",\"runs\":" + String(counts.runs) + "}"

def _maintenance_json(plan: JournalMaintenancePlan) -> String:
    var runs = "["
    var first = True
    for item in plan.retention.runs:
        if not first: runs += ","
        first = False
        var deleted = "false"
        if item.deleted: deleted = "true"
        runs += "{\"run_id\":" + _quote(item.run_id) + ",\"status\":" + _quote(item.status) + ",\"created_at\":" + _quote(item.created_at) + ",\"updated_at\":" + _quote(item.updated_at) + ",\"finished_at\":" + _quote(item.finished_at) + ",\"deleted\":" + deleted + ",\"row_counts\":" + _count_json(item.row_counts) + "}"
    runs += "]"
    var candidates = "["
    first = True
    for digest in plan.reaction_gc.candidates:
        if not first: candidates += ","
        first = False
        candidates += _quote(digest)
    candidates += "]"
    var deleted_reactions = "["
    first = True
    for digest in plan.reaction_gc.deleted:
        if not first: deleted_reactions += ","
        first = False
        deleted_reactions += _quote(digest)
    deleted_reactions += "]"
    var dry_run = "false"
    if plan.dry_run: dry_run = "true"
    var vacuum = "false"
    if plan.vacuum: vacuum = "true"
    var vacuumed = "false"
    if plan.vacuumed: vacuumed = "true"
    var gc_dry_run = "false"
    if plan.reaction_gc.dry_run: gc_dry_run = "true"
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"maintenance\",\"maintenance\":{\"dry_run\":" + dry_run + ",\"older_than_days\":" + String(plan.older_than_days) + ",\"keep_last\":" + String(plan.keep_last) + ",\"vacuum\":" + vacuum + ",\"before\":" + _quote(plan.before) + ",\"retention\":{\"dry_run\":" + dry_run + ",\"before\":" + _quote(plan.retention.before) + ",\"candidate_count\":" + String(plan.retention.candidate_count) + ",\"deleted_run_count\":" + String(plan.retention.deleted_run_count) + ",\"row_counts\":" + _count_json(plan.retention.row_counts) + ",\"runs\":" + runs + "},\"reaction_gc\":{\"dry_run\":" + gc_dry_run + ",\"referenced_count\":" + String(plan.reaction_gc.referenced_count) + ",\"blob_count\":" + String(plan.reaction_gc.blob_count) + ",\"candidate_count\":" + String(plan.reaction_gc.candidate_count) + ",\"deleted_count\":" + String(plan.reaction_gc.deleted_count) + ",\"candidates\":" + candidates + ",\"deleted\":" + deleted_reactions + "},\"vacuumed\":" + vacuumed + "}}"

def _gc_json(plan: ReactionGarbageCollectionPlan) -> String:
    var run_ids = "["
    var first = True
    for value in plan.run_ids:
        if not first: run_ids += ","
        first = False
        run_ids += _quote(value)
    run_ids += "]"
    var scanned = "["
    first = True
    for value in plan.scanned_run_ids:
        if not first: scanned += ","
        first = False
        scanned += _quote(value)
    scanned += "]"
    var collectable = "["
    first = True
    for value in plan.collectable:
        if not first: collectable += ","
        first = False
        collectable += _quote(value)
    collectable += "]"
    var deleted = "["
    first = True
    for value in plan.deleted:
        if not first: deleted += ","
        first = False
        deleted += _quote(value)
    deleted += "]"
    var dry_run = "false"
    if plan.dry_run: dry_run = "true"
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"gc\",\"dry_run\":" + dry_run + ",\"reaction_root\":" + _quote(plan.reaction_root) + ",\"run_ids\":" + run_ids + ",\"scanned_run_ids\":" + scanned + ",\"referenced_count\":" + String(plan.referenced_count) + ",\"blob_count\":" + String(plan.blob_count) + ",\"kept_count\":" + String(plan.kept_count) + ",\"collectable_count\":" + String(plan.candidate_count) + ",\"deleted_count\":" + String(plan.deleted_count) + ",\"bytes_reclaimable\":" + String(plan.bytes_reclaimable) + ",\"bytes_reclaimed\":" + String(plan.bytes_reclaimed) + ",\"collectable\":" + collectable + ",\"deleted\":" + deleted + "}"

def _gc(command: String) raises SQLiteError -> String:
    if _has_option(command, "--older-than"):
        return _error("native_boundary", "gc --older-than requires native filesystem metadata support")
    var path = _path(command)
    var reaction_root = _flag(command, "--reaction-root", "")
    var run_id = _flag(command, "--run-id", "")
    var dry_run = not _has_option(command, "--delete")
    if _has_option(command, "--dry-run"): dry_run = True
    var store = NativeDomainStore.open(path)
    store.initialize()
    var plan = collect_reaction_garbage(store, reaction_root, run_id, dry_run)
    store.close()
    return _gc_json(plan)
def _maintain_journal(command: String) raises SQLiteError -> String:
    var path = _path(command)
    var older_than_days = _maintenance_number(command, "--older-than-days", -1.0)
    if older_than_days < 0.0: raise SQLiteError(code=2, message="argument_error: --older-than-days is required and must be non-negative")
    var retention_keep_last = _maintenance_integer(command, "--keep-last", -1)
    if retention_keep_last < -1: raise SQLiteError(code=2, message="argument_error: --keep-last must be non-negative")
    var vacuum = True
    if _has_option(command, "--no-vacuum"): vacuum = False
    if _has_option(command, "--vacuum"): vacuum = True
    var dry_run = True
    if _has_option(command, "--delete"): dry_run = False
    if _has_option(command, "--dry-run"): dry_run = True
    var reaction_root = _flag(command, "--reaction-root", "")
    var store = NativeDomainStore.open(path)
    store.initialize()
    var plan = maintain_journal(store, older_than_days, retention_keep_last, vacuum, dry_run, reaction_root)
    store.close()
    return _maintenance_json(plan)

def _projection_rebuild(command: String) raises SQLiteError -> String:
    var path = _path(command)
    var run_id = _flag(command, "--run-id")
    if run_id == "": raise SQLiteError(code=2, message="argument_error: --run-id is required")
    var names = List[String]()
    var requested = _flag(command, "--name", "")
    if requested != "": names.append(requested)
    var updated_at = _flag(command, "--now")
    var store = NativeDomainStore.open(path)
    store.initialize()
    var rebuilt = rebuild_projections(store, run_id, names, updated_at)
    store.close()
    var items = "["
    var first = True
    for projection in rebuilt:
        if not first: items += ","
        first = False
        items += projection.to_json()
    items += "]"
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"projections\",\"count\":" + String(len(rebuilt)) + ",\"projections\":" + items + "}"
def _bridge_path(path: String, label: String) raises SQLiteError -> Path:
    if path == "": raise SQLiteError(code=2, message="argument_error: " + label + " path is required")
    if not _safe(path) or path.find("://") >= 0 or path == ":memory:" or path.startswith("file:"):
        raise SQLiteError(code=2, message="unsafe_path: invalid " + label + " path")
    try:
        var value = Path(path).expanduser()
        if not path.startswith("/"): value = cwd() / value
        return value
    except err:
        raise SQLiteError(code=2, message="unsafe_path: invalid " + label + " path")

def _bridge_string(value: Value, path: String) raises -> String:
    if not value.is_string(): raise Error("invalid_type at " + path + ": expected string")
    return value.string()

def _bridge_int(value: Value, path: String) raises -> Int:
    if value.is_int(): return Int(value.int())
    if value.is_uint(): return Int(value.uint())
    raise Error("invalid_type at " + path + ": expected integer")

def _bridge_object(value: Value, path: String) raises -> Object:
    if not value.is_object(): raise Error("invalid_type at " + path + ": expected object")
    return value.object().copy()

def _bridge_known(object: Object, names: List[String], path: String) raises:
    for pair in object.items():
        var known = False
        for name in names:
            if pair.key == name: known = True
        if not known: raise Error("unknown_field at " + path + "/" + pair.key)

def _bridge_runtime(value: Value, path: String) raises -> RuntimeRef:
    var object = _bridge_object(value, path)
    _bridge_known(object, ["id", "uri", "metadata"], path)
    if "id" not in object: raise Error("missing_field at " + path + "/id")
    var id = _bridge_string(object["id"].copy(), path + "/id")
    var uri = ""
    if "uri" in object: uri = _bridge_string(object["uri"].copy(), path + "/uri")
    var metadata = "{}"
    if "metadata" in object:
        var m = object["metadata"].copy()
        if not m.is_object(): raise Error("invalid_type at " + path + "/metadata: expected object")
        metadata = canonical_json_text(to_string(m^))
    return RuntimeRef(id, uri, metadata)

def _bridge_run(value: Value, path: String) raises -> RunRef:
    var object = _bridge_object(value, path)
    _bridge_known(object, ["runtime", "run_id"], path)
    if "runtime" not in object or "run_id" not in object: raise Error("missing_field at " + path)
    return RunRef(_bridge_runtime(object["runtime"].copy(), path + "/runtime"), _bridge_string(object["run_id"].copy(), path + "/run_id"))

def _bridge_event(value: Value, path: String) raises -> EventRef:
    if value.is_null(): return EventRef(RuntimeRef("runtime"), "run")
    var object = _bridge_object(value, path)
    _bridge_known(object, ["runtime", "run_id", "event_id", "sequence"], path)
    if "runtime" not in object or "run_id" not in object or "event_id" not in object or "sequence" not in object: raise Error("missing_field at " + path)
    return EventRef(_bridge_runtime(object["runtime"].copy(), path + "/runtime"), _bridge_string(object["run_id"].copy(), path + "/run_id"), _bridge_string(object["event_id"].copy(), path + "/event_id"), _bridge_int(object["sequence"].copy(), path + "/sequence"))

def _bridge_impulse(value: Value, path: String) raises -> Impulse:
    var object = _bridge_object(value, path)
    _bridge_known(object, ["id", "run_id", "impulse_type", "payload", "metadata", "created_at", "updated_at"], path)
    for name in ["id", "run_id", "impulse_type", "payload", "metadata", "created_at", "updated_at"]:
        if name not in object: raise Error("missing_field at " + path + "/" + name)
    var payload = object["payload"].copy(); var metadata = object["metadata"].copy()
    if not payload.is_object() and not payload.is_array(): raise Error("invalid_type at " + path + "/payload")
    if not metadata.is_object(): raise Error("invalid_type at " + path + "/metadata")
    return Impulse(_bridge_string(object["id"].copy(), path + "/id"), _bridge_string(object["run_id"].copy(), path + "/run_id"), _bridge_string(object["impulse_type"].copy(), path + "/impulse_type"), canonical_json_text(to_string(payload^)), canonical_json_text(to_string(metadata^)), _bridge_string(object["created_at"].copy(), path + "/created_at"), _bridge_string(object["updated_at"].copy(), path + "/updated_at"))

def _bridge_budget(value: Value, path: String) raises -> RuntimeBudget:
    var object = _bridge_object(value, path)
    var names = ["runtime_hops", "spawned_runs", "impulse_count", "wall_time_seconds", "attempts", "reaction_bytes"]
    _bridge_known(object, names, path)
    var values = List[Int](length=6, fill=0); var limited = List[Bool](length=6, fill=False)
    for i in range(6):
        if names[i] in object:
            var item = object[names[i]].copy()
            if not item.is_null(): values[i] = _bridge_int(item^, path + "/" + names[i]); limited[i] = True
    return RuntimeBudget(values[0], values[1], values[2], values[3], values[4], values[5], limited[0], limited[1], limited[2], limited[3], limited[4], limited[5])

def _decode_bridge_delivery(text: String) raises -> BridgeDelivery:
    var root = Value(parse_string=text)
    var object = _bridge_object(root, "/")
    var names = ["id", "run_id", "idempotency_key", "source", "target", "impulse", "event_ref", "pool_id", "budget", "status", "attempts", "metadata", "created_at", "updated_at"]
    _bridge_known(object, names, "")
    for name in names:
        if name not in object: raise Error("missing_field at /" + name)
    var pool = ""
    if not object["pool_id"].is_null(): pool = _bridge_string(object["pool_id"].copy(), "/pool_id")
    var metadata = object["metadata"].copy()
    if not metadata.is_object(): raise Error("invalid_type at /metadata")
    var row = BridgeDelivery(_bridge_string(object["id"].copy(), "/id"), _bridge_string(object["run_id"].copy(), "/run_id"), _bridge_string(object["idempotency_key"].copy(), "/idempotency_key"), _bridge_run(object["source"].copy(), "/source"), _bridge_run(object["target"].copy(), "/target"), _bridge_impulse(object["impulse"].copy(), "/impulse"), _bridge_event(object["event_ref"].copy(), "/event_ref"), pool, _bridge_budget(object["budget"].copy(), "/budget"), _bridge_string(object["status"].copy(), "/status"), _bridge_int(object["attempts"].copy(), "/attempts"), canonical_json_text(to_string(metadata^)), _bridge_string(object["created_at"].copy(), "/created_at"), _bridge_string(object["updated_at"].copy(), "/updated_at"))
    if not row.is_valid(): raise Error("invalid_value at /: invalid BridgeDelivery")
    return row^
def _bridge_atomic_write(path: Path, text: String) raises SQLiteError:
    var temp = Path(path.__fspath__() + ".tmp")
    try:
        temp.write_text(text + "\n")
        var source_text = temp.__fspath__() + "\0"
        var target_text = path.__fspath__() + "\0"
        var source_c = CStringSlice(source_text)
        var target_c = CStringSlice(target_text)
        var result = external_call["rename", c_int](source_c, target_c)
        if result != 0: raise SQLiteError(code=1, message="bridge export failed")
    except err:
        try: remove(temp)
        except: pass
        raise SQLiteError(code=1, message="bridge export failed")
def _bridge_export(command: String) raises SQLiteError -> String:
    var db_path = _bridge_path(_flag(command, "--db"), "database")
    var out_path = _bridge_path(_flag(command, "--out"), "output")
    if db_path.__fspath__() == out_path.__fspath__(): raise SQLiteError(code=2, message="unsafe_path: database and output paths must differ")
    var store = NativeDomainStore.open(db_path.__fspath__()); store.initialize()
    var delivery_id = _flag(command, "--delivery-id")
    var lookup = store.db.query("SELECT run_id FROM bridge_outbox WHERE id=?")
    lookup.bind_text(1, delivery_id)
    if not lookup.step():
        lookup.close(); store.close(); return "{\"ok\":false,\"runtime\":\"mojo\",\"error\":{\"type\":\"not_found\",\"message\":\"bridge delivery not found\"}}"
    var run_id = lookup.column_text(0); lookup.close()
    var row = get_outbox_delivery(store, run_id, delivery_id)
    var text = ""
    try:
        text = canonical_json_text(row.to_json())
    except err:
        store.close(); raise SQLiteError(code=1, message="bridge export failed")
    store.close()
    _bridge_atomic_write(out_path, text)
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"bridge\",\"delivery\":" + text + "}"

def _bridge_import(command: String) raises SQLiteError -> String:
    var db_path = _bridge_path(_flag(command, "--db"), "database")
    var file_path = _bridge_path(_flag(command, "--file"), "input")
    var text = ""
    try:
        text = file_path.read_text()
    except err:
        raise SQLiteError(code=1, message="bridge import failed: unable to read input")
    var row: BridgeDelivery
    try: row = _decode_bridge_delivery(text)
    except err: raise SQLiteError(code=2, message="invalid_json: malformed BridgeDelivery")
    var key = _flag(command, "--idempotency-key", "bridge.file.import:" + row.id)
    if key == "": raise SQLiteError(code=2, message="argument_error: idempotency key must not be empty")
    var store = NativeDomainStore.open(db_path.__fspath__()); store.initialize(); var imported = import_bridge_delivery(store, row, key); store.close()
    var serialized = ""
    try: serialized = imported.to_json()
    except err: raise SQLiteError(code=1, message="bridge import failed")
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"bridge\",\"delivery\":" + serialized + "}"


def _error(kind: String, message: String = "") -> String:
    var detail = message
    if detail == "": detail = kind
    return "{\"ok\":false,\"runtime\":\"mojo\",\"error\":{\"type\":" + _quote(kind) + ",\"message\":" + _quote(detail) + "}}"


def _bridge_deliver(command: String) raises SQLiteError -> String:
    var source_path = _flag(command, "--db")
    var target_path = _flag(command, "--target-db")
    var run_id = _flag(command, "--run-id")
    var delivery_id = _flag(command, "--delivery-id")
    var updated_at = _flag(command, "--now")
    var delivery_key = _flag(command, "--idempotency-key", "")
    var import_key = _flag(command, "--import-idempotency-key", "")
    var result = deliver_local_bridge(
        source_path, target_path, run_id, delivery_id, updated_at,
        delivery_key, import_key,
    )
    return "{\"ok\":true,\"runtime\":\"mojo\",\"delivered\":" + result.source_delivery.to_json() + ",\"imported\":" + result.imported_delivery.to_json() + ",\"delivery_replayed\":" + ("true" if result.source_replayed else "false") + ",\"import_replayed\":" + ("true" if result.imported_replayed else "false") + "}"


def _transition(command: String, operation: String) raises SQLiteError -> String:
    var path = _path(command)
    var run_id = _flag(command, "--run-id")
    if run_id == "": raise SQLiteError(code=2, message="argument_error: --run-id is required")
    var now = _flag(command, "--now")
    var key = _flag(command, "--idempotency-key", "run." + operation)
    var lifecycle = RunLifecycle.open(path)
    lifecycle.initialize()
    var row = RunRow(id="", status="", title="", metadata="", created_at="", updated_at="")
    if operation == "start":
        row = lifecycle.start(run_id, now, key)
    elif operation == "wait":
        row = lifecycle.wait(run_id, now, key)
    elif operation == "complete":
        row = lifecycle.complete(run_id, now, key)
    elif operation == "fail":
        row = lifecycle.fail(run_id, now, key)
    elif operation == "request_cancel":
        row = lifecycle.request_cancel(run_id, now, key, reason=_flag(command, "--reason", "cancel_requested"), reason_present=_has_option(command, "--reason"), actor="cli:user")
    elif operation == "cancel":
        row = lifecycle.request_cancel(run_id, now, key, reason=_flag(command, "--reason", ""), reason_present=_has_option(command, "--reason"), actor="cli:user")
    elif operation == "timeout":
        row = lifecycle.timeout(run_id, now, key)
    else:
        lifecycle.close()
        raise SQLiteError(code=2, message="argument_error: unknown lifecycle operation")
    lifecycle.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"run\",\"id\":" + _quote(row.id) + ",\"status\":" + _quote(row.status) + "}"

def dispatch_native_command(command: String) raises -> String:
    try:
        var first = _word(command, 0)
        var second = _word(command, 1)
        # Progressive disclosure: `ops <cmd>...` is an alias for operator tools.
        if first == "ops" and second != "":
            var rest = second
            var idx = 2
            while idx < _count(command):
                rest += " " + _word(command, idx)
                idx += 1
            return dispatch_native_command(rest)
        if first == "init": _validate(command, "init"); return _init(command)
        if first == "gc":
            _validate(command, "gc")
            return _gc(command)
        elif first == "archive-run" or first == "archive-gc": return _error("native_boundary", first + " requires native filesystem archive support")
        if first == "run-until-idle": return _error("native_boundary", "run-until-idle requires a native execution adapter registry")
        if first == "replay-execution": return _error("native_boundary", "replay-execution requires a native execution replay host")
        if first == "doctor" and (_has_option(command, "--package") or _has_option(command, "--output")): return _error("native_boundary", "doctor package/YAML and output paths require native filesystem support")
        if first == "schema": _validate(command, "schema", True)
        elif first == "db":
            _require_db_value(command, "db")
            if second != "status" and _has_option(command, "--ensure-schema"): return _error("argument_error", "--ensure-schema is supported only for db status")
            _validate(command, "db", True)
        elif first == "doctor":
            _require_db_value(command, "doctor")
            _validate(command, "doctor", True)
        elif first == "events" and second == "validate-schema": _validate(command, "event-schema")
        elif first == "projections" and second == "rebuild": _validate(command, "projection")
        elif first == "runs" and second == "list": _validate(command, "run-list")
        elif first == "runs" and second == "observe": _validate(command, "run-observe")
        elif first == "runs" and second == "start": _validate(command, "transition"); return _transition(command, "start")
        elif first == "runs" and second == "wait": _validate(command, "transition"); return _transition(command, "wait")
        elif first == "runs" and second == "complete": _validate(command, "transition"); return _transition(command, "complete")
        elif first == "runs" and second == "fail": _validate(command, "transition"); return _transition(command, "fail")
        elif first == "runs" and second == "request-cancel": _validate(command, "transition"); return _transition(command, "request_cancel")
        elif first == "runs" and second == "cancel": _validate(command, "transition"); return _transition(command, "cancel")
        elif first == "runs" and second == "timeout": _validate(command, "transition"); return _transition(command, "timeout")
        elif first == "homeostats" and second == "list": _validate(command, "homeostats-list")
        elif first == "homeostats" and second == "inspect": return _error("unsupported_command")
        if (first == "commands" or first == "events" or first == "processes" or first == "impulses" or first == "impulse-types" or first == "impulse-relations" or first == "relations" or first == "associations" or first == "reactions" or first == "projections" or first == "bridge" or first == "bridges" or first == "runs") and second == "inspect": _validate(command, "inspect")
        if command == "commands list" or command.startswith("commands list "): _validate(command, "commands-list"); return _command_rows(_path(command), command)
        elif first == "reactions" and second == "record": _validate(command, "reaction-record"); return _reaction_record(command)
        if command == "trace" or command.startswith("trace "): _validate(command, "trace"); return _trace(_path(command), command)
        if command == "diagnose-waits" or command.startswith("diagnose-waits "): _validate(command, "diagnose-waits"); return _diagnose_waits(_path(command), _flag(command, "--run-id"), _flag(command, "--impulse-id"))
        if first == "bridge" and second == "deliver": _validate(command, "bridge-deliver"); return _bridge_deliver(command)
        if first == "bridge" and (second == "export" or second == "import"): 
            if second == "export": _validate(command, "bridge-export"); return _bridge_export(command)
            _validate(command, "bridge-import"); return _bridge_import(command)
        if command == "commands inspect" or command.startswith("commands inspect "): return _command_inspect(_path(command), _flag(command, "--run-id"), _flag(command, "--command-id"))
        if command == "impulses inspect" or command.startswith("impulses inspect "): return _impulse_inspect(_path(command), _flag(command, "--run-id"), _flag(command, "--impulse-id"))
        elif first == "maintain-journal": _validate(command, "maintenance")
        if command == "schema impulse": return "{\"ok\":true,\"runtime\":\"mojo\",\"schema\":\"impulse\"}"
        if command == "processes inspect" or command.startswith("processes inspect "): return _process_inspect(_path(command), _flag(command, "--run-id"), _flag(command, "--process-id"))
        if command == "impulse-types inspect" or command.startswith("impulse-types inspect "): return _domain_inspect(_path(command), "impulse_type", "impulse_types", _flag(command, "--run-id"), _flag(command, "--impulse-type-id"), "--impulse-type-id")
        if command == "impulse-relations inspect" or command.startswith("impulse-relations inspect ") or command == "relations inspect" or command.startswith("relations inspect "): return _domain_inspect(_path(command), "impulse_relation", "impulse_relations", _flag(command, "--run-id"), _flag(command, "--relation-id"), "--relation-id")
        if command == "reactions inspect" or command.startswith("reactions inspect "): return _domain_inspect(_path(command), "reaction", "reactions", _flag(command, "--run-id"), _flag(command, "--reaction-id"), "--reaction-id")
        if command == "associations inspect" or command.startswith("associations inspect "): return _domain_inspect(_path(command), "association", "associations", _flag(command, "--run-id"), _flag(command, "--association-id"), "--association-id")
        if command == "schema model" or command.startswith("schema model "): return _schema_model()
        if command == "schema fala-package" or command.startswith("schema fala-package "): return _error("native_boundary", "schema fala-package requires a native model schema encoder")
        if command == "db init" or command == "db migrate": return _error("argument_error", "--db is required")
        if command.startswith("db init "): return initialize_database(_path(command))
        if command.startswith("db migrate "): return initialize_database(_path(command))
        if command == "runs list" or command.startswith("runs list "): return _runs(_path(command), _flag(command,"--status"), _flag(command,"--run-id"), _limit(command), _bool_option(command, "--jsonl"))
        if command == "runs observe" or command.startswith("runs observe "):
            return _run_observe(_path(command), _flag(command, "--run-id"))
        if command == "runs inspect" or command.startswith("runs inspect "): _validate(command, "inspect"); return _run_inspect(_path(command), _flag(command, "--run-id"))
        if command == "db status" or command.startswith("db status "):
            var status_path = _path(command)
            if _has_option(command, "--ensure-schema"): _ = initialize_database(status_path)
            return _status(status_path)
        if command == "db schema" or command.startswith("db schema "): return _schema_model()
        if command == "db vacuum" or command.startswith("db vacuum "): return _vacuum(_path(command))
        if command == "maintain-journal" or command.startswith("maintain-journal "): return _maintain_journal(command)
        if command == "create-run" or command.startswith("create-run "): _validate(command, "create"); return _create(command)
        if command == "runs create" or command.startswith("runs create ") or command == "run create" or command.startswith("run create "): return _error("legacy_alias", "use create-run")
        if command == "projections rebuild" or command.startswith("projections rebuild "): return _projection_rebuild(command)
        if command == "impulses create" or command.startswith("impulses create "): _validate(command, "impulse-create"); return _impulse_create(command)
        if command == "processes schedule" or command.startswith("processes schedule "): _validate(command, "process-schedule"); return _process_schedule(command)
        if command == "processes cancel" or command.startswith("processes cancel "): _validate(command, "process-transition"); return _process_transition(command, "cancel")
        if command == "processes timeout" or command.startswith("processes timeout "): _validate(command, "process-transition"); return _process_transition(command, "timeout")
        if command == "homeostats expire" or command.startswith("homeostats expire ") or command == "homeostat expire" or command.startswith("homeostat expire "): _validate(command, "homeostat-domain-transition"); return _homeostat_domain(command, "expire")
        if command == "homeostats reopen" or command.startswith("homeostats reopen ") or command == "homeostat reopen" or command.startswith("homeostat reopen "): _validate(command, "homeostat-transition"); return _homeostat_transition(command, "reopen")
        if command == "associations append" or command.startswith("associations append "): _validate(command, "association-append"); return _association_append(command)
        if command == "homeostats open" or command.startswith("homeostats open ") or command == "homeostat open" or command.startswith("homeostat open "): _validate(command, "homeostat-domain-open"); return _homeostat_domain(command, "open")
        if command == "homeostats complete" or command.startswith("homeostats complete ") or command == "homeostat complete" or command.startswith("homeostat complete "): _validate(command, "homeostat-domain-transition"); return _homeostat_domain(command, "complete")
        if command == "homeostats cancel" or command.startswith("homeostats cancel ") or command == "homeostat cancel" or command.startswith("homeostat cancel "): _validate(command, "homeostat-domain-transition"); return _homeostat_domain(command, "cancel")
        if command == "projections list" or command.startswith("projections list "): _validate(command, "projections-list"); return _rows(_path(command), "projections", "projections", command)
        if command == "commands list" or command.startswith("commands list "): _validate(command, "commands-list"); return _command_rows(_path(command), command)
        if command == "events list" or command.startswith("events list "): _validate(command, "events-list"); return _rows(_path(command), "events", "runtime_events", command)
        if command == "processes list" or command.startswith("processes list "): _validate(command, "processes-list"); return _rows(_path(command), "processes", "processes", command)
        if command == "impulses list" or command.startswith("impulses list "): _validate(command, "impulses-list"); return _rows(_path(command), "impulses", "impulses", command)
        if command == "events validate-schema" or command.startswith("events validate-schema "): return _events_validate_schema(_path(command), command)
        if command == "impulse-types list" or command.startswith("impulse-types list "): _validate(command, "impulse-types-list"); return _rows(_path(command), "impulse-types", "impulse_types", command)
        if command == "impulse-relations list" or command.startswith("impulse-relations list "): _validate(command, "impulse-relations-list"); return _rows(_path(command), "impulse-relations", "impulse_relations", command)
        if command == "relations list" or command.startswith("relations list "): _validate(command, "impulse-relations-list"); return _rows(_path(command), "relations", "impulse_relations", command)
        if command == "associations list" or command.startswith("associations list "): _validate(command, "associations-list"); return _rows(_path(command), "associations", "associations", command)
        if command == "reactions list" or command.startswith("reactions list "): _validate(command, "reactions-list"); return _rows(_path(command), "reactions", "reactions", command)
        if command == "homeostats list" or command.startswith("homeostats list "): _validate(command, "homeostats-list"); return _rows(_path(command), "homeostats", "homeostats", command)
        if command == "bridge list" or command.startswith("bridge list "):
            _validate(command, "bridge-list")
            var bridge_table = "bridge_outbox"
            if _flag(command, "--box", "outbox") == "inbox": bridge_table = "bridge_inbox"
            return _rows(_path(command), "bridge", bridge_table, command)
        if command == "bridges list" or command.startswith("bridges list "):
            _validate(command, "bridge-list")
            var bridges_table = "bridge_outbox"
            if _flag(command, "--box", "outbox") == "inbox": bridges_table = "bridge_inbox"
            return _rows(_path(command), "bridges", bridges_table, command)
        if command == "export" or command.startswith("export ") or command.startswith("export-html") or command.startswith("export-bundle"):
            return _error("native_boundary", "export requires a native file encoder")
        if command == "doctor" or command.startswith("doctor "):
            var doctor_path = _path(command)
            if _has_option(command, "--ensure-schema"): _ = initialize_database(doctor_path)
            return _status(doctor_path)
        return _error("unsupported_command")
    except err:
        var detail = String(err)
        if detail.find("unsafe_path") >= 0: return _error("unsafe_path", "invalid database path")
        if detail.find("invalid_json") >= 0: return _error("invalid_json", "invalid JSON input")
        if detail.find("argument_error") >= 0 or detail.find("Invalid value '") >= 0: return _error("argument_error", detail)
        return _error("storage_error", "native command failed")


from .native_cli_help import cli_surface_help

def dispatch_command(command: String) raises -> String:
    return dispatch_native_command(command)

