"""Argument, option, and database-path parsing for the native CLI."""
from std.collections import List
from fala.sqlite import SQLiteError
from fala.json import parse_json, canonical_json_text, quote_json_string as _quote
from emberjson import Value, Object, to_string

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
    if kind == "graph" and (option == "--package" or option == "--before" or option == "--after"): return True
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
    if kind == "explain" and (option == "--run-id" or option == "--package" or option == "--process-id" or option == "--terminal"): return True
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


def _validate_status_value(kind: String, value: String) raises:
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
        raise Error(String(SQLiteError(code=2, message="argument_error: invalid value for --status: " + value)))
def _has_option(command: String, name: String) -> Bool:
    var index = 0
    while index < _count(command):
        var item = _word(command, index)
        if _option_base(item) == name: return True
        index += 1
    return False
def _bool_option(command: String, name: String) raises -> Bool:
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
            raise Error(String(SQLiteError(code=2, message="argument_error: invalid boolean value for " + name)))
        index += 1
    return False



def _limit(command: String) raises -> Int:
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
        raise Error(String(SQLiteError(code=2, message="argument_error: invalid integer value for --limit")))


def _after_sequence(command: String) raises -> Int:
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
        raise Error(String(SQLiteError(code=2, message="argument_error: invalid nonnegative integer value for --after-sequence")))
 
 
 
 
def _maintenance_number(command: String, name: String, default: Float64 = 0.0) raises -> Float64:
    var raw = _flag(command, name, "")
    if raw == "": return default
    try:
        var parsed = parse_json(raw)
        if parsed.value.is_float(): return parsed.value.float()
        if parsed.value.is_int(): return Float64(parsed.value.int())
        if parsed.value.is_uint(): return Float64(parsed.value.uint())
    except err:
        pass
    raise Error(String(SQLiteError(code=2, message="argument_error: invalid numeric value for " + name)))


def _maintenance_integer(command: String, name: String, default: Int = -1) raises -> Int:
    var raw = _flag(command, name, "")
    if raw == "": return default
    try:
        var parsed = parse_json(raw)
        if parsed.value.is_int(): return Int(parsed.value.int())
        if parsed.value.is_uint(): return Int(parsed.value.uint())
    except err:
        pass
    raise Error(String(SQLiteError(code=2, message="argument_error: invalid integer value for " + name)))


def _validate(command: String, kind: String, positional: Bool = False) raises:
    var index = 0
    var count = _count(command)
    if kind == "run-list" or kind == "run-observe" or kind == "event-schema" or kind == "homeostat-list" or kind == "inspect" or kind == "rows" or kind == "commands-list" or kind == "events-list" or kind == "processes-list" or kind == "impulses-list" or kind == "impulse-types-list" or kind == "impulse-relations-list" or kind == "associations-list" or kind == "reactions-list" or kind == "homeostats-list" or kind == "projections-list" or kind == "bridge-list" or kind == "projection" or kind == "reaction-record" or kind == "bridge-deliver" or kind == "bridge-export" or kind == "bridge-import" or kind == "transition" or kind == "impulse-create" or kind == "process-schedule" or kind == "process-transition" or kind == "association-append" or kind == "homeostat-open" or kind == "homeostat-transition" or kind == "homeostat-domain-open" or kind == "homeostat-domain-transition": index = 2
    elif kind == "doctor" or kind == "trace" or kind == "diagnose-waits" or kind == "explain": index = 1
    elif kind == "init": index = 1
    elif kind == "create" or kind == "schema" or kind == "maintenance" or kind == "gc": index = 1
    elif kind == "db" or kind == "graph": index = 2
    var has_positional = False
    var has_db_option = False
    while index < count:
        var item = _word(command, index)
        if item.startswith("--"):
            if not _known_option(kind, item):
                raise Error(String(SQLiteError(code=2, message="argument_error: unknown argument " + item)))
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
                    raise Error(String(SQLiteError(code=2, message="argument_error: missing value for " + option)))
                index += 1
                continue
            if index + 1 >= count or _word(command, index + 1).startswith("--"):
                raise Error(String(SQLiteError(code=2, message="argument_error: missing value for " + option)))
            index += 2
        else:
            if not positional or has_positional or has_db_option:
                raise Error(String(SQLiteError(code=2, message="argument_error: unknown argument " + item)))
            has_positional = True
            index += 1
    if kind == "graph":
        if _word(command, 1) == "diff":
            if _flag(command, "--before", "") == "" or _flag(command, "--after", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --before and --after are required")))
        elif _flag(command, "--package", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --package is required")))
    if kind == "schema" and not has_positional:
        raise Error(String(SQLiteError(code=2, message="argument_error: schema model is required")))
    if kind == "schema":
        var model = _word(command, 1)
        if model != "impulse" and model != "model" and model != "fala-package":
            raise Error(String(SQLiteError(code=2, message="argument_error: unknown schema model " + model)))
    if kind == "reaction-record":
        if _flag(command, "--db", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --db is required")))
        if _flag(command, "--run-id", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
        if _flag(command, "--reaction-root", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --reaction-root is required")))
        if _flag(command, "--path", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --path is required")))
        if _flag(command, "--kind", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --kind is required")))
    if kind == "bridge-deliver":
        if _flag(command, "--db", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --db is required")))
        if _flag(command, "--run-id", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
        if _flag(command, "--delivery-id", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --delivery-id is required")))
        if _flag(command, "--target-db", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --target-db is required")))
        if _flag(command, "--now", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --now is required")))
    if kind == "bridge-export":
        if _flag(command, "--db", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --db is required")))
        if _flag(command, "--run-id", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
    if kind == "homeostat-list" or kind == "homeostats-list":
        if _flag(command, "--run-id", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
    if kind == "homeostat-domain-open" or kind == "homeostat-domain-transition":
        if _flag(command, "--db", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --db is required")))
        if _flag(command, "--run-id", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
    if kind == "homeostat-domain-open" and _flag(command, "--kind", "") == "":
        raise Error(String(SQLiteError(code=2, message="argument_error: --kind is required")))
    if kind == "homeostat-domain-transition" and _flag(command, "--homeostat-id", "") == "":
        raise Error(String(SQLiteError(code=2, message="argument_error: --homeostat-id is required")))
    if kind == "commands-list" or kind == "events-list" or kind == "processes-list" or kind == "impulses-list" or kind == "impulse-types-list" or kind == "impulse-relations-list" or kind == "associations-list" or kind == "reactions-list" or kind == "homeostats-list" or kind == "projections-list" or kind == "bridge-list":
        if _flag(command, "--run-id", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
    if kind == "bridge-import":
        if _flag(command, "--db", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --db is required")))
        if _flag(command, "--file", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --file is required")))
    if kind == "gc":
        if _flag(command, "--db", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --db is required")))
        if _flag(command, "--reaction-root", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --reaction-root is required")))
    if kind == "rows":
        var first = _word(command, 0)
        var requires_run = first == "runs" or first == "commands" or first == "events" or first == "processes" or first == "bridge" or first == "trace" or first == "diagnose-waits" or first == "impulses" or first == "impulse-types" or first == "impulse-relations" or first == "relations" or first == "associations" or first == "reactions" or first == "homeostats" or first == "projections"
        if requires_run and _flag(command, "--run-id") == "":
            raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
    if kind == "trace" and _flag(command, "--run-id") == "":
        raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
    if kind == "explain":
        if _flag(command, "--db", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --db is required")))
        if _flag(command, "--package", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --package is required")))
        if _flag(command, "--run-id", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
        if _flag(command, "--process-id", "") != "" and _flag(command, "--terminal", "") != "": raise Error(String(SQLiteError(code=2, message="argument_error: choose --process-id or --terminal")))
    if kind == "diagnose-waits":
        if _flag(command, "--db", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --db is required")))
        if _flag(command, "--run-id", "") == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))

    if kind == "projection" and _flag(command, "--run-id", "") == "":
        raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
    if kind == "create" or kind == "transition" or kind == "impulse-create" or kind == "process-schedule" or kind == "process-transition" or kind == "association-append" or kind == "homeostat-open" or kind == "homeostat-transition" or kind == "projection" or kind == "reaction-record":
        if _flag(command, "--now", "") == "":
            raise Error(String(SQLiteError(code=2, message="argument_error: --now is required")))
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


def _database_url(value: String) raises -> String:
    var rest = ""
    if value.startswith("sqlite://"):
        rest = String(value[byte=8:])
    elif value.startswith("sqlite3://"):
        rest = String(value[byte=9:])
    else:
        var colon = value.find(":")
        if colon > 0: raise Error(String(SQLiteError(code=2, message="argument_error: unsupported database URL scheme")))
        return value
    var path = ""
    if rest.startswith("/localhost/"):
        path = "/" + String(rest[byte=10:])
    elif rest.startswith("localhost/"):
        path = "/" + String(rest[byte=9:])
    elif rest.startswith("localhost"):
        raise Error(String(SQLiteError(code=2, message="argument_error: SQLite URL must include a database path")))
    elif rest.startswith("//"):
        # sqlite:////tmp/db encodes an absolute path.
        path = String(rest[byte=1:])
    elif rest.startswith("/"):
        # sqlite:///relative.db is relative, matching the reference resolver.
        path = String(rest[byte=1:])
    else:
        raise Error(String(SQLiteError(code=2, message="argument_error: SQLite URL host must be empty or localhost")))
    if path == "": raise Error(String(SQLiteError(code=2, message="argument_error: SQLite URL must include a database path")))
    return _url_decode(path)


def _parent_directory(path: String) -> String:
    var last = -1
    for i in range(path.byte_length()):
        if path[byte=i] == "/": last = i
    if last < 0: return "."
    if last == 0: return "/"
    return String(path[byte=0:last])

def _path(command: String) raises -> String:
    var path = _flag(command, "--db", "")
    var first = _word(command, 0)
    if path == "":
        if first == "db": path = _positional_path(command, 2)
        elif first == "doctor": path = _positional_path(command, 1)
    if path == "" and first != "doctor" and first != "init":
        raise Error(String(SQLiteError(code=2, message="argument_error: --db is required")))
    if path == "": path = ".fala/state.sqlite"
    path = _database_url(path)
    if not _safe(path): raise Error(String(SQLiteError(code=2, message="unsafe_path: invalid database path")))
    return path


def _require_db_value(command: String, kind: String) raises:
    var count = _count(command)
    var index = 0
    if kind == "db": index = 2
    elif kind == "doctor": index = 1
    while index < count:
        var item = _word(command, index)
        if item == "--db":
            if index + 1 >= count or _word(command, index + 1).startswith("--"):
                raise Error(String(SQLiteError(code=2, message="argument_error: missing value for --db")))
            return
        if _option_base(item) == "--db":
            var equals = item.find("=")
            if equals < 0 or equals + 1 >= item.byte_length():
                raise Error(String(SQLiteError(code=2, message="argument_error: missing value for --db")))
            return
        index += 1


def _json(value: String) raises:
    try:
        var parsed = parse_json(value)
        _ = parsed
    except err:
        raise Error(String(SQLiteError(code=2, message="invalid_json")))

def _metadata_value(command: String) raises -> String:
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
            raise Error(String(SQLiteError(code=2, message="argument_error: repeated --metadata JSON objects are ambiguous")))
        try:
            var parsed = parse_json(values[0])
            return canonical_json_text(to_string(parsed.value))
        except err:
            raise Error(String(SQLiteError(code=2, message="invalid_json: metadata")))
    var metadata = Object(capacity=len(values))
    for item in values:
        var equals = item.find("=")
        if equals <= 0:
            raise Error(String(SQLiteError(code=2, message="argument_error: invalid value " + item + "; expected key=value")))
        var key = String(item[byte=0:equals])
        var value = String(item[byte=equals + 1:])
        metadata[key] = Value(value)
    try:
        return canonical_json_text(to_string(Value(metadata^)))
    except err:
        raise Error(String(SQLiteError(code=2, message="invalid_json: metadata")))

def _repeat_values(command: String, name: String) raises -> List[String]:
    var values = List[String]()
    var index = 0
    var count = _count(command)
    while index < count:
        var item = _word(command, index)
        if _option_base(item) == name:
            var equals = item.find("=")
            if equals >= 0:
                if equals + 1 >= item.byte_length(): raise Error(String(SQLiteError(code=2, message="argument_error: missing value for " + name)))
                var value = String(item[byte=equals + 1:])
                values.append(value^)
            else:
                if index + 1 >= count or _word(command, index + 1).startswith("--"):
                    raise Error(String(SQLiteError(code=2, message="argument_error: missing value for " + name)))
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
