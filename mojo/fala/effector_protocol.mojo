"""Fala parent-child protocol: identity, status, payload, contract."""

from emberjson import Object, Value, to_string
from fala.json import canonical_json_text, quote_json_string as quote
from fala.reactions import content_address_json, sha256_bytes


comptime PROTOCOL = "fala"


@fieldwise_init
struct ContractPair(Copyable, Movable):
    """Which written output contract both sides speak."""

    var id: String
    var version: String


def _digest(body: String) raises -> String:
    return "msg:sha256:" + sha256_bytes(body)


def _closed(ref value: Value, allowed: List[String], path: String) raises:
    for pair in value.object().items():
        var known = False
        for key in allowed:
            if pair.key == key: known = True
        if not known: raise Error("fep.unknown_field at " + path + "/" + pair.key)


def _required_string(ref value: Value, key: String, path: String) raises -> String:
    if key not in value.object() or not value.object()[key].is_string() or value.object()[key].string() == "":
        raise Error("fep.required at " + path + "/" + key)
    return value.object()[key].string()


def contract_pair_from_schema(schema_json: String) raises -> ContractPair:
    """Address the frozen output_schema as the only spoken contract."""
    var digest = content_address_json(schema_json)
    return ContractPair(id="schema:sha256:" + digest, version="1")


def _contract_fields_json(contract_id: String, contract_version: String) raises -> String:
    if contract_id == "": raise Error("fep.required at /contract_id")
    if contract_version == "": raise Error("fep.required at /contract_version")
    return ",\"contract_id\":" + quote(contract_id) + ",\"contract_version\":" + quote(contract_version)


def _require_contract_pair(ref value: Value) raises:
    _ = _required_string(value, "contract_id", "")
    _ = _required_string(value, "contract_version", "")


def _contract_field(ref value: Value, key: String) raises -> String:
    return _required_string(value, key, "")


def request_message(sender: String, recipient: String, job: String, payload_json: String, config_json: String = "{}", contract_id: String = "", contract_version: String = "") raises -> String:
    if sender == "" or recipient == "" or job == "": raise Error("fep.request_invalid: required identity is missing")
    var payload = canonical_json_text(payload_json)
    var config = canonical_json_text(config_json)
    var body = canonical_json_text("{\"config\":" + config + ",\"from\":" + quote(sender) + ",\"job\":" + quote(job) + ",\"kind\":\"request\",\"payload\":" + payload + ",\"protocol\":\"" + PROTOCOL + "\",\"to\":" + quote(recipient) + _contract_fields_json(contract_id, contract_version) + "}")
    return canonical_json_text(body[byte=:body.byte_length()-1] + ",\"id\":" + quote(_digest(body)) + "}")


def result_message(sender: String, recipient: String, job: String, request_id: String, payload_json: String, status: String = "ok", contract_id: String = "", contract_version: String = "") raises -> String:
    if sender == "" or recipient == "" or job == "" or request_id == "" or (status != "ok" and status != "error"):
        raise Error("fep.result_invalid: identity or status missing")
    var payload = canonical_json_text(payload_json)
    var body = canonical_json_text("{\"from\":" + quote(sender) + ",\"job\":" + quote(job) + ",\"kind\":\"result\",\"payload\":" + payload + ",\"protocol\":\"" + PROTOCOL + "\",\"ref\":" + quote(request_id) + ",\"status\":" + quote(status) + ",\"to\":" + quote(recipient) + _contract_fields_json(contract_id, contract_version) + "}")
    return canonical_json_text(body[byte=:body.byte_length()-1] + ",\"id\":" + quote(_digest(body)) + "}")


def validate_message(text: String, expected_kind: String = "") raises -> String:
    var value = Value(parse_string=text)
    if not value.is_object(): raise Error("fep.invalid_json: message must be object")
    var protocol = _required_string(value, "protocol", "")
    if protocol != PROTOCOL: raise Error("fep.unknown_protocol: " + protocol)
    var kind = _required_string(value, "kind", "")
    if kind != "request" and kind != "result": raise Error("fep.unknown_message_kind: " + kind)
    if expected_kind != "" and kind != expected_kind: raise Error("fep.unexpected_message_kind: " + kind)
    var allowed = List[String]()
    _ = _required_string(value, "from", "")
    _ = _required_string(value, "to", "")
    _ = _required_string(value, "job", "")
    if "payload" not in value.object() or not value.object()["payload"].is_object(): raise Error("fep.type_invalid at /payload")
    if kind == "request":
        for key in ["protocol", "kind", "id", "from", "to", "job", "payload", "config", "contract_id", "contract_version"]: allowed.append(key)
        if "config" not in value.object() or not value.object()["config"].is_object(): raise Error("fep.type_invalid at /config")
    else:
        for key in ["protocol", "kind", "id", "from", "to", "job", "ref", "status", "payload", "contract_id", "contract_version"]: allowed.append(key)
        _ = _required_string(value, "ref", "")
        var status = _required_string(value, "status", "")
        if status != "ok" and status != "error": raise Error("fep.status_invalid at /status")
    _closed(value, allowed, "")
    _require_contract_pair(value)
    var supplied = _required_string(value, "id", "")
    var object = value.object().copy(); _ = object.pop("id")
    var body = canonical_json_text(to_string(Value(object^)))
    if supplied != _digest(body): raise Error("fep.digest_mismatch")
    return canonical_json_text(text)


def assert_same_contract(request_text: String, result_text: String) raises:
    var request = Value(parse_string=validate_message(request_text, "request"))
    var result = Value(parse_string=validate_message(result_text, "result"))
    if _contract_field(request, "contract_id") != _contract_field(result, "contract_id") or _contract_field(request, "contract_version") != _contract_field(result, "contract_version"):
        raise Error("fep.contract_mismatch at /contract_id")


def assert_answers(request_text: String, result_text: String) raises:
    """Fail closed unless result answers request (direction, job, ref, contract)."""
    var request = Value(parse_string=validate_message(request_text, "request"))
    var result = Value(parse_string=validate_message(result_text, "result"))
    if _required_string(result, "ref", "") != _required_string(request, "id", ""):
        raise Error("fep.ref_mismatch at /ref")
    if _required_string(result, "job", "") != _required_string(request, "job", ""):
        raise Error("fep.job_mismatch at /job")
    if _required_string(result, "from", "") != _required_string(request, "to", ""):
        raise Error("fep.direction_mismatch at /from")
    if _required_string(result, "to", "") != _required_string(request, "from", ""):
        raise Error("fep.direction_mismatch at /to")
    assert_same_contract(request_text, result_text)


def domain_payload(text: String) raises -> String:
    var value = Value(parse_string=text)
    if value.is_object() and "protocol" in value.object() and "kind" in value.object() and value.object()["kind"].is_string() and value.object()["kind"].string() == "result" and "payload" in value.object() and value.object()["payload"].is_object():
        var wire = Object(capacity=len(value.object()))
        for pair in value.object().items():
            if pair.key != "adapter": wire[pair.key] = pair.value.copy()
        _ = validate_message(to_string(Value(wire^)), "result")
        return canonical_json_text(to_string(value.object()["payload"].copy()))
    return canonical_json_text(text)
