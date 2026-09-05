"""Fala Effector Protocol v1: transport-neutral canonical messages."""

from emberjson import Object, Value, to_string
from fala.json import canonical_json_text, quote_json_string as quote
from fala.reactions import sha256_bytes


comptime PROTOCOL = "fala-effector/1"


def _digest(body: String) raises -> String:
    return "msg:sha256:" + sha256_bytes(body)


def _closed(ref value: Value, allowed: List[String], path: String) raises:
    for pair in value.object().items():
        var known = False
        for key in allowed:
            if pair.key == key: known = True
        if not known: raise Error("fep.unknown_field at " + path + "/" + pair.key)


def _required_string(ref value: Value, key: String, path: String) raises -> String:
    if key not in value.object() or not value.object()[key].is_string() or value.object()[key].string() == "": raise Error("fep.required at " + path + "/" + key)
    return value.object()[key].string()


def request_message(run_id: String, process_id: String, execution_id: String, attempt: Int, impulse_id: String, process_fingerprint: String, path_digest: String, capability: String, input_json: String, config_json: String, output_contract_ref: String) raises -> String:
    if run_id == "" or process_id == "" or execution_id == "" or attempt < 1 or process_fingerprint == "" or path_digest == "" or capability == "" or output_contract_ref == "": raise Error("fep.request_invalid: required identity is missing")
    var input = canonical_json_text(input_json); var config = canonical_json_text(config_json)
    var body = canonical_json_text("{\"attempt\":" + String(attempt) + ",\"capability\":" + quote(capability) + ",\"config\":" + config + ",\"execution_id\":" + quote(execution_id) + ",\"impulse_id\":" + quote(impulse_id) + ",\"input\":" + input + ",\"message_kind\":\"effector.request\",\"output_contract_ref\":" + quote(output_contract_ref) + ",\"path_digest\":" + quote(path_digest) + ",\"process_fingerprint\":" + quote(process_fingerprint) + ",\"process_id\":" + quote(process_id) + ",\"protocol\":\"" + PROTOCOL + "\",\"run_id\":" + quote(run_id) + "}")
    return canonical_json_text(body[byte=:body.byte_length()-1] + ",\"message_id\":" + quote(_digest(body)) + "}")


def result_message(request_id: String, execution_id: String, attempt: Int, values_json: String = "{}", associations_json: String = "[]", reactions_json: String = "[]", metadata_json: String = "{}", evidence_refs_json: String = "[]", provenance_json: String = "{}", usage_json: String = "{}") raises -> String:
    if request_id == "" or execution_id == "" or attempt < 1: raise Error("fep.result_invalid: causation identity missing")
    var body = canonical_json_text("{\"associations\":" + associations_json + ",\"attempt\":" + String(attempt) + ",\"causation\":{\"request_id\":" + quote(request_id) + "},\"evidence_refs\":" + evidence_refs_json + ",\"execution_id\":" + quote(execution_id) + ",\"message_kind\":\"effector.result\",\"metadata\":" + metadata_json + ",\"protocol\":\"" + PROTOCOL + "\",\"provenance\":" + provenance_json + ",\"reactions\":" + reactions_json + ",\"request_id\":" + quote(request_id) + ",\"usage\":" + usage_json + ",\"values\":" + values_json + "}")
    return canonical_json_text(body[byte=:body.byte_length()-1] + ",\"message_id\":" + quote(_digest(body)) + "}")


def validate_message(text: String, expected_kind: String = "") raises -> String:
    var value = Value(parse_string=text)
    if not value.is_object(): raise Error("fep.invalid_json: message must be object")
    var protocol = _required_string(value, "protocol", "")
    if protocol != PROTOCOL: raise Error("fep.unknown_protocol: " + protocol)
    var kind = _required_string(value, "message_kind", "")
    if kind != "effector.request" and kind != "effector.result": raise Error("fep.unknown_message_kind: " + kind)
    if expected_kind != "" and kind != expected_kind: raise Error("fep.unexpected_message_kind: " + kind)
    var allowed = List[String]()
    if kind == "effector.request":
        for key in ["protocol","message_kind","message_id","run_id","process_id","execution_id","attempt","impulse_id","process_fingerprint","path_digest","capability","input","config","output_contract_ref"]: allowed.append(key)
        _ = _required_string(value,"run_id",""); _ = _required_string(value,"process_id",""); _ = _required_string(value,"execution_id",""); _ = _required_string(value,"process_fingerprint",""); _ = _required_string(value,"path_digest",""); _ = _required_string(value,"capability",""); _ = _required_string(value,"output_contract_ref","")
    else:
        for key in ["protocol","message_kind","message_id","request_id","causation","execution_id","attempt","values","associations","reactions","metadata","evidence_refs","provenance","usage"]: allowed.append(key)
        var request_id = _required_string(value,"request_id",""); _ = _required_string(value,"execution_id","")
        if "causation" not in value.object() or not value.object()["causation"].is_object() or _required_string(value.object()["causation"],"request_id","/causation") != request_id: raise Error("fep.causation_invalid: exactly one matching request is required")
        if not value.object()["values"].is_object() or not value.object()["associations"].is_array() or not value.object()["reactions"].is_array() or not value.object()["metadata"].is_object() or not value.object()["evidence_refs"].is_array(): raise Error("fep.output_contract_invalid: typed result fields required")
    _closed(value, allowed, "")
    if "attempt" not in value.object() or not value.object()["attempt"].is_int() or value.object()["attempt"].int() < 1: raise Error("fep.attempt_invalid")
    var supplied = _required_string(value,"message_id","")
    var object = value.object().copy(); _ = object.pop("message_id")
    var body = canonical_json_text(to_string(Value(object^)))
    if supplied != _digest(body): raise Error("fep.digest_mismatch")
    return canonical_json_text(text)
