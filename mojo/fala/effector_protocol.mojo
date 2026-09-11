"""Fala parent-child protocol: identity, status, payload."""

from emberjson import Object, Value, to_string
from fala.json import canonical_json_text, quote_json_string as quote
from fala.reactions import sha256_bytes


comptime PROTOCOL = "fala"


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


def request_message(sender: String, recipient: String, job: String, payload_json: String, config_json: String = "{}") raises -> String:
    if sender == "" or recipient == "" or job == "": raise Error("fep.request_invalid: required identity is missing")
    var payload = canonical_json_text(payload_json)
    var config = canonical_json_text(config_json)
    var body = canonical_json_text("{\"config\":" + config + ",\"from\":" + quote(sender) + ",\"job\":" + quote(job) + ",\"kind\":\"request\",\"payload\":" + payload + ",\"protocol\":\"" + PROTOCOL + "\",\"to\":" + quote(recipient) + "}")
    return canonical_json_text(body[byte=:body.byte_length()-1] + ",\"id\":" + quote(_digest(body)) + "}")


def result_message(sender: String, recipient: String, job: String, request_id: String, payload_json: String, status: String = "ok") raises -> String:
    if sender == "" or recipient == "" or job == "" or request_id == "" or (status != "ok" and status != "error"):
        raise Error("fep.result_invalid: identity or status missing")
    var payload = canonical_json_text(payload_json)
    var body = canonical_json_text("{\"from\":" + quote(sender) + ",\"job\":" + quote(job) + ",\"kind\":\"result\",\"payload\":" + payload + ",\"protocol\":\"" + PROTOCOL + "\",\"ref\":" + quote(request_id) + ",\"status\":" + quote(status) + ",\"to\":" + quote(recipient) + "}")
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
        for key in ["protocol", "kind", "id", "from", "to", "job", "payload", "config"]: allowed.append(key)
        if "config" not in value.object() or not value.object()["config"].is_object(): raise Error("fep.type_invalid at /config")
    else:
        for key in ["protocol", "kind", "id", "from", "to", "job", "ref", "status", "payload"]: allowed.append(key)
        _ = _required_string(value, "ref", "")
        var status = _required_string(value, "status", "")
        if status != "ok" and status != "error": raise Error("fep.status_invalid at /status")
    _closed(value, allowed, "")
    var supplied = _required_string(value, "id", "")
    var object = value.object().copy(); _ = object.pop("id")
    var body = canonical_json_text(to_string(Value(object^)))
    if supplied != _digest(body): raise Error("fep.digest_mismatch")
    return canonical_json_text(text)


def domain_payload(text: String) raises -> String:
    var value = Value(parse_string=text)
    if value.is_object() and "protocol" in value.object() and "kind" in value.object() and value.object()["kind"].is_string() and value.object()["kind"].string() == "result" and "payload" in value.object() and value.object()["payload"].is_object():
        var wire = Object(capacity=len(value.object()))
        for pair in value.object().items():
            if pair.key != "adapter": wire[pair.key] = pair.value.copy()
        _ = validate_message(to_string(Value(wire^)), "result")
        return canonical_json_text(to_string(value.object()["payload"].copy()))
    return canonical_json_text(text)
