"""Tiny Python codec for Fala Effector Protocol v1 (not a runtime)."""
from __future__ import annotations
import hashlib, json
from typing import Any, Mapping

PROTOCOL = "fala-effector/1"

class FEPError(ValueError):
    def __init__(self, code: str, pointer: str):
        self.code, self.pointer = code, pointer
        super().__init__(f"{code} at {pointer or '/'}")

def canonical(value: Mapping[str, Any]) -> str:
    return json.dumps(dict(value), ensure_ascii=False, separators=(",", ":"), sort_keys=True)

def _message_id(body: Mapping[str, Any]) -> str:
    return "msg:sha256:" + hashlib.sha256(canonical(body).encode()).hexdigest()

def validate(message: Mapping[str, Any], expected_kind: str | None = None) -> dict[str, Any]:
    value = dict(message)
    if value.get("protocol") != PROTOCOL: raise FEPError("fep.unknown_protocol", "/protocol")
    kind = value.get("message_kind")
    if kind not in ("effector.request", "effector.result"): raise FEPError("fep.unknown_message_kind", "/message_kind")
    if expected_kind and kind != expected_kind: raise FEPError("fep.unexpected_message_kind", "/message_kind")
    request = {"protocol","message_kind","message_id","run_id","process_id","execution_id","attempt","impulse_id","process_fingerprint","path_digest","capability","input","config","output_contract_ref"}
    result = {"protocol","message_kind","message_id","request_id","causation","execution_id","attempt","values","associations","reactions","metadata","evidence_refs","provenance","usage"}
    allowed = request if kind == "effector.request" else result
    unknown = value.keys() - allowed
    if unknown: raise FEPError("fep.unknown_field", "/" + sorted(unknown)[0])
    if not isinstance(value.get("attempt"), int) or isinstance(value.get("attempt"), bool) or value["attempt"] < 1: raise FEPError("fep.attempt_invalid", "/attempt")
    if kind == "effector.request":
        for key in ("run_id","process_id","execution_id","process_fingerprint","path_digest","capability","output_contract_ref"):
            if not isinstance(value.get(key), str) or not value[key]: raise FEPError("fep.required", "/"+key)
        if not isinstance(value.get("input"), dict) or not isinstance(value.get("config"), dict): raise FEPError("fep.output_contract_invalid", "/input")
    else:
        if not isinstance(value.get("request_id"), str) or not value["request_id"]: raise FEPError("fep.required", "/request_id")
        if value.get("causation") != {"request_id": value["request_id"]}: raise FEPError("fep.causation_invalid", "/causation/request_id")
        expected = (("values",dict),("associations",list),("reactions",list),("metadata",dict),("evidence_refs",list),("provenance",dict),("usage",dict))
        for key, typ in expected:
            if not isinstance(value.get(key), typ): raise FEPError("fep.output_contract_invalid", "/"+key)
        for index, reaction in enumerate(value["reactions"]):
            if not isinstance(reaction, dict): raise FEPError("fep.output_contract_invalid", f"/reactions/{index}")
            if any(key in reaction for key in ("content","bytes","payload")): raise FEPError("fep.embedded_artifact", f"/reactions/{index}")
    supplied = value.pop("message_id", None)
    if supplied != _message_id(value): raise FEPError("fep.digest_mismatch", "/message_id")
    value["message_id"] = supplied
    return dict(sorted(value.items()))

def parse(text: str, expected_kind: str | None = None) -> dict[str, Any]:
    try: value = json.loads(text)
    except json.JSONDecodeError as exc: raise FEPError("fep.invalid_json", "/") from exc
    if not isinstance(value, dict): raise FEPError("fep.invalid_json", "/")
    return validate(value, expected_kind)

def build_result(request: Mapping[str, Any], *, values: Mapping[str,Any]|None=None, associations:list[dict[str,Any]]|None=None, reactions:list[dict[str,Any]]|None=None, metadata:Mapping[str,Any]|None=None, evidence_refs:list[str]|None=None, provenance:Mapping[str,Any]|None=None, usage:Mapping[str,Any]|None=None) -> dict[str, Any]:
    request = validate(request, "effector.request")
    body = {"protocol":PROTOCOL,"message_kind":"effector.result","request_id":request["message_id"],"causation":{"request_id":request["message_id"]},"execution_id":request["execution_id"],"attempt":request["attempt"],"values":dict(values or {}),"associations":associations or [],"reactions":reactions or [],"metadata":dict(metadata or {}),"evidence_refs":evidence_refs or [],"provenance":dict(provenance or {}),"usage":dict(usage or {})}
    body["message_id"] = _message_id(body)
    return validate(body, "effector.result")
