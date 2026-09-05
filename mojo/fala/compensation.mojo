"""Explicit, effectively-once compensation contract for confirmed effects."""

from emberjson import Value, to_string
from fala.effect_protocol import EffectIntent, EffectObservation, reconcile_effect
from fala.journal import NativeJournal
from fala.json import canonical_json_text, quote_json_string as quote


@fieldwise_init
struct CompensationDecision(Copyable, Movable):
    var terminal: String
    var action: String
    var input_json: String


def compensation_input(effect_confirmation_json: String, compensation_path_id: String, compensation_capability: String, idempotency_key: String) raises -> String:
    var receipt = Value(parse_string=effect_confirmation_json)
    if not receipt.is_object() or "authoritative_identity" not in receipt.object() or "evidence_ref" not in receipt.object(): raise Error("compensation.not_compensable: confirmed effect receipt is required")
    if compensation_path_id == "" or compensation_capability == "" or idempotency_key == "": raise Error("compensation.not_compensable: declared path, capability, and idempotency key are required")
    return canonical_json_text("{\"capability\":" + quote(compensation_capability) + ",\"effect_confirmation\":" + canonical_json_text(to_string(receipt)) + ",\"idempotency_key\":" + quote(idempotency_key) + ",\"path_id\":" + quote(compensation_path_id) + "}")


def reconcile_compensation(mut journal: NativeJournal, run_id: String, effect_confirmation_json: String, compensation_path_id: String, compensation_capability: String, idempotency_key: String, observation: EffectObservation, at: String) raises -> CompensationDecision:
    """Observe before reversal; callers act only on the explicit `act` decision."""
    var input = compensation_input(effect_confirmation_json, compensation_path_id, compensation_capability, idempotency_key)
    var receipt = Value(parse_string=effect_confirmation_json)
    var identity = canonical_json_text(to_string(receipt.object()["authoritative_identity"].copy()))
    if observation.state == "absent":
        return CompensationDecision(terminal="already_absent", action="confirm", input_json=input)
    if observation.state == "conflicting":
        return CompensationDecision(terminal="compensation_failed", action="fail", input_json=input)
    var intent = EffectIntent("compensation:" + idempotency_key, identity, compensation_capability)
    var decision = reconcile_effect(journal, run_id, intent, observation, at)
    if decision.action == "act": return CompensationDecision(terminal="", action="act", input_json=input)
    return CompensationDecision(terminal="compensated", action="confirm", input_json=input)
