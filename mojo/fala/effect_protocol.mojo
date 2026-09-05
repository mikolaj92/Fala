"""Domain-neutral reconciliation contract for confirmed external effects.

Execution remains at-least-once. This protocol records intent before action and
requires authoritative observation plus evidence before confirmation.
"""

from emberjson import Object, Value, to_string
from fala.json import canonical_json_text, quote_json_string as quote
from fala.journal import NativeJournal


struct EffectIntent(Copyable, Movable):
    var idempotency_key: String
    var desired_identity_json: String
    var capability: String

    def __init__(out self, idempotency_key: String, desired_identity_json: String, capability: String) raises:
        if idempotency_key == "" or capability == "": raise Error("effect.intent: idempotency_key and capability are required")
        var desired = Value(parse_string=desired_identity_json)
        if not desired.is_object(): raise Error("effect.intent: desired identity must be an object")
        self.idempotency_key = idempotency_key
        self.desired_identity_json = canonical_json_text(to_string(desired))
        self.capability = capability

    def to_json(self) -> String:
        return "{\"capability\":" + quote(self.capability) + ",\"desired_identity\":" + self.desired_identity_json + ",\"idempotency_key\":" + quote(self.idempotency_key) + "}"


struct EffectObservation(Copyable, Movable):
    var state: String
    var identity_json: String
    var evidence_ref: String

    def __init__(out self, state: String, identity_json: String = "{}", evidence_ref: String = "") raises:
        if state != "absent" and state != "matching" and state != "conflicting": raise Error("effect.observe: state must be absent, matching, or conflicting")
        var identity = Value(parse_string=identity_json)
        if not identity.is_object(): raise Error("effect.observe: identity must be an object")
        self.state = state; self.identity_json = canonical_json_text(to_string(identity)); self.evidence_ref = evidence_ref


@fieldwise_init
struct EffectDecision(Copyable, Movable):
    var action: String
    var confirmed: Bool
    var confirmation_json: String


def record_effect_intent(mut journal: NativeJournal, run_id: String, intent: EffectIntent, at: String) raises -> Bool:
    """Persist intent before any caller performs the external act."""
    var key = "effect.intent:" + intent.idempotency_key
    var existing = journal.db.query("SELECT command_type,payload FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
    existing.bind_text(1, run_id); existing.bind_text(2, key)
    if existing.step():
        if existing.column_text(0) != "effect.intent" or existing.column_text(1) != intent.to_json(): raise Error("effect.intent: idempotency conflict")
        existing.close(); return True
    existing.close()
    _ = journal.append_command(run_id, key, "effect.intent", key, intent.to_json(), at, actor="effect-protocol")
    return False


def reconcile_effect(mut journal: NativeJournal, run_id: String, intent: EffectIntent, observation: EffectObservation, at: String) raises -> EffectDecision:
    """Choose act or confirm after authoritative observation; conflicts fail closed."""
    _ = record_effect_intent(journal, run_id, intent, at)
    if observation.state == "conflicting": raise Error("effect.conflict: desired identity differs from authoritative observation")
    if observation.state == "absent": return EffectDecision(action="act", confirmed=False, confirmation_json="null")
    if observation.identity_json != intent.desired_identity_json: raise Error("effect.conflict: matching observation identity differs from desired identity")
    if observation.evidence_ref == "": raise Error("effect.confirm: evidence_ref is required")
    var confirmation = canonical_json_text("{\"authoritative_identity\":" + observation.identity_json + ",\"evidence_ref\":" + quote(observation.evidence_ref) + ",\"idempotency_key\":" + quote(intent.idempotency_key) + "}")
    var key = "effect.confirm:" + intent.idempotency_key
    var existing = journal.db.query("SELECT command_type,payload FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
    existing.bind_text(1, run_id); existing.bind_text(2, key)
    if existing.step():
        if existing.column_text(0) != "effect.confirm" or existing.column_text(1) != confirmation: raise Error("effect.confirm: idempotency conflict")
        existing.close()
    else:
        existing.close(); _ = journal.append_command(run_id, key, "effect.confirm", key, confirmation, at, actor="effect-protocol")
    return EffectDecision(action="confirm", confirmed=True, confirmation_json=confirmation)
