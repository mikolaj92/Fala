from fala.effect_protocol import EffectIntent, EffectObservation, record_effect_intent, reconcile_effect
from fala.journal import NativeJournal


def expect(value: Bool, message: String) raises:
    if not value: raise Error(message)


def main() raises:
    var journal = NativeJournal(":memory:\0"); journal.initialize()
    _ = journal.create_run("effect-run", "active", "{}", "2026-01-01T00:00:00Z")
    var intent = EffectIntent("stable-key", "{\"kind\":\"artifact\",\"name\":\"release\"}", "publish")
    expect(not record_effect_intent(journal, "effect-run", intent, "2026-01-01T00:00:01Z"), "intent first write")
    var absent = reconcile_effect(journal, "effect-run", intent, EffectObservation("absent"), "2026-01-01T00:00:01Z")
    expect(absent.action == "act" and not absent.confirmed, "absent world effect requests act")
    # Simulate crash after act and before process-result commit. Resume starts at
    # observe; fixture provider reports the already-created authoritative effect.
    var resumed = reconcile_effect(journal, "effect-run", intent, EffectObservation("matching", "{\"name\":\"release\",\"kind\":\"artifact\"}", "fala-reaction://sha256/evidence"), "2026-01-01T00:00:02Z")
    expect(resumed.action == "confirm" and resumed.confirmed and resumed.confirmation_json.find("authoritative_identity") >= 0 and resumed.confirmation_json.find("evidence_ref") >= 0, "crash resume confirms without second act")
    var duplicate = reconcile_effect(journal, "effect-run", intent, EffectObservation("matching", "{\"kind\":\"artifact\",\"name\":\"release\"}", "fala-reaction://sha256/evidence"), "2026-01-01T00:00:02Z")
    expect(duplicate.action == "confirm", "duplicate observation replays confirmation")
    var conflict = False
    try: _ = reconcile_effect(journal, "effect-run", intent, EffectObservation("conflicting", "{\"name\":\"other\"}", "evidence"), "2026-01-01T00:00:03Z")
    except err: conflict = String(err).find("effect.conflict") >= 0
    expect(conflict, "conflict fails closed")
    var missing_evidence = False
    try: _ = reconcile_effect(journal, "effect-run", intent, EffectObservation("matching", intent.desired_identity_json), "2026-01-01T00:00:03Z")
    except err: missing_evidence = String(err).find("evidence_ref") >= 0
    expect(missing_evidence, "confirmation requires evidence")
    var commands = journal.list_commands("effect-run")
    var intent_index = -1; var confirm_index = -1
    for index in range(len(commands)):
        if commands[index].command_type == "effect.intent": intent_index = index
        if commands[index].command_type == "effect.confirm": confirm_index = index
    expect(intent_index >= 0 and confirm_index > intent_index, "intent precedes confirmation durably")
    journal.close(); print("effect protocol smoke ok")
