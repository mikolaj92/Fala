from fala.compensation import compensation_input, reconcile_compensation
from fala.effect_protocol import EffectObservation
from fala.native_package import validate_package_json_text
from fala.journal import NativeJournal


def expect(value: Bool, message: String) raises:
    if not value: raise Error(message)


def main() raises:
    var manifest = validate_package_json_text("{\"id\":\"p\",\"capabilities\":[{\"id\":\"create\"},{\"id\":\"remove\"}],\"correlation_paths\":[{\"id\":\"main\",\"effectors\":[{\"id\":\"draft\",\"capability\":\"create\",\"adapter\":{\"kind\":\"manual_homeostat\"},\"compensation\":{\"path_id\":\"undo\",\"capability\":\"remove\"}}]},{\"id\":\"undo\",\"effectors\":[{\"id\":\"remove\",\"capability\":\"remove\",\"adapter\":{\"kind\":\"manual_homeostat\"}}]}]}")
    expect(manifest.correlation_paths[0].effectors[0].compensation_json.find("undo") >= 0, "separate declared compensation")
    var journal = NativeJournal(":memory:\0"); journal.initialize(); _ = journal.create_run("r", "active", "{}", "t")
    var receipt = "{\"authoritative_identity\":{\"id\":\"draft-1\"},\"evidence_ref\":\"evidence:create\"}"
    var first = reconcile_compensation(journal, "r", receipt, "undo", "remove", "undo-draft-1", EffectObservation("matching", "{\"id\":\"draft-1\"}", "evidence:present"), "t1")
    expect(first.action == "confirm" and first.terminal == "compensated" and first.input_json.find("evidence:create") >= 0, "exact receipt reaches compensation")
    # Crash/retry observes the same authoritative state and replays confirmation,
    # rather than issuing another reversal.
    var replay = reconcile_compensation(journal, "r", receipt, "undo", "remove", "undo-draft-1", EffectObservation("matching", "{\"id\":\"draft-1\"}", "evidence:present"), "t2")
    expect(replay.terminal == "compensated" and replay.action == "confirm", "one confirmed compensation after crash")
    var absent = reconcile_compensation(journal, "r", receipt, "undo", "remove", "absent", EffectObservation("absent"), "t3")
    expect(absent.terminal == "already_absent", "already absent terminal")
    var conflict = reconcile_compensation(journal, "r", receipt, "undo", "remove", "conflict", EffectObservation("conflicting", "{\"id\":\"other\"}", "e"), "t4")
    expect(conflict.terminal == "compensation_failed", "failure remains explicit")
    var not_compensable = False
    try: _ = compensation_input("{}", "undo", "remove", "key")
    except err: not_compensable = String(err).find("not_compensable") >= 0
    expect(not_compensable, "effect without receipt is never reversed")
    journal.close(); print("compensation smoke ok")
