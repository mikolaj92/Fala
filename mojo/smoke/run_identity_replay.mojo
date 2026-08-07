"""Focused durable-identity create/replay checks for native correlation runs."""

from std.collections import List
from std.os import remove

from fala import (
    AdapterBinding,
    AdapterSpec,
    CorrelationEffectorSpec,
    CorrelationPathSpec,
    NativeFunctionRegistry,
    NativeJournal,
    RunLifecycle,
    instantiate_correlation_path,
    run_correlation_path,
)


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("run identity replay smoke: " + message)


def _cleanup(path: String):
    try:
        remove(path)
    except err:
        pass
    try:
        remove(path + "-wal")
    except err:
        pass
    try:
        remove(path + "-shm")
    except err:
        pass


def _native(input_json: String, config_json: String) raises -> String:
    return "{\"value\":1}"


def _bindings(run_id: String, path_id: String) -> List[AdapterBinding]:
    var bindings = List[AdapterBinding]()
    bindings.append(AdapterBinding(path_id + ":root", AdapterSpec.native_function("native.identity"), run_id))
    bindings.append(AdapterBinding(path_id + ":leaf", AdapterSpec.native_function("native.identity"), run_id))
    return bindings^


def main() raises:
    var path = "/tmp/fala-run-identity-replay-smoke.sqlite"
    _cleanup(path)

    var effectors = List[CorrelationEffectorSpec]()
    effectors.append(CorrelationEffectorSpec.create("root", "native")^)
    var upstream = List[String]()
    upstream.append("root")
    effectors.append(CorrelationEffectorSpec.create("leaf", "native", upstream^)^)
    var correlation_path = CorrelationPathSpec("identity", effectors^)
    var plan = instantiate_correlation_path(correlation_path, "identity-run")

    var registry = NativeFunctionRegistry()
    registry.register("native.identity", _native)
    var bindings = _bindings("identity-run", plan.correlation_path_id)

    var package_id = "pkg.identity"
    var package_version = "1.0.0"
    var package_digest = "pkg-digest-identity"
    var path_digest = "path-digest-identity"
    var runtime_version = "0.7.15"
    var backend_version = "native-sqlite"

    var journal = NativeJournal.open(path)
    journal.initialize()

    # Matching create + terminal replay preserves identity and does not redrive.
    var first = run_correlation_path(
        journal,
        "identity-run",
        plan,
        bindings,
        registry,
        "2026-01-01T00:00:00Z",
        "identity-worker",
        "2026-01-01T00:00:01Z",
        "2026-01-01T00:01:00Z",
        4,
        package_id=package_id,
        package_version=package_version,
        package_digest=package_digest,
        correlation_path_id=plan.correlation_path_id,
        correlation_path_digest=path_digest,
        runtime_version=runtime_version,
        backend_version=backend_version,
    )
    _check(not first.replayed and first.run_status == "completed", "initial identified run completes")
    var stored = journal.get_run_record("identity-run")
    _check(stored.package_id == package_id, "stored package_id")
    _check(stored.package_version == package_version, "stored package_version")
    _check(stored.package_digest == package_digest, "stored package_digest")
    _check(stored.correlation_path_id == plan.correlation_path_id, "stored correlation_path_id")
    _check(stored.correlation_path_digest == path_digest, "stored correlation_path_digest")
    _check(stored.runtime_version == runtime_version, "stored runtime_version")
    _check(stored.backend_version == backend_version, "stored backend_version")

    var matched_replay = run_correlation_path(
        journal,
        "identity-run",
        plan,
        bindings,
        registry,
        "2026-01-01T00:00:00Z",
        "identity-worker",
        "2026-01-01T00:00:01Z",
        "2026-01-01T00:01:00Z",
        4,
        package_id=package_id,
        package_version=package_version,
        package_digest=package_digest,
        correlation_path_id=plan.correlation_path_id,
        correlation_path_digest=path_digest,
        runtime_version=runtime_version,
        backend_version=backend_version,
    )
    _check(matched_replay.replayed and matched_replay.run_status == "completed", "matching terminal replay")
    _check(matched_replay.drive_result.ticks == 0, "matching replay does not drive")

    # Mismatched package digest must fail closed before drive/finalize.
    var mismatch_rejected = False
    var mismatch_detail = ""
    try:
        var mismatched = run_correlation_path(
            journal,
            "identity-run",
            plan,
            bindings,
            registry,
            "2026-01-01T00:00:00Z",
            "identity-worker",
            "2026-01-01T00:00:01Z",
            "2026-01-01T00:01:00Z",
            4,
            package_id=package_id,
            package_version=package_version,
            package_digest="pkg-digest-other",
            correlation_path_id=plan.correlation_path_id,
            correlation_path_digest=path_digest,
            runtime_version=runtime_version,
            backend_version=backend_version,
        )
        _ = mismatched
    except err:
        mismatch_rejected = True
        mismatch_detail = String(err)
    _check(mismatch_rejected and mismatch_detail.find("durable run identity mismatch") >= 0, "package digest mismatch rejected")

    # Incomplete requested identity is fail-closed.
    var incomplete_rejected = False
    var incomplete_detail = ""
    try:
        var incomplete = run_correlation_path(
            journal,
            "identity-run",
            plan,
            bindings,
            registry,
            "2026-01-01T00:00:00Z",
            "identity-worker",
            "2026-01-01T00:00:01Z",
            "2026-01-01T00:01:00Z",
            4,
            package_id=package_id,
            package_version=package_version,
            package_digest=package_digest,
            correlation_path_id=plan.correlation_path_id,
            correlation_path_digest="",
            runtime_version=runtime_version,
            backend_version=backend_version,
        )
        _ = incomplete
    except err:
        incomplete_rejected = True
        incomplete_detail = String(err)
    _check(incomplete_rejected and incomplete_detail.find("requested run identity is incomplete") >= 0, "incomplete identity rejected")

    # Idempotency-key reuse through RunLifecycle must compare identity.
    journal.close()
    var lifecycle = RunLifecycle(path)
    lifecycle.initialize()
    var create_once = lifecycle.create_result(
        "lifecycle-run",
        "2026-01-01T00:00:00Z",
        package_id=package_id,
        package_version=package_version,
        package_digest=package_digest,
        correlation_path_id=plan.correlation_path_id,
        correlation_path_digest=path_digest,
        runtime_version=runtime_version,
        backend_version=backend_version,
    )
    _check(not create_once.replayed, "first lifecycle create is not replayed")
    var create_again = lifecycle.create_result(
        "lifecycle-run",
        "2026-01-01T00:00:00Z",
        package_id=package_id,
        package_version=package_version,
        package_digest=package_digest,
        correlation_path_id=plan.correlation_path_id,
        correlation_path_digest=path_digest,
        runtime_version=runtime_version,
        backend_version=backend_version,
    )
    _check(create_again.replayed, "matching lifecycle create replays")
    var lifecycle_mismatch = False
    var lifecycle_detail = ""
    try:
        var bad = lifecycle.create_result(
            "lifecycle-run",
            "2026-01-01T00:00:00Z",
            package_id=package_id,
            package_version=package_version,
            package_digest="pkg-digest-other",
            correlation_path_id=plan.correlation_path_id,
            correlation_path_digest=path_digest,
            runtime_version=runtime_version,
            backend_version=backend_version,
        )
        _ = bad
    except err:
        lifecycle_mismatch = True
        lifecycle_detail = String(err)
    _check(lifecycle_mismatch and lifecycle_detail.find("run lifecycle identity mismatch") >= 0, "lifecycle identity mismatch rejected")
    lifecycle.close()

    # Non-terminal run-id reuse with mismatched identity fails before drive.
    var journal2 = NativeJournal.open(path)
    journal2.initialize()
    var nonterminal_plan = instantiate_correlation_path(correlation_path, "nonterminal-run")
    _ = journal2.create_run(
        "nonterminal-run",
        "created",
        "{}",
        "2026-01-01T00:00:00Z",
        package_id=package_id,
        package_version=package_version,
        package_digest=package_digest,
        correlation_path_id=nonterminal_plan.correlation_path_id,
        correlation_path_digest=path_digest,
        runtime_version=runtime_version,
        backend_version=backend_version,
    )
    var nonterminal_rejected = False
    var nonterminal_detail = ""
    try:
        var nonterminal = run_correlation_path(
            journal2,
            "nonterminal-run",
            nonterminal_plan,
            _bindings("nonterminal-run", nonterminal_plan.correlation_path_id),
            registry,
            "2026-01-01T00:00:00Z",
            "identity-worker",
            "2026-01-01T00:00:01Z",
            "2026-01-01T00:01:00Z",
            4,
            package_id=package_id,
            package_version=package_version,
            package_digest=package_digest,
            correlation_path_id=nonterminal_plan.correlation_path_id,
            correlation_path_digest="path-digest-other",
            runtime_version=runtime_version,
            backend_version=backend_version,
        )
        _ = nonterminal
    except err:
        nonterminal_rejected = True
        nonterminal_detail = String(err)
    _check(nonterminal_rejected and nonterminal_detail.find("durable run identity mismatch") >= 0, "non-terminal identity mismatch rejected")

    journal2.close()
    _cleanup(path)
    print("run identity replay smoke ok: match mismatch incomplete lifecycle nonterminal")
