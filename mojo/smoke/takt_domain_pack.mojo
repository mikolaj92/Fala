"""Takt domain pack smoke: pure mapping + durable domain_store path."""

from std.os import remove
from fala.domain import Homeostat, Impulse
from fala.domain_packs.takt import (
    TAKT_ACTUATION,
    TAKT_CASCADE_REQUEST,
    TAKT_DOMAIN_PACK_ID,
    TAKT_ERROR_SIGNAL,
    TAKT_PLANT_LAYER,
    TAKT_SAFETY_INTERLOCK,
    TaktCascadeRequest,
    cascade_from_impulse,
    cascade_projection,
    error_signal_association,
    impulse_from_cascade,
    plant_layer_association,
    process_semantics_json,
    safety_interlock_homeostat,
)
from fala.domain_store import NativeDomainStore
from fala.journal import NativeJournal
from fala.package import load_package_toml
from std.pathlib import Path


def _check(ok: Bool, msg: String) raises:
    if not ok:
        raise Error("takt domain pack smoke: " + msg)


def _clean(path: String):
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


def main() raises:
    var plant = (
        "[{\"id\":\"hunk:0\",\"value\":0.8,\"has_children\":false,\"layer\":0,\"kind\":\"hunk\"}]"
    )
    var layers = "[{\"layer\":0,\"tolerance\":0.1,\"min_confidence\":0.5,\"entropy_threshold\":0.35}]"
    var request = TaktCascadeRequest(
        "takt_req_1",
        "evaluate",
        plant,
        layers,
        "[]",
        0,
        "{}",
    )
    _check(request.is_valid(), "request valid")
    var impulse = impulse_from_cascade(request, "run_takt", "2026-01-01T00:00:00Z")
    _check(impulse.impulse_type == String(TAKT_CASCADE_REQUEST), "impulse type")
    _check(impulse.payload.find("hunk:0") >= 0 and impulse.payload.find("evaluate") >= 0, "payload fields")
    _check(impulse.metadata.find(String(TAKT_DOMAIN_PACK_ID)) >= 0, "domain_pack metadata")
    var back = cascade_from_impulse(impulse)
    _check(back.mode == "evaluate" and back.plant_nodes_json.find("hunk:0") >= 0, "cascade_from_impulse round-trip")

    var layer = plant_layer_association(impulse, 0, 0.1, 0.5, 0.35, "", "2026-01-01T00:00:01Z")
    _check(layer.kind == String(TAKT_PLANT_LAYER) and layer.values.find("\"layer\":0") >= 0, "plant layer")

    var err_sig = error_signal_association(
        impulse, "hunk:0", 0.8, 0.8, 0.3, "fallback", "", "2026-01-01T00:00:02Z"
    )
    _check(err_sig.kind == String(TAKT_ERROR_SIGNAL) and err_sig.values.find("aberration") >= 0, "error signal")

    var interlock = safety_interlock_homeostat(
        impulse, "hunk:0", "residual entropy high", "open", "2026-01-01T00:00:03Z"
    )
    _check(
        interlock.kind == String(TAKT_SAFETY_INTERLOCK)
        and interlock.id.find("takt.interlock:") >= 0,
        "safety interlock homeostat",
    )

    var projection = cascade_projection(impulse, "actuation", "hunk:0", "", "2026-01-01T00:00:04Z")
    _check(
        projection.name == "takt.cascade:takt_req_1" and projection.data.find("actuation") >= 0,
        "cascade projection",
    )
    _check(process_semantics_json().find("cascade") >= 0, "process semantics")
    _check(String(TAKT_ACTUATION) == "takt.actuation", "actuation reaction kind constant")

    var bad = False
    try:
        _ = cascade_from_impulse(
            Impulse("x", "run_takt", "other.type", "{}", "{}", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
        )
    except err:
        bad = True
    _check(bad, "non-takt impulse rejected")

    # Durable path
    var path = "/tmp/fala-takt-domain-pack.sqlite"
    _clean(path)
    var journal = NativeJournal.open(path)
    journal.initialize()
    _ = journal.create_run("run_takt", "active", "{}", "2026-01-01T00:00:00Z", "Takt cascade example")
    journal.close()

    var store = NativeDomainStore.open(path)
    store.initialize()
    var accepted = store.accept_impulse(
        impulse,
        "run_takt:impulse.accept:takt_req_1",
        "2026-01-01T00:00:00Z",
        "takt-smoke",
    )
    _check(not accepted.replayed, "impulse accepted")
    var stored_impulse = store.get_impulse("run_takt", "takt_req_1")
    _check(stored_impulse.impulse_type == String(TAKT_CASCADE_REQUEST), "stored impulse type")

    _ = store.record_association(
        layer,
        "run_takt:association.layer:takt_req_1",
        "association.record",
        "run_takt:association.layer:takt_req_1",
        "2026-01-01T00:00:01Z",
    )
    _ = store.record_association(
        err_sig,
        "run_takt:association.error:takt_req_1",
        "association.record",
        "run_takt:association.error:takt_req_1",
        "2026-01-01T00:00:02Z",
    )
    _ = store.save_homeostat(
        interlock,
        "run_takt:homeostat.interlock:takt_req_1",
        "homeostat.open",
        "run_takt:homeostat.interlock:takt_req_1",
        "2026-01-01T00:00:03Z",
    )
    var open_h = store.get_homeostat("run_takt", interlock.id)
    _check(open_h.status == "open", "homeostat open")

    var completed = Homeostat(
        id=interlock.id,
        run_id=interlock.run_id,
        kind=interlock.kind,
        impulse_id=interlock.impulse_id,
        status="completed",
        values=interlock.values,
        metadata=interlock.metadata,
        attempt=interlock.attempt,
        max_attempts=interlock.max_attempts,
        created_at=interlock.created_at,
        updated_at="2026-01-01T00:00:05Z",
    )
    _ = store.transition_homeostat(
        completed,
        "run_takt:homeostat.interlock.complete:takt_req_1",
        "homeostat.complete",
        "run_takt:homeostat.interlock.complete:takt_req_1",
        "2026-01-01T00:00:05Z",
    )
    _ = store.save_projection(
        projection,
        "run_takt:projection.cascade:takt_req_1",
        "projection.save",
        "run_takt:projection.cascade:takt_req_1",
        "2026-01-01T00:00:04Z",
    )
    var stored_proj = store.get_projection("run_takt", projection.name)
    _check(stored_proj.name == "takt.cascade:takt_req_1", "projection durable")
    store.close()
    _clean(path)

    # Vocabulary package loads
    var pkg_path = "examples/domain-packs/takt/fala-package.toml"
    if not Path(pkg_path).exists():
        pkg_path = "../../examples/domain-packs/takt/fala-package.toml"
    var manifest = load_package_toml(pkg_path)
    _check(manifest.id == "takt_cascade_basic", "package id")
    _check(len(manifest.correlation_paths) == 1, "one correlation path")
    var cpath = manifest.correlation_paths[0].copy()
    _check(len(cpath.effectors) >= 1, "package has effectors")
    var first = cpath.effectors[0].copy()
    _check(
        first.adapter_kind == "subprocess" or first.adapter_kind == "manual_homeostat",
        "first effector is subprocess or manual_homeostat",
    )

    print("takt domain pack smoke ok: pure map durable package")
