"""Splot domain pack smoke: pure mapping + durable domain_store path."""

from std.os import remove
from fala.domain import Homeostat, Impulse
from fala.domain_packs.splot import (
    SPLOT_ARBITRATION_CASE,
    SPLOT_DOMAIN_PACK_ID,
    SPLOT_JURISDICTION,
    SPLOT_REVIEW,
    SplotArbitrationCase,
    case_from_impulse,
    case_projection,
    impulse_from_case,
    jurisdiction_association,
    process_semantics_json,
    review_homeostat,
)
from fala.domain_store import NativeDomainStore
from fala.journal import NativeJournal
from fala.package import load_package_toml


def _check(ok: Bool, msg: String) raises:
    if not ok:
        raise Error("splot domain pack smoke: " + msg)


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
    # Pure mapping: case → impulse → case round-trip.
    var arbitration_case = SplotArbitrationCase(
        "splot_case_1",
        "SP-1",
        "Alice",
        "Beta LLC",
        "1200",
        "EUR",
        "splot-fast-track",
        "{}",
        "{}",
        "[{\"id\":\"statement\",\"kind\":\"claim_statement\",\"uri\":\"file:///tmp/statement.pdf\"}]",
    )
    _check(arbitration_case.is_valid(), "case valid")
    var impulse = impulse_from_case(arbitration_case, "run_splot", "2026-01-01T00:00:00Z")
    _check(impulse.impulse_type == String(SPLOT_ARBITRATION_CASE), "impulse type")
    _check(impulse.payload.find("SP-1") >= 0 and impulse.payload.find("Alice") >= 0, "payload fields")
    _check(impulse.metadata.find(String(SPLOT_DOMAIN_PACK_ID)) >= 0, "domain_pack metadata")
    var back = case_from_impulse(impulse)
    _check(
        back.claim_id == "SP-1"
        and back.claimant == "Alice"
        and back.respondent == "Beta LLC"
        and back.currency == "EUR",
        "case_from_impulse round-trip",
    )
    var assoc = jurisdiction_association(
        impulse, True, "contract clause present", "", "2026-01-01T00:00:01Z"
    )
    _check(assoc.kind == String(SPLOT_JURISDICTION) and assoc.values.find("\"admissible\":true") >= 0, "jurisdiction association")
    var homeostat = review_homeostat(impulse, "open", "2026-01-01T00:00:02Z")
    _check(homeostat.id == "splot_review:SP-1" and homeostat.kind == String(SPLOT_REVIEW), "review homeostat")
    var projection = case_projection(impulse, "", "2026-01-01T00:00:03Z")
    _check(
        projection.name == "splot.case:SP-1"
        and projection.data.find("Alice") >= 0
        and projection.data.find("\"reaction_count\":1") >= 0,
        "case projection",
    )
    _check(process_semantics_json().find("intake") >= 0, "process semantics present")

    # Reject non-splot impulse.
    var bad = False
    try:
        _ = case_from_impulse(
            Impulse(
                "x", "run_splot", "other.type", "{}", "{}", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"
            )
        )
    except err:
        bad = True
    _check(bad, "non-splot impulse rejected")

    # Durable path: journal run + domain_store accept/association/homeostat/projection.
    var path = "/tmp/fala-splot-domain-pack.sqlite"
    _clean(path)
    var journal = NativeJournal.open(path)
    journal.initialize()
    _ = journal.create_run("run_splot", "active", "{}", "2026-01-01T00:00:00Z", "Splot arbitration example")
    journal.close()

    var store = NativeDomainStore.open(path)
    store.initialize()
    var accepted = store.accept_impulse(
        impulse,
        "run_splot:impulse.accept:splot_case_1",
        "2026-01-01T00:00:00Z",
        "splot-smoke",
    )
    _check(not accepted.replayed, "impulse accepted")
    var stored_impulse = store.get_impulse("run_splot", "splot_case_1")
    _check(stored_impulse.impulse_type == String(SPLOT_ARBITRATION_CASE), "stored impulse type")

    _ = store.record_association(
        assoc,
        "run_splot:association.jurisdiction:splot_case_1",
        "association.record",
        "run_splot:association.jurisdiction:splot_case_1",
        "2026-01-01T00:00:01Z",
    )
    var stored_assoc = store.get_association("run_splot", assoc.id)
    _check(stored_assoc.kind == String(SPLOT_JURISDICTION), "association durable")

    _ = store.save_homeostat(
        homeostat,
        "run_splot:homeostat.review:splot_case_1",
        "homeostat.open",
        "run_splot:homeostat.review:splot_case_1",
        "2026-01-01T00:00:02Z",
    )
    var open_h = store.get_homeostat("run_splot", homeostat.id)
    _check(open_h.status == "open", "homeostat open")

    var completed = Homeostat(
        id=homeostat.id,
        run_id=homeostat.run_id,
        kind=homeostat.kind,
        impulse_id=homeostat.impulse_id,
        status="completed",
        values="{\"claim_id\":\"SP-1\",\"decision\":\"approved\"}",
        metadata=homeostat.metadata,
        attempt=homeostat.attempt,
        max_attempts=homeostat.max_attempts,
        created_at=homeostat.created_at,
        updated_at="2026-01-01T00:00:03Z",
    )
    var done = store.transition_homeostat(
        completed,
        "run_splot:homeostat.review.complete:splot_case_1",
        "homeostat.complete",
        "run_splot:homeostat.review.complete:splot_case_1",
        "2026-01-01T00:00:03Z",
    )
    _check(done.homeostat.status == "completed" and not done.submission.replayed, "homeostat completed")

    _ = store.save_projection(
        projection,
        "run_splot:projection.case:splot_case_1",
        "projection.save",
        "run_splot:projection.case:splot_case_1",
        "2026-01-01T00:00:04Z",
    )
    var proj = store.get_projection("run_splot", projection.name)
    _check(proj.data.find("Beta LLC") >= 0, "projection durable")

    # Package manifest loads as native TOML (manual_homeostat review path).
    var manifest_path = "../../examples/domain-packs/splot/fala-package.toml"
    var manifest = load_package_toml(manifest_path)
    _check(manifest.id == "splot_arbitration_basic", "package id")
    _check(len(manifest.correlation_paths) == 1, "one correlation path")
    _check(
        manifest.correlation_paths[0].effectors[0].adapter_kind == "manual_homeostat",
        "manual_homeostat review effector",
    )

    store.close()
    _clean(path)
    print("splot domain pack smoke ok: pure map durable package")
