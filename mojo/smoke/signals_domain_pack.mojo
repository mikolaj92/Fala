"""Signals domain pack smoke."""

from fala.domain_packs.signals import (
    SIGNALS_DOMAIN_PACK_ID,
    SIGNALS_READING,
    SignalReading,
    impulse_from_reading,
    channel_association,
    threshold_homeostat,
    signal_projection,
    regulation_decision,
)


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("signals domain pack smoke: " + message)


def main() raises:
    _check(SIGNALS_DOMAIN_PACK_ID == "signals", "pack id")
    var reading = SignalReading("sig-1", "pressure", 101.3, "kPa", "2026-01-01T00:00:00Z")
    var impulse = impulse_from_reading("run-s", reading, "2026-01-01T00:00:00Z")
    _check(impulse.impulse_type == SIGNALS_READING, "reading impulse type")
    var assoc = channel_association("run-s", "a1", impulse.id, "pressure", "2026-01-01T00:00:00Z")
    _check(assoc.kind == "signals.channel", "channel association")
    var homeostat = threshold_homeostat(
        "run-s", "h1", impulse.id, "pressure", 101.3, 100.0, "2026-01-01T00:00:00Z"
    )
    _check(homeostat.kind == "signals.threshold_review", "threshold homeostat")
    _check(regulation_decision(101.3, 100.0) == "complete", "EV complete")
    var proj = signal_projection("run-s", "channel_pressure", "pressure", 101.3, 1, "2026-01-01T00:00:00Z")
    _check(proj.name == "channel_pressure", "projection name")
    print("signals domain pack smoke ok: pure map vocabulary")
