"""Essential-variable regulation path (non-manual decision helper)."""

from fala.domain_packs.signals import (
    SignalReading,
    evaluate_essential_variable,
    regulation_decision,
    threshold_homeostat,
    impulse_from_reading,
)


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("homeostat regulation smoke: " + message)


def main() raises:
    _check(evaluate_essential_variable(10.0, 5.0, "gte"), "10 >= 5")
    _check(not evaluate_essential_variable(3.0, 5.0, "gte"), "3 < 5")
    _check(evaluate_essential_variable(3.0, 5.0, "lte"), "3 <= 5")
    _check(regulation_decision(10.0, 5.0) == "complete", "complete when EV ok")
    _check(regulation_decision(1.0, 5.0) == "wait", "wait when EV not ok")

    var reading = SignalReading("r1", "temp_c", 21.5, "C", "2026-01-01T00:00:00Z")
    var impulse = impulse_from_reading("run-1", reading, "2026-01-01T00:00:00Z")
    _check(impulse.impulse_type == "signals.reading", "impulse type")
    var homeostat = threshold_homeostat(
        "run-1", "h1", impulse.id, "temp_c", 21.5, 20.0, "2026-01-01T00:00:00Z"
    )
    _check(homeostat.kind == "signals.threshold_review", "homeostat kind")
    _check(homeostat.status == "open", "opens for review when mapped")
    # EV already satisfied => regulation would complete without operator
    _check(regulation_decision(21.5, 20.0) == "complete", "threshold met auto-complete intent")
    print("homeostat regulation smoke ok: essential variable gte/lte decisions")
