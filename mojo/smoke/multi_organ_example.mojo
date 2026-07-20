"""Multi-organ example: load real package + dual domain vocabularies."""

from std.collections import List
from std.pathlib import Path
from fala.package import load_package_toml
from fala.domain_packs.signals import (
    SignalReading,
    impulse_from_reading,
    regulation_decision,
)
from fala.domain_packs.splot import SPLOT_DOMAIN_PACK_ID


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("multi-organ example smoke: " + message)


def _package_path() raises -> String:
    var candidates = List[String]()
    candidates.append("examples/multi-organ/fala-package.toml")
    candidates.append("../../examples/multi-organ/fala-package.toml")
    for path in candidates:
        if Path(path).exists():
            return path
    raise Error("multi-organ example smoke: fala-package.toml not found")


def main() raises:
    var package_path = _package_path()
    var package = load_package_toml(package_path)
    _check(package.id == "multi-organ-demo", "package id multi-organ-demo")
    _check(len(package.correlation_paths) >= 1, "at least one correlation path")
    var path = package.correlation_paths[0].copy()
    _check(len(path.effectors) == 3, "three effectors on signal_then_arbitrate")

    var has_native = False
    var has_manual = False
    var has_subprocess = False
    var subprocess_has_child_db = False
    for effector in path.effectors:
        var kind = effector.adapter_kind
        if kind == "native_function":
            has_native = True
            _check(effector.adapter_ref == "signals.ingest", "native_function ref signals.ingest")
        if kind == "manual_homeostat":
            has_manual = True
        if kind == "subprocess":
            has_subprocess = True
            for part in effector.adapter_command:
                if part == "child.sqlite" or part.find("child.sqlite") >= 0:
                    subprocess_has_child_db = True

    _check(has_native, "native_function effector present")
    _check(has_manual, "manual_homeostat effector present")
    _check(has_subprocess, "subprocess effector present")
    _check(subprocess_has_child_db, "subprocess uses separate child.sqlite journal")

    # Conduction edges: gate after ingest, arbitrate after gate
    var gate = path.effectors[1].copy()
    var arbitrate = path.effectors[2].copy()
    _check(len(gate.conduction) >= 1 and gate.conduction[0] == "ingest_signal", "gate conducts from ingest")
    _check(len(arbitrate.conduction) >= 1 and arbitrate.conduction[0] == "threshold_gate", "arbitrate conducts from gate")

    var reading = SignalReading("r1", "temp_c", 22.5, "C", "2026-01-01T00:00:00Z")
    var impulse = impulse_from_reading("run-demo", reading, "2026-01-01T00:00:00Z")
    _check(impulse.impulse_type == "signals.reading", "signals impulse")
    _check(regulation_decision(22.5, 20.0) == "complete", "EV may skip operator gate")
    _check(SPLOT_DOMAIN_PACK_ID == "splot", "splot pack available for organ handoff")

    print(
        "multi-organ example smoke ok: load_package_toml native_function+manual_homeostat+subprocess child.sqlite"
    )
