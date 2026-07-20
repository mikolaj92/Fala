"""Structural smoke: multi-organ example tree + dual domain vocabularies."""

from std.pathlib import Path
from fala.domain_packs.signals import (
    SignalReading,
    impulse_from_reading,
    regulation_decision,
)
from fala.domain_packs.splot import SPLOT_DOMAIN_PACK_ID


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("multi-organ example smoke: " + message)


def main() raises:
    var root = Path("examples/multi-organ")
    # CWD when run via pixi may be vendor/sqlite.fire — resolve from repo markers.
    if not (root / "README.md").exists():
        root = Path("../../examples/multi-organ")
    _check((root / "README.md").exists(), "README present")
    _check((root / "fala-package.toml").exists(), "package present")
    _check((root / "request.json").exists(), "request present")
    var readme = (root / "README.md").read_text()
    _check(readme.find("separate journal") >= 0 or readme.find("own journal") >= 0, "README stresses separate journals")
    var reading = SignalReading("r1", "temp_c", 22.5, "C", "2026-01-01T00:00:00Z")
    var impulse = impulse_from_reading("run-demo", reading, "2026-01-01T00:00:00Z")
    _check(impulse.impulse_type == "signals.reading", "signals impulse")
    _check(regulation_decision(22.5, 20.0) == "complete", "EV may skip operator gate")
    _check(SPLOT_DOMAIN_PACK_ID == "splot", "splot pack available for organ handoff")
    print("multi-organ example smoke ok: package + signals + splot vocabulary")
