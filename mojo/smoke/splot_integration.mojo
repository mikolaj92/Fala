"""Fala hosts Splot 0.3+ Mojo via subprocess (no Python Splot)."""

from std.collections import List
from std.os import getenv, remove
from std.pathlib import Path, cwd
from fala.adapters import AdapterSpec, EffectorRequest, execute_subprocess


def _check(ok: Bool, msg: String) raises:
    if not ok:
        raise Error("splot integration smoke: " + msg)


def _splot_root() raises -> String:
    var env = getenv("SPLOT_ROOT")
    if env.byte_length() > 0:
        return env
    # Default: sibling checkout next to Fala.
    # When run via mojo_sql_run, cwd is vendor/sqlite.fire.
    var here = cwd().__fspath__()
    if here.find("vendor/sqlite.fire") >= 0:
        return here + "/../../../Splot"
    return here + "/../Splot"


def _fala_pixi_prefix() raises -> String:
    # Prefer live pixi env (smoke runs under `pixi run`).
    var conda = getenv("CONDA_PREFIX")
    if conda.byte_length() > 0:
        return conda
    var here = cwd().__fspath__()
    if here.find("vendor/sqlite.fire") >= 0:
        return here + "/../../.pixi/envs/default"
    return here + "/.pixi/envs/default"


def _cleanup(root: String):
    # Remove files; also remove a mistaken *file* named input/output from older runs.
    for name in [
        "input/manifest.json",
        "input/request.json",
        "output/result.json",
        "output/stdout.txt",
        "output/stderr.txt",
        "input",
        "output",
    ]:
        try:
            remove(root + "/" + name)
        except err:
            pass


def main() raises:
    var splot = _splot_root()
    var step = splot + "/tools/splot_step.sh"
    _check(Path(step).exists(), "Splot step script missing at " + step + " (set SPLOT_ROOT)")

    var profile = splot + "/examples/fixtures/player_camera_director.profile.toml"
    _check(Path(profile).exists(), "Splot profile fixture missing at " + profile)

    # Absolute profile so Splot cwd does not matter for the profile path.
    var request = (
        "{"
        + "\"profile\":\""
        + profile
        + "\","
        + "\"candidates\":["
        + "{\"id\":\"cam_a\",\"payload\":{\"visibility\":0.95,\"face_angle\":0.8,\"sharpness\":0.7,\"occlusion\":0.1,\"available\":true}},"
        + "{\"id\":\"cam_b\",\"payload\":{\"visibility\":0.70,\"face_angle\":0.5,\"sharpness\":0.6,\"occlusion\":0.3,\"available\":true}},"
        + "{\"id\":\"cam_offline\",\"payload\":{\"visibility\":0.99,\"available\":false}}"
        + "],"
        + "\"now\":\"2026-01-01T12:00:00Z\""
        + "}"
    )

    var work = "/tmp/fala-splot-integration-work"
    _cleanup(work)

    var pixi = _fala_pixi_prefix()
    var pixi_bin = pixi + "/bin"
    var home = getenv("HOME")
    var tmpdir = getenv("TMPDIR")
    if tmpdir.byte_length() == 0:
        tmpdir = "/tmp"

    var command = List[String]()
    command.append(step)
    var adapter = AdapterSpec.subprocess(command)
    # Fala process host uses a sanitized env; pass what Mojo + the shell need.
    adapter.env["PATH"] = pixi_bin + ":/usr/bin:/bin"
    adapter.env["HOME"] = home
    adapter.env["TMPDIR"] = tmpdir
    adapter.env["CONDA_PREFIX"] = pixi
    adapter.env["MODULAR_HOME"] = pixi + "/share/max"
    adapter.env["FALA_PIXI_ENV"] = pixi_bin
    adapter.env["SPLOT_ROOT"] = splot

    # Fala effector contract: input_json lands in the manifest; Splot step_main
    # reads FALA_EFFECTOR_MANIFEST → input (or input/request.json).
    var req = EffectorRequest(
        "splot_integration",
        adapter,
        "impulse-splot",
        request,
        "{}",
        work,
    )
    var result = execute_subprocess(req)
    _check(
        result.success,
        "subprocess succeeded: " + result.error.message + " stderr=" + result.stderr,
    )
    _check(result.output_json.find("\"status\":\"selected\"") >= 0, "decision selected")
    _check(
        result.output_json.find("\"selected_candidate_id\":\"cam_a\"") >= 0,
        "best live camera wins",
    )
    _check(result.output_json.find("decision_report") < 0, "no report product from Splot 0.3")

    print("splot integration smoke ok: Fala host → Splot Mojo step")
