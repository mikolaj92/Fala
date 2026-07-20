from fala.package import load_package_toml
from fala.toml import parse_toml_json
from std.pathlib import Path


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("toml smoke: " + message)


def _expect_error(text: String, needle: String) raises:
    var matched = False
    try:
        _ = parse_toml_json(text, "<smoke>")
    except err:
        matched = String(err).find(needle) >= 0
    _check(matched, "diagnostic contains '" + needle + "'")


def main() raises:
    var text = """
id = "pkg"
version = "2"
tags = ["one", "two"]

[[correlation_paths]]
id = "path"
[[correlation_paths.effectors]]
id = "eff-one"
adapter = { kind = "manual_homeostat" }
[[correlation_paths.effectors]]
id = "eff-two"
adapter = { kind = "manual_homeostat" }
"""
    var json = parse_toml_json(text, "<manifest>")
    _check(json.find("eff-one") >= 0 and json.find("eff-two") >= 0, "repeated nested effector routing")
    _check(json.find("\"id\":\"pkg\"") >= 0, "canonical root id")
    _check(json.find("\"manual_homeostat\"") >= 0, "nested inline adapter")
    var path = "/tmp/fala-toml-manifest-smoke.toml"
    Path(path).write_text(text)
    var manifest = load_package_toml(path)
    _check(manifest.id == "pkg" and len(manifest.correlation_paths) == 1 and len(manifest.correlation_paths[0].effectors) == 2, "validated package manifest and repeated effectors")

    _expect_error("a = 1\na = 2\n", "duplicate key")
    _expect_error("a = 2026-01-01\n", "date/time values are unsupported")
    _expect_error("a = \"\"\"multi\nline\"\"\"\n", "multiline strings are unsupported")
    _expect_error("a = { b = 1,\n c = 2 }\n", "expected ',' or '}' in inline table")
    _expect_error("a = inf\n", "special float values are unsupported")
    print("toml smoke ok: parser canonical package validation malformed unsupported")
