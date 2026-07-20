"""Public native package boundary.

Package manifests may be authored as TOML and are normalized through the
existing validated canonical JSON model. Strict JSON loading remains available
for callers that already have canonical package JSON. No Python or YAML path is
used.
"""

from std.pathlib import Path
from fala.native_package import (
    PackageManifestError,
    PackageEffector,
    PackageCorrelationPath,
    PackageManifest,
    load_package_json as _native_load_package_json,
    validate_package_json_text,
)
from fala.toml import parse_toml_json

# Public name used by native callers while the concrete manifest type remains
# available for callers that need its explicit schema name.
comptime NativePackage = PackageManifest


def _ensure_json_object(path: String) raises:
    var text = Path(path).read_text()
    var index = 0
    while index < text.byte_length():
        var c = String(text[byte=index])
        if c != " " and c != "\t" and c != "\n" and c != "\r":
            if c != "{":
                raise Error("unsupported package format at " + path + ": native loader accepts strict JSON only; use TOML for authored manifests")
            return
        index += 1
    raise Error("unsupported package format at " + path + ": empty input is not a JSON object")


def load_package_json(path: String) raises -> PackageManifest:
    """Load strict JSON after rejecting non-object input explicitly."""
    _ensure_json_object(path)
    return _native_load_package_json(path)

def load_package_toml(path: String) raises -> PackageManifest:
    """Load a TOML manifest through canonical JSON validation."""
    var text = Path(path).read_text()
    return validate_package_json_text(parse_toml_json(text, path), path)

def load_fala_package_json(path: String) raises -> NativePackage:
    return load_package_json(path)

def load_fala_package_toml(path: String) raises -> NativePackage:
    return load_package_toml(path)


def main():
    # Library module; direct build is a compile smoke only.
    pass
