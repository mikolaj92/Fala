"""Protective regressions for UTF-8 / StringSlice boundaries in adapter helpers.

Fala#121: Mojo aborts when String slices land mid-codepoint. These pure unit
checks cover ``redact_environment`` without a process host — run them whenever
touching string walks in adapters or native_driver.

```bash
mise exec -- pixi run utf8-adapter-boundaries
```
"""

from std.collections import Dict
from fala import redact_environment


def _check(condition: Bool, message: String) raises:
    if not condition: raise Error("utf8 adapter boundaries: " + message)


def main() raises:
    # Explicit adapter values are redacted regardless of length; ambient keys excluded.
    var env = Dict[String, String]()
    env["SECRET"] = "top-secret"
    env["PATH"] = "/usr/bin:/bin"
    env["TZ"] = "300"  # base environment value: ordinary stream text remains intact

    # Multi-byte Polish + CJK around an ASCII secret (stream redaction path).
    # Include ambient PATH text in the stream so we prove it is NOT scrubbed.
    var stream = "przed top-secret żółć ąęść héllo 世界 path=/usr/bin:/bin po"
    var redacted = redact_environment(stream, env)
    _check(redacted.find("top-secret") < 0, "secret removed from multi-byte stream")
    _check(redacted.find("<redacted>") >= 0, "redaction marker present")
    _check(redacted.find("żółć") >= 0, "Polish text preserved")
    _check(redacted.find("世界") >= 0, "CJK text preserved")
    _check(redacted.find("/usr/bin:/bin") >= 0, "PATH value not scrubbed as secret")

    # Nonempty short secrets must be scrubbed from operator-facing streams.
    env["SHORT_SECRET"] = "shrt"
    var short_stream = "stdout shrt / stderr shrt"
    var short_out = redact_environment(short_stream, env)
    _check(short_out.find("shrt") < 0, "short secret removed")

    _check(short_out.find("<redacted>") >= 0, "short secret marker present")
    env["EMPTY_SECRET"] = ""
    var empty_value_out = redact_environment("unchanged", env)
    _check(empty_value_out == "unchanged", "empty secret leaves output intact")

    # Ambient values must not rewrite ordinary text.
    var numbers = "timeout=300 tokens=4096"
    var numbers_out = redact_environment(numbers, env)
    _check(numbers_out.find("300") >= 0, "ambient timeout value not redacted")
    _check(numbers_out.find("<redacted>") < 0, "no redaction of ambient values")

    # Empty / no secrets: identity (and must not abort on multi-byte alone).
    var empty_env = Dict[String, String]()
    var only_unicode = "tylko żółć i 世界"
    var identity = redact_environment(only_unicode, empty_env)
    _check(identity == only_unicode, "no-secret multi-byte is identity")

    # Secret immediately adjacent to multi-byte codepoints (boundary edges).
    var edge = "żółćtop-secret世界"
    var edge_out = redact_environment(edge, env)
    _check(edge_out.find("top-secret") < 0, "secret redacted at codepoint edge")
    _check(edge_out.find("żółć") >= 0 and edge_out.find("世界") >= 0, "neighbors preserved")

    print("utf8 adapter boundaries ok: redact multi-byte streams + short secrets")
