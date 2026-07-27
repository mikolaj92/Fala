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
    # Secret long enough to be redacted (>= 6 bytes); ambient PATH excluded.
    var env = Dict[String, String]()
    env["SECRET"] = "top-secret"
    env["PATH"] = "/usr/bin:/bin"
    env["LMPROVIDER_TIMEOUT"] = "300"  # short: must NOT be treated as secret

    # Multi-byte Polish + CJK around an ASCII secret (stream redaction path).
    # Include ambient PATH text in the stream so we prove it is NOT scrubbed.
    var stream = "przed top-secret żółć ąęść héllo 世界 path=/usr/bin:/bin po"
    var redacted = redact_environment(stream, env)
    _check(redacted.find("top-secret") < 0, "secret removed from multi-byte stream")
    _check(redacted.find("<redacted>") >= 0, "redaction marker present")
    _check(redacted.find("żółć") >= 0, "Polish text preserved")
    _check(redacted.find("世界") >= 0, "CJK text preserved")
    _check(redacted.find("/usr/bin:/bin") >= 0, "PATH value not scrubbed as secret")

    # Short ambient numbers must not rewrite ordinary text (#121 model tighten).
    var numbers = "timeout=300 tokens=4096"
    var numbers_out = redact_environment(numbers, env)
    _check(numbers_out.find("300") >= 0, "short timeout value not redacted")
    _check(numbers_out.find("<redacted>") < 0, "no redaction of short ambient values")

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

    print("utf8 adapter boundaries ok: redact multi-byte streams + short-value policy")
