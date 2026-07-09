from __future__ import annotations

from fala.sdk import input_values, output, run_manifest_step


def run(manifest):
    source = input_values(manifest).get("source", "unknown")
    return output(values={"source": source, "chars": len(str(source))})


if __name__ == "__main__":
    raise SystemExit(run_manifest_step(run))
