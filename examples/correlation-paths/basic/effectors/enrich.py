from __future__ import annotations

from fala.sdk import conduction, output, run_manifest_effector


def run(manifest):
    ingest = conduction(manifest).get("ingest", {})
    source = str(ingest.get("source", "unknown"))
    return output(values={"source": source, "label": source.upper()})


if __name__ == "__main__":
    raise SystemExit(run_manifest_effector(run))
