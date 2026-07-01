from __future__ import annotations

from fala.sdk import needs, output, run_manifest_step


def run(manifest):
    ingest = needs(manifest).get("ingest", {})
    source = str(ingest.get("source", "unknown"))
    return output(values={"source": source, "label": source.upper()})


if __name__ == "__main__":
    raise SystemExit(run_manifest_step(run))
