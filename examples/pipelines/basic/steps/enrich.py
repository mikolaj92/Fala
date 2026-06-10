from __future__ import annotations

from fala.sdk import emit_event, needs, output, run_stdio


def run(context):
    emit_event("process.progress", status="running", data={"stage": "enrich"})
    ingest = needs(context).get("ingest", {})
    source = str(ingest.get("source", "unknown"))
    return output(values={"source": source, "label": source.upper()})


if __name__ == "__main__":
    raise SystemExit(run_stdio(run))
