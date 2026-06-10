from __future__ import annotations

from fala.sdk import emit_event, initial, output, run_stdio


def run(context):
    emit_event("process.progress", status="running", data={"stage": "ingest"})
    source = initial(context).get("source", "unknown")
    return output(values={"source": source, "chars": len(str(source))})


if __name__ == "__main__":
    raise SystemExit(run_stdio(run))
