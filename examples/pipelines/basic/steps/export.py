from __future__ import annotations

from fala.sdk import emit_event, needs, output, run_stdio


def run(context):
    emit_event("process.progress", status="running", data={"stage": "export"})
    enrich = needs(context).get("enrich", {})
    return output(values={"status": "ok", "label": enrich.get("label")})


if __name__ == "__main__":
    raise SystemExit(run_stdio(run))
