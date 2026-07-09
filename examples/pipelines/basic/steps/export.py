from __future__ import annotations

from fala.sdk import needs, output, run_manifest_step


def run(manifest):
    enrich = needs(manifest).get("enrich", {})
    return output(values={"status": "ok", "label": enrich.get("label")})


if __name__ == "__main__":
    raise SystemExit(run_manifest_step(run))
