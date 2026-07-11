from __future__ import annotations

from fala.sdk import conduction, output, run_manifest_effector


def run(manifest):
    enrich = conduction(manifest).get("enrich", {})
    return output(values={"status": "ok", "label": enrich.get("label")})


if __name__ == "__main__":
    raise SystemExit(run_manifest_effector(run))
