from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

from fala.carrier_runtime import FalaRuntime
from fala.domain_packs.splot import (
    SplotArbitrationCase,
    carrier_from_case,
    case_projection,
    jurisdiction_observation,
    review_gate,
)
from fala.runtime_backend import GateStatus


async def main(db_path: Path) -> dict:
    runtime = FalaRuntime.sqlite(db_path)
    case = SplotArbitrationCase(
        id="splot_case_1",
        claim_id="SP-1",
        claimant="Alice",
        respondent="Beta LLC",
        amount=1200,
        currency="EUR",
        rules="splot-fast-track",
        artifacts=[
            {
                "id": "statement",
                "kind": "claim_statement",
                "uri": "file:///tmp/statement.pdf",
            }
        ],
    )
    carrier = carrier_from_case(case, run_id="run_splot")
    await runtime.accept_carrier(
        carrier,
        idempotency_key="run_splot:carrier.accept:splot_case_1",
    )
    await runtime.record_observation(
        jurisdiction_observation(
            carrier,
            admissible=True,
            reason="contract clause present",
        ),
        idempotency_key="run_splot:observation.jurisdiction:splot_case_1",
    )
    await runtime.save_gate(
        review_gate(carrier, status=GateStatus.completed),
        idempotency_key="run_splot:gate.review:splot_case_1",
    )
    await runtime.save_projection(
        case_projection(carrier),
        idempotency_key="run_splot:projection.case:splot_case_1",
    )
    events = await runtime.list_events(run_id="run_splot")
    return {
        "db": str(db_path),
        "carrier_type": carrier.carrier_type,
        "event_types": [event.event_type for event in events],
    }


if __name__ == "__main__":
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("splot.sqlite")
    print(json.dumps(asyncio.run(main(path)), indent=2, sort_keys=True))
