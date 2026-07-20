from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

from fala.runtime import AutonomousCorrelator
from fala.runtime_backend import Run
from fala.domain_packs.splot import (
    SplotArbitrationCase,
    impulse_from_case,
    case_projection,
    jurisdiction_association,
    review_homeostat,
)


async def main(db_path: Path) -> dict:
    runtime = AutonomousCorrelator.sqlite(db_path)
    await runtime.create_run(
        Run(id="run_splot", title="Splot arbitration example"),
        idempotency_key="run_splot:create",
    )
    case = SplotArbitrationCase(
        id="splot_case_1",
        claim_id="SP-1",
        claimant="Alice",
        respondent="Beta LLC",
        amount=1200,
        currency="EUR",
        rules="splot-fast-track",
        reactions=[
            {
                "id": "statement",
                "kind": "claim_statement",
                "uri": "file:///tmp/statement.pdf",
            }
        ],
    )
    impulse = impulse_from_case(case, run_id="run_splot")
    await runtime.accept_impulse(
        impulse,
        idempotency_key="run_splot:impulse.accept:splot_case_1",
    )
    await runtime.record_association(
        jurisdiction_association(
            impulse,
            admissible=True,
            reason="contract clause present",
        ),
        idempotency_key="run_splot:association.jurisdiction:splot_case_1",
    )
    homeostat, _ = await runtime.open_homeostat(
        review_homeostat(impulse),
        idempotency_key="run_splot:homeostat.review:splot_case_1",
    )
    await runtime.complete_homeostat(
        run_id=impulse.run_id,
        homeostat_id=homeostat.id,
        values={"decision": "approved"},
        idempotency_key="run_splot:homeostat.review.complete:splot_case_1",
    )
    await runtime.save_projection(
        case_projection(impulse),
        idempotency_key="run_splot:projection.case:splot_case_1",
    )
    events = await runtime.list_events(run_id="run_splot")
    return {
        "db": str(db_path),
        "impulse_type": impulse.impulse_type,
        "event_types": [event.event_type for event in events],
    }


if __name__ == "__main__":
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("splot.sqlite")
    print(json.dumps(asyncio.run(main(path)), indent=2, sort_keys=True))
