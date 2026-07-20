from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

from fala.runtime import AutonomousCorrelator
from fala.runtime_backend import Impulse, Homeostat, HomeostatStatus, Association, Projection, Run


async def main(db_path: Path) -> dict:
    runtime = AutonomousCorrelator.sqlite(db_path)
    await runtime.create_run(
        Run(id="run_local", title="Local impulse run"),
        idempotency_key="run_local:run.create",
    )

    case = Impulse(
        id="impulse_case_1",
        run_id="run_local",
        impulse_type="arbitration_case",
        payload={"claim_id": "CLM-1", "amount": 1200},
    )
    await runtime.accept_impulse(
        case,
        idempotency_key="run_local:impulse.accept:impulse_case_1",
    )
    await runtime.record_association(
        Association(
            run_id="run_local",
            impulse_id=case.id,
            kind="case.score",
            values={"score": 0.98},
        ),
        idempotency_key="run_local:association.case_score:impulse_case_1",
    )
    await runtime.save_homeostat(
        Homeostat(
            id="homeostat_case_review",
            run_id="run_local",
            impulse_id=case.id,
            kind="human.review",
            status=HomeostatStatus.completed,
        ),
        idempotency_key="run_local:homeostat.case_review:impulse_case_1",
    )
    await runtime.save_projection(
        Projection(
            id="projection_case_summary",
            run_id="run_local",
            name="case_summary",
            data={"case_count": 1, "last_impulse_id": case.id},
            source_event_sequence=1,
        ),
        idempotency_key="run_local:projection.case_summary",
    )

    events = await runtime.list_events(run_id="run_local")
    return {
        "db": str(db_path),
        "event_types": [event.event_type for event in events],
        "event_count": len(events),
    }


if __name__ == "__main__":
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("runtime.sqlite")
    print(json.dumps(asyncio.run(main(path)), indent=2, sort_keys=True))
