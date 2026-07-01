from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

from fala.carrier_runtime import FalaRuntime
from fala.runtime_backend import Carrier, Gate, GateStatus, Observation, Projection, Run


async def main(db_path: Path) -> dict:
    runtime = FalaRuntime.sqlite(db_path)
    await runtime.create_run(
        Run(id="run_local", title="Local carrier run"),
        idempotency_key="run_local:run.create",
    )

    case = Carrier(
        id="carrier_case_1",
        run_id="run_local",
        carrier_type="arbitration_case",
        payload={"claim_id": "CLM-1", "amount": 1200},
    )
    await runtime.accept_carrier(
        case,
        idempotency_key="run_local:carrier.accept:carrier_case_1",
    )
    await runtime.record_observation(
        Observation(
            run_id="run_local",
            carrier_id=case.id,
            kind="case.score",
            values={"score": 0.98},
        ),
        idempotency_key="run_local:observation.case_score:carrier_case_1",
    )
    await runtime.save_gate(
        Gate(
            id="gate_case_review",
            run_id="run_local",
            carrier_id=case.id,
            kind="human.review",
            status=GateStatus.completed,
        ),
        idempotency_key="run_local:gate.case_review:carrier_case_1",
    )
    await runtime.save_projection(
        Projection(
            id="projection_case_summary",
            run_id="run_local",
            name="case_summary",
            data={"case_count": 1, "last_carrier_id": case.id},
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
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("carrier-runtime.sqlite")
    print(json.dumps(asyncio.run(main(path)), indent=2, sort_keys=True))
