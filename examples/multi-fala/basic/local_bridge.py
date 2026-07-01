from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

from fala.runtime_backend import (
    BridgeDelivery,
    Carrier,
    EventRef,
    Run,
    RunRef,
    RuntimeBackendService,
    RuntimeBudget,
    RuntimeRef,
)


async def main(root: Path) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    source_db = root / "source.sqlite"
    target_db = root / "target.sqlite"
    source = RuntimeBackendService.sqlite(source_db)
    target = RuntimeBackendService.sqlite(target_db)

    source_ref = RuntimeRef(id="source", uri=f"sqlite://{source_db}")
    target_ref = RuntimeRef(id="target", uri=f"sqlite://{target_db}")
    source_run = Run(id="run_source", title="Source runtime")
    target_run = Run(id="run_target", title="Target runtime")
    await source.create_run(source_run, idempotency_key="run_source:create")
    await target.create_run(target_run, idempotency_key="run_target:create")

    carrier = Carrier(
        id="carrier_case_1",
        run_id=source_run.id,
        carrier_type="case",
        payload={"claim_id": "CLM-1"},
    )
    await source.accept_carrier(
        carrier,
        idempotency_key="run_source:carrier.accept:carrier_case_1",
    )
    source_events = await source.backend.list_events(run_id=source_run.id)

    delivery = BridgeDelivery(
        id="bridge_case_1",
        run_id=source_run.id,
        idempotency_key="run_source:bridge.case_1",
        source=RunRef(runtime=source_ref, run_id=source_run.id),
        target=RunRef(runtime=target_ref, run_id=target_run.id),
        carrier=carrier,
        event_ref=EventRef(
            runtime=source_ref,
            run_id=source_run.id,
            event_id=source_events[-1].id,
            sequence=source_events[-1].sequence,
        ),
        budget=RuntimeBudget(runtime_hops=1, carrier_count=1, attempts=2),
    )
    await source.enqueue_bridge_delivery(delivery)
    delivered, imported, _, _ = await source.deliver_bridge_delivery(
        run_id=source_run.id,
        delivery_id=delivery.id,
        target=target,
        idempotency_key="run_source:bridge.deliver:bridge_case_1",
    )
    target_carrier = await target.backend.get_carrier(
        run_id=target_run.id,
        carrier_id=carrier.id,
    )

    return {
        "source_db": str(source_db),
        "target_db": str(target_db),
        "delivered_status": delivered.status.value,
        "imported_status": imported.status.value,
        "target_carrier": target_carrier.model_dump(mode="json")
        if target_carrier is not None
        else None,
    }


if __name__ == "__main__":
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".fala/multi-fala-basic")
    print(json.dumps(asyncio.run(main(root)), indent=2, sort_keys=True))
