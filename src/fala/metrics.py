from __future__ import annotations

from typing import Iterable

from fala.models import (
    RuntimeCapabilityDemand,
    RuntimeCapabilityDemandSummary,
    RuntimeQueueMetrics,
    RuntimeWorkerDemand,
)


def render_prometheus_metrics(
    queue_metrics: RuntimeQueueMetrics,
    capability_demands: RuntimeCapabilityDemandSummary | None = None,
) -> str:
    lines: list[str] = [
        "# HELP fala_runtime_queue_queued_total Queued process instances.",
        "# TYPE fala_runtime_queue_queued_total gauge",
        _sample(
            "fala_runtime_queue_queued_total",
            {"run_id": queue_metrics.run_id},
            queue_metrics.queued_count,
        ),
        "# HELP fala_runtime_queue_running_total Running process instances.",
        "# TYPE fala_runtime_queue_running_total gauge",
        _sample(
            "fala_runtime_queue_running_total",
            {"run_id": queue_metrics.run_id},
            queue_metrics.running_count,
        ),
        "# HELP fala_runtime_worker_deficit_total Missing healthy workers for current demand.",
        "# TYPE fala_runtime_worker_deficit_total gauge",
        _sample(
            "fala_runtime_worker_deficit_total",
            {"run_id": queue_metrics.run_id},
            queue_metrics.worker_deficit_count,
        ),
        "# HELP fala_runtime_process_claimable_queued Claimable queued instances per process.",
        "# TYPE fala_runtime_process_claimable_queued gauge",
    ]
    for demand in queue_metrics.worker_demands:
        lines.extend(_worker_demand_samples(queue_metrics.run_id, demand))

    if capability_demands is not None:
        lines.extend(
            [
                "# HELP fala_runtime_capability_target_workers Target worker count per capability.",
                "# TYPE fala_runtime_capability_target_workers gauge",
            ]
        )
        for demand in capability_demands.demands:
            lines.extend(_capability_demand_samples(capability_demands.run_id, demand))

    return "\n".join(lines) + "\n"


def _worker_demand_samples(
    run_id: str,
    demand: RuntimeWorkerDemand,
) -> Iterable[str]:
    base_labels = {
        "run_id": run_id,
        "pipeline_id": demand.pipeline_id or "",
        "process_id": demand.process_id,
        "capability": demand.capability or "",
        "operation_type": demand.operation_type or "",
        "adapter_kind": demand.adapter_kind or "",
        "resource_pool": demand.resource_pool,
    }
    yield _sample(
        "fala_runtime_process_claimable_queued",
        base_labels,
        demand.claimable_queued_count,
    )
    yield _sample(
        "fala_runtime_process_target_workers",
        base_labels,
        demand.target_worker_count,
    )
    yield _sample(
        "fala_runtime_process_healthy_workers",
        base_labels,
        demand.healthy_worker_count,
    )
    yield _sample(
        "fala_runtime_process_worker_deficit",
        base_labels,
        demand.worker_deficit_count,
    )
    for worker_id in demand.package_worker_ids:
        worker_labels = {**base_labels, "package_worker_id": worker_id}
        yield _sample(
            "fala_runtime_worker_target_workers",
            worker_labels,
            demand.target_worker_count,
        )
        yield _sample(
            "fala_runtime_worker_deficit",
            worker_labels,
            demand.worker_deficit_count,
        )


def _capability_demand_samples(
    run_id: str,
    demand: RuntimeCapabilityDemand,
) -> Iterable[str]:
    labels = {
        "run_id": run_id,
        "capability": demand.capability or "",
        "operation_type": demand.operation_type or "",
        "adapter_kind": demand.adapter_kind or "",
        "resource_pool": demand.resource_pool,
    }
    yield _sample(
        "fala_runtime_capability_target_workers",
        labels,
        demand.target_worker_count,
    )
    yield _sample(
        "fala_runtime_capability_claimable_queued",
        labels,
        demand.claimable_queued_count,
    )
    yield _sample(
        "fala_runtime_capability_worker_deficit",
        labels,
        demand.worker_deficit_count,
    )


def _sample(name: str, labels: dict[str, str], value: int | float) -> str:
    encoded = ",".join(
        f'{key}="{_escape_label(value)}"'
        for key, value in sorted(labels.items())
    )
    return f"{name}{{{encoded}}} {value}"


def _escape_label(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')
