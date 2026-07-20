from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from fala.runtime_backend import Impulse, Association, Projection

SIGNALS_DOMAIN_PACK_ID = "signals"
SIGNAL_METRIC_SAMPLE = "metric_sample"
SIGNAL_THRESHOLD_READING = "threshold_reading"


class SignalMetricSample(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    name: str
    value: float
    unit: str | None = None
    values: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


def impulse_from_metric_sample(sample: SignalMetricSample, *, run_id: str) -> Impulse:
    return Impulse(
        id=sample.id,
        run_id=run_id,
        impulse_type=SIGNAL_METRIC_SAMPLE,
        payload={
            "name": sample.name,
            "value": sample.value,
            "unit": sample.unit,
            "values": sample.values,
        },
        metadata={
            **sample.metadata,
            "domain_pack": SIGNALS_DOMAIN_PACK_ID,
        },
    )


def metric_sample_from_impulse(impulse: Impulse) -> SignalMetricSample:
    if impulse.impulse_type != SIGNAL_METRIC_SAMPLE:
        raise ValueError(f"Impulse {impulse.id!r} is not a signal metric sample")
    return SignalMetricSample(
        id=impulse.id,
        name=str(impulse.payload["name"]),
        value=float(impulse.payload["value"]),
        unit=_optional_str(impulse.payload.get("unit")),
        values=_dict(impulse.payload.get("values")),
        metadata={
            key: value
            for key, value in impulse.metadata.items()
            if key != "domain_pack"
        },
    )


def threshold_association(
    impulse: Impulse,
    *,
    warning_threshold: float = 70,
    critical_threshold: float = 90,
) -> Association:
    sample = metric_sample_from_impulse(impulse)
    if sample.value >= critical_threshold:
        state = "critical"
        threshold = critical_threshold
    elif sample.value >= warning_threshold:
        state = "warning"
        threshold = warning_threshold
    else:
        state = "normal"
        threshold = warning_threshold
    return Association(
        run_id=impulse.run_id,
        impulse_id=impulse.id,
        kind=SIGNAL_THRESHOLD_READING,
        values={
            "name": sample.name,
            "value": sample.value,
            "unit": sample.unit,
            "state": state,
            "threshold": threshold,
        },
        metadata={"domain_pack": SIGNALS_DOMAIN_PACK_ID},
    )


def signal_projection(impulse: Impulse, association: Association | None = None) -> Projection:
    sample = metric_sample_from_impulse(impulse)
    data: dict[str, Any] = {
        "impulse_id": impulse.id,
        "name": sample.name,
        "value": sample.value,
        "unit": sample.unit,
        "values": sample.values,
    }
    if association is not None:
        data.update(
            {
                "state": association.values.get("state"),
                "threshold": association.values.get("threshold"),
            }
        )
    return Projection(
        run_id=impulse.run_id,
        name=f"signal:{impulse.id}",
        data=data,
        source_event_sequence=0,
    )


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _dict(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


__all__ = [
    "SIGNAL_METRIC_SAMPLE",
    "SIGNAL_THRESHOLD_READING",
    "SIGNALS_DOMAIN_PACK_ID",
    "SignalMetricSample",
    "impulse_from_metric_sample",
    "metric_sample_from_impulse",
    "signal_projection",
    "threshold_association",
]
