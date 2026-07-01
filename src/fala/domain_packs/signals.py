from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from fala.runtime_backend import Carrier, Observation, Projection

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


def carrier_from_metric_sample(sample: SignalMetricSample, *, run_id: str) -> Carrier:
    return Carrier(
        id=sample.id,
        run_id=run_id,
        carrier_type=SIGNAL_METRIC_SAMPLE,
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


def metric_sample_from_carrier(carrier: Carrier) -> SignalMetricSample:
    if carrier.carrier_type != SIGNAL_METRIC_SAMPLE:
        raise ValueError(f"Carrier {carrier.id!r} is not a signal metric sample")
    return SignalMetricSample(
        id=carrier.id,
        name=str(carrier.payload["name"]),
        value=float(carrier.payload["value"]),
        unit=_optional_str(carrier.payload.get("unit")),
        values=_dict(carrier.payload.get("values")),
        metadata={
            key: value
            for key, value in carrier.metadata.items()
            if key != "domain_pack"
        },
    )


def threshold_observation(
    carrier: Carrier,
    *,
    warning_threshold: float = 70,
    critical_threshold: float = 90,
) -> Observation:
    sample = metric_sample_from_carrier(carrier)
    if sample.value >= critical_threshold:
        state = "critical"
        threshold = critical_threshold
    elif sample.value >= warning_threshold:
        state = "warning"
        threshold = warning_threshold
    else:
        state = "normal"
        threshold = warning_threshold
    return Observation(
        run_id=carrier.run_id,
        carrier_id=carrier.id,
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


def signal_projection(carrier: Carrier, observation: Observation | None = None) -> Projection:
    sample = metric_sample_from_carrier(carrier)
    data: dict[str, Any] = {
        "carrier_id": carrier.id,
        "name": sample.name,
        "value": sample.value,
        "unit": sample.unit,
        "values": sample.values,
    }
    if observation is not None:
        data.update(
            {
                "state": observation.values.get("state"),
                "threshold": observation.values.get("threshold"),
            }
        )
    return Projection(
        run_id=carrier.run_id,
        name=f"signal:{carrier.id}",
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
    "carrier_from_metric_sample",
    "metric_sample_from_carrier",
    "signal_projection",
    "threshold_observation",
]
