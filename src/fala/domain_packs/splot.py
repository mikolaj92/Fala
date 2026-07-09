from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from fala.runtime_backend import Carrier, Gate, GateStatus, Observation, Projection

SPLOT_DOMAIN_PACK_ID = "splot"
SPLOT_ARBITRATION_CASE = "splot.arbitration_case"


class SplotArbitrationCase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    claim_id: str
    claimant: str
    respondent: str
    amount: float | None = Field(default=None, ge=0)
    currency: str | None = None
    rules: str | None = None
    values: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[dict[str, Any]] = Field(default_factory=list)


SPLOT_PROCESS_SEMANTICS = {
    "intake": "accept arbitration case carrier and source artifacts",
    "jurisdiction": "record jurisdiction and admissibility observations",
    "triage": "open or complete human review gates",
    "award_projection": "maintain case summary projection for operators",
}


def carrier_from_case(case: SplotArbitrationCase, *, run_id: str) -> Carrier:
    return Carrier(
        id=case.id,
        run_id=run_id,
        carrier_type=SPLOT_ARBITRATION_CASE,
        payload={
            "claim_id": case.claim_id,
            "claimant": case.claimant,
            "respondent": case.respondent,
            "amount": case.amount,
            "currency": case.currency,
            "rules": case.rules,
            "values": case.values,
            "artifacts": case.artifacts,
        },
        metadata={
            **case.metadata,
            "domain_pack": SPLOT_DOMAIN_PACK_ID,
        },
    )


def case_from_carrier(carrier: Carrier) -> SplotArbitrationCase:
    if carrier.carrier_type != SPLOT_ARBITRATION_CASE:
        raise ValueError(f"Carrier {carrier.id!r} is not a Splot arbitration case")
    return SplotArbitrationCase(
        id=carrier.id,
        claim_id=str(carrier.payload["claim_id"]),
        claimant=str(carrier.payload["claimant"]),
        respondent=str(carrier.payload["respondent"]),
        amount=_optional_float(carrier.payload.get("amount")),
        currency=_optional_str(carrier.payload.get("currency")),
        rules=_optional_str(carrier.payload.get("rules")),
        values=_dict(carrier.payload.get("values")),
        artifacts=_list_of_dicts(carrier.payload.get("artifacts")),
        metadata={
            key: value
            for key, value in carrier.metadata.items()
            if key != "domain_pack"
        },
    )


def jurisdiction_observation(
    carrier: Carrier,
    *,
    admissible: bool,
    reason: str | None = None,
) -> Observation:
    case = case_from_carrier(carrier)
    return Observation(
        run_id=carrier.run_id,
        carrier_id=carrier.id,
        kind="splot.jurisdiction",
        values={
            "claim_id": case.claim_id,
            "admissible": admissible,
            "reason": reason,
        },
        metadata={"domain_pack": SPLOT_DOMAIN_PACK_ID},
    )


def review_gate(
    carrier: Carrier,
    *,
    status: GateStatus = GateStatus.open,
) -> Gate:
    case = case_from_carrier(carrier)
    return Gate(
        id=f"splot_review:{case.claim_id}",
        run_id=carrier.run_id,
        carrier_id=carrier.id,
        kind="splot.review",
        status=status,
        values={"claim_id": case.claim_id},
        metadata={"domain_pack": SPLOT_DOMAIN_PACK_ID},
    )


def case_projection(carrier: Carrier) -> Projection:
    case = case_from_carrier(carrier)
    return Projection(
        run_id=carrier.run_id,
        name=f"splot.case:{case.claim_id}",
        data={
            "carrier_id": carrier.id,
            "claim_id": case.claim_id,
            "claimant": case.claimant,
            "respondent": case.respondent,
            "amount": case.amount,
            "currency": case.currency,
            "rules": case.rules,
            "artifact_count": len(case.artifacts),
        },
        source_event_sequence=0,
    )


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _optional_float(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) else None


def _dict(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _list_of_dicts(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


__all__ = [
    "SPLOT_ARBITRATION_CASE",
    "SPLOT_DOMAIN_PACK_ID",
    "SPLOT_PROCESS_SEMANTICS",
    "SplotArbitrationCase",
    "carrier_from_case",
    "case_from_carrier",
    "case_projection",
    "jurisdiction_observation",
    "review_gate",
]
