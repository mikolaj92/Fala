from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from fala.runtime_backend import Impulse, Homeostat, HomeostatStatus, Association, Projection

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
    reactions: list[dict[str, Any]] = Field(default_factory=list)


SPLOT_PROCESS_SEMANTICS = {
    "intake": "accept arbitration case impulse and source reactions",
    "jurisdiction": "record jurisdiction and admissibility associations",
    "triage": "open or complete human review homeostats",
    "award_projection": "maintain case summary projection for operators",
}


def impulse_from_case(case: SplotArbitrationCase, *, run_id: str) -> Impulse:
    return Impulse(
        id=case.id,
        run_id=run_id,
        impulse_type=SPLOT_ARBITRATION_CASE,
        payload={
            "claim_id": case.claim_id,
            "claimant": case.claimant,
            "respondent": case.respondent,
            "amount": case.amount,
            "currency": case.currency,
            "rules": case.rules,
            "values": case.values,
            "reactions": case.reactions,
        },
        metadata={
            **case.metadata,
            "domain_pack": SPLOT_DOMAIN_PACK_ID,
        },
    )


def case_from_impulse(impulse: Impulse) -> SplotArbitrationCase:
    if impulse.impulse_type != SPLOT_ARBITRATION_CASE:
        raise ValueError(f"Impulse {impulse.id!r} is not a Splot arbitration case")
    return SplotArbitrationCase(
        id=impulse.id,
        claim_id=str(impulse.payload["claim_id"]),
        claimant=str(impulse.payload["claimant"]),
        respondent=str(impulse.payload["respondent"]),
        amount=_optional_float(impulse.payload.get("amount")),
        currency=_optional_str(impulse.payload.get("currency")),
        rules=_optional_str(impulse.payload.get("rules")),
        values=_dict(impulse.payload.get("values")),
        reactions=_list_of_dicts(impulse.payload.get("reactions")),
        metadata={
            key: value
            for key, value in impulse.metadata.items()
            if key != "domain_pack"
        },
    )


def jurisdiction_association(
    impulse: Impulse,
    *,
    admissible: bool,
    reason: str | None = None,
) -> Association:
    case = case_from_impulse(impulse)
    return Association(
        run_id=impulse.run_id,
        impulse_id=impulse.id,
        kind="splot.jurisdiction",
        values={
            "claim_id": case.claim_id,
            "admissible": admissible,
            "reason": reason,
        },
        metadata={"domain_pack": SPLOT_DOMAIN_PACK_ID},
    )


def review_homeostat(
    impulse: Impulse,
    *,
    status: HomeostatStatus = HomeostatStatus.open,
) -> Homeostat:
    case = case_from_impulse(impulse)
    return Homeostat(
        id=f"splot_review:{case.claim_id}",
        run_id=impulse.run_id,
        impulse_id=impulse.id,
        kind="splot.review",
        status=status,
        values={"claim_id": case.claim_id},
        metadata={"domain_pack": SPLOT_DOMAIN_PACK_ID},
    )


def case_projection(impulse: Impulse) -> Projection:
    case = case_from_impulse(impulse)
    return Projection(
        run_id=impulse.run_id,
        name=f"splot.case:{case.claim_id}",
        data={
            "impulse_id": impulse.id,
            "claim_id": case.claim_id,
            "claimant": case.claimant,
            "respondent": case.respondent,
            "amount": case.amount,
            "currency": case.currency,
            "rules": case.rules,
            "reaction_count": len(case.reactions),
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
    "impulse_from_case",
    "case_from_impulse",
    "case_projection",
    "jurisdiction_association",
    "review_homeostat",
]
