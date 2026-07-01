from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from fala.runtime_backend import Carrier, Observation, Projection

DOCUMENT_DOMAIN_PACK_ID = "documents"


class DocumentCarrierInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    document_type: str = "generic_document"
    title: str | None = None
    relation: str | None = None
    media_type: str | None = None
    source_uri: str | None = None
    values: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[dict[str, Any]] = Field(default_factory=list)


def carrier_from_document(
    document: DocumentCarrierInput,
    *,
    run_id: str,
) -> Carrier:
    return Carrier(
        id=document.id,
        run_id=run_id,
        carrier_type=f"document.{document.document_type}",
        payload={
            "document_type": document.document_type,
            "title": document.title,
            "relation": document.relation,
            "media_type": document.media_type,
            "source_uri": document.source_uri,
            "values": document.values,
            "artifacts": document.artifacts,
        },
        metadata={
            **document.metadata,
            "domain_pack": DOCUMENT_DOMAIN_PACK_ID,
        },
    )


def document_from_carrier(carrier: Carrier) -> DocumentCarrierInput:
    if carrier.metadata.get("domain_pack") != DOCUMENT_DOMAIN_PACK_ID:
        raise ValueError(f"Carrier {carrier.id!r} is not a document domain carrier")
    document_type = str(
        carrier.payload.get("document_type")
        or carrier.carrier_type.removeprefix("document.")
        or "generic_document"
    )
    return DocumentCarrierInput(
        id=carrier.id,
        document_type=document_type,
        title=_optional_str(carrier.payload.get("title")),
        relation=_optional_str(carrier.payload.get("relation")),
        media_type=_optional_str(carrier.payload.get("media_type")),
        source_uri=_optional_str(carrier.payload.get("source_uri")),
        values=_dict(carrier.payload.get("values")),
        artifacts=_list_of_dicts(carrier.payload.get("artifacts")),
        metadata={
            key: value
            for key, value in carrier.metadata.items()
            if key != "domain_pack"
        },
    )


def document_observation(carrier: Carrier) -> Observation:
    document = document_from_carrier(carrier)
    return Observation(
        run_id=carrier.run_id,
        carrier_id=carrier.id,
        kind="document.accepted",
        values={
            "document_type": document.document_type,
            "media_type": document.media_type,
            "source_uri": document.source_uri,
            "artifact_count": len(document.artifacts),
        },
        metadata={"domain_pack": DOCUMENT_DOMAIN_PACK_ID},
    )


def document_projection(carrier: Carrier) -> Projection:
    document = document_from_carrier(carrier)
    return Projection(
        run_id=carrier.run_id,
        name=f"document:{carrier.id}",
        data={
            "carrier_id": carrier.id,
            "document_type": document.document_type,
            "title": document.title,
            "relation": document.relation,
            "media_type": document.media_type,
            "source_uri": document.source_uri,
            "values": document.values,
            "artifact_count": len(document.artifacts),
        },
        source_event_sequence=0,
    )


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _dict(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _list_of_dicts(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


__all__ = [
    "DOCUMENT_DOMAIN_PACK_ID",
    "DocumentCarrierInput",
    "carrier_from_document",
    "document_from_carrier",
    "document_observation",
    "document_projection",
]
