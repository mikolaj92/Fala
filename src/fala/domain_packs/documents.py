from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from fala.runtime_backend import Impulse, Association, Projection

DOCUMENT_DOMAIN_PACK_ID = "documents"


class DocumentImpulseInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    document_type: str = "generic_document"
    title: str | None = None
    relation: str | None = None
    media_type: str | None = None
    source_uri: str | None = None
    values: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    reactions: list[dict[str, Any]] = Field(default_factory=list)


def impulse_from_document(
    document: DocumentImpulseInput,
    *,
    run_id: str,
) -> Impulse:
    return Impulse(
        id=document.id,
        run_id=run_id,
        impulse_type=f"document.{document.document_type}",
        payload={
            "document_type": document.document_type,
            "title": document.title,
            "relation": document.relation,
            "media_type": document.media_type,
            "source_uri": document.source_uri,
            "values": document.values,
            "reactions": document.reactions,
        },
        metadata={
            **document.metadata,
            "domain_pack": DOCUMENT_DOMAIN_PACK_ID,
        },
    )


def document_from_impulse(impulse: Impulse) -> DocumentImpulseInput:
    if impulse.metadata.get("domain_pack") != DOCUMENT_DOMAIN_PACK_ID:
        raise ValueError(f"Impulse {impulse.id!r} is not a document domain impulse")
    document_type = str(
        impulse.payload.get("document_type")
        or impulse.impulse_type.removeprefix("document.")
        or "generic_document"
    )
    return DocumentImpulseInput(
        id=impulse.id,
        document_type=document_type,
        title=_optional_str(impulse.payload.get("title")),
        relation=_optional_str(impulse.payload.get("relation")),
        media_type=_optional_str(impulse.payload.get("media_type")),
        source_uri=_optional_str(impulse.payload.get("source_uri")),
        values=_dict(impulse.payload.get("values")),
        reactions=_list_of_dicts(impulse.payload.get("reactions")),
        metadata={
            key: value
            for key, value in impulse.metadata.items()
            if key != "domain_pack"
        },
    )


def document_association(impulse: Impulse) -> Association:
    document = document_from_impulse(impulse)
    return Association(
        run_id=impulse.run_id,
        impulse_id=impulse.id,
        kind="document.accepted",
        values={
            "document_type": document.document_type,
            "media_type": document.media_type,
            "source_uri": document.source_uri,
            "reaction_count": len(document.reactions),
        },
        metadata={"domain_pack": DOCUMENT_DOMAIN_PACK_ID},
    )


def document_projection(impulse: Impulse) -> Projection:
    document = document_from_impulse(impulse)
    return Projection(
        run_id=impulse.run_id,
        name=f"document:{impulse.id}",
        data={
            "impulse_id": impulse.id,
            "document_type": document.document_type,
            "title": document.title,
            "relation": document.relation,
            "media_type": document.media_type,
            "source_uri": document.source_uri,
            "values": document.values,
            "reaction_count": len(document.reactions),
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
    "DocumentImpulseInput",
    "impulse_from_document",
    "document_from_impulse",
    "document_association",
    "document_projection",
]
