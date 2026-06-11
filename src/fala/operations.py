from __future__ import annotations

from collections.abc import Iterable

from fala.models import OperationTypeSpec


_OPERATION_TYPE_CATALOG: dict[str, dict[str, str]] = {
    "ingest": {
        "title": "Ingest",
        "category": "intake",
        "description": "Acquire and register source document data.",
    },
    "parse": {
        "title": "Parse",
        "category": "extraction",
        "description": "Parse document structure or container parts.",
    },
    "extract": {
        "title": "Extract",
        "category": "extraction",
        "description": "Extract text, fields, pages, attachments, or other content.",
    },
    "split": {
        "title": "Split",
        "category": "transformation",
        "description": "Split documents into pages, chunks, records, or segments.",
    },
    "normalize": {
        "title": "Normalize",
        "category": "transformation",
        "description": "Normalize document content into a canonical shape.",
    },
    "transform": {
        "title": "Transform",
        "category": "transformation",
        "description": "Transform document content or artifacts.",
    },
    "enrich": {
        "title": "Enrich",
        "category": "enrichment",
        "description": "Add derived metadata, labels, links, or context.",
    },
    "embed": {
        "title": "Embed",
        "category": "indexing",
        "description": "Generate vector or search representations.",
    },
    "retrieve": {
        "title": "Retrieve",
        "category": "indexing",
        "description": "Retrieve context, matches, or candidate records.",
    },
    "classify": {
        "title": "Classify",
        "category": "analysis",
        "description": "Classify document, part, or artifact content.",
    },
    "detect": {
        "title": "Detect",
        "category": "analysis",
        "description": "Detect sensitive data, entities, defects, or signals.",
    },
    "route": {
        "title": "Route",
        "category": "orchestration",
        "description": "Route documents, parts, or records into downstream workflows.",
    },
    "analyze": {
        "title": "Analyze",
        "category": "analysis",
        "description": "Analyze document structure, quality, profile, or metrics.",
    },
    "plan": {
        "title": "Plan",
        "category": "generation",
        "description": "Plan generated content, media, or downstream work.",
    },
    "generate": {
        "title": "Generate",
        "category": "generation",
        "description": "Generate text, scripts, images, audio, video, or assets.",
    },
    "render": {
        "title": "Render",
        "category": "generation",
        "description": "Render generated assets into final media.",
    },
    "assemble": {
        "title": "Assemble",
        "category": "transformation",
        "description": "Assemble child documents, parts, or artifacts.",
    },
    "validate": {
        "title": "Validate",
        "category": "quality",
        "description": "Validate extracted, generated, or transformed content.",
    },
    "redact": {
        "title": "Redact",
        "category": "transformation",
        "description": "Redact, anonymize, mask, or remove sensitive content.",
    },
    "translate": {
        "title": "Translate",
        "category": "transformation",
        "description": "Translate or localize document content.",
    },
    "aggregate": {
        "title": "Aggregate",
        "category": "analysis",
        "description": "Aggregate records, chunks, signals, or document-level results.",
    },
    "review": {
        "title": "Review",
        "category": "quality",
        "description": "Human or automated review gate.",
    },
    "index": {
        "title": "Index",
        "category": "indexing",
        "description": "Write content into an index, database, or knowledge store.",
    },
    "export": {
        "title": "Export",
        "category": "delivery",
        "description": "Export final records, documents, media, or reports.",
    },
}

_STEP_OPERATION_RULES: tuple[tuple[str, str], ...] = (
    ("ingest", "ingest"),
    ("parse", "parse"),
    ("extract", "extract"),
    ("split", "split"),
    ("chunk", "split"),
    ("normalize", "normalize"),
    ("enrich", "enrich"),
    ("embed", "embed"),
    ("retrieve", "retrieve"),
    ("classify", "classify"),
    ("detect", "detect"),
    ("route", "route"),
    ("profile", "analyze"),
    ("analyze", "analyze"),
    ("plan", "plan"),
    ("generate", "generate"),
    ("render", "render"),
    ("assemble", "assemble"),
    ("validate", "validate"),
    ("redact", "redact"),
    ("translate", "translate"),
    ("aggregate", "aggregate"),
    ("review", "review"),
    ("index", "index"),
    ("export", "export"),
)


def operation_type_for_step(step_id: str) -> str:
    normalized = step_id.lower().replace("-", "_")
    for token, operation_type in _STEP_OPERATION_RULES:
        if token in normalized:
            return operation_type
    return "transform"


def operation_type_spec(operation_type: str) -> OperationTypeSpec:
    data = _OPERATION_TYPE_CATALOG.get(operation_type)
    if data is None:
        return OperationTypeSpec(
            id=operation_type,
            title=operation_type.replace("_", " ").title(),
        )
    return OperationTypeSpec(id=operation_type, **data)


def operation_type_specs(operation_types: Iterable[str]) -> list[OperationTypeSpec]:
    seen: set[str] = set()
    specs: list[OperationTypeSpec] = []
    for operation_type in operation_types:
        if operation_type in seen:
            continue
        seen.add(operation_type)
        specs.append(operation_type_spec(operation_type))
    return specs
