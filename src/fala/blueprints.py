from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fala.models import (
    DocumentRelationSpec,
    DocumentTypeSpec,
    OperationTypeSpec,
    StreamSpec,
)
from fala.operations import operation_type_for_step, operation_type_specs

__all__ = [
    "SCAFFOLD_BLUEPRINTS",
    "ScaffoldBlueprint",
    "document_source_value_schema",
    "get_scaffold_blueprint",
    "list_scaffold_blueprints",
    "scaffold_blueprint_from_mapping",
    "scaffold_blueprint_summary",
]


@dataclass(frozen=True)
class _ScaffoldBlueprint:
    id: str
    title: str
    document_type: str
    document_media_types: tuple[str, ...]
    steps: tuple[str, ...]
    artifact_kind_by_step: dict[str, str]
    capability_by_step: dict[str, str]
    operation_type_by_step: dict[str, str] | None = None
    document_extensions: tuple[str, ...] = ()
    additional_document_types: tuple[DocumentTypeSpec, ...] = ()
    additional_document_relations: tuple[DocumentRelationSpec, ...] = ()
    operation_types: tuple[OperationTypeSpec, ...] = ()
    document_value_schema: dict[str, Any] | None = None
    document_metadata_schema: dict[str, Any] | None = None
    needs_by_step: dict[str, tuple[str, ...]] | None = None
    accepted_document_types_by_step: dict[str, tuple[str, ...]] | None = None
    emitted_document_types_by_step: dict[str, tuple[str, ...]] | None = None
    artifact_media_types_by_step: dict[str, tuple[str, ...]] | None = None
    artifact_extensions_by_step: dict[str, tuple[str, ...]] | None = None
    artifact_value_schema_by_step: dict[str, dict[str, Any]] | None = None
    capability_output_schema_by_step: dict[str, dict[str, Any]] | None = None
    capability_streams_by_step: dict[str, tuple[StreamSpec, ...]] | None = None
    step_policy_by_step: dict[str, dict[str, Any]] | None = None


def _stream_text_value_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["text"],
        "properties": {"text": {"type": "string"}},
        "additionalProperties": True,
    }


def _document_source_value_schema(
    *,
    extras: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    properties: dict[str, dict[str, Any]] = {
        "source": {"type": "string"},
    }
    properties.update(extras or {})
    return {
        "type": "object",
        "properties": properties,
        "additionalProperties": True,
    }


def _document_metadata_schema(
    *,
    extras: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": extras or {},
        "additionalProperties": True,
    }


def _stream_page_metadata_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {"page_number": {"type": "integer", "minimum": 1}},
        "additionalProperties": True,
    }


def _stream_chunk_metadata_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "chunk_index": {"type": "integer", "minimum": 0},
            "source_page_numbers": {
                "type": "array",
                "items": {"type": "integer", "minimum": 1},
            },
        },
        "additionalProperties": True,
    }


def _stream_part_metadata_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "content_type": {"type": "string"},
            "part_id": {"type": "string"},
        },
        "additionalProperties": True,
    }


def _stream_attachment_value_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["filename"],
        "properties": {
            "filename": {"type": "string"},
            "media_type": {"type": "string"},
            "source_uri": {"type": "string"},
        },
        "additionalProperties": True,
    }


def _stream_package_item_value_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["item_id", "path"],
        "properties": {
            "item_id": {"type": "string"},
            "path": {"type": "string"},
            "media_type": {"type": "string"},
            "document_type": {"type": "string"},
            "source_uri": {"type": "string"},
        },
        "additionalProperties": True,
    }


def _stream_asset_value_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["asset_id", "status"],
        "properties": {
            "asset_id": {"type": "string"},
            "status": {"type": "string"},
            "uri": {"type": "string"},
        },
        "additionalProperties": True,
    }


def _stream_frame_value_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["frame"],
        "properties": {
            "frame": {"type": "integer", "minimum": 0},
            "uri": {"type": "string"},
        },
        "additionalProperties": True,
    }


def _stream_embedding_value_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["embedding_id"],
        "properties": {
            "embedding_id": {"type": "string"},
            "dimension": {"type": "integer", "minimum": 1},
        },
        "additionalProperties": True,
    }


def _stream_retrieval_match_value_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["document_id", "score"],
        "properties": {
            "document_id": {"type": "string"},
            "score": {"type": "number"},
        },
        "additionalProperties": True,
    }


def _stream_field_value_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["field"],
        "properties": {
            "field": {"type": "string"},
            "value": {},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        },
        "additionalProperties": True,
    }


def _stream_sensitive_span_value_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["span_id", "label"],
        "properties": {
            "span_id": {"type": "string"},
            "label": {"type": "string"},
            "text": {"type": "string"},
            "start": {"type": "integer", "minimum": 0},
            "end": {"type": "integer", "minimum": 0},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "page_number": {"type": "integer", "minimum": 1},
        },
        "additionalProperties": True,
    }


def _stream_translation_segment_value_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["segment_id", "text"],
        "properties": {
            "segment_id": {"type": "string"},
            "text": {"type": "string"},
            "translated_text": {"type": "string"},
            "source_language": {"type": "string"},
            "target_language": {"type": "string"},
            "page_number": {"type": "integer", "minimum": 1},
        },
        "additionalProperties": True,
    }


def _stream_row_value_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["row_index", "record"],
        "properties": {
            "row_index": {"type": "integer", "minimum": 0},
            "record": {"type": "object"},
            "source_sheet": {"type": "string"},
        },
        "additionalProperties": True,
    }


def _stream_validation_issue_value_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["row_index", "field", "severity"],
        "properties": {
            "row_index": {"type": "integer", "minimum": 0},
            "field": {"type": "string"},
            "severity": {"type": "string"},
            "message": {"type": "string"},
        },
        "additionalProperties": True,
    }


def _structured_fields_value_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["fields"],
        "properties": {
            "fields": {"type": "object"},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "schema_id": {"type": "string"},
        },
        "additionalProperties": True,
    }


def _validation_report_value_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["status"],
        "properties": {
            "status": {"type": "string"},
            "errors": {"type": "array"},
            "warnings": {"type": "array"},
        },
        "additionalProperties": True,
    }


_SCAFFOLD_BLUEPRINTS: dict[str, _ScaffoldBlueprint] = {
    "document_digitalization": _ScaffoldBlueprint(
        id="document_digitalization",
        title="Document digitalization",
        document_type="generic_document",
        document_media_types=(
            "application/octet-stream",
            "application/pdf",
            "image/*",
            "text/plain",
        ),
        document_extensions=(".pdf", ".png", ".jpg", ".jpeg", ".txt"),
        additional_document_types=(
            DocumentTypeSpec(
                id="page_document",
                title="Page document",
                media_types=("application/pdf", "image/*", "text/plain"),
                extensions=(".pdf", ".png", ".jpg", ".jpeg", ".txt"),
                value_schema=_document_source_value_schema(
                    extras={
                        "page_number": {"type": "integer", "minimum": 1},
                    }
                ),
                metadata_schema=_document_metadata_schema(
                    extras={
                        "parent_document_id": {"type": "string"},
                        "page_number": {"type": "integer", "minimum": 1},
                    }
                ),
            ),
        ),
        additional_document_relations=(
            DocumentRelationSpec(
                id="page",
                title="Page",
                source_document_types=["generic_document"],
                target_document_types=["page_document"],
            ),
        ),
        steps=("ingest", "extract", "normalize", "enrich", "assemble", "export"),
        artifact_kind_by_step={
            "ingest": "source_payload",
            "extract": "extracted_content",
            "normalize": "normalized_document",
            "enrich": "enriched_document",
            "assemble": "assembled_document",
            "export": "exported_document",
        },
        capability_by_step={
            "ingest": "ingest_document",
            "extract": "extract_content",
            "normalize": "normalize_document",
            "enrich": "enrich_document",
            "assemble": "assemble_document",
            "export": "export_document",
        },
        document_value_schema=_document_source_value_schema(),
        document_metadata_schema=_document_metadata_schema(
            extras={
                "source_system": {"type": "string"},
                "collection": {"type": "string"},
                "case_id": {"type": "string"},
            }
        ),
        needs_by_step={
            "extract": ("ingest",),
            "normalize": ("ingest",),
            "enrich": ("normalize",),
            "assemble": ("extract",),
            "export": ("enrich",),
        },
        accepted_document_types_by_step={
            "ingest": ("generic_document", "page_document"),
            "extract": ("generic_document",),
            "normalize": ("page_document",),
            "enrich": ("page_document",),
            "assemble": ("generic_document",),
            "export": ("page_document",),
        },
        emitted_document_types_by_step={
            "extract": ("page_document",),
        },
        artifact_media_types_by_step={
            "ingest": ("application/json", "application/octet-stream"),
            "extract": ("application/json", "text/plain", "text/markdown"),
            "normalize": ("application/json",),
            "enrich": ("application/json",),
            "assemble": ("application/json", "text/markdown"),
            "export": ("application/json", "text/markdown"),
        },
        artifact_extensions_by_step={
            "ingest": (".json", ".bin"),
            "extract": (".json", ".txt", ".md"),
            "normalize": (".json",),
            "enrich": (".json",),
            "assemble": (".json", ".md"),
            "export": (".json", ".md"),
        },
        capability_streams_by_step={
            "extract": (
                StreamSpec(
                    stream_id="pages",
                    kinds=["page"],
                    consumers=["normalize"],
                    max_buffered_chunks=128,
                    value_schema=_stream_text_value_schema(),
                    metadata_schema=_stream_page_metadata_schema(),
                ),
            ),
        },
        step_policy_by_step={
            "extract": {
                "priority": 50,
                "max_concurrency": 4,
                "resource_pool": "ocr_pool",
                "when": {
                    "document_types": ["generic_document"],
                },
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 30,
                    "retry_error_kinds": ["transient_io", "rate_limited"],
                    "terminal_error_kinds": ["validation_error"],
                },
            },
            "enrich": {
                "priority": 20,
                "when": {
                    "document_types": ["page_document"],
                },
                "retry": {
                    "max_attempts": 2,
                    "delay_seconds": 15,
                    "retry_error_kinds": ["transient_io", "rate_limited"],
                },
            },
            "normalize": {
                "when": {
                    "document_types": ["page_document"],
                },
            },
            "assemble": {
                "priority": 10,
                "when": {
                    "document_types": ["generic_document"],
                },
                "wait_for_children": {
                    "from_processes": ["extract"],
                    "document_types": ["page_document"],
                    "relations": ["page"],
                    "min_count": 1,
                },
            },
            "export": {
                "when": {
                    "document_types": ["page_document"],
                },
            },
        },
    ),
    "email_processing": _ScaffoldBlueprint(
        id="email_processing",
        title="Email processing",
        document_type="email_document",
        document_media_types=(
            "message/rfc822",
            "application/vnd.ms-outlook",
            "application/octet-stream",
        ),
        document_extensions=(".eml", ".msg"),
        additional_document_types=(
            DocumentTypeSpec(
                id="email_attachment_document",
                title="Email attachment document",
                media_types=(
                    "application/octet-stream",
                    "application/pdf",
                    "image/*",
                    "text/plain",
                    "text/csv",
                    "application/json",
                ),
                extensions=(
                    ".bin",
                    ".pdf",
                    ".png",
                    ".jpg",
                    ".jpeg",
                    ".txt",
                    ".csv",
                    ".json",
                ),
                value_schema=_document_source_value_schema(
                    extras={
                        "filename": {"type": "string"},
                        "media_type": {"type": "string"},
                    }
                ),
                metadata_schema=_document_metadata_schema(
                    extras={
                        "parent_document_id": {"type": "string"},
                        "message_id": {"type": "string"},
                        "filename": {"type": "string"},
                    }
                ),
            ),
        ),
        additional_document_relations=(
            DocumentRelationSpec(
                id="attachment",
                title="Attachment",
                source_document_types=["email_document"],
                target_document_types=["email_attachment_document"],
            ),
        ),
        steps=(
            "ingest_email",
            "parse_message",
            "extract_attachments",
            "classify",
            "export",
        ),
        artifact_kind_by_step={
            "ingest_email": "email_payload",
            "parse_message": "parsed_message",
            "extract_attachments": "extracted_attachments",
            "classify": "classification",
            "export": "exported_record",
        },
        capability_by_step={
            "ingest_email": "ingest_email",
            "parse_message": "parse_message",
            "extract_attachments": "extract_attachments",
            "classify": "classify_message",
            "export": "export_record",
        },
        document_value_schema=_document_source_value_schema(
            extras={
                "mailbox": {"type": "string"},
                "message_id": {"type": "string"},
            }
        ),
        document_metadata_schema=_document_metadata_schema(
            extras={
                "mailbox": {"type": "string"},
                "account": {"type": "string"},
                "received_at": {"type": "string"},
            }
        ),
        needs_by_step={
            "parse_message": ("ingest_email",),
            "extract_attachments": ("parse_message",),
            "classify": ("ingest_email",),
            "export": ("classify",),
        },
        accepted_document_types_by_step={
            "ingest_email": ("email_document", "email_attachment_document"),
            "parse_message": ("email_document",),
            "extract_attachments": ("email_document",),
            "classify": ("email_document", "email_attachment_document"),
            "export": ("email_document", "email_attachment_document"),
        },
        emitted_document_types_by_step={
            "extract_attachments": ("email_attachment_document",),
        },
        artifact_media_types_by_step={
            "ingest_email": ("application/json", "message/rfc822"),
            "parse_message": ("application/json",),
            "extract_attachments": ("application/json", "application/octet-stream"),
            "classify": ("application/json",),
            "export": ("application/json",),
        },
        artifact_extensions_by_step={
            "ingest_email": (".json", ".eml"),
            "parse_message": (".json",),
            "extract_attachments": (".json", ".bin"),
            "classify": (".json",),
            "export": (".json",),
        },
        capability_streams_by_step={
            "parse_message": (
                StreamSpec(
                    stream_id="parts",
                    kinds=["mime_part"],
                    consumers=["extract_attachments"],
                    max_buffered_chunks=256,
                    value_schema=_stream_text_value_schema(),
                    metadata_schema=_stream_part_metadata_schema(),
                ),
            ),
            "extract_attachments": (
                StreamSpec(
                    stream_id="attachments",
                    kinds=["attachment"],
                    consumers=["classify"],
                    max_buffered_chunks=128,
                    value_schema=_stream_attachment_value_schema(),
                ),
            ),
        },
        step_policy_by_step={
            "parse_message": {
                "priority": 40,
                "max_concurrency": 8,
                "when": {
                    "document_types": ["email_document"],
                },
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 15,
                    "retry_error_kinds": ["transient_io"],
                    "terminal_error_kinds": ["validation_error"],
                },
            },
            "extract_attachments": {
                "priority": 30,
                "max_concurrency": 4,
                "when": {
                    "document_types": ["email_document"],
                },
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 15,
                    "retry_error_kinds": ["transient_io"],
                },
            },
            "classify": {
                "priority": 20,
                "retry": {
                    "max_attempts": 2,
                    "delay_seconds": 30,
                    "retry_error_kinds": ["rate_limited"],
                },
            },
        },
    ),
    "document_package_processing": _ScaffoldBlueprint(
        id="document_package_processing",
        title="Document package processing",
        document_type="document_package",
        document_media_types=(
            "application/zip",
            "application/x-tar",
            "application/gzip",
            "application/octet-stream",
        ),
        document_extensions=(".zip", ".tar", ".tar.gz", ".tgz", ".bin"),
        additional_document_types=(
            DocumentTypeSpec(
                id="packaged_document",
                title="Packaged document",
                media_types=(
                    "application/pdf",
                    "image/*",
                    "text/plain",
                    "text/csv",
                    "application/json",
                    "message/rfc822",
                    "application/octet-stream",
                ),
                extensions=(
                    ".pdf",
                    ".png",
                    ".jpg",
                    ".jpeg",
                    ".txt",
                    ".csv",
                    ".json",
                    ".eml",
                    ".bin",
                ),
                value_schema=_document_source_value_schema(
                    extras={
                        "package_path": {"type": "string"},
                        "item_id": {"type": "string"},
                    }
                ),
                metadata_schema=_document_metadata_schema(
                    extras={
                        "parent_document_id": {"type": "string"},
                        "package_path": {"type": "string"},
                        "item_index": {"type": "integer", "minimum": 0},
                    }
                ),
            ),
        ),
        additional_document_relations=(
            DocumentRelationSpec(
                id="package_item",
                title="Package item",
                source_document_types=["document_package"],
                target_document_types=["packaged_document"],
            ),
        ),
        steps=(
            "ingest_package",
            "inspect_package",
            "extract_items",
            "classify_item",
            "route_item",
            "export_item",
            "export_manifest",
        ),
        artifact_kind_by_step={
            "ingest_package": "package_payload",
            "inspect_package": "package_manifest",
            "extract_items": "extracted_items",
            "classify_item": "item_classification",
            "route_item": "item_route",
            "export_item": "exported_item_record",
            "export_manifest": "exported_package_manifest",
        },
        capability_by_step={
            "ingest_package": "ingest_package",
            "inspect_package": "inspect_package",
            "extract_items": "extract_package_items",
            "classify_item": "classify_package_item",
            "route_item": "route_package_item",
            "export_item": "export_item_record",
            "export_manifest": "export_package_manifest",
        },
        operation_type_by_step={
            "inspect_package": "analyze",
        },
        document_value_schema=_document_source_value_schema(
            extras={
                "package_id": {"type": "string"},
                "expected_item_count": {"type": "integer", "minimum": 0},
                "source_system": {"type": "string"},
            }
        ),
        document_metadata_schema=_document_metadata_schema(
            extras={
                "collection": {"type": "string"},
                "tenant": {"type": "string"},
                "received_at": {"type": "string"},
            }
        ),
        needs_by_step={
            "inspect_package": ("ingest_package",),
            "extract_items": ("inspect_package",),
            "classify_item": ("ingest_package",),
            "route_item": ("classify_item",),
            "export_item": ("route_item",),
            "export_manifest": ("extract_items",),
        },
        accepted_document_types_by_step={
            "ingest_package": ("document_package", "packaged_document"),
            "inspect_package": ("document_package",),
            "extract_items": ("document_package",),
            "classify_item": ("packaged_document",),
            "route_item": ("packaged_document",),
            "export_item": ("packaged_document",),
            "export_manifest": ("document_package",),
        },
        emitted_document_types_by_step={
            "extract_items": ("packaged_document",),
        },
        artifact_media_types_by_step={
            "ingest_package": ("application/json", "application/octet-stream"),
            "inspect_package": ("application/json",),
            "extract_items": ("application/json", "application/octet-stream"),
            "classify_item": ("application/json",),
            "route_item": ("application/json",),
            "export_item": ("application/json",),
            "export_manifest": ("application/json",),
        },
        artifact_extensions_by_step={
            "ingest_package": (".json", ".bin"),
            "inspect_package": (".json",),
            "extract_items": (".json", ".bin"),
            "classify_item": (".json",),
            "route_item": (".json",),
            "export_item": (".json",),
            "export_manifest": (".json",),
        },
        artifact_value_schema_by_step={
            "inspect_package": {
                "type": "object",
                "properties": {
                    "item_count": {"type": "integer", "minimum": 0},
                    "total_bytes": {"type": "integer", "minimum": 0},
                    "items": {"type": "array"},
                },
                "additionalProperties": True,
            },
            "route_item": {
                "type": "object",
                "required": ["route"],
                "properties": {
                    "route": {"type": "string"},
                    "document_type": {"type": "string"},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                },
                "additionalProperties": True,
            },
        },
        capability_streams_by_step={
            "inspect_package": (
                StreamSpec(
                    stream_id="manifest_items",
                    kinds=["package_item"],
                    consumers=["extract_items"],
                    max_buffered_chunks=4096,
                    value_schema=_stream_package_item_value_schema(),
                ),
            ),
            "extract_items": (
                StreamSpec(
                    stream_id="extracted_items",
                    kinds=["package_item"],
                    consumers=["classify_item"],
                    max_buffered_chunks=4096,
                    value_schema=_stream_package_item_value_schema(),
                ),
            ),
        },
        step_policy_by_step={
            "inspect_package": {
                "priority": 50,
                "max_concurrency": 4,
                "resource_pool": "io_pool",
                "when": {
                    "document_types": ["document_package"],
                },
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 15,
                    "retry_error_kinds": ["transient_io"],
                    "terminal_error_kinds": ["validation_error"],
                },
            },
            "extract_items": {
                "priority": 40,
                "max_concurrency": 2,
                "resource_pool": "io_pool",
                "when": {
                    "document_types": ["document_package"],
                },
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 15,
                    "retry_error_kinds": ["transient_io"],
                },
            },
            "classify_item": {
                "priority": 30,
                "max_concurrency": 4,
                "resource_pool": "llm_pool",
                "when": {
                    "document_types": ["packaged_document"],
                },
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 30,
                    "retry_error_kinds": ["transient_io", "rate_limited"],
                },
            },
            "route_item": {
                "priority": 20,
                "when": {
                    "document_types": ["packaged_document"],
                },
            },
            "export_item": {
                "priority": 10,
                "when": {
                    "document_types": ["packaged_document"],
                },
            },
            "export_manifest": {
                "priority": 10,
                "when": {
                    "document_types": ["document_package"],
                },
                "wait_for_children": {
                    "from_processes": ["extract_items"],
                    "document_types": ["packaged_document"],
                    "relations": ["package_item"],
                    "min_count": 1,
                },
            },
        },
    ),
    "document_redaction_review": _ScaffoldBlueprint(
        id="document_redaction_review",
        title="Document redaction and review",
        document_type="sensitive_document",
        document_media_types=(
            "application/pdf",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "text/plain",
            "text/markdown",
            "image/*",
        ),
        document_extensions=(".pdf", ".docx", ".txt", ".md", ".png", ".jpg", ".jpeg"),
        additional_document_types=(
            DocumentTypeSpec(
                id="redacted_document",
                title="Redacted document",
                media_types=(
                    "application/pdf",
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    "text/plain",
                    "text/markdown",
                    "image/*",
                ),
                extensions=(
                    ".pdf",
                    ".docx",
                    ".txt",
                    ".md",
                    ".png",
                    ".jpg",
                    ".jpeg",
                ),
                value_schema=_document_source_value_schema(
                    extras={
                        "redaction_profile": {"type": "string"},
                        "source_document_id": {"type": "string"},
                    }
                ),
                metadata_schema=_document_metadata_schema(
                    extras={
                        "source_document_id": {"type": "string"},
                        "redaction_policy": {"type": "string"},
                        "review_status": {"type": "string"},
                    }
                ),
            ),
        ),
        additional_document_relations=(
            DocumentRelationSpec(
                id="redacted",
                title="Redacted",
                source_document_types=["sensitive_document"],
                target_document_types=["redacted_document"],
            ),
        ),
        steps=(
            "ingest",
            "extract_text",
            "detect_sensitive_data",
            "redact_document",
            "review_redaction",
            "export",
        ),
        artifact_kind_by_step={
            "ingest": "source_payload",
            "extract_text": "extracted_text",
            "detect_sensitive_data": "sensitive_spans",
            "redact_document": "redacted_document_artifact",
            "review_redaction": "redaction_review",
            "export": "exported_redacted_document",
        },
        capability_by_step={
            "ingest": "ingest_document",
            "extract_text": "extract_text",
            "detect_sensitive_data": "detect_sensitive_data",
            "redact_document": "redact_document",
            "review_redaction": "review_redaction",
            "export": "export_redacted_document",
        },
        operation_type_by_step={
            "review_redaction": "review",
        },
        document_value_schema=_document_source_value_schema(
            extras={
                "redaction_policy": {"type": "string"},
                "language": {"type": "string"},
                "jurisdiction": {"type": "string"},
            }
        ),
        document_metadata_schema=_document_metadata_schema(
            extras={
                "case_id": {"type": "string"},
                "tenant": {"type": "string"},
                "classification": {"type": "string"},
            }
        ),
        accepted_document_types_by_step={
            "ingest": ("sensitive_document", "redacted_document"),
            "extract_text": ("sensitive_document",),
            "detect_sensitive_data": ("sensitive_document",),
            "redact_document": ("sensitive_document",),
            "review_redaction": ("sensitive_document",),
            "export": ("sensitive_document",),
        },
        emitted_document_types_by_step={
            "redact_document": ("redacted_document",),
        },
        artifact_media_types_by_step={
            "ingest": ("application/json", "application/octet-stream"),
            "extract_text": ("application/json", "text/plain", "text/markdown"),
            "detect_sensitive_data": ("application/json",),
            "redact_document": (
                "application/json",
                "application/pdf",
                "application/octet-stream",
            ),
            "review_redaction": ("application/json",),
            "export": ("application/json", "application/pdf", "application/octet-stream"),
        },
        artifact_extensions_by_step={
            "ingest": (".json", ".bin"),
            "extract_text": (".json", ".txt", ".md"),
            "detect_sensitive_data": (".json",),
            "redact_document": (".json", ".pdf", ".bin"),
            "review_redaction": (".json",),
            "export": (".json", ".pdf", ".bin"),
        },
        artifact_value_schema_by_step={
            "detect_sensitive_data": {
                "type": "object",
                "required": ["findings"],
                "properties": {
                    "findings": {"type": "array"},
                    "risk_score": {"type": "number", "minimum": 0, "maximum": 1},
                    "policy": {"type": "string"},
                },
                "additionalProperties": True,
            },
            "redact_document": {
                "type": "object",
                "required": ["redaction_count"],
                "properties": {
                    "redaction_count": {"type": "integer", "minimum": 0},
                    "artifact_id": {"type": "string"},
                    "document_type": {"type": "string"},
                },
                "additionalProperties": True,
            },
            "review_redaction": {
                "type": "object",
                "required": ["decision"],
                "properties": {
                    "decision": {"type": "string"},
                    "reviewer": {"type": "string"},
                    "notes": {"type": "string"},
                },
                "additionalProperties": True,
            },
        },
        capability_output_schema_by_step={
            "detect_sensitive_data": {
                "type": "object",
                "required": ["findings"],
                "properties": {
                    "findings": {"type": "array"},
                    "risk_score": {"type": "number", "minimum": 0, "maximum": 1},
                },
                "additionalProperties": True,
            },
            "redact_document": {
                "type": "object",
                "required": ["redaction_count"],
                "properties": {
                    "redaction_count": {"type": "integer", "minimum": 0},
                    "artifact_id": {"type": "string"},
                },
                "additionalProperties": True,
            },
        },
        capability_streams_by_step={
            "extract_text": (
                StreamSpec(
                    stream_id="pages",
                    kinds=["page"],
                    consumers=["detect_sensitive_data"],
                    max_buffered_chunks=256,
                    value_schema=_stream_text_value_schema(),
                    metadata_schema=_stream_page_metadata_schema(),
                ),
            ),
            "detect_sensitive_data": (
                StreamSpec(
                    stream_id="sensitive_spans",
                    kinds=["span"],
                    consumers=["redact_document", "review_redaction"],
                    max_buffered_chunks=4096,
                    value_schema=_stream_sensitive_span_value_schema(),
                ),
            ),
            "redact_document": (
                StreamSpec(
                    stream_id="redactions",
                    kinds=["redaction"],
                    consumers=["review_redaction"],
                    max_buffered_chunks=4096,
                    value_schema=_stream_sensitive_span_value_schema(),
                ),
            ),
        },
        step_policy_by_step={
            "extract_text": {
                "priority": 50,
                "max_concurrency": 4,
                "resource_pool": "ocr_pool",
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 30,
                    "retry_error_kinds": ["transient_io", "rate_limited"],
                    "terminal_error_kinds": ["validation_error"],
                },
            },
            "detect_sensitive_data": {
                "priority": 40,
                "max_concurrency": 4,
                "resource_pool": "pii_pool",
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 30,
                    "retry_error_kinds": ["transient_io", "rate_limited"],
                    "terminal_error_kinds": ["validation_error"],
                },
            },
            "redact_document": {
                "priority": 30,
                "max_concurrency": 2,
                "resource_pool": "redaction_pool",
                "retry": {
                    "max_attempts": 2,
                    "delay_seconds": 15,
                    "retry_error_kinds": ["transient_io"],
                },
            },
            "review_redaction": {
                "title": "Human redaction review",
                "adapter": {"kind": "manual"},
                "priority": 10,
                "config": {"form": "redaction_review"},
            },
        },
    ),
    "document_translation_review": _ScaffoldBlueprint(
        id="document_translation_review",
        title="Document translation and review",
        document_type="translatable_document",
        document_media_types=(
            "application/pdf",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "text/plain",
            "text/markdown",
            "text/html",
        ),
        document_extensions=(".pdf", ".docx", ".txt", ".md", ".html", ".htm"),
        additional_document_types=(
            DocumentTypeSpec(
                id="translated_document",
                title="Translated document",
                media_types=(
                    "application/pdf",
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    "text/plain",
                    "text/markdown",
                    "text/html",
                ),
                extensions=(".pdf", ".docx", ".txt", ".md", ".html", ".htm"),
                value_schema=_document_source_value_schema(
                    extras={
                        "source_document_id": {"type": "string"},
                        "source_language": {"type": "string"},
                        "target_language": {"type": "string"},
                    }
                ),
                metadata_schema=_document_metadata_schema(
                    extras={
                        "source_document_id": {"type": "string"},
                        "translation_profile": {"type": "string"},
                        "review_status": {"type": "string"},
                    }
                ),
            ),
        ),
        additional_document_relations=(
            DocumentRelationSpec(
                id="translated",
                title="Translated",
                source_document_types=["translatable_document"],
                target_document_types=["translated_document"],
            ),
        ),
        steps=(
            "ingest",
            "extract_text",
            "segment_text",
            "translate_segments",
            "review_translation",
            "assemble_translation",
            "export",
        ),
        artifact_kind_by_step={
            "ingest": "source_payload",
            "extract_text": "extracted_text",
            "segment_text": "translation_segments",
            "translate_segments": "translated_segments",
            "review_translation": "translation_review",
            "assemble_translation": "translated_document_artifact",
            "export": "exported_translation",
        },
        capability_by_step={
            "ingest": "ingest_document",
            "extract_text": "extract_text",
            "segment_text": "segment_text",
            "translate_segments": "translate_segments",
            "review_translation": "review_translation",
            "assemble_translation": "assemble_translation",
            "export": "export_translation",
        },
        operation_type_by_step={
            "segment_text": "split",
            "translate_segments": "translate",
            "review_translation": "review",
        },
        document_value_schema=_document_source_value_schema(
            extras={
                "source_language": {"type": "string"},
                "target_language": {"type": "string"},
                "translation_profile": {"type": "string"},
            }
        ),
        document_metadata_schema=_document_metadata_schema(
            extras={
                "tenant": {"type": "string"},
                "collection": {"type": "string"},
                "domain": {"type": "string"},
            }
        ),
        accepted_document_types_by_step={
            "ingest": ("translatable_document", "translated_document"),
            "extract_text": ("translatable_document",),
            "segment_text": ("translatable_document",),
            "translate_segments": ("translatable_document",),
            "review_translation": ("translatable_document",),
            "assemble_translation": ("translatable_document",),
            "export": ("translatable_document",),
        },
        emitted_document_types_by_step={
            "assemble_translation": ("translated_document",),
        },
        artifact_media_types_by_step={
            "ingest": ("application/json", "application/octet-stream"),
            "extract_text": ("application/json", "text/plain", "text/markdown"),
            "segment_text": ("application/json", "application/x-ndjson"),
            "translate_segments": ("application/json", "application/x-ndjson"),
            "review_translation": ("application/json",),
            "assemble_translation": (
                "application/json",
                "application/pdf",
                "application/octet-stream",
            ),
            "export": ("application/json", "application/pdf", "application/octet-stream"),
        },
        artifact_extensions_by_step={
            "ingest": (".json", ".bin"),
            "extract_text": (".json", ".txt", ".md"),
            "segment_text": (".json", ".ndjson"),
            "translate_segments": (".json", ".ndjson"),
            "review_translation": (".json",),
            "assemble_translation": (".json", ".pdf", ".bin"),
            "export": (".json", ".pdf", ".bin"),
        },
        artifact_value_schema_by_step={
            "segment_text": {
                "type": "object",
                "required": ["segments"],
                "properties": {
                    "segments": {"type": "array"},
                    "segment_count": {"type": "integer", "minimum": 0},
                },
                "additionalProperties": True,
            },
            "translate_segments": {
                "type": "object",
                "required": ["translated_segment_count"],
                "properties": {
                    "translated_segment_count": {"type": "integer", "minimum": 0},
                    "source_language": {"type": "string"},
                    "target_language": {"type": "string"},
                },
                "additionalProperties": True,
            },
            "review_translation": {
                "type": "object",
                "required": ["decision"],
                "properties": {
                    "decision": {"type": "string"},
                    "reviewer": {"type": "string"},
                    "notes": {"type": "string"},
                },
                "additionalProperties": True,
            },
        },
        capability_output_schema_by_step={
            "translate_segments": {
                "type": "object",
                "required": ["translated_segment_count"],
                "properties": {
                    "translated_segment_count": {"type": "integer", "minimum": 0},
                    "target_language": {"type": "string"},
                },
                "additionalProperties": True,
            },
        },
        capability_streams_by_step={
            "extract_text": (
                StreamSpec(
                    stream_id="pages",
                    kinds=["page"],
                    consumers=["segment_text"],
                    max_buffered_chunks=256,
                    value_schema=_stream_text_value_schema(),
                    metadata_schema=_stream_page_metadata_schema(),
                ),
            ),
            "segment_text": (
                StreamSpec(
                    stream_id="segments",
                    kinds=["segment"],
                    consumers=["translate_segments"],
                    max_buffered_chunks=4096,
                    value_schema=_stream_translation_segment_value_schema(),
                ),
            ),
            "translate_segments": (
                StreamSpec(
                    stream_id="translations",
                    kinds=["segment_translation"],
                    consumers=["review_translation", "assemble_translation"],
                    max_buffered_chunks=4096,
                    value_schema=_stream_translation_segment_value_schema(),
                ),
            ),
        },
        step_policy_by_step={
            "extract_text": {
                "priority": 50,
                "max_concurrency": 4,
                "resource_pool": "ocr_pool",
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 30,
                    "retry_error_kinds": ["transient_io", "rate_limited"],
                    "terminal_error_kinds": ["validation_error"],
                },
            },
            "translate_segments": {
                "priority": 40,
                "max_concurrency": 3,
                "resource_pool": "translation_pool",
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 60,
                    "retry_error_kinds": ["transient_io", "rate_limited"],
                },
            },
            "review_translation": {
                "title": "Human translation review",
                "adapter": {"kind": "manual"},
                "priority": 20,
                "config": {"form": "translation_review"},
            },
            "assemble_translation": {
                "priority": 10,
                "resource_pool": "render_pool",
                "retry": {
                    "max_attempts": 2,
                    "delay_seconds": 15,
                    "retry_error_kinds": ["transient_io"],
                },
            },
        },
    ),
    "generative_media": _ScaffoldBlueprint(
        id="generative_media",
        title="Generative media workflow",
        document_type="creative_brief",
        document_media_types=("text/plain", "application/json"),
        document_extensions=(".txt", ".json"),
        steps=("ingest_brief", "plan", "generate_assets", "render", "export"),
        artifact_kind_by_step={
            "ingest_brief": "creative_brief_payload",
            "plan": "generation_plan",
            "generate_assets": "generated_assets",
            "render": "rendered_media",
            "export": "exported_media",
        },
        capability_by_step={
            "ingest_brief": "ingest_brief",
            "plan": "plan_generation",
            "generate_assets": "generate_assets",
            "render": "render_media",
            "export": "export_media",
        },
        document_value_schema=_document_source_value_schema(
            extras={
                "prompt": {"type": "string"},
                "subject": {"type": "string"},
                "style": {"type": "string"},
            }
        ),
        document_metadata_schema=_document_metadata_schema(
            extras={
                "project_id": {"type": "string"},
                "owner": {"type": "string"},
                "campaign": {"type": "string"},
            }
        ),
        artifact_media_types_by_step={
            "ingest_brief": ("application/json", "text/plain"),
            "plan": ("application/json", "text/markdown"),
            "generate_assets": ("application/json", "image/*", "audio/*", "video/*"),
            "render": ("application/json", "image/*", "video/*"),
            "export": ("application/json", "image/*", "video/*"),
        },
        artifact_extensions_by_step={
            "ingest_brief": (".json", ".txt"),
            "plan": (".json", ".md"),
            "generate_assets": (".json", ".png", ".jpg", ".wav", ".mp4"),
            "render": (".json", ".png", ".jpg", ".mp4"),
            "export": (".json", ".png", ".jpg", ".mp4"),
        },
        capability_streams_by_step={
            "plan": (
                StreamSpec(
                    stream_id="plan",
                    kinds=["section"],
                    consumers=["generate_assets"],
                    max_buffered_chunks=64,
                    value_schema=_stream_text_value_schema(),
                ),
            ),
            "generate_assets": (
                StreamSpec(
                    stream_id="assets",
                    kinds=["asset"],
                    consumers=["render"],
                    max_buffered_chunks=256,
                    value_schema=_stream_asset_value_schema(),
                ),
            ),
            "render": (
                StreamSpec(
                    stream_id="frames",
                    kinds=["frame"],
                    consumers=["export"],
                    max_buffered_chunks=1024,
                    value_schema=_stream_frame_value_schema(),
                ),
            ),
        },
        step_policy_by_step={
            "generate_assets": {
                "priority": 50,
                "max_concurrency": 2,
                "resource_pool": "accelerator_pool",
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 60,
                    "retry_error_kinds": ["transient_io", "rate_limited"],
                },
            },
            "render": {
                "priority": 40,
                "max_concurrency": 1,
                "resource_pool": "render_pool",
                "retry": {
                    "max_attempts": 2,
                    "delay_seconds": 30,
                    "retry_error_kinds": ["transient_io"],
                },
            },
        },
    ),
    "llm_document_processing": _ScaffoldBlueprint(
        id="llm_document_processing",
        title="LLM document processing",
        document_type="llm_document",
        document_media_types=(
            "application/pdf",
            "message/rfc822",
            "text/markdown",
            "text/plain",
        ),
        document_extensions=(".pdf", ".eml", ".md", ".txt"),
        steps=(
            "ingest",
            "extract_text",
            "chunk",
            "embed",
            "retrieve",
            "generate",
            "review",
            "export",
        ),
        artifact_kind_by_step={
            "ingest": "source_payload",
            "extract_text": "extracted_text",
            "chunk": "document_chunks",
            "embed": "chunk_embeddings",
            "retrieve": "retrieval_context",
            "generate": "generated_response",
            "review": "review_decision",
            "export": "exported_result",
        },
        capability_by_step={
            "ingest": "ingest_document",
            "extract_text": "extract_text",
            "chunk": "chunk_document",
            "embed": "embed_chunks",
            "retrieve": "retrieve_context",
            "generate": "generate_response",
            "review": "review_output",
            "export": "export_result",
        },
        document_value_schema=_document_source_value_schema(
            extras={
                "question": {"type": "string"},
                "task": {"type": "string"},
                "language": {"type": "string"},
            }
        ),
        document_metadata_schema=_document_metadata_schema(
            extras={
                "collection": {"type": "string"},
                "tenant": {"type": "string"},
                "source_system": {"type": "string"},
            }
        ),
        artifact_media_types_by_step={
            "ingest": ("application/json", "application/octet-stream"),
            "extract_text": ("application/json", "text/plain", "text/markdown"),
            "chunk": ("application/json",),
            "embed": ("application/json", "application/x-ndjson"),
            "retrieve": ("application/json",),
            "generate": ("application/json", "text/markdown", "text/plain"),
            "review": ("application/json",),
            "export": ("application/json", "text/markdown", "text/plain"),
        },
        artifact_extensions_by_step={
            "ingest": (".json", ".pdf", ".eml", ".md", ".txt"),
            "extract_text": (".json", ".txt", ".md"),
            "chunk": (".json",),
            "embed": (".json", ".ndjson"),
            "retrieve": (".json",),
            "generate": (".json", ".md", ".txt"),
            "review": (".json",),
            "export": (".json", ".md", ".txt"),
        },
        capability_streams_by_step={
            "extract_text": (
                StreamSpec(
                    stream_id="pages",
                    kinds=["page"],
                    consumers=["chunk"],
                    max_buffered_chunks=128,
                    value_schema=_stream_text_value_schema(),
                    metadata_schema=_stream_page_metadata_schema(),
                ),
            ),
            "chunk": (
                StreamSpec(
                    stream_id="chunks",
                    kinds=["chunk"],
                    consumers=["embed"],
                    max_buffered_chunks=4096,
                    value_schema=_stream_text_value_schema(),
                    metadata_schema=_stream_chunk_metadata_schema(),
                ),
            ),
            "embed": (
                StreamSpec(
                    stream_id="embeddings",
                    kinds=["embedding"],
                    consumers=["retrieve"],
                    max_buffered_chunks=4096,
                    value_schema=_stream_embedding_value_schema(),
                ),
            ),
            "retrieve": (
                StreamSpec(
                    stream_id="matches",
                    kinds=["match"],
                    consumers=["generate"],
                    max_buffered_chunks=1024,
                    value_schema=_stream_retrieval_match_value_schema(),
                ),
            ),
            "generate": (
                StreamSpec(
                    stream_id="tokens",
                    kinds=["token"],
                    consumers=["review"],
                    max_buffered_chunks=8192,
                    value_schema=_stream_text_value_schema(),
                ),
            ),
        },
        step_policy_by_step={
            "extract_text": {
                "priority": 50,
                "max_concurrency": 4,
                "resource_pool": "ocr_pool",
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 30,
                    "retry_error_kinds": ["transient_io", "rate_limited"],
                    "terminal_error_kinds": ["validation_error"],
                },
            },
            "embed": {
                "priority": 30,
                "max_concurrency": 4,
                "resource_pool": "embedding_pool",
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 30,
                    "retry_error_kinds": ["transient_io", "rate_limited"],
                },
            },
            "generate": {
                "priority": 20,
                "max_concurrency": 2,
                "resource_pool": "llm_pool",
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 60,
                    "retry_error_kinds": ["transient_io", "rate_limited"],
                },
            },
            "review": {
                "title": "Human review",
                "adapter": {"kind": "manual"},
                "priority": 10,
                "config": {"form": "review_output"},
            },
        },
    ),
    "knowledge_base_ingestion": _ScaffoldBlueprint(
        id="knowledge_base_ingestion",
        title="Knowledge base ingestion",
        document_type="knowledge_document",
        document_media_types=(
            "application/pdf",
            "text/html",
            "text/markdown",
            "text/plain",
            "message/rfc822",
        ),
        document_extensions=(".pdf", ".html", ".htm", ".md", ".txt", ".eml"),
        steps=(
            "ingest",
            "extract_text",
            "split_chunks",
            "enrich_metadata",
            "embed",
            "index",
        ),
        artifact_kind_by_step={
            "ingest": "source_payload",
            "extract_text": "extracted_text",
            "split_chunks": "document_chunks",
            "enrich_metadata": "enriched_chunks",
            "embed": "chunk_embeddings",
            "index": "indexed_record",
        },
        capability_by_step={
            "ingest": "ingest_document",
            "extract_text": "extract_text",
            "split_chunks": "split_chunks",
            "enrich_metadata": "enrich_metadata",
            "embed": "embed_chunks",
            "index": "index_chunks",
        },
        document_value_schema=_document_source_value_schema(
            extras={
                "collection": {"type": "string"},
                "tenant": {"type": "string"},
                "access_policy": {"type": "string"},
            }
        ),
        document_metadata_schema=_document_metadata_schema(
            extras={
                "collection": {"type": "string"},
                "tenant": {"type": "string"},
                "source_system": {"type": "string"},
                "language": {"type": "string"},
            }
        ),
        artifact_media_types_by_step={
            "ingest": ("application/json", "application/octet-stream"),
            "extract_text": ("application/json", "text/plain", "text/markdown"),
            "split_chunks": ("application/json", "application/x-ndjson"),
            "enrich_metadata": ("application/json",),
            "embed": ("application/json", "application/x-ndjson"),
            "index": ("application/json",),
        },
        artifact_extensions_by_step={
            "ingest": (".json", ".bin"),
            "extract_text": (".json", ".txt", ".md"),
            "split_chunks": (".json", ".ndjson"),
            "enrich_metadata": (".json",),
            "embed": (".json", ".ndjson"),
            "index": (".json",),
        },
        artifact_value_schema_by_step={
            "split_chunks": {
                "type": "object",
                "properties": {"chunks": {"type": "array"}},
                "additionalProperties": True,
            },
            "embed": {
                "type": "object",
                "properties": {"embeddings": {"type": "array"}},
                "additionalProperties": True,
            },
            "index": {
                "type": "object",
                "properties": {
                    "indexed_count": {"type": "integer", "minimum": 0},
                    "collection": {"type": "string"},
                },
                "additionalProperties": True,
            },
        },
        capability_streams_by_step={
            "extract_text": (
                StreamSpec(
                    stream_id="pages",
                    kinds=["page"],
                    consumers=["split_chunks"],
                    max_buffered_chunks=256,
                    value_schema=_stream_text_value_schema(),
                    metadata_schema=_stream_page_metadata_schema(),
                ),
            ),
            "split_chunks": (
                StreamSpec(
                    stream_id="chunks",
                    kinds=["chunk"],
                    consumers=["embed"],
                    max_buffered_chunks=8192,
                    value_schema=_stream_text_value_schema(),
                    metadata_schema=_stream_chunk_metadata_schema(),
                ),
            ),
            "embed": (
                StreamSpec(
                    stream_id="embeddings",
                    kinds=["embedding"],
                    consumers=["index"],
                    max_buffered_chunks=8192,
                    value_schema=_stream_embedding_value_schema(),
                ),
            ),
        },
        step_policy_by_step={
            "extract_text": {
                "priority": 50,
                "max_concurrency": 4,
                "resource_pool": "ocr_pool",
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 30,
                    "retry_error_kinds": ["transient_io", "rate_limited"],
                    "terminal_error_kinds": ["validation_error"],
                },
            },
            "embed": {
                "priority": 30,
                "max_concurrency": 4,
                "resource_pool": "embedding_pool",
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 30,
                    "retry_error_kinds": ["transient_io", "rate_limited"],
                },
            },
            "index": {
                "priority": 20,
                "max_concurrency": 2,
                "resource_pool": "index_pool",
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 30,
                    "retry_error_kinds": ["transient_io"],
                },
            },
        },
    ),
    "structured_extraction_review": _ScaffoldBlueprint(
        id="structured_extraction_review",
        title="Structured extraction and review",
        document_type="structured_document",
        document_media_types=(
            "application/pdf",
            "image/*",
            "text/plain",
            "application/json",
            "text/csv",
        ),
        document_extensions=(
            ".pdf",
            ".png",
            ".jpg",
            ".jpeg",
            ".txt",
            ".json",
            ".csv",
        ),
        steps=(
            "ingest",
            "extract_text",
            "extract_fields",
            "validate_fields",
            "review",
            "export",
        ),
        artifact_kind_by_step={
            "ingest": "source_payload",
            "extract_text": "extracted_text",
            "extract_fields": "structured_fields",
            "validate_fields": "validation_report",
            "review": "review_decision",
            "export": "exported_record",
        },
        capability_by_step={
            "ingest": "ingest_document",
            "extract_text": "extract_text",
            "extract_fields": "extract_structured_fields",
            "validate_fields": "validate_fields",
            "review": "review_fields",
            "export": "export_record",
        },
        document_value_schema=_document_source_value_schema(
            extras={
                "schema_id": {"type": "string"},
                "document_class": {"type": "string"},
                "language": {"type": "string"},
            }
        ),
        document_metadata_schema=_document_metadata_schema(
            extras={
                "source_system": {"type": "string"},
                "case_id": {"type": "string"},
                "priority": {"type": "string"},
            }
        ),
        artifact_media_types_by_step={
            "ingest": ("application/json", "application/octet-stream"),
            "extract_text": ("application/json", "text/plain", "text/markdown"),
            "extract_fields": ("application/json",),
            "validate_fields": ("application/json",),
            "review": ("application/json",),
            "export": ("application/json", "text/csv"),
        },
        artifact_extensions_by_step={
            "ingest": (".json", ".bin"),
            "extract_text": (".json", ".txt", ".md"),
            "extract_fields": (".json",),
            "validate_fields": (".json",),
            "review": (".json",),
            "export": (".json", ".csv"),
        },
        artifact_value_schema_by_step={
            "extract_fields": _structured_fields_value_schema(),
            "validate_fields": _validation_report_value_schema(),
            "review": {
                "type": "object",
                "required": ["decision"],
                "properties": {
                    "decision": {"type": "string"},
                    "reviewer": {"type": "string"},
                    "notes": {"type": "string"},
                },
                "additionalProperties": True,
            },
        },
        capability_output_schema_by_step={
            "extract_fields": _structured_fields_value_schema(),
            "validate_fields": _validation_report_value_schema(),
        },
        capability_streams_by_step={
            "extract_text": (
                StreamSpec(
                    stream_id="pages",
                    kinds=["page"],
                    consumers=["extract_fields"],
                    max_buffered_chunks=256,
                    value_schema=_stream_text_value_schema(),
                    metadata_schema=_stream_page_metadata_schema(),
                ),
            ),
            "extract_fields": (
                StreamSpec(
                    stream_id="fields",
                    kinds=["field"],
                    consumers=["validate_fields"],
                    max_buffered_chunks=512,
                    value_schema=_stream_field_value_schema(),
                ),
            ),
        },
        step_policy_by_step={
            "extract_text": {
                "priority": 50,
                "max_concurrency": 4,
                "resource_pool": "ocr_pool",
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 30,
                    "retry_error_kinds": ["transient_io", "rate_limited"],
                    "terminal_error_kinds": ["validation_error"],
                },
            },
            "extract_fields": {
                "priority": 40,
                "max_concurrency": 3,
                "resource_pool": "llm_pool",
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 60,
                    "retry_error_kinds": ["transient_io", "rate_limited"],
                },
            },
            "review": {
                "title": "Human extraction review",
                "adapter": {"kind": "manual"},
                "priority": 10,
                "config": {"form": "structured_extraction_review"},
            },
        },
    ),
    "tabular_data_processing": _ScaffoldBlueprint(
        id="tabular_data_processing",
        title="Tabular data processing",
        document_type="tabular_document",
        document_media_types=(
            "text/csv",
            "text/tab-separated-values",
            "application/json",
            "application/vnd.ms-excel",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ),
        document_extensions=(".csv", ".tsv", ".json", ".xls", ".xlsx"),
        steps=(
            "ingest",
            "profile_table",
            "normalize_rows",
            "validate_rows",
            "enrich_records",
            "aggregate",
            "export",
        ),
        artifact_kind_by_step={
            "ingest": "source_table",
            "profile_table": "table_profile",
            "normalize_rows": "normalized_rows",
            "validate_rows": "validation_report",
            "enrich_records": "enriched_records",
            "aggregate": "aggregate_report",
            "export": "exported_dataset",
        },
        capability_by_step={
            "ingest": "ingest_table",
            "profile_table": "profile_table",
            "normalize_rows": "normalize_rows",
            "validate_rows": "validate_rows",
            "enrich_records": "enrich_records",
            "aggregate": "aggregate_records",
            "export": "export_dataset",
        },
        operation_type_by_step={
            "profile_table": "analyze",
            "aggregate": "aggregate",
        },
        document_value_schema=_document_source_value_schema(
            extras={
                "dataset_id": {"type": "string"},
                "sheet_name": {"type": "string"},
                "delimiter": {"type": "string"},
                "schema_id": {"type": "string"},
            }
        ),
        document_metadata_schema=_document_metadata_schema(
            extras={
                "source_system": {"type": "string"},
                "owner": {"type": "string"},
                "reporting_period": {"type": "string"},
            }
        ),
        artifact_media_types_by_step={
            "ingest": ("application/json", "text/csv", "application/octet-stream"),
            "profile_table": ("application/json",),
            "normalize_rows": ("application/json", "application/x-ndjson"),
            "validate_rows": ("application/json",),
            "enrich_records": ("application/json", "application/x-ndjson"),
            "aggregate": ("application/json",),
            "export": ("application/json", "text/csv", "application/x-ndjson"),
        },
        artifact_extensions_by_step={
            "ingest": (".json", ".csv", ".tsv", ".xls", ".xlsx"),
            "profile_table": (".json",),
            "normalize_rows": (".json", ".ndjson"),
            "validate_rows": (".json",),
            "enrich_records": (".json", ".ndjson"),
            "aggregate": (".json",),
            "export": (".json", ".csv", ".ndjson"),
        },
        artifact_value_schema_by_step={
            "profile_table": {
                "type": "object",
                "properties": {
                    "row_count": {"type": "integer", "minimum": 0},
                    "column_count": {"type": "integer", "minimum": 0},
                    "columns": {"type": "array"},
                },
                "additionalProperties": True,
            },
            "validate_rows": _validation_report_value_schema(),
            "aggregate": {
                "type": "object",
                "properties": {
                    "record_count": {"type": "integer", "minimum": 0},
                    "group_count": {"type": "integer", "minimum": 0},
                    "metrics": {"type": "object"},
                },
                "additionalProperties": True,
            },
        },
        capability_output_schema_by_step={
            "profile_table": {
                "type": "object",
                "properties": {
                    "row_count": {"type": "integer", "minimum": 0},
                    "column_count": {"type": "integer", "minimum": 0},
                    "columns": {"type": "array"},
                },
                "additionalProperties": True,
            },
            "validate_rows": _validation_report_value_schema(),
        },
        capability_streams_by_step={
            "profile_table": (
                StreamSpec(
                    stream_id="rows",
                    kinds=["row"],
                    consumers=["normalize_rows"],
                    max_buffered_chunks=16384,
                    value_schema=_stream_row_value_schema(),
                ),
            ),
            "normalize_rows": (
                StreamSpec(
                    stream_id="normalized_rows",
                    kinds=["row"],
                    consumers=["validate_rows", "enrich_records"],
                    max_buffered_chunks=16384,
                    value_schema=_stream_row_value_schema(),
                ),
            ),
            "validate_rows": (
                StreamSpec(
                    stream_id="validation_issues",
                    kinds=["issue"],
                    consumers=["aggregate"],
                    max_buffered_chunks=4096,
                    value_schema=_stream_validation_issue_value_schema(),
                ),
            ),
            "enrich_records": (
                StreamSpec(
                    stream_id="records",
                    kinds=["record"],
                    consumers=["aggregate"],
                    max_buffered_chunks=16384,
                    value_schema=_stream_row_value_schema(),
                ),
            ),
        },
        step_policy_by_step={
            "profile_table": {
                "priority": 50,
                "max_concurrency": 4,
                "resource_pool": "tabular_pool",
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 15,
                    "retry_error_kinds": ["transient_io"],
                    "terminal_error_kinds": ["validation_error"],
                },
            },
            "normalize_rows": {
                "priority": 40,
                "max_concurrency": 4,
                "resource_pool": "tabular_pool",
            },
            "validate_rows": {
                "priority": 30,
                "max_concurrency": 4,
                "resource_pool": "tabular_pool",
                "retry": {
                    "max_attempts": 2,
                    "delay_seconds": 15,
                    "retry_error_kinds": ["transient_io"],
                },
            },
            "enrich_records": {
                "priority": 20,
                "max_concurrency": 2,
                "resource_pool": "llm_pool",
                "retry": {
                    "max_attempts": 3,
                    "delay_seconds": 60,
                    "retry_error_kinds": ["transient_io", "rate_limited"],
                },
            },
            "aggregate": {
                "priority": 10,
                "resource_pool": "tabular_pool",
            },
        },
    ),
}


def _scaffold_blueprint_summary(blueprint: _ScaffoldBlueprint) -> dict[str, Any]:
    needs_by_step = blueprint.needs_by_step or {}
    accepted_document_types_by_step = blueprint.accepted_document_types_by_step or {}
    emitted_document_types_by_step = blueprint.emitted_document_types_by_step or {}
    artifact_media_types_by_step = blueprint.artifact_media_types_by_step or {}
    artifact_extensions_by_step = blueprint.artifact_extensions_by_step or {}
    capability_streams_by_step = blueprint.capability_streams_by_step or {}
    step_policy_by_step = blueprint.step_policy_by_step or {}
    operation_type_by_step = blueprint.operation_type_by_step or {}
    resolved_operation_types = [
        operation_type_by_step.get(step_id) or operation_type_for_step(step_id)
        for step_id in blueprint.steps
    ]
    steps: list[dict[str, Any]] = []
    for index, step_id in enumerate(blueprint.steps):
        policy = step_policy_by_step.get(step_id) or {}
        adapter = policy.get("adapter") if isinstance(policy.get("adapter"), dict) else {}
        retry = policy.get("retry") if isinstance(policy.get("retry"), dict) else None
        steps.append(
            {
                "id": step_id,
                "needs": list(
                    needs_by_step.get(
                        step_id,
                        ((blueprint.steps[index - 1],) if index > 0 else ()),
                    )
                ),
                "capability": blueprint.capability_by_step[step_id],
                "operation_type": operation_type_by_step.get(step_id)
                or operation_type_for_step(step_id),
                "artifact_kind": blueprint.artifact_kind_by_step[step_id],
                "accepts_document_types": list(
                    accepted_document_types_by_step.get(step_id)
                    or ((blueprint.document_type,) if step_id == blueprint.steps[0] else ())
                ),
                "emits_document_types": list(
                    emitted_document_types_by_step.get(step_id) or ()
                ),
                "artifact_media_types": list(
                    artifact_media_types_by_step.get(step_id) or ()
                ),
                "artifact_extensions": list(
                    artifact_extensions_by_step.get(step_id) or ()
                ),
                "streams": [
                    stream.model_dump(mode="json", exclude_none=True)
                    for stream in capability_streams_by_step.get(step_id, ())
                ],
                "policy": {
                    key: value
                    for key, value in {
                        "adapter_kind": adapter.get("kind"),
                        "priority": policy.get("priority"),
                        "max_concurrency": policy.get("max_concurrency"),
                        "resource_pool": policy.get("resource_pool"),
                        "retry": retry,
                        "when": policy.get("when"),
                        "wait_for_children": policy.get("wait_for_children"),
                        "config": policy.get("config"),
                    }.items()
                    if value is not None
                },
            }
        )
    return {
        "id": blueprint.id,
        "title": blueprint.title,
        "document": {
            "type": blueprint.document_type,
            "media_types": list(blueprint.document_media_types),
            "extensions": list(blueprint.document_extensions),
            "value_schema": blueprint.document_value_schema or {},
            "metadata_schema": blueprint.document_metadata_schema or {},
        },
        "additional_document_types": [
            document_type.model_dump(mode="json", exclude_none=True)
            for document_type in blueprint.additional_document_types
        ],
        "additional_document_relations": [
            relation.model_dump(mode="json", exclude_none=True)
            for relation in blueprint.additional_document_relations
        ],
        "operation_types": [
            operation.model_dump(mode="json", exclude_none=True)
            for operation in (
                list(blueprint.operation_types)
                or operation_type_specs(resolved_operation_types)
            )
        ],
        "steps": steps,
        "step_count": len(steps),
        "capabilities": [step["capability"] for step in steps],
        "artifact_kinds": [step["artifact_kind"] for step in steps],
        "stream_count": sum(len(step["streams"]) for step in steps),
        "manual_steps": [
            step["id"]
            for step in steps
            if step["policy"].get("adapter_kind") == "manual"
        ],
        "resource_pools": sorted(
            {
                str(step["policy"]["resource_pool"])
                for step in steps
                if step["policy"].get("resource_pool")
            }
        ),
        "scaffold_command": (
            "uv run fala scaffold --blueprint "
            f"{blueprint.id} --output-dir ./pipelines/{blueprint.id} "
            f"--package-id {blueprint.id} --pipeline-id {blueprint.id}_flow"
        ),
    }


ScaffoldBlueprint = _ScaffoldBlueprint
SCAFFOLD_BLUEPRINTS = _SCAFFOLD_BLUEPRINTS


def scaffold_blueprint_summary(blueprint: ScaffoldBlueprint) -> dict[str, Any]:
    return _scaffold_blueprint_summary(blueprint)


def list_scaffold_blueprints(query: str | None = None) -> list[dict[str, Any]]:
    summaries = [
        scaffold_blueprint_summary(blueprint)
        for blueprint in SCAFFOLD_BLUEPRINTS.values()
    ]
    if not query or not query.strip():
        return summaries
    terms = [term for term in query.lower().split() if term]
    return [
        summary
        for summary in summaries
        if all(term in _blueprint_search_text(summary) for term in terms)
    ]


def _blueprint_search_text(summary: dict[str, Any]) -> str:
    values: list[str] = [
        str(summary.get("id") or ""),
        str(summary.get("title") or ""),
    ]
    document = summary.get("document")
    if isinstance(document, dict):
        values.extend(
            [
                str(document.get("type") or ""),
                *[str(item) for item in document.get("media_types") or []],
                *[str(item) for item in document.get("extensions") or []],
            ]
        )
    for key in (
        "capabilities",
        "artifact_kinds",
        "manual_steps",
        "resource_pools",
    ):
        values.extend(str(item) for item in summary.get(key) or [])
    for operation in summary.get("operation_types") or []:
        if isinstance(operation, dict):
            values.extend(
                str(operation.get(key) or "")
                for key in ("id", "title", "category", "description")
            )
    for document_type in summary.get("additional_document_types") or []:
        if isinstance(document_type, dict):
            values.append(str(document_type.get("id") or ""))
            values.append(str(document_type.get("title") or ""))
            values.extend(str(item) for item in document_type.get("media_types") or [])
            values.extend(str(item) for item in document_type.get("extensions") or [])
    for relation in summary.get("additional_document_relations") or []:
        if isinstance(relation, dict):
            values.append(str(relation.get("id") or ""))
            values.append(str(relation.get("title") or ""))
    for step in summary.get("steps") or []:
        if not isinstance(step, dict):
            continue
        values.extend(
            str(step.get(key) or "")
            for key in ("id", "capability", "operation_type", "artifact_kind")
        )
        values.extend(str(item) for item in step.get("accepts_document_types") or [])
        values.extend(str(item) for item in step.get("emits_document_types") or [])
        values.extend(str(item) for item in step.get("artifact_media_types") or [])
        values.extend(str(item) for item in step.get("artifact_extensions") or [])
        policy = step.get("policy")
        if isinstance(policy, dict):
            values.extend(str(value) for value in policy.values())
        for stream in step.get("streams") or []:
            if not isinstance(stream, dict):
                continue
            values.append(str(stream.get("stream_id") or ""))
            values.extend(str(item) for item in stream.get("kinds") or [])
            values.extend(str(item) for item in stream.get("consumers") or [])
    return "\n".join(values).lower()


def get_scaffold_blueprint(blueprint_id: str) -> ScaffoldBlueprint | None:
    return SCAFFOLD_BLUEPRINTS.get(blueprint_id)


def scaffold_blueprint_from_mapping(
    value: dict[str, Any],
    *,
    source: str = "blueprint",
) -> ScaffoldBlueprint:
    if not isinstance(value, dict):
        raise ValueError(f"{source} must be an object")
    document = _required_mapping(value, "document", source=source)
    raw_steps = value.get("steps")
    if not isinstance(raw_steps, list) or not raw_steps:
        raise ValueError(f"{source}.steps must be a non-empty list")

    steps: list[str] = []
    needs_by_step: dict[str, tuple[str, ...]] = {}
    artifact_kind_by_step: dict[str, str] = {}
    capability_by_step: dict[str, str] = {}
    operation_type_by_step: dict[str, str] = {}
    accepted_document_types_by_step: dict[str, tuple[str, ...]] = {}
    emitted_document_types_by_step: dict[str, tuple[str, ...]] = {}
    artifact_media_types_by_step: dict[str, tuple[str, ...]] = {}
    artifact_extensions_by_step: dict[str, tuple[str, ...]] = {}
    artifact_value_schema_by_step: dict[str, dict[str, Any]] = {}
    capability_output_schema_by_step: dict[str, dict[str, Any]] = {}
    capability_streams_by_step: dict[str, tuple[StreamSpec, ...]] = {}
    step_policy_by_step: dict[str, dict[str, Any]] = {}

    for index, raw_step in enumerate(raw_steps):
        step_source = f"{source}.steps[{index}]"
        if not isinstance(raw_step, dict):
            raise ValueError(f"{step_source} must be an object")
        step_id = _required_string(raw_step, "id", source=step_source)
        if step_id in artifact_kind_by_step:
            raise ValueError(f"{source}.steps contains duplicate id: {step_id}")
        steps.append(step_id)
        if raw_step.get("needs") is not None:
            needs_by_step[step_id] = _string_tuple(
                raw_step["needs"],
                source=f"{step_source}.needs",
            )
        artifact_kind_by_step[step_id] = _required_string(
            raw_step,
            "artifact_kind",
            source=step_source,
        )
        capability_by_step[step_id] = _required_string(
            raw_step,
            "capability",
            source=step_source,
        )
        if raw_step.get("operation_type") is not None:
            operation_type_by_step[step_id] = _required_string(
                raw_step,
                "operation_type",
                source=step_source,
            )
        if raw_step.get("accepts_document_types") is not None:
            accepted_document_types_by_step[step_id] = _string_tuple(
                raw_step["accepts_document_types"],
                source=f"{step_source}.accepts_document_types",
            )
        if raw_step.get("emits_document_types") is not None:
            emitted_document_types_by_step[step_id] = _string_tuple(
                raw_step["emits_document_types"],
                source=f"{step_source}.emits_document_types",
            )
        if raw_step.get("artifact_media_types") is not None:
            artifact_media_types_by_step[step_id] = _string_tuple(
                raw_step["artifact_media_types"],
                source=f"{step_source}.artifact_media_types",
            )
        if raw_step.get("artifact_extensions") is not None:
            artifact_extensions_by_step[step_id] = _string_tuple(
                raw_step["artifact_extensions"],
                source=f"{step_source}.artifact_extensions",
            )
        if raw_step.get("artifact_value_schema") is not None:
            artifact_value_schema_by_step[step_id] = _plain_mapping(
                raw_step["artifact_value_schema"],
                source=f"{step_source}.artifact_value_schema",
            )
        if raw_step.get("output_schema") is not None:
            capability_output_schema_by_step[step_id] = _plain_mapping(
                raw_step["output_schema"],
                source=f"{step_source}.output_schema",
            )
        if raw_step.get("streams") is not None:
            capability_streams_by_step[step_id] = _stream_specs(
                raw_step["streams"],
                source=f"{step_source}.streams",
            )
        if raw_step.get("policy") is not None:
            step_policy_by_step[step_id] = _plain_mapping(
                raw_step["policy"],
                source=f"{step_source}.policy",
            )

    _validate_blueprint_needs(
        needs_by_step,
        steps=steps,
        source=source,
    )

    return ScaffoldBlueprint(
        id=_required_string(value, "id", source=source),
        title=_optional_string(value, "title") or _required_string(
            value,
            "id",
            source=source,
        ),
        document_type=_required_string(document, "type", source=f"{source}.document"),
        document_media_types=_string_tuple(
            document.get("media_types") or (),
            source=f"{source}.document.media_types",
        ),
        document_extensions=_string_tuple(
            document.get("extensions") or (),
            source=f"{source}.document.extensions",
        ),
        additional_document_types=_document_type_specs(
            value.get("additional_document_types") or (),
            source=f"{source}.additional_document_types",
        ),
        additional_document_relations=_document_relation_specs(
            value.get("additional_document_relations") or (),
            source=f"{source}.additional_document_relations",
        ),
        operation_types=_operation_type_specs(
            value.get("operation_types") or (),
            source=f"{source}.operation_types",
        ),
        document_value_schema=(
            _plain_mapping(
                document["value_schema"],
                source=f"{source}.document.value_schema",
            )
            if document.get("value_schema") is not None
            else None
        ),
        document_metadata_schema=(
            _plain_mapping(
                document["metadata_schema"],
                source=f"{source}.document.metadata_schema",
            )
            if document.get("metadata_schema") is not None
            else None
        ),
        steps=tuple(steps),
        needs_by_step=needs_by_step or None,
        artifact_kind_by_step=artifact_kind_by_step,
        capability_by_step=capability_by_step,
        operation_type_by_step=operation_type_by_step or None,
        accepted_document_types_by_step=accepted_document_types_by_step or None,
        emitted_document_types_by_step=emitted_document_types_by_step or None,
        artifact_media_types_by_step=artifact_media_types_by_step or None,
        artifact_extensions_by_step=artifact_extensions_by_step or None,
        artifact_value_schema_by_step=artifact_value_schema_by_step or None,
        capability_output_schema_by_step=capability_output_schema_by_step or None,
        capability_streams_by_step=capability_streams_by_step or None,
        step_policy_by_step=step_policy_by_step or None,
    )


def _validate_blueprint_needs(
    needs_by_step: dict[str, tuple[str, ...]],
    *,
    steps: list[str],
    source: str,
) -> None:
    known = set(steps)
    for step_id, needs in needs_by_step.items():
        missing = sorted(set(needs) - known)
        if missing:
            raise ValueError(
                f"{source}.steps {step_id!r} depends on unknown step id(s): "
                f"{', '.join(missing)}"
            )
        if step_id in needs:
            raise ValueError(f"{source}.steps {step_id!r} cannot depend on itself")
        duplicates = sorted({need for need in needs if needs.count(need) > 1})
        if duplicates:
            raise ValueError(
                f"{source}.steps {step_id!r} has duplicate dependency id(s): "
                f"{', '.join(duplicates)}"
            )


def document_source_value_schema(
    *,
    extras: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return _document_source_value_schema(extras=extras)


def _required_mapping(
    value: dict[str, Any],
    key: str,
    *,
    source: str,
) -> dict[str, Any]:
    item = value.get(key)
    if not isinstance(item, dict):
        raise ValueError(f"{source}.{key} must be an object")
    return item


def _plain_mapping(value: Any, *, source: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{source} must be an object")
    return dict(value)


def _document_type_specs(value: Any, *, source: str) -> tuple[DocumentTypeSpec, ...]:
    if value in (None, ()):
        return ()
    if not isinstance(value, list):
        raise ValueError(f"{source} must be a list")
    document_types: list[DocumentTypeSpec] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ValueError(f"{source}[{index}] must be an object")
        document_types.append(DocumentTypeSpec.model_validate(item))
    return tuple(document_types)


def _document_relation_specs(
    value: Any,
    *,
    source: str,
) -> tuple[DocumentRelationSpec, ...]:
    if value in (None, ()):
        return ()
    if not isinstance(value, list):
        raise ValueError(f"{source} must be a list")
    relations: list[DocumentRelationSpec] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ValueError(f"{source}[{index}] must be an object")
        relations.append(DocumentRelationSpec.model_validate(item))
    return tuple(relations)


def _operation_type_specs(
    value: Any,
    *,
    source: str,
) -> tuple[OperationTypeSpec, ...]:
    if value in (None, ()):
        return ()
    if not isinstance(value, list):
        raise ValueError(f"{source} must be a list")
    operation_types: list[OperationTypeSpec] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ValueError(f"{source}[{index}] must be an object")
        operation_types.append(OperationTypeSpec.model_validate(item))
    return tuple(operation_types)


def _required_string(
    value: dict[str, Any],
    key: str,
    *,
    source: str,
) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item.strip():
        raise ValueError(f"{source}.{key} must be a non-empty string")
    return item.strip()


def _optional_string(value: dict[str, Any], key: str) -> str | None:
    item = value.get(key)
    if item is None:
        return None
    if not isinstance(item, str) or not item.strip():
        raise ValueError(f"{key} must be a non-empty string")
    return item.strip()


def _string_tuple(value: Any, *, source: str) -> tuple[str, ...]:
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, (list, tuple)):
        values = list(value)
    else:
        raise ValueError(f"{source} must be a string or list of strings")
    result: list[str] = []
    for item in values:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{source} entries must be non-empty strings")
        result.append(item.strip())
    return tuple(result)


def _stream_specs(value: Any, *, source: str) -> tuple[StreamSpec, ...]:
    if isinstance(value, dict):
        values = [value]
    elif isinstance(value, list):
        values = value
    else:
        raise ValueError(f"{source} must be an object or list of objects")
    streams: list[StreamSpec] = []
    for index, item in enumerate(values):
        if not isinstance(item, dict):
            raise ValueError(f"{source}[{index}] must be an object")
        streams.append(StreamSpec.model_validate(item))
    return tuple(streams)
