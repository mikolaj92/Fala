from __future__ import annotations

import fnmatch
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from fala.models import RuntimeDocumentInput
from fala.registry import PipelineRegistry


def coerce_document_routes(value: Any, *, source: str = "document route") -> list[dict[str, Any]]:
    if isinstance(value, dict):
        raw_routes = value.get("routes")
        if raw_routes is None:
            raw_routes = [value]
    elif isinstance(value, list):
        raw_routes = value
    else:
        raise ValueError(f"{source} must be an object or list")
    if not isinstance(raw_routes, list):
        raise ValueError(f"{source} routes must be a list")
    routes: list[dict[str, Any]] = []
    for index, route in enumerate(raw_routes):
        if not isinstance(route, dict):
            raise ValueError(f"{source} routes[{index}] must be an object")
        routes.append(route)
    return routes


def apply_document_routes(
    documents: list[RuntimeDocumentInput],
    routes: list[dict[str, Any]],
) -> list[RuntimeDocumentInput]:
    if not routes:
        return documents
    return [_apply_document_route(document, routes) for document in documents]


def apply_auto_document_routes(
    documents: list[RuntimeDocumentInput],
    routes: list[dict[str, Any]],
) -> list[RuntimeDocumentInput]:
    if not routes:
        return documents
    routed: list[RuntimeDocumentInput] = []
    for document in documents:
        if document.pipeline_id is not None and document.document_type is not None:
            routed.append(document)
            continue
        matches = [
            route
            for route in routes
            if _auto_document_route_matches(route, document)
        ]
        if len(matches) > 1:
            candidates = ", ".join(
                sorted(
                    f"{match.get('pipeline_id') or match.get('pipeline')}:"
                    f"{match.get('document_type')}"
                    for match in matches
                )
            )
            raise ValueError(
                "Ambiguous auto-route for document "
                f"{document.document_id!r}: {candidates}. "
                "Use explicit routes or source-list pipeline_id/document_type to "
                "disambiguate."
            )
        if matches:
            routed.append(_apply_matching_document_route(document, matches[0]))
        else:
            routed.append(document)
    return routed


def auto_document_routes_from_registry(
    registry: PipelineRegistry,
) -> list[dict[str, Any]]:
    routes: list[dict[str, Any]] = []
    for pipeline in registry.all():
        package_id = registry.pipeline_package_id(pipeline.id)
        if package_id is None:
            continue
        package = registry.package(package_id)
        document_types = {
            document_type.id: document_type
            for document_type in package.document_types
        }
        capabilities = {
            capability.id: capability
            for capability in package.capabilities
        }
        accepted_document_types = sorted(
            {
                document_type
                for step in pipeline.steps
                if not step.needs and step.capability is not None
                for document_type in (
                    capabilities.get(step.capability).accepts_document_types
                    if capabilities.get(step.capability) is not None
                    else []
                )
            }
        )
        for document_type_id in accepted_document_types:
            document_type = document_types.get(document_type_id)
            if document_type is None:
                continue
            match: dict[str, Any] = {}
            if document_type.extensions:
                match["extensions"] = list(document_type.extensions)
            if document_type.media_types:
                match["media_types"] = list(document_type.media_types)
            if not match:
                match["document_types"] = [document_type.id]
            routes.append(
                {
                    "id": f"auto:{pipeline.id}:{document_type.id}",
                    "match": match,
                    "set": {
                        "pipeline_id": pipeline.id,
                        "document_type": document_type.id,
                    },
                    "pipeline_id": pipeline.id,
                    "document_type": document_type.id,
                }
            )
    return routes


def route_runtime_documents(
    documents: list[RuntimeDocumentInput],
    *,
    routes: list[dict[str, Any]] | None = None,
    auto_routes: list[dict[str, Any]] | None = None,
) -> list[RuntimeDocumentInput]:
    routed, _report = route_runtime_documents_with_report(
        documents,
        routes=routes,
        auto_routes=auto_routes,
    )
    return routed


def route_runtime_documents_with_report(
    documents: list[RuntimeDocumentInput],
    *,
    routes: list[dict[str, Any]] | None = None,
    auto_routes: list[dict[str, Any]] | None = None,
) -> tuple[list[RuntimeDocumentInput], dict[str, Any]]:
    routed: list[RuntimeDocumentInput] = []
    decisions: list[dict[str, Any]] = []
    for document in documents:
        current = document
        applied_routes: list[dict[str, Any]] = []
        candidates: list[dict[str, Any]] = []
        for route in routes or []:
            candidate = _route_candidate(
                route,
                current,
                kind="explicit",
                auto=False,
            )
            candidates.append(candidate)
            if not candidate["match"]:
                continue
            applied_routes.append(
                _route_decision(
                    route,
                    current,
                    kind="explicit",
                )
            )
            current = _apply_matching_document_route(current, route)
            break

        if current.pipeline_id is None or current.document_type is None:
            auto_candidates = [
                _route_candidate(
                    route,
                    current,
                    kind="auto",
                    auto=True,
                )
                for route in auto_routes or []
            ]
            candidates.extend(auto_candidates)
            matches = [
                route
                for route, candidate in zip(auto_routes or [], auto_candidates, strict=True)
                if candidate["match"]
            ]
            if len(matches) > 1:
                candidates_text = ", ".join(
                    sorted(
                        f"{match.get('pipeline_id') or match.get('pipeline')}:"
                        f"{match.get('document_type')}"
                        for match in matches
                    )
                )
                raise ValueError(
                    "Ambiguous auto-route for document "
                    f"{current.document_id!r}: {candidates_text}. "
                    "Use explicit routes or source-list pipeline_id/document_type to "
                    "disambiguate."
                )
            if matches:
                applied_routes.append(
                    _route_decision(
                        matches[0],
                        current,
                        kind="auto",
                    )
                )
                current = _apply_matching_document_route(current, matches[0])

        routed.append(current)
        decisions.append(
            _route_report_item(
                document,
                current,
                applied_routes,
                candidates,
            )
        )

    routed_count = sum(1 for decision in decisions if decision["route_count"])
    return routed, {
        "document_count": len(decisions),
        "routed_count": routed_count,
        "unrouted_count": len(decisions) - routed_count,
        "candidate_count": sum(
            decision["candidate_count"] for decision in decisions
        ),
        "matched_candidate_count": sum(
            decision["matched_candidate_count"] for decision in decisions
        ),
        "missing_pipeline_count": sum(
            1 for decision in decisions if not decision["routed"]["pipeline_id"]
        ),
        "missing_document_type_count": sum(
            1 for decision in decisions if not decision["routed"]["document_type"]
        ),
        "documents": decisions,
    }


def _auto_document_route_matches(
    route: dict[str, Any],
    document: RuntimeDocumentInput,
) -> bool:
    route_pipeline_id = route.get("pipeline_id") or route.get("pipeline")
    route_document_type = route.get("document_type")
    if document.pipeline_id is not None and route_pipeline_id != document.pipeline_id:
        return False
    if (
        document.document_type is not None
        and route_document_type != document.document_type
    ):
        return False
    if document.document_type is not None and route_document_type == document.document_type:
        return True
    match = route.get("match") if isinstance(route.get("match"), dict) else route
    extensions = {
        _normalize_extension(extension)
        for extension in _route_string_list(match.get("extensions"))
    }
    media_types = _route_string_list(match.get("media_types"))
    document_extension = _document_extension(document)
    extension_matches = bool(
        extensions
        and document_extension
        and document_extension in extensions
    )
    media_type_matches = bool(
        media_types
        and document.media_type
        and _route_any_match(document.media_type, media_types)
    )
    if extensions and document_extension and not extension_matches:
        return False
    if media_types and document.media_type and not media_type_matches:
        return False
    if extension_matches or media_type_matches:
        return True
    return _document_route_matches(route, document)


def _route_report_item(
    original: RuntimeDocumentInput,
    routed: RuntimeDocumentInput,
    applied_routes: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    original_summary = _document_route_summary(original)
    routed_summary = _document_route_summary(routed)
    matched_candidate_count = sum(1 for candidate in candidates if candidate["match"])
    return {
        "document_id": original.document_id,
        "original": original_summary,
        "routed": routed_summary,
        "changed": original_summary != routed_summary,
        "route_count": len(applied_routes),
        "routes": applied_routes,
        "candidate_count": len(candidates),
        "matched_candidate_count": matched_candidate_count,
        "candidates": candidates,
        "unmatched_reasons": [
            {
                "kind": candidate["kind"],
                "route_id": candidate["route_id"],
                "reasons": candidate["reasons"],
            }
            for candidate in candidates
            if not candidate["match"]
        ],
    }


def _document_route_summary(document: RuntimeDocumentInput) -> dict[str, Any]:
    return {
        "pipeline_id": document.pipeline_id,
        "document_type": document.document_type,
        "media_type": document.media_type,
        "source_uri": document.source_uri,
        "value_keys": sorted(document.values),
        "metadata_keys": sorted(document.metadata),
    }


def _route_decision(
    route: dict[str, Any],
    document: RuntimeDocumentInput,
    *,
    kind: str,
) -> dict[str, Any]:
    route_set = route.get("set") if isinstance(route.get("set"), dict) else route
    return {
        "kind": kind,
        "route_id": str(route.get("id") or ""),
        "pipeline_id": route_set.get("pipeline_id") or route_set.get("pipeline"),
        "document_type": route_set.get("document_type"),
        "media_type": route_set.get("media_type"),
        "evidence": _route_match_evidence(route, document),
    }


def _route_candidate(
    route: dict[str, Any],
    document: RuntimeDocumentInput,
    *,
    kind: str,
    auto: bool,
) -> dict[str, Any]:
    route_set = route.get("set") if isinstance(route.get("set"), dict) else route
    match = (
        _auto_document_route_matches(route, document)
        if auto
        else _document_route_matches(route, document)
    )
    return {
        "kind": kind,
        "route_id": str(route.get("id") or ""),
        "pipeline_id": route_set.get("pipeline_id") or route_set.get("pipeline"),
        "document_type": route_set.get("document_type"),
        "media_type": route_set.get("media_type"),
        "match": match,
        "evidence": _route_match_evidence(route, document) if match else [],
        "reasons": [] if match else _route_reject_reasons(route, document, auto=auto),
    }


def _route_match_evidence(
    route: dict[str, Any],
    document: RuntimeDocumentInput,
) -> list[dict[str, Any]]:
    match = route.get("match") if isinstance(route.get("match"), dict) else route
    evidence: list[dict[str, Any]] = []
    extensions = {
        _normalize_extension(extension)
        for extension in _route_string_list(match.get("extensions"))
    }
    document_extension = _document_extension(document)
    if extensions and document_extension and document_extension in extensions:
        evidence.append(
            {
                "field": "extension",
                "value": document_extension,
                "patterns": sorted(extensions),
            }
        )
    media_types = _route_string_list(match.get("media_types"))
    if (
        media_types
        and document.media_type
        and _route_any_match(document.media_type, media_types)
    ):
        evidence.append(
            {
                "field": "media_type",
                "value": document.media_type,
                "patterns": media_types,
            }
        )
    document_types = _route_string_list(match.get("document_types"))
    if document_types and document.document_type in set(document_types):
        evidence.append(
            {
                "field": "document_type",
                "value": document.document_type,
                "patterns": document_types,
            }
        )
    source_globs = _route_string_list(
        match.get("source_uri_globs") or match.get("source_globs")
    )
    if (
        source_globs
        and document.source_uri
        and _route_any_match(document.source_uri, source_globs)
    ):
        evidence.append(
            {
                "field": "source_uri",
                "value": document.source_uri,
                "patterns": source_globs,
            }
        )
    document_id_globs = _route_string_list(match.get("document_id_globs"))
    if document_id_globs and _route_any_match(document.document_id, document_id_globs):
        evidence.append(
            {
                "field": "document_id",
                "value": document.document_id,
                "patterns": document_id_globs,
            }
        )
    title_globs = _route_string_list(match.get("title_globs"))
    if document.title and title_globs and _route_any_match(document.title, title_globs):
        evidence.append(
            {
                "field": "title",
                "value": document.title,
                "patterns": title_globs,
            }
        )
    evidence.extend(
        _route_mapping_evidence(
            "values",
            match.get("values"),
            document.values,
        )
    )
    evidence.extend(
        _route_mapping_evidence(
            "metadata",
            match.get("metadata"),
            document.metadata,
        )
    )
    return evidence


def _route_reject_reasons(
    route: dict[str, Any],
    document: RuntimeDocumentInput,
    *,
    auto: bool,
) -> list[dict[str, Any]]:
    reasons: list[dict[str, Any]] = []
    if auto:
        route_pipeline_id = route.get("pipeline_id") or route.get("pipeline")
        route_document_type = route.get("document_type")
        if document.pipeline_id is not None and route_pipeline_id != document.pipeline_id:
            reasons.append(
                _route_reject_reason(
                    "pipeline_id",
                    "pipeline_id_mismatch",
                    actual=document.pipeline_id,
                    expected=[route_pipeline_id],
                )
            )
        if (
            document.document_type is not None
            and route_document_type != document.document_type
        ):
            reasons.append(
                _route_reject_reason(
                    "document_type",
                    "document_type_mismatch",
                    actual=document.document_type,
                    expected=[route_document_type],
                )
            )
        if reasons:
            return reasons

    match = route.get("match") if isinstance(route.get("match"), dict) else route
    extensions = {
        _normalize_extension(extension)
        for extension in _route_string_list(match.get("extensions"))
    }
    document_extension = _document_extension(document)
    if extensions and document_extension not in extensions:
        reasons.append(
            _route_reject_reason(
                "extension",
                "extension_mismatch" if document_extension else "extension_missing",
                actual=document_extension or None,
                expected=sorted(extensions),
            )
        )

    media_types = _route_string_list(match.get("media_types"))
    if media_types and not _route_any_match(document.media_type or "", media_types):
        reasons.append(
            _route_reject_reason(
                "media_type",
                "media_type_mismatch" if document.media_type else "media_type_missing",
                actual=document.media_type,
                expected=media_types,
            )
        )

    document_types = _route_string_list(match.get("document_types"))
    if document_types and (document.document_type or "") not in set(document_types):
        reasons.append(
            _route_reject_reason(
                "document_type",
                (
                    "document_type_mismatch"
                    if document.document_type
                    else "document_type_missing"
                ),
                actual=document.document_type,
                expected=document_types,
            )
        )

    source_globs = _route_string_list(
        match.get("source_uri_globs") or match.get("source_globs")
    )
    if source_globs and not _route_any_match(document.source_uri or "", source_globs):
        reasons.append(
            _route_reject_reason(
                "source_uri",
                "source_uri_mismatch" if document.source_uri else "source_uri_missing",
                actual=document.source_uri,
                expected=source_globs,
            )
        )

    document_id_globs = _route_string_list(match.get("document_id_globs"))
    if document_id_globs and not _route_any_match(document.document_id, document_id_globs):
        reasons.append(
            _route_reject_reason(
                "document_id",
                "document_id_mismatch",
                actual=document.document_id,
                expected=document_id_globs,
            )
        )

    title_globs = _route_string_list(match.get("title_globs"))
    if title_globs and not _route_any_match(document.title or "", title_globs):
        reasons.append(
            _route_reject_reason(
                "title",
                "title_mismatch" if document.title else "title_missing",
                actual=document.title,
                expected=title_globs,
            )
        )

    reasons.extend(
        _route_mapping_reject_reasons(
            "values",
            match.get("values"),
            document.values,
        )
    )
    reasons.extend(
        _route_mapping_reject_reasons(
            "metadata",
            match.get("metadata"),
            document.metadata,
        )
    )
    return reasons or [
        _route_reject_reason(
            "route",
            "route_not_matched",
            actual=None,
            expected=[],
        )
    ]


def _route_mapping_reject_reasons(
    field: str,
    expected: Any,
    actual: dict[str, Any],
) -> list[dict[str, Any]]:
    if expected is None:
        return []
    if not isinstance(expected, dict):
        raise ValueError("Document route values/metadata match must be an object")
    reasons: list[dict[str, Any]] = []
    for key, value in expected.items():
        actual_key = str(key)
        actual_value = actual.get(actual_key)
        if str(actual.get(actual_key, "")) == str(value):
            continue
        reasons.append(
            _route_reject_reason(
                f"{field}.{key}",
                f"{field}_mismatch" if actual_value is not None else f"{field}_missing",
                actual=actual_value,
                expected=[value],
            )
        )
    return reasons


def _route_reject_reason(
    field: str,
    reason: str,
    *,
    actual: Any,
    expected: list[Any],
) -> dict[str, Any]:
    return {
        "field": field,
        "reason": reason,
        "actual": actual,
        "expected": expected,
    }


def _route_mapping_evidence(
    field: str,
    expected: Any,
    actual: dict[str, Any],
) -> list[dict[str, Any]]:
    if expected is None:
        return []
    if not isinstance(expected, dict):
        raise ValueError("Document route values/metadata match must be an object")
    evidence: list[dict[str, Any]] = []
    for key, value in expected.items():
        if str(actual.get(str(key), "")) == str(value):
            evidence.append(
                {
                    "field": f"{field}.{key}",
                    "value": actual.get(str(key)),
                    "patterns": [value],
                }
            )
    return evidence


def _apply_document_route(
    document: RuntimeDocumentInput,
    routes: list[dict[str, Any]],
) -> RuntimeDocumentInput:
    for route in routes:
        if not _document_route_matches(route, document):
            continue
        return _apply_matching_document_route(document, route)
    return document


def _apply_matching_document_route(
    document: RuntimeDocumentInput,
    route: dict[str, Any],
) -> RuntimeDocumentInput:
    updates: dict[str, Any] = {}
    route_set = route.get("set") if isinstance(route.get("set"), dict) else route
    pipeline_id = route_set.get("pipeline_id") or route_set.get("pipeline")
    if document.pipeline_id is None and pipeline_id:
        updates["pipeline_id"] = str(pipeline_id).strip()
    document_type = route_set.get("document_type")
    if document.document_type is None and document_type:
        updates["document_type"] = str(document_type).strip()
    media_type = route_set.get("media_type")
    if (
        document.media_type is None
        or document.media_type == "application/octet-stream"
    ) and media_type:
        updates["media_type"] = str(media_type).strip()
    values = route_set.get("values")
    if isinstance(values, dict):
        updates["values"] = {**values, **document.values}
    metadata = route_set.get("metadata")
    if isinstance(metadata, dict):
        updates["metadata"] = {**metadata, **document.metadata}
    payload = document.model_dump(mode="python")
    payload.update(updates)
    return RuntimeDocumentInput.model_validate(payload)


def _document_route_matches(
    route: dict[str, Any],
    document: RuntimeDocumentInput,
) -> bool:
    match = route.get("match") if isinstance(route.get("match"), dict) else route
    extensions = _route_string_list(match.get("extensions"))
    if extensions and _document_extension(document) not in {
        _normalize_extension(extension) for extension in extensions
    }:
        return False
    media_types = _route_string_list(match.get("media_types"))
    if media_types and not _route_any_match(document.media_type or "", media_types):
        return False
    document_types = _route_string_list(match.get("document_types"))
    if document_types and (document.document_type or "") not in set(document_types):
        return False
    source_globs = _route_string_list(
        match.get("source_uri_globs") or match.get("source_globs")
    )
    if source_globs and not _route_any_match(document.source_uri or "", source_globs):
        return False
    document_id_globs = _route_string_list(match.get("document_id_globs"))
    if document_id_globs and not _route_any_match(document.document_id, document_id_globs):
        return False
    title_globs = _route_string_list(match.get("title_globs"))
    if title_globs and not _route_any_match(document.title or "", title_globs):
        return False
    if not _route_mapping_matches(match.get("values"), document.values):
        return False
    if not _route_mapping_matches(match.get("metadata"), document.metadata):
        return False
    return True


def _route_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        if not all(isinstance(item, str) for item in value):
            raise ValueError("Document route match lists must contain strings")
        return list(value)
    raise ValueError("Document route match value must be a string or list")


def _route_any_match(value: str, patterns: list[str]) -> bool:
    folded = value.lower()
    return any(fnmatch.fnmatch(folded, pattern.lower()) for pattern in patterns)


def _route_mapping_matches(expected: Any, actual: dict[str, Any]) -> bool:
    if expected is None:
        return True
    if not isinstance(expected, dict):
        raise ValueError("Document route values/metadata match must be an object")
    for key, value in expected.items():
        if str(actual.get(str(key), "")) != str(value):
            return False
    return True


def _document_extension(document: RuntimeDocumentInput) -> str:
    names = [
        document.source_uri or "",
        document.title or "",
        document.document_id,
    ]
    for name in names:
        parsed = urlparse(name)
        suffix = Path(unquote(parsed.path or name)).suffix
        if suffix:
            return _normalize_extension(suffix)
    return ""


def _normalize_extension(value: str) -> str:
    normalized = value.strip().lower()
    if normalized and not normalized.startswith("."):
        normalized = f".{normalized}"
    return normalized
