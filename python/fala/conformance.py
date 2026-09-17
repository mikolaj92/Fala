"""Shared FEP checkers and conformance corpus for effector packages.

Other packages can validate wire results without the Fala runtime::

    from fala.conformance import check_answer, check_message, check_payload
    from fala.protocol import Result

    request = check_message(manifest_text, expected_kind="request")
    result = Result.from_request(request, payload={"text": "hello"})
    check_answer(request, result)
    check_payload(result.payload, schema)

Partial suite (envelope + dialogue + optional payload cases)::

    from fala.conformance import run_conformance
    report = run_conformance()
    assert report["ok"]
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any

from .protocol import (
    Message,
    ProtocolError,
    Request,
    Result,
    assert_answers,
    parse,
    validate,
)

_REPO_CORPUS = Path(__file__).resolve().parents[2] / "conformance" / "fala"
_PACKAGE_CORPUS = Path(__file__).resolve().parent / "conformance"


def corpus_dir() -> Path:
    """Return the shared conformance directory (repo checkout or wheel data)."""

    if (_REPO_CORPUS / "request.valid.json").is_file():
        return _REPO_CORPUS
    packaged = _PACKAGE_CORPUS
    if (packaged / "request.valid.json").is_file():
        return packaged
    try:
        root = resources.files("fala").joinpath("conformance")
        path = Path(str(root))
        if (path / "request.valid.json").is_file():
            return path
    except (TypeError, FileNotFoundError, ModuleNotFoundError):
        pass
    raise FileNotFoundError("fala conformance corpus not found (request.valid.json)")


def _load_json(name: str) -> Any:
    return json.loads((corpus_dir() / name).read_text(encoding="utf-8"))


def valid_request_text() -> str:
    return (corpus_dir() / "request.valid.json").read_text(encoding="utf-8").strip()


def valid_result_text() -> str:
    return (corpus_dir() / "result.valid.json").read_text(encoding="utf-8").strip()


def envelope_negatives() -> list[dict[str, Any]]:
    """L0 single-message negatives: ``name``, ``code``, ``pointer``, ``message``."""

    cases = _load_json("negative.json")
    for case in cases:
        case.setdefault("layer", "L0")
    return cases


def dialogue_negatives() -> list[dict[str, Any]]:
    """L1 request/result pair negatives."""

    return list(_load_json("dialogue.negative.json"))


def payload_cases() -> list[dict[str, Any]]:
    """L2 payload/schema cases (partial subset for unit checks)."""

    return list(_load_json("payload.cases.json"))


def check_message(
    message: Mapping[str, Any] | Message | str,
    *,
    expected_kind: str | None = None,
) -> Message:
    """L0: validate one FEP envelope; return the typed message."""

    if isinstance(message, str):
        return parse(message, expected_kind)
    return validate(message, expected_kind)


def check_answer(
    request: Mapping[str, Any] | Request | str,
    result: Mapping[str, Any] | Result | str,
) -> None:
    """L1: ``result`` must answer ``request`` (direction, job, ref, contract)."""

    assert_answers(request, result)


def _json_pointer_token(key: str) -> str:
    return key.replace("~", "~0").replace("/", "~1")


def _payload_type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _type_matches(value: Any, declared: str) -> bool:
    actual = _payload_type_name(value)
    if declared == "number":
        return actual in {"number", "integer"}
    if declared == "integer":
        return actual == "integer"
    return actual == declared


_SUPPORTED_SCHEMA_KEYS = frozenset(
    {
        "type",
        "const",
        "enum",
        "required",
        "properties",
        "items",
        "minimum",
        "maximum",
        "minLength",
        "maxLength",
        "minItems",
        "maxItems",
        "additionalProperties",
        "oneOf",
        "anyOf",
        "allOf",
        "title",
        "description",
        "$comment",
    }
)


def check_payload(payload: Any, schema: Mapping[str, Any], *, path: str = "/payload") -> None:
    """L2 (partial): validate ``payload`` against a supported schema subset.

    Full journal evaluation remains authoritative for production hosts. This
    checker is for effector packages that need local fail-closed unit checks
    without opening a Fala journal.
    """

    if not isinstance(schema, Mapping):
        raise ProtocolError("fep.payload_invalid", path)
    unknown = set(schema) - _SUPPORTED_SCHEMA_KEYS
    if unknown:
        raise ProtocolError("fep.payload_invalid", path + "/" + _json_pointer_token(sorted(unknown)[0]))

    if "const" in schema and payload != schema["const"]:
        raise ProtocolError("fep.payload_invalid", path)
    if "enum" in schema:
        options = schema["enum"]
        if not isinstance(options, list) or payload not in options:
            raise ProtocolError("fep.payload_invalid", path)

    declared_type = schema.get("type")
    if isinstance(declared_type, str):
        if not _type_matches(payload, declared_type):
            raise ProtocolError("fep.payload_invalid", path)
    elif isinstance(declared_type, list):
        if not any(isinstance(item, str) and _type_matches(payload, item) for item in declared_type):
            raise ProtocolError("fep.payload_invalid", path)

    if isinstance(payload, str):
        if "minLength" in schema and len(payload) < int(schema["minLength"]):
            raise ProtocolError("fep.payload_invalid", path)
        if "maxLength" in schema and len(payload) > int(schema["maxLength"]):
            raise ProtocolError("fep.payload_invalid", path)

    if isinstance(payload, bool):
        pass
    elif isinstance(payload, (int, float)):
        if "minimum" in schema and payload < schema["minimum"]:
            raise ProtocolError("fep.payload_invalid", path)
        if "maximum" in schema and payload > schema["maximum"]:
            raise ProtocolError("fep.payload_invalid", path)

    if isinstance(payload, list):
        if "minItems" in schema and len(payload) < int(schema["minItems"]):
            raise ProtocolError("fep.payload_invalid", path)
        if "maxItems" in schema and len(payload) > int(schema["maxItems"]):
            raise ProtocolError("fep.payload_invalid", path)
        item_schema = schema.get("items")
        if isinstance(item_schema, Mapping):
            for index, item in enumerate(payload):
                check_payload(item, item_schema, path=f"{path}/{index}")

    if isinstance(payload, dict):
        required = schema.get("required", [])
        if isinstance(required, list):
            for key in required:
                if not isinstance(key, str) or key not in payload:
                    raise ProtocolError("fep.payload_invalid", path + "/" + _json_pointer_token(str(key)))
        properties = schema.get("properties")
        if isinstance(properties, Mapping):
            for key, child_schema in properties.items():
                if key in payload and isinstance(child_schema, Mapping):
                    check_payload(payload[key], child_schema, path=path + "/" + _json_pointer_token(key))
        additional = schema.get("additionalProperties")
        if additional is False and isinstance(properties, Mapping):
            for key in payload:
                if key not in properties:
                    raise ProtocolError("fep.payload_invalid", path + "/" + _json_pointer_token(key))

    for combinator in ("oneOf", "anyOf", "allOf"):
        options = schema.get(combinator)
        if options is None:
            continue
        if not isinstance(options, list) or not options:
            raise ProtocolError("fep.payload_invalid", path + "/" + combinator)
        matches = 0
        for option in options:
            if not isinstance(option, Mapping):
                raise ProtocolError("fep.payload_invalid", path + "/" + combinator)
            try:
                check_payload(payload, option, path=path)
            except ProtocolError:
                continue
            matches += 1
        if combinator == "oneOf" and matches != 1:
            raise ProtocolError("fep.payload_invalid", path)
        if combinator == "anyOf" and matches < 1:
            raise ProtocolError("fep.payload_invalid", path)
        if combinator == "allOf" and matches != len(options):
            raise ProtocolError("fep.payload_invalid", path)


def check_result(
    request: Mapping[str, Any] | Request | str,
    result: Mapping[str, Any] | Result | str,
    *,
    schema: Mapping[str, Any] | None = None,
) -> Result:
    """Validate a result against its request; optionally check payload schema."""

    typed_result = check_message(result, expected_kind="result")
    if not isinstance(typed_result, Result):
        raise ProtocolError("fep.unexpected_message_kind", "/kind")
    check_answer(request, typed_result)
    if schema is not None:
        check_payload(typed_result.payload, schema)
    return typed_result


@dataclass(frozen=True)
class ConformanceFailure:
    layer: str
    name: str
    code: str
    pointer: str
    detail: str = ""


def run_conformance(
    *,
    layers: Iterable[str] = ("L0", "L1", "L2"),
) -> dict[str, Any]:
    """Run the shared corpus and return a JSON-serializable report.

    Ports and sibling packages call this for partial or full FEP conformance
    without constructing a journal.
    """

    wanted = {layer.upper() for layer in layers}
    failures: list[ConformanceFailure] = []

    if "L0" in wanted:
        check_message(valid_request_text(), expected_kind="request")
        check_message(valid_result_text(), expected_kind="result")
        for case in envelope_negatives():
            try:
                check_message(case["message"])
            except ProtocolError as exc:
                if (exc.code, exc.pointer) != (case["code"], case["pointer"]):
                    failures.append(
                        ConformanceFailure(
                            "L0",
                            case["name"],
                            exc.code,
                            exc.pointer,
                            f"expected {case['code']} at {case['pointer']}",
                        )
                    )
            else:
                failures.append(
                    ConformanceFailure("L0", case["name"], "expected_failure", "/", "negative accepted")
                )

    if "L1" in wanted:
        request = check_message(valid_request_text(), expected_kind="request")
        result = check_message(valid_result_text(), expected_kind="result")
        check_answer(request, result)
        for case in dialogue_negatives():
            try:
                check_answer(case["request"], case["result"])
            except ProtocolError as exc:
                if (exc.code, exc.pointer) != (case["code"], case["pointer"]):
                    failures.append(
                        ConformanceFailure(
                            "L1",
                            case["name"],
                            exc.code,
                            exc.pointer,
                            f"expected {case['code']} at {case['pointer']}",
                        )
                    )
            else:
                failures.append(
                    ConformanceFailure("L1", case["name"], "expected_failure", "/", "negative accepted")
                )

    if "L2" in wanted:
        for case in payload_cases():
            try:
                check_payload(case["payload"], case["schema"])
            except ProtocolError as exc:
                if case.get("accept", False):
                    failures.append(
                        ConformanceFailure("L2", case["name"], exc.code, exc.pointer, "valid case rejected")
                    )
                elif case.get("code") and (exc.code, exc.pointer) != (case["code"], case["pointer"]):
                    failures.append(
                        ConformanceFailure(
                            "L2",
                            case["name"],
                            exc.code,
                            exc.pointer,
                            f"expected {case['code']} at {case['pointer']}",
                        )
                    )
            else:
                if not case.get("accept", False):
                    failures.append(
                        ConformanceFailure("L2", case["name"], "expected_failure", "/", "negative accepted")
                    )

    return {
        "ok": not failures,
        "layers": sorted(wanted),
        "failures": [
            {
                "layer": item.layer,
                "name": item.name,
                "code": item.code,
                "pointer": item.pointer,
                "detail": item.detail,
            }
            for item in failures
        ],
    }


__all__ = [
    "ConformanceFailure",
    "check_answer",
    "check_message",
    "check_payload",
    "check_result",
    "corpus_dir",
    "dialogue_negatives",
    "envelope_negatives",
    "payload_cases",
    "run_conformance",
    "valid_request_text",
    "valid_result_text",
]
