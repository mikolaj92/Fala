from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from jsonschema import Draft202012Validator, SchemaError


def validate_json_schema(schema: dict[str, Any], *, label: str) -> None:
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:
        raise ValueError(f"{label} JSON schema is invalid: {exc.message}") from exc


def validate_json_value(
    value: Any,
    schema: dict[str, Any],
    *,
    label: str,
) -> None:
    if not schema:
        return
    validate_json_schema(schema, label=label)
    validator = Draft202012Validator(schema)
    errors = sorted(
        validator.iter_errors(value),
        key=lambda error: list(error.absolute_path),
    )
    if not errors:
        return
    error = errors[0]
    raise ValueError(
        f"{label} does not match schema at {_json_path(error.absolute_path)}: "
        f"{error.message}"
    )


def _json_path(path: Iterable[object]) -> str:
    rendered = "$"
    for item in path:
        if isinstance(item, int):
            rendered += f"[{item}]"
        elif isinstance(item, str) and item.isidentifier():
            rendered += f".{item}"
        else:
            rendered += f"[{item!r}]"
    return rendered
