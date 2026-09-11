"""Typed parent-child protocol for Fala.

The wire representation is JSON, but the public construction boundary is the
pair of immutable :class:`Request` and :class:`Result` objects.  Mapping values
are accepted only while decoding an already-existing wire document; callers
must construct a typed object before speaking or writing a result.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, TypeAlias

PROTOCOL = "fala"
_REQUEST_KIND = "request"
_RESULT_KIND = "result"
_REQUEST_FIELDS = frozenset(
    {"protocol", "kind", "id", "from", "to", "job", "payload", "config"}
)
_RESULT_FIELDS = frozenset(
    {"protocol", "kind", "id", "from", "to", "job", "ref", "status", "payload"}
)
_STATUSES = frozenset({"ok", "error"})


class ProtocolError(ValueError):
    """Stable protocol validation error with a JSON pointer."""

    def __init__(self, code: str, pointer: str):
        self.code, self.pointer = code, pointer
        super().__init__(f"{code} at {pointer or '/'}")


def _wire_value(value: Any) -> dict[str, Any]:
    if isinstance(value, Request | Result):
        return value.to_message()
    if not isinstance(value, Mapping):
        raise TypeError("fala message must be a Request or Result")
    return dict(value)


def canonical(value: Mapping[str, Any] | Request | Result) -> str:
    """Serialize one message with the canonical JSON spelling."""

    return json.dumps(
        _wire_value(value),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _message_id(body: Mapping[str, Any]) -> str:
    return "msg:sha256:" + hashlib.sha256(canonical(body).encode("utf-8")).hexdigest()


def _require_text(value: Mapping[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ProtocolError("fep.required", "/" + key)
    return item


def _require_object(value: Mapping[str, Any], key: str) -> dict[str, Any]:
    item = value.get(key)
    if not isinstance(item, dict):
        raise ProtocolError("fep.type_invalid", "/" + key)
    return dict(item)


@dataclass(frozen=True)
class Request:
    """One parent-to-child Fala request.

    ``id`` is derived from the canonical body and is never caller-supplied.
    ``recipient`` is the object-level spelling of the wire field ``to``.
    """

    sender: str
    recipient: str
    job: str
    payload: Mapping[str, Any]
    config: Mapping[str, Any] = field(default_factory=dict)
    id: str = field(init=False)

    def __post_init__(self) -> None:
        if not self.sender or not self.recipient or not self.job:
            raise ProtocolError("fep.required", "/from")
        if not isinstance(self.payload, Mapping):
            raise ProtocolError("fep.type_invalid", "/payload")
        if not isinstance(self.config, Mapping):
            raise ProtocolError("fep.type_invalid", "/config")
        object.__setattr__(self, "payload", dict(self.payload))
        object.__setattr__(self, "config", dict(self.config))
        body = {
            "protocol": PROTOCOL,
            "kind": _REQUEST_KIND,
            "from": self.sender,
            "to": self.recipient,
            "job": self.job,
            "payload": dict(self.payload),
            "config": dict(self.config),
        }
        object.__setattr__(self, "id", _message_id(body))

    @property
    def kind(self) -> str:
        return _REQUEST_KIND

    def to_message(self) -> dict[str, Any]:
        return {
            "protocol": PROTOCOL,
            "kind": _REQUEST_KIND,
            "from": self.sender,
            "to": self.recipient,
            "job": self.job,
            "payload": dict(self.payload),
            "config": dict(self.config),
            "id": self.id,
        }

    def __getitem__(self, key: str) -> Any:
        return self.to_message()[key]


@dataclass(frozen=True)
class Result:
    """One child-to-parent Fala result.

    ``id`` is derived from the result body.  ``ref`` is the id of the request
    being answered; it is deliberately distinct from the result's own id.
    """

    sender: str
    job: str
    ref: str
    payload: Mapping[str, Any]
    status: str = "ok"
    recipient: str = "parent"
    id: str = field(init=False)

    def __post_init__(self) -> None:
        if not self.sender or not self.recipient or not self.job or not self.ref:
            raise ProtocolError("fep.required", "/ref")
        if self.status not in _STATUSES:
            raise ProtocolError("fep.status_invalid", "/status")
        if not isinstance(self.payload, Mapping):
            raise ProtocolError("fep.type_invalid", "/payload")
        object.__setattr__(self, "payload", dict(self.payload))
        body = {
            "protocol": PROTOCOL,
            "kind": _RESULT_KIND,
            "from": self.sender,
            "to": self.recipient,
            "job": self.job,
            "ref": self.ref,
            "status": self.status,
            "payload": dict(self.payload),
        }
        object.__setattr__(self, "id", _message_id(body))

    @property
    def kind(self) -> str:
        return _RESULT_KIND

    def to_message(self) -> dict[str, Any]:
        return {
            "protocol": PROTOCOL,
            "kind": _RESULT_KIND,
            "from": self.sender,
            "to": self.recipient,
            "job": self.job,
            "ref": self.ref,
            "status": self.status,
            "payload": dict(self.payload),
            "id": self.id,
        }

    def __getitem__(self, key: str) -> Any:
        return self.to_message()[key]

    @classmethod
    def ok(
        cls,
        *,
        sender: str,
        job: str,
        ref: str,
        payload: Mapping[str, Any],
        recipient: str = "parent",
    ) -> Result:
        return cls(
            sender=sender,
            job=job,
            ref=ref,
            payload=payload,
            status="ok",
            recipient=recipient,
        )

    @classmethod
    def error(
        cls,
        *,
        sender: str,
        job: str,
        ref: str,
        payload: Mapping[str, Any],
        recipient: str = "parent",
    ) -> Result:
        return cls(
            sender=sender,
            job=job,
            ref=ref,
            payload=payload,
            status="error",
            recipient=recipient,
        )

    @classmethod
    def from_request(
        cls,
        request: Request,
        *,
        payload: Mapping[str, Any],
        status: str = "ok",
    ) -> Result:
        return cls(
            sender=request.recipient,
            recipient=request.sender,
            job=request.job,
            ref=request.id,
            payload=payload,
            status=status,
        )


Message: TypeAlias = Request | Result


def _validated_wire(message: Mapping[str, Any], expected_kind: str | None) -> Message:
    value = dict(message)
    if value.get("protocol") != PROTOCOL:
        raise ProtocolError("fep.unknown_protocol", "/protocol")
    kind = value.get("kind")
    if kind not in (_REQUEST_KIND, _RESULT_KIND):
        raise ProtocolError("fep.unknown_message_kind", "/kind")
    if expected_kind and kind != expected_kind:
        raise ProtocolError("fep.unexpected_message_kind", "/kind")
    allowed = _REQUEST_FIELDS if kind == _REQUEST_KIND else _RESULT_FIELDS
    unknown = value.keys() - allowed
    if unknown:
        raise ProtocolError("fep.unknown_field", "/" + sorted(unknown)[0])
    sender = _require_text(value, "from")
    recipient = _require_text(value, "to")
    job = _require_text(value, "job")
    payload = _require_object(value, "payload")
    supplied = _require_text(value, "id")
    if kind == _REQUEST_KIND:
        config = _require_object(value, "config")
        body = dict(value)
        body.pop("id")
        if supplied != _message_id(body):
            raise ProtocolError("fep.digest_mismatch", "/id")
        request = Request(
            sender=sender,
            recipient=recipient,
            job=job,
            payload=payload,
            config=config,
        )
        if request.id != supplied:
            raise ProtocolError("fep.digest_mismatch", "/id")
        return request
    ref = _require_text(value, "ref")
    status = value.get("status")
    if status not in _STATUSES:
        raise ProtocolError("fep.status_invalid", "/status")
    body = dict(value)
    body.pop("id")
    if supplied != _message_id(body):
        raise ProtocolError("fep.digest_mismatch", "/id")
    result = Result(
        sender=sender,
        recipient=recipient,
        job=job,
        ref=ref,
        status=status,
        payload=payload,
    )
    if result.id != supplied:
        raise ProtocolError("fep.digest_mismatch", "/id")
    return result


def validate(
    message: Mapping[str, Any] | Request | Result,
    expected_kind: str | None = None,
) -> Message:
    """Validate an existing message and return its typed representation."""

    if isinstance(message, Request | Result):
        if expected_kind and message.kind != expected_kind:
            raise ProtocolError("fep.unexpected_message_kind", "/kind")
        return _validated_wire(message.to_message(), expected_kind)
    return _validated_wire(message, expected_kind)


def parse(
    text: str | Request | Result,
    expected_kind: str | None = None,
) -> Message:
    """Parse one wire document into a typed :class:`Request` or :class:`Result`."""

    if isinstance(text, Request | Result):
        return validate(text, expected_kind)
    try:
        value = json.loads(text)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ProtocolError("fep.invalid_json", "/") from exc
    if not isinstance(value, dict):
        raise ProtocolError("fep.invalid_json", "/")
    return validate(value, expected_kind)


def request_message(
    *,
    sender: str = "parent",
    recipient: str,
    job: str,
    payload: Mapping[str, Any],
    config: Mapping[str, Any] | None = None,
) -> Request:
    """Construct a typed request; the request id is derived automatically."""

    return Request(
        sender=sender,
        recipient=recipient,
        job=job,
        payload=payload,
        config={} if config is None else config,
    )


def result_message(
    *,
    sender: str,
    job: str,
    ref: str,
    payload: Mapping[str, Any],
    status: str = "ok",
    recipient: str = "parent",
) -> Result:
    """Construct a typed result; the result id is derived automatically."""

    return Result(
        sender=sender,
        recipient=recipient,
        job=job,
        ref=ref,
        status=status,
        payload=payload,
    )


def build_result(
    request: Mapping[str, Any] | Request,
    *,
    payload: Mapping[str, Any],
    status: str = "ok",
) -> Result:
    """Build a typed result that answers ``request``."""

    typed = request if isinstance(request, Request) else validate(request, "request")
    if not isinstance(typed, Request):
        raise ProtocolError("fep.unexpected_message_kind", "/kind")
    return Result.from_request(typed, payload=payload, status=status)


def speak(
    message: Message | str,
    expected_kind: str | None = None,
) -> Message:
    """Validate a typed message after a canonical bytes round-trip.

    A mapping is intentionally not accepted here.  Transport code must first
    construct :class:`Request` or :class:`Result`, which is where identity,
    status, and payload shape become mandatory.
    """

    if isinstance(message, str):
        return parse(message, expected_kind)
    if not isinstance(message, Request | Result):
        raise TypeError("fala.speak accepts only Request or Result")
    parsed = parse(canonical(message), expected_kind)
    if type(parsed) is not type(message):
        raise ProtocolError("fep.unexpected_message_kind", "/kind")
    return parsed


__all__ = [
    "ProtocolError",
    "Message",
    "PROTOCOL",
    "Request",
    "Result",
    "canonical",
    "parse",
    "build_result",
    "request_message",
    "result_message",
    "speak",
    "validate",
]
