"""Public FEP checkers usable by sibling effector packages."""

from __future__ import annotations

import json
import pytest

from fala.conformance import (
    check_message,
    check_result,
    exercise_handler,
    make_request,
    run_conformance,
    valid_request_text,
)
from fala.protocol import (
    ProtocolError,
    Result,
    build_result,
    contract_pair_from_schema,
    parse,
)

_ECHO_SCHEMA = {
    "type": "object",
    "required": ["text"],
    "properties": {"text": {"type": "string"}},
}


def test_check_result_with_schema():
    request = parse(valid_request_text(), "request")
    result = build_result(request, payload={"text": "hello"})
    checked = check_result(
        request,
        result,
        schema={"type": "object", "required": ["text"], "properties": {"text": {"type": "string"}}},
    )
    assert checked.payload == {"text": "hello"}


def test_run_conformance_full_ok():
    report = run_conformance()
    assert report["ok"], json.dumps(report, indent=2)
    assert set(report["layers"]) == {"L0", "L1", "L2"}


def test_run_conformance_partial_layers():
    report = run_conformance(layers=["L0", "L1"])
    assert report["ok"]
    assert set(report["layers"]) == {"L0", "L1"}


def test_make_request_stamps_schema_contract():
    request = make_request("echo", {"text": "hello"}, schema=_ECHO_SCHEMA)
    contract_id, contract_version = contract_pair_from_schema(_ECHO_SCHEMA)
    assert request.job == "echo"
    assert request.recipient == "echo"
    assert request.payload == {"text": "hello"}
    assert request.contract_id == contract_id
    assert request.contract_version == contract_version
    check_message(request, expected_kind="request")


def test_exercise_handler_accepts_echo():
    def echo(request):
        return Result.from_request(request, payload=dict(request.payload))

    result = exercise_handler(echo, {"text": "hello"}, _ECHO_SCHEMA, job="echo")
    assert result.payload == {"text": "hello"}
    assert result.job == "echo"


def test_exercise_handler_rejects_payload_outside_schema():
    def liar(request):
        return Result.from_request(request, payload={"n": 1})

    with pytest.raises(ProtocolError) as caught:
        exercise_handler(liar, {"text": "hello"}, _ECHO_SCHEMA, job="echo")
    assert caught.value.code == "fep.payload_invalid"


def test_exercise_handler_rejects_wrong_job():
    def liar(request):
        return Result(
            sender=request.recipient,
            job="other",
            ref=request.id,
            payload=dict(request.payload),
            recipient=request.sender,
            contract_id=request.contract_id,
            contract_version=request.contract_version,
        )

    with pytest.raises(ProtocolError) as caught:
        exercise_handler(liar, {"text": "hello"}, _ECHO_SCHEMA, job="echo")
    assert caught.value.code == "fep.job_mismatch"
