"""Public FEP checkers usable by sibling effector packages."""

from __future__ import annotations

import json
import pytest

from fala.conformance import (
    check_answer,
    check_message,
    check_payload,
    check_result,
    corpus_dir,
    dialogue_negatives,
    envelope_negatives,
    exercise_handler,
    make_request,
    payload_cases,
    run_conformance,
    valid_request_text,
    valid_result_text,
)
from fala.protocol import (
    ProtocolError,
    Result,
    assert_answers,
    build_result,
    contract_pair_from_schema,
    parse,
)

_ECHO_SCHEMA = {
    "type": "object",
    "required": ["text"],
    "properties": {"text": {"type": "string"}},
}


def test_corpus_dir_resolves_shared_vectors():
    root = corpus_dir()
    assert (root / "request.valid.json").is_file()
    assert (root / "dialogue.negative.json").is_file()
    assert (root / "payload.cases.json").is_file()


def test_check_message_accepts_goldens():
    request = check_message(valid_request_text(), expected_kind="request")
    result = check_message(valid_result_text(), expected_kind="result")
    check_answer(request, result)


@pytest.mark.parametrize("case", envelope_negatives())
def test_envelope_negatives_via_checker(case):
    with pytest.raises(ProtocolError) as caught:
        check_message(case["message"])
    assert (caught.value.code, caught.value.pointer) == (case["code"], case["pointer"])


@pytest.mark.parametrize("case", dialogue_negatives())
def test_dialogue_negatives_via_checker(case):
    with pytest.raises(ProtocolError) as caught:
        check_answer(case["request"], case["result"])
    assert (caught.value.code, caught.value.pointer) == (case["code"], case["pointer"])


@pytest.mark.parametrize("case", payload_cases())
def test_payload_cases_via_checker(case):
    if case.get("accept", False):
        check_payload(case["payload"], case["schema"])
        return
    with pytest.raises(ProtocolError) as caught:
        check_payload(case["payload"], case["schema"])
    assert (caught.value.code, caught.value.pointer) == (case["code"], case["pointer"])


def test_check_result_with_schema():
    request = parse(valid_request_text(), "request")
    result = build_result(request, payload={"text": "hello"})
    checked = check_result(
        request,
        result,
        schema={"type": "object", "required": ["text"], "properties": {"text": {"type": "string"}}},
    )
    assert checked.payload == {"text": "hello"}


def test_assert_answers_rejects_wrong_direction():
    request = parse(valid_request_text(), "request")
    result = Result(
        sender=request.sender,
        job=request.job,
        ref=request.id,
        payload={"text": "hello"},
        recipient=request.sender,
        contract_id=request.contract_id,
        contract_version=request.contract_version,
    )
    with pytest.raises(ProtocolError) as caught:
        assert_answers(request, result)
    assert caught.value.code == "fep.direction_mismatch"


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
