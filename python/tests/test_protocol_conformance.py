import json
from pathlib import Path
import pytest
from fala.protocol import (
    PROTOCOL,
    ProtocolError,
    Request,
    Result,
    assert_answers,
    build_result,
    canonical,
    parse,
    speak,
)

from fala.sdk import load_manifest, write_result

ROOT = Path(__file__).resolve().parents[2] / "conformance" / "fala"
NEGATIVES = json.loads((ROOT / "negative.json").read_text())


def test_protocol_is_unversioned_fala():
    assert PROTOCOL == "fala"
    assert "fala/" not in PROTOCOL


@pytest.mark.parametrize(
    "result",
    [
        {"protocol": "unknown", "kind": "result", "from": "echo", "to": "parent", "job": "echo", "ref": "x", "status": "ok", "payload": {}, "id": "y", "contract_id": "echo-output", "contract_version": "1"},
    ],
)
def test_write_result_rejects_unknown_protocol(tmp_path, result):
    with pytest.raises(Exception):
        write_result(result, env={"FALA_EFFECTOR_OUTPUT_DIR": str(tmp_path)})


def test_write_result_preserves_valid_protocol_message(tmp_path):
    result = parse((ROOT / "result.valid.json").read_text(), "result")
    path = write_result(result, env={"FALA_EFFECTOR_OUTPUT_DIR": str(tmp_path)})
    assert path.exists()
    assert parse(path.read_text(), "result") == result


def test_write_result_rejects_mapping_even_when_it_is_a_valid_message(tmp_path):
    request = parse((ROOT / "request.valid.json").read_text(), "request")
    result = build_result(request, payload={"text": "hello"})
    with pytest.raises(TypeError):
        write_result(result.to_message(), env={"FALA_EFFECTOR_OUTPUT_DIR": str(tmp_path)})


def test_write_result_accepts_result_object(tmp_path):
    request = parse((ROOT / "request.valid.json").read_text(), "request")
    path = write_result(
        build_result(request, payload={"text": "hello"}),
        env={"FALA_EFFECTOR_OUTPUT_DIR": str(tmp_path)},
    )
    assert parse(path.read_text(), "result").payload == {"text": "hello"}


@pytest.mark.parametrize("case", NEGATIVES)
def test_write_result_rejects_invalid_protocol_before_writing(tmp_path, case):
    with pytest.raises(Exception):
        write_result(case["message"], env={"FALA_EFFECTOR_OUTPUT_DIR": str(tmp_path)})


def test_write_result_rejects_request_message(tmp_path):
    request = parse((ROOT / "request.valid.json").read_text(), "request")
    with pytest.raises(Exception):
        write_result(request, env={"FALA_EFFECTOR_OUTPUT_DIR": str(tmp_path)})


def test_python_codec_matches_shared_goldens():
    request_text = (ROOT / "request.valid.json").read_text().strip()
    result_text = (ROOT / "result.valid.json").read_text().strip()
    request = parse(request_text, "request")
    assert canonical(request) == request_text
    result = build_result(request, payload={"text": "hello"})
    assert canonical(result) == result_text
    assert speak(result) == result


@pytest.mark.parametrize("case", NEGATIVES)
def test_python_codec_stable_negative_classes(case):
    with pytest.raises(ProtocolError) as caught:
        parse(json.dumps(case["message"]))
    assert (caught.value.code, caught.value.pointer) == (case["code"], case["pointer"])


def test_required_contract_pair_round_trips():
    request = Request(
        sender="parent",
        recipient="echo",
        job="echo",
        payload={"text": "hello"},
        contract_id="echo-output",
        contract_version="1",
    )
    spoken = speak(request, "request")
    assert spoken.contract_id == "echo-output"
    assert spoken.contract_version == "1"
    wire = canonical(spoken)
    assert '"contract_id":"echo-output"' in wire
    assert '"contract_version":"1"' in wire
    assert "fala/" not in wire
    result = build_result(spoken, payload={"text": "hello"})
    assert result.contract_id == "echo-output"
    assert result.contract_version == "1"
    assert_answers(spoken, result)
    golden = parse((ROOT / "request.valid.json").read_text(), "request")
    assert golden.contract_id == "echo-output"


def test_lone_contract_field_is_required():
    with pytest.raises(ProtocolError) as caught:
        Request(sender="parent", recipient="echo", job="echo", payload={"text": "hello"}, contract_id="echo-output")
    assert (caught.value.code, caught.value.pointer) == ("fep.required", "/contract_version")


def test_missing_contract_pair_is_required():
    with pytest.raises(ProtocolError) as caught:
        Request(sender="parent", recipient="echo", job="echo", payload={"text": "hello"})
    assert (caught.value.code, caught.value.pointer) == ("fep.required", "/contract_id")


def test_empty_contract_field_is_required():
    with pytest.raises(ProtocolError) as caught:
        parse(
            json.dumps(
                {
                    "protocol": PROTOCOL,
                    "kind": "request",
                    "id": "msg:sha256:bad",
                    "from": "parent",
                    "to": "echo",
                    "job": "echo",
                    "payload": {"text": "hello"},
                    "config": {},
                    "contract_id": "",
                    "contract_version": "1",
                }
            )
        )
    assert (caught.value.code, caught.value.pointer) == ("fep.required", "/contract_id")


def test_result_must_echo_request_contract():
    request = parse((ROOT / "request.valid.json").read_text(), "request")
    result = Result(
        sender=request.recipient,
        job=request.job,
        ref=request.id,
        payload={"text": "hello"},
        recipient=request.sender,
        contract_id="other-output",
        contract_version="1",
    )
    with pytest.raises(ProtocolError) as caught:
        assert_answers(request, result)
    assert (caught.value.code, caught.value.pointer) == ("fep.contract_mismatch", "/contract_id")


def test_contract_version_must_echo_exactly():
    request = Request(
        sender="parent",
        recipient="echo",
        job="echo",
        payload={"text": "hello"},
        contract_id="echo-output",
        contract_version="1",
    )
    result = Result(
        sender=request.recipient,
        job=request.job,
        ref=request.id,
        payload={"text": "hello"},
        recipient=request.sender,
        contract_id="echo-output",
        contract_version="2",
    )
    with pytest.raises(ProtocolError) as caught:
        assert_answers(request, result)
    assert (caught.value.code, caught.value.pointer) == ("fep.contract_mismatch", "/contract_id")


def test_assert_answers_accepts_golden_pair():
    request = parse((ROOT / "request.valid.json").read_text(), "request")
    result = parse((ROOT / "result.valid.json").read_text(), "result")
    assert_answers(request, result)


def test_assert_answers_rejects_ref_mismatch():
    request = parse((ROOT / "request.valid.json").read_text(), "request")
    result = Result(
        sender=request.recipient,
        job=request.job,
        ref="msg:sha256:" + ("0" * 64),
        payload={"text": "hello"},
        recipient=request.sender,
        contract_id=request.contract_id,
        contract_version=request.contract_version,
    )
    with pytest.raises(ProtocolError) as caught:
        assert_answers(request, result)
    assert (caught.value.code, caught.value.pointer) == ("fep.ref_mismatch", "/ref")
