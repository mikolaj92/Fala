import json
from pathlib import Path
import pytest
from fala.protocol import PROTOCOL, ProtocolError, Request, Result, canonical, parse, speak

from fala.sdk import load_manifest, write_result

ROOT = Path(__file__).parents[2] / "conformance" / "fala"


def test_protocol_is_unversioned_fala():
    assert PROTOCOL == "fala"
    assert (ROOT / "request.valid.json").is_file()
    assert "fala/" not in PROTOCOL
    request = json.loads((ROOT / "request.valid.json").read_text())
    assert request["protocol"] == "fala"
    for path in ROOT.iterdir():
        text = path.read_text()
        assert "fala/" not in text
        assert "fala-effector" not in text

@pytest.mark.parametrize("result", [{}, {"payload": {}}, {"protocol": "unknown"}])
def test_write_result_rejects_unversioned_or_unknown_protocol(tmp_path, result):
    with pytest.raises(ProtocolError, match="/protocol"):
        write_result(result, env={"FALA_EFFECTOR_OUTPUT_DIR": str(tmp_path)})
    assert not (tmp_path / "result.json").exists()


def test_write_result_preserves_valid_protocol_message(tmp_path):
    result = parse((ROOT / "result.valid.json").read_text(), "result")
    assert isinstance(result, Result)
    path = write_result(result, env={"FALA_EFFECTOR_OUTPUT_DIR": str(tmp_path)})
    assert parse(path.read_text(), "result") == result


def test_parse_returns_typed_request_and_result():
    request = parse((ROOT / "request.valid.json").read_text(), "request")
    assert isinstance(request, Request)
    assert request.sender == "parent"
    assert request.recipient == "echo"
    assert request.job == "echo"
    assert request.payload == {"text": "hello"}
    assert request.config == {}

    result = parse((ROOT / "result.valid.json").read_text(), "result")
    assert isinstance(result, Result)
    assert result.sender == "echo"
    assert result.recipient == "parent"
    assert result.ref == request.id
    assert result.payload == {"text": "hello"}


def test_load_manifest_returns_typed_request(tmp_path, monkeypatch):
    manifest = tmp_path / "manifest.json"
    manifest.write_text((ROOT / "request.valid.json").read_text(), encoding="utf-8")
    monkeypatch.setenv("FALA_EFFECTOR_MANIFEST", str(manifest))
    request = load_manifest()
    assert isinstance(request, Request)
    assert request.sender == "parent"
    assert request.recipient == "echo"
    assert request.job == "echo"
    assert request.payload == {"text": "hello"}


def test_write_result_rejects_mapping_even_when_it_is_a_valid_message(tmp_path):
    request = parse((ROOT / "request.valid.json").read_text(), "request")
    result = Result.ok(sender=request.recipient, job=request.job, ref=request.id, payload={"text": "hello"})
    with pytest.raises(TypeError, match="Result"):
        write_result(result.to_message(), env={"FALA_EFFECTOR_OUTPUT_DIR": str(tmp_path)})
    assert not (tmp_path / "result.json").exists()


def test_write_result_accepts_result_object(tmp_path):
    request = parse((ROOT / "request.valid.json").read_text(), "request")
    path = write_result(
        Result.ok(sender=request["to"], job=request["job"], ref=request["id"], payload={"text": "hello"}),
        env={"FALA_EFFECTOR_OUTPUT_DIR": str(tmp_path)},
    )
    assert parse(path.read_text(), "result")["payload"] == {"text": "hello"}


@pytest.mark.parametrize("case", json.loads((ROOT / "negative.json").read_text()), ids=lambda c:c["name"])
def test_write_result_rejects_invalid_protocol_before_writing(tmp_path, case):
    with pytest.raises(ProtocolError):
        write_result(case["message"], env={"FALA_EFFECTOR_OUTPUT_DIR": str(tmp_path)})
    assert not (tmp_path / "result.json").exists()

def test_write_result_rejects_request_message(tmp_path):
    request = parse((ROOT / "request.valid.json").read_text())
    with pytest.raises(ProtocolError, match="fep.unexpected_message_kind"):
        write_result(request, env={"FALA_EFFECTOR_OUTPUT_DIR": str(tmp_path)})
    assert not (tmp_path / "result.json").exists()

def test_python_codec_matches_golden_bytes():
    request_text = (ROOT / "request.valid.json").read_text().strip()
    request = parse(request_text, "request")
    assert canonical(request) == request_text
    result_text = (ROOT / "result.valid.json").read_text().strip()
    result = Result.ok(sender=request.recipient, job=request.job, ref=request.id, payload={"text":"hello"})
    assert canonical(result) == result_text
    assert parse(result_text, "result") == result
    assert speak(result) == result

@pytest.mark.parametrize("case", json.loads((ROOT / "negative.json").read_text()), ids=lambda c:c["name"])
def test_python_codec_stable_negative_classes(case):
    with pytest.raises(ProtocolError) as caught:
        parse(json.dumps(case["message"]))
    assert (caught.value.code, caught.value.pointer) == (case["code"], case["pointer"])
