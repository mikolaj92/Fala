import json
from pathlib import Path
import pytest
from fala.fep import FEPError, build_result, canonical, parse

from fala.sdk import write_result

ROOT = Path(__file__).parents[2] / "conformance" / "fep-v1"

@pytest.mark.parametrize("result", [{}, {"values": {}}, {"protocol": "fala-effector/9"}])
def test_write_result_rejects_unversioned_or_unknown_protocol(tmp_path, result):
    with pytest.raises(FEPError, match="/protocol"):
        write_result(result, env={"FALA_EFFECTOR_OUTPUT_DIR": str(tmp_path)})
    assert not (tmp_path / "result.json").exists()


def test_write_result_preserves_valid_fep_message(tmp_path):
    result = parse((ROOT / "result.valid.json").read_text(), "effector.result")
    path = write_result(result, env={"FALA_EFFECTOR_OUTPUT_DIR": str(tmp_path)})
    assert parse(path.read_text(), "effector.result") == result

@pytest.mark.parametrize("case", json.loads((ROOT / "negative.json").read_text()), ids=lambda c:c["name"])
def test_write_result_rejects_invalid_fep_before_writing(tmp_path, case):
    with pytest.raises(FEPError):
        write_result(case["message"], env={"FALA_EFFECTOR_OUTPUT_DIR": str(tmp_path)})
    assert not (tmp_path / "result.json").exists()

def test_write_result_rejects_request_message(tmp_path):
    request = parse((ROOT / "request.valid.json").read_text())
    with pytest.raises(FEPError, match="fep.unexpected_message_kind"):
        write_result(request, env={"FALA_EFFECTOR_OUTPUT_DIR": str(tmp_path)})
    assert not (tmp_path / "result.json").exists()

def test_python_codec_matches_golden_bytes():
    request_text = (ROOT / "request.valid.json").read_text().strip()
    request = parse(request_text, "effector.request")
    assert canonical(request) == request_text
    result_text = (ROOT / "result.valid.json").read_text().strip()
    result = build_result(request, values={"text":"hello"})
    assert canonical(result) == result_text
    assert parse(result_text, "effector.result") == result

@pytest.mark.parametrize("case", json.loads((ROOT / "negative.json").read_text()), ids=lambda c:c["name"])
def test_python_codec_stable_negative_classes(case):
    with pytest.raises(FEPError) as caught:
        parse(json.dumps(case["message"]))
    assert (caught.value.code, caught.value.pointer) == (case["code"], case["pointer"])
