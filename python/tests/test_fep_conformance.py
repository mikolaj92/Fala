import json
from pathlib import Path
import pytest
from fala.fep import FEPError, build_result, canonical, parse

ROOT = Path(__file__).parents[2] / "conformance" / "fep-v1"

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
