"""Real FEP subprocess boundary (not just codec conformance)."""
import json
import sys
from pathlib import Path

import pytest

import fala


def test_package_enforces_domain_output_before_consumer(tmp_path):
    golden_request = Path(__file__).parents[2] / "conformance/fep-v1/request.valid.json"
    request_setup = f"import json\nrequest = json.loads(open({str(golden_request)!r}).read())\n"
    source = tmp_path / "source.py"
    source.write_text(
        "from fala.fep import build_result\n"
        "from fala.sdk import load_manifest, write_result\n"
        "m = load_manifest()\n"
        + request_setup
        + "write_result(build_result(request, values=m['input']['payload']))\n"
    )
    marker = tmp_path / "consumer-called"
    consumer = tmp_path / "consumer.py"
    consumer.write_text(
        "from pathlib import Path\n"
        "from fala.fep import build_result\n"
        "from fala.sdk import load_manifest, write_result\n"
        "m = load_manifest()\n"
        f"Path({str(marker)!r}).write_text('called')\n"
        + request_setup
        + "write_result(build_result(request, values={'accepted': m['input']['conduction']['source']['artifact']}))\n"
    )
    schema = {
        "type": "object", "required": ["route"],
        "properties": {"route": {"enum": ["ready", "wait"]}},
        "oneOf": [
            {"properties": {"route": {"const": "ready"}, "artifact": {"type": "string"}}, "required": ["artifact"]},
            {"properties": {"route": {"const": "wait"}, "reason": {"type": "string"}}, "required": ["reason"]},
        ],
    }
    package = tmp_path / "package.json"
    package.write_text(json.dumps({"id": "contracts", "correlation_paths": [{
        "id": "ship", "effectors": [
            {"id": "source", "output_schema": schema, "adapter": {"kind": "subprocess", "command": [sys.executable, str(source)]}},
            {"id": "consumer", "conduction": ["source"], "when": {"upstream": "source", "path": "route", "equals": "ready"},
             "output_schema": {"type": "object", "required": ["accepted"]},
             "adapter": {"kind": "subprocess", "command": [sys.executable, str(consumer)]}},
        ], "terminals": [{"id": "done", "source_effector": "consumer", "status": "succeeded", "output_schema": {"required": ["accepted"]}}],
    }]}))
    result = fala.host_run_package(db_path=tmp_path / "good.sqlite", package_path=package, path_id="ship", inputs={"payload": {"route": "ready", "artifact": "local.txt"}})
    assert marker.read_text() == "called"
    assert result["path_result"]["values"] == {"accepted": "local.txt"}
    marker.unlink()
    with pytest.raises(Exception, match="oneOf"):
        fala.host_run_package(db_path=tmp_path / "bad.sqlite", package_path=package, path_id="ship", inputs={"payload": {"route": "ready", "reason": "wrong variant"}})
    assert not marker.exists()
