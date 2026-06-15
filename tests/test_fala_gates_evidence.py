from __future__ import annotations

import json
import sys
import zipfile
from pathlib import Path

from fala.gates import run_gate_suite_from_file


def test_gate_suite_writes_evidence_pack(tmp_path: Path) -> None:
    workbook = tmp_path / "workbook.xlsx"
    metrics = tmp_path / "metrics.json"
    report_out = tmp_path / "evidence.json"
    _write_minimal_workbook(workbook)
    metrics.write_text(json.dumps({"samples_failed": 0, "strict": {"recall": 0.99}}), encoding="utf-8")

    config = tmp_path / "gates.yaml"
    config.write_text(
        """
workflow_id: excel_only_mvp
run_id: run-1
metadata:
  owner: test
artifacts:
  - kind: final_results_xlsx
    path: workbook.xlsx
    metadata:
      deliverable: true
gates:
  - id: workbook_exists
    type: artifact_exists
    path: workbook.xlsx
    min_size_bytes: 100
  - id: no_failed_samples
    type: json_metric
    path: metrics.json
    json_path: samples_failed
    operator: eq
    expected: 0
  - id: strict_recall
    type: json_metric
    path: metrics.json
    json_path: strict.recall
    operator: gte
    expected: 0.95
  - id: workbook_contract
    type: xlsx_workbook
    path: workbook.xlsx
    sheets: [SDS_Working_Sheet, 15_Methodology_Cards]
    headers:
      SDS_Working_Sheet: [source_file, name, cas]
      15_Methodology_Cards: [source_file, calculation_formula]
    formulas:
      - sheet: 15_Methodology_Cards
        header: calculation_formula
        row: 2
        contains: A2
""",
        encoding="utf-8",
    )

    report = run_gate_suite_from_file(config, evidence_output=report_out)

    assert report.ok is True
    assert report.passed == 4
    assert report.failed == 0
    assert report.evidence_pack == str(report_out)
    payload = json.loads(report_out.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "fala-evidence-pack-v1"
    assert payload["workflow_id"] == "excel_only_mvp"
    assert payload["summary"]["artifact_count"] == 1
    assert payload["summary"]["gate_count"] == 4
    assert payload["artifacts"][0]["sha256"]
    assert payload["gates"][0]["status"] == "passed"


def test_command_gate_reports_failure_without_failing_optional_suite(tmp_path: Path) -> None:
    config = tmp_path / "gates.yaml"
    config.write_text(
        f"""
gates:
  - id: optional_command
    type: command
    required: false
    command: [{sys.executable!r}, -c, "import sys; sys.exit(7)"]
    expected_exit_codes: [0]
""",
        encoding="utf-8",
    )

    report = run_gate_suite_from_file(config)

    assert report.ok is True
    assert report.failed == 1
    assert report.results[0].required is False
    assert report.results[0].data["exit_code"] == 7


def _write_minimal_workbook(path: Path) -> None:
    workbook_xml = """<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
          xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="SDS_Working_Sheet" sheetId="1" r:id="rId1"/>
    <sheet name="15_Methodology_Cards" sheetId="2" r:id="rId2"/>
  </sheets>
</workbook>
"""
    rels_xml = """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/>
</Relationships>
"""
    sheet1_xml = """<?xml version="1.0" encoding="UTF-8"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1">
      <c r="A1" t="inlineStr"><is><t>source_file</t></is></c>
      <c r="B1" t="inlineStr"><is><t>name</t></is></c>
      <c r="C1" t="inlineStr"><is><t>cas</t></is></c>
    </row>
  </sheetData>
</worksheet>
"""
    sheet2_xml = """<?xml version="1.0" encoding="UTF-8"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1">
      <c r="A1" t="inlineStr"><is><t>source_file</t></is></c>
      <c r="B1" t="inlineStr"><is><t>calculation_formula</t></is></c>
    </row>
    <row r="2">
      <c r="A2" t="inlineStr"><is><t>sample.pdf</t></is></c>
      <c r="B2"><f>A2</f><v>0</v></c>
    </row>
  </sheetData>
</worksheet>
"""
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", rels_xml)
        archive.writestr("xl/worksheets/sheet1.xml", sheet1_xml)
        archive.writestr("xl/worksheets/sheet2.xml", sheet2_xml)
