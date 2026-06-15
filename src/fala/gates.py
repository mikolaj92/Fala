from __future__ import annotations

import json
import os
import posixpath
import shlex
import subprocess
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal
from urllib.parse import unquote, urlparse
from xml.etree import ElementTree

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from fala.evidence import build_evidence_pack, write_evidence_pack

GateType = Literal["artifact_exists", "command", "json_metric", "xlsx_workbook"]
GateStatus = Literal["passed", "failed", "skipped"]


class XlsxFormulaExpectation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sheet: str
    cell: str | None = None
    header: str | None = None
    row: int | None = Field(default=None, ge=1)
    equals: str | None = None
    contains: str | None = None

    @model_validator(mode="after")
    def validate_locator(self) -> "XlsxFormulaExpectation":
        if self.cell is None and not (self.header and self.row):
            raise ValueError("XLSX formula expectation needs cell or header+row")
        if self.equals is None and self.contains is None:
            raise ValueError("XLSX formula expectation needs equals or contains")
        return self


class GateSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    type: GateType
    description: str | None = None
    required: bool = True
    path: str | None = None
    uri: str | None = None
    min_size_bytes: int | None = Field(default=None, ge=0)
    sha256: str | None = None
    command: str | list[str] | None = None
    cwd: str | None = None
    env: dict[str, str] = Field(default_factory=dict)
    timeout_seconds: float | None = Field(default=None, gt=0)
    expected_exit_codes: list[int] = Field(default_factory=lambda: [0])
    json_path: str | None = None
    operator: str | None = None
    expected: Any = None
    sheets: list[str] = Field(default_factory=list)
    headers: dict[str, list[str]] = Field(default_factory=dict)
    formulas: list[XlsxFormulaExpectation] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class GateResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    type: GateType
    status: GateStatus
    required: bool
    message: str
    data: dict[str, Any] = Field(default_factory=dict)
    started_at: datetime
    finished_at: datetime
    duration_seconds: float


class GateSuiteSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow_id: str | None = None
    run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[dict[str, Any]] = Field(default_factory=list)
    gates: list[GateSpec] = Field(default_factory=list)


class GateRunReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool
    workflow_id: str | None = None
    run_id: str | None = None
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    results: list[GateResult] = Field(default_factory=list)
    evidence_pack: str | None = None


def load_gate_suite(source: str | Path) -> GateSuiteSpec:
    path = Path(source)
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Gate suite YAML must contain an object: {path}")
    return GateSuiteSpec.model_validate(data)


def run_gate_suite_from_file(
    source: str | Path,
    *,
    base_dir: str | Path | None = None,
    evidence_output: str | Path | None = None,
) -> GateRunReport:
    source_path = Path(source)
    suite = load_gate_suite(source_path)
    resolved_base_dir = Path(base_dir) if base_dir is not None else source_path.parent
    return run_gate_suite(
        suite,
        base_dir=resolved_base_dir,
        evidence_output=evidence_output,
    )


def run_gate_suite(
    suite: GateSuiteSpec,
    *,
    base_dir: str | Path | None = None,
    evidence_output: str | Path | None = None,
) -> GateRunReport:
    results = [run_gate(gate, base_dir=base_dir) for gate in suite.gates]
    failed_required = [item for item in results if item.status == "failed" and item.required]
    report = GateRunReport(
        ok=not failed_required,
        workflow_id=suite.workflow_id,
        run_id=suite.run_id,
        passed=sum(1 for item in results if item.status == "passed"),
        failed=sum(1 for item in results if item.status == "failed"),
        skipped=sum(1 for item in results if item.status == "skipped"),
        results=results,
    )
    if evidence_output is not None:
        pack = build_evidence_pack(
            workflow_id=suite.workflow_id,
            run_id=suite.run_id,
            artifacts=suite.artifacts,
            gate_results=results,
            metadata=suite.metadata,
            base_dir=base_dir,
            status="passed" if report.ok else "failed",
        )
        report.evidence_pack = str(write_evidence_pack(pack, evidence_output))
    return report


def run_gate(gate: GateSpec, *, base_dir: str | Path | None = None) -> GateResult:
    started = datetime.now(timezone.utc)
    monotonic_started = time.monotonic()
    try:
        status, message, data = _run_gate_inner(gate, base_dir=base_dir)
    except Exception as exc:
        status, message, data = "failed", str(exc), {"error": type(exc).__name__}
    finished = datetime.now(timezone.utc)
    return GateResult(
        id=gate.id,
        type=gate.type,
        status=status,
        required=gate.required,
        message=message,
        data=data,
        started_at=started,
        finished_at=finished,
        duration_seconds=round(time.monotonic() - monotonic_started, 6),
    )


def _run_gate_inner(
    gate: GateSpec,
    *,
    base_dir: str | Path | None,
) -> tuple[GateStatus, str, dict[str, Any]]:
    if gate.type == "artifact_exists":
        return _run_artifact_exists_gate(gate, base_dir=base_dir)
    if gate.type == "command":
        return _run_command_gate(gate, base_dir=base_dir)
    if gate.type == "json_metric":
        return _run_json_metric_gate(gate, base_dir=base_dir)
    if gate.type == "xlsx_workbook":
        return _run_xlsx_workbook_gate(gate, base_dir=base_dir)
    raise ValueError(f"Unsupported gate type: {gate.type}")


def _run_artifact_exists_gate(
    gate: GateSpec,
    *,
    base_dir: str | Path | None,
) -> tuple[GateStatus, str, dict[str, Any]]:
    path = _gate_path(gate, base_dir=base_dir)
    if path is None:
        raise ValueError("artifact_exists gate requires path or file:// uri")
    if not path.exists():
        return "failed", f"Artifact does not exist: {path}", {"path": str(path)}
    if not path.is_file():
        return "failed", f"Artifact is not a file: {path}", {"path": str(path)}
    size = path.stat().st_size
    if gate.min_size_bytes is not None and size < gate.min_size_bytes:
        return (
            "failed",
            f"Artifact is smaller than required: {size} < {gate.min_size_bytes}",
            {"path": str(path), "size_bytes": size},
        )
    digest = _sha256(path) if gate.sha256 else None
    if gate.sha256 and digest != gate.sha256:
        return (
            "failed",
            "Artifact sha256 mismatch",
            {"path": str(path), "sha256": digest, "expected_sha256": gate.sha256},
        )
    return "passed", "Artifact exists", {"path": str(path), "size_bytes": size, "sha256": digest}


def _run_command_gate(
    gate: GateSpec,
    *,
    base_dir: str | Path | None,
) -> tuple[GateStatus, str, dict[str, Any]]:
    if gate.command is None:
        raise ValueError("command gate requires command")
    command = shlex.split(gate.command) if isinstance(gate.command, str) else list(gate.command)
    if not command:
        raise ValueError("command gate command cannot be empty")
    cwd = _resolve_path(gate.cwd, base_dir=base_dir) if gate.cwd else (Path(base_dir) if base_dir else None)
    env = {**os.environ, **gate.env}
    proc = subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        env=env,
        text=True,
        capture_output=True,
        timeout=gate.timeout_seconds,
        check=False,
    )
    data = {
        "command": command,
        "cwd": str(cwd) if cwd else None,
        "exit_code": proc.returncode,
        "stdout": proc.stdout[-4000:],
        "stderr": proc.stderr[-4000:],
    }
    if proc.returncode not in gate.expected_exit_codes:
        return "failed", f"Command exited with {proc.returncode}", data
    return "passed", "Command exited with an expected code", data


def _run_json_metric_gate(
    gate: GateSpec,
    *,
    base_dir: str | Path | None,
) -> tuple[GateStatus, str, dict[str, Any]]:
    path = _gate_path(gate, base_dir=base_dir)
    if path is None:
        raise ValueError("json_metric gate requires path or file:// uri")
    if gate.json_path is None:
        raise ValueError("json_metric gate requires json_path")
    operator = gate.operator or "eq"
    value = _json_path_get(json.loads(path.read_text(encoding="utf-8")), gate.json_path)
    passed = _compare_metric(value, operator, gate.expected)
    data = {"path": str(path), "json_path": gate.json_path, "value": value, "operator": operator, "expected": gate.expected}
    if not passed:
        return "failed", f"JSON metric {gate.json_path} failed {operator}", data
    return "passed", f"JSON metric {gate.json_path} passed", data


def _run_xlsx_workbook_gate(
    gate: GateSpec,
    *,
    base_dir: str | Path | None,
) -> tuple[GateStatus, str, dict[str, Any]]:
    path = _gate_path(gate, base_dir=base_dir)
    if path is None:
        raise ValueError("xlsx_workbook gate requires path or file:// uri")
    workbook = _read_xlsx_workbook(path)
    missing_sheets = [sheet for sheet in gate.sheets if sheet not in workbook]
    if missing_sheets:
        return "failed", "Workbook missing required sheets", {"path": str(path), "missing_sheets": missing_sheets}
    header_failures: dict[str, list[str]] = {}
    for sheet, required_headers in gate.headers.items():
        if sheet not in workbook:
            header_failures[sheet] = list(required_headers)
            continue
        present = workbook[sheet]["rows"].get(1, [])
        missing = [header for header in required_headers if header not in present]
        if missing:
            header_failures[sheet] = missing
    if header_failures:
        return "failed", "Workbook headers missing", {"path": str(path), "missing_headers": header_failures}
    formula_failures: list[dict[str, Any]] = []
    for expected in gate.formulas:
        actual = _formula_for_expectation(workbook, expected)
        if expected.equals is not None and actual != expected.equals:
            formula_failures.append({"expectation": expected.model_dump(mode="json"), "actual": actual})
        if expected.contains is not None and (actual is None or expected.contains not in actual):
            formula_failures.append({"expectation": expected.model_dump(mode="json"), "actual": actual})
    if formula_failures:
        return "failed", "Workbook formulas missing or mismatched", {"path": str(path), "formula_failures": formula_failures}
    return (
        "passed",
        "Workbook structure passed",
        {"path": str(path), "sheets": sorted(workbook), "checked_header_sheets": sorted(gate.headers)},
    )


def _gate_path(gate: GateSpec, *, base_dir: str | Path | None) -> Path | None:
    if gate.path:
        return _resolve_path(gate.path, base_dir=base_dir)
    if gate.uri:
        parsed = urlparse(gate.uri)
        if parsed.scheme == "file":
            return Path(unquote(parsed.path))
        if parsed.scheme == "":
            return _resolve_path(gate.uri, base_dir=base_dir)
    return None


def _resolve_path(value: str | None, *, base_dir: str | Path | None) -> Path:
    if value is None:
        raise ValueError("Missing path")
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return (Path(base_dir) / path).resolve() if base_dir is not None else path


def _sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_path_get(value: Any, path: str) -> Any:
    current = value
    for token in _json_path_tokens(path):
        if isinstance(token, int):
            if not isinstance(current, list):
                raise ValueError(f"JSON path {path!r} expected list before index {token}")
            current = current[token]
        else:
            if not isinstance(current, dict):
                raise ValueError(f"JSON path {path!r} expected object before key {token!r}")
            current = current[token]
    return current


def _json_path_tokens(path: str) -> list[str | int]:
    clean = path.removeprefix("$.").removeprefix("$")
    if not clean:
        return []
    tokens: list[str | int] = []
    for part in clean.split("."):
        rest = part
        while "[" in rest:
            key, _, tail = rest.partition("[")
            if key:
                tokens.append(key)
            index, _, rest = tail.partition("]")
            tokens.append(int(index))
        if rest:
            tokens.append(rest)
    return tokens


def _compare_metric(value: Any, operator: str, expected: Any) -> bool:
    if operator in {"eq", "=="}:
        return value == expected
    if operator in {"ne", "!="}:
        return value != expected
    if operator in {"gt", ">"}:
        return value > expected
    if operator in {"gte", ">="}:
        return value >= expected
    if operator in {"lt", "<"}:
        return value < expected
    if operator in {"lte", "<="}:
        return value <= expected
    if operator == "contains":
        return expected in value
    if operator == "in":
        return value in expected
    if operator == "not_empty":
        return value not in (None, "", [], {})
    raise ValueError(f"Unsupported json_metric operator: {operator}")


def _read_xlsx_workbook(path: Path) -> dict[str, dict[str, Any]]:
    with zipfile.ZipFile(path) as archive:
        shared_strings = _read_shared_strings(archive)
        sheet_targets = _read_sheet_targets(archive)
        workbook: dict[str, dict[str, Any]] = {}
        for sheet_name, target in sheet_targets.items():
            xml_path = _xlsx_workbook_part_path(target)
            rows, formulas = _read_sheet_xml(archive.read(xml_path), shared_strings)
            workbook[sheet_name] = {"rows": rows, "formulas": formulas}
        return workbook


def _xlsx_workbook_part_path(target: str) -> str:
    clean = target.lstrip("/")
    if clean.startswith("xl/"):
        return posixpath.normpath(clean)
    return posixpath.normpath(posixpath.join("xl", clean))


def _read_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ElementTree.fromstring(archive.read("xl/sharedStrings.xml"))
    strings: list[str] = []
    for si in root.findall("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}si"):
        parts = [
            text.text or ""
            for text in si.findall(".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t")
        ]
        strings.append("".join(parts))
    return strings


def _read_sheet_targets(archive: zipfile.ZipFile) -> dict[str, str]:
    workbook_root = ElementTree.fromstring(archive.read("xl/workbook.xml"))
    rels_root = ElementTree.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    rels = {
        item.attrib["Id"]: item.attrib["Target"]
        for item in rels_root.findall("{http://schemas.openxmlformats.org/package/2006/relationships}Relationship")
    }
    sheets: dict[str, str] = {}
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    rid_name = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
    for sheet in workbook_root.findall(f"{ns}sheets/{ns}sheet"):
        name = sheet.attrib["name"]
        rid = sheet.attrib[rid_name]
        sheets[name] = rels[rid]
    return sheets


def _read_sheet_xml(xml: bytes, shared_strings: list[str]) -> tuple[dict[int, list[str]], dict[str, str]]:
    root = ElementTree.fromstring(xml)
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    rows: dict[int, list[str]] = {}
    formulas: dict[str, str] = {}
    for row in root.findall(f".//{ns}row"):
        row_index = int(row.attrib["r"])
        values: list[str] = []
        for cell in row.findall(f"{ns}c"):
            cell_ref = cell.attrib.get("r", "")
            formula = cell.find(f"{ns}f")
            if formula is not None:
                formulas[cell_ref] = formula.text or ""
            values.append(_cell_value(cell, shared_strings))
        rows[row_index] = values
    return rows, formulas


def _cell_value(cell: ElementTree.Element, shared_strings: list[str]) -> str:
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        return "".join(text.text or "" for text in cell.findall(f".//{ns}t"))
    value = cell.find(f"{ns}v")
    if value is None or value.text is None:
        return ""
    if cell_type == "s":
        index = int(value.text)
        return shared_strings[index] if 0 <= index < len(shared_strings) else ""
    return value.text


def _formula_for_expectation(
    workbook: dict[str, dict[str, Any]],
    expectation: XlsxFormulaExpectation,
) -> str | None:
    sheet = workbook.get(expectation.sheet)
    if sheet is None:
        return None
    if expectation.cell:
        return sheet["formulas"].get(expectation.cell)
    assert expectation.header is not None and expectation.row is not None
    headers = sheet["rows"].get(1, [])
    try:
        column_index = headers.index(expectation.header) + 1
    except ValueError:
        return None
    return sheet["formulas"].get(f"{_column_letter(column_index)}{expectation.row}")


def _column_letter(index: int) -> str:
    letters = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters
