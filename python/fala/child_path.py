"""Subprocess implementation of the declarative ``child_path`` adapter."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

from fala.host import host_run_package
from fala.sdk import load_manifest, write_result


def _value_at(value: Any, path: str) -> Any:
    current = value
    for segment in path.split("."):
        if not isinstance(current, dict) or segment not in current:
            raise ValueError(f"child_path input mapping path is missing: {path}")
        current = current[segment]
    return current


def main() -> int:
    try:
        spec = json.loads(os.environ["FALA_CHILD_PATH_SPEC"])
        manifest = load_manifest()
        parent_db = Path(os.environ["FALA_PARENT_DB"]).resolve()
        parent_run = os.environ["FALA_PARENT_RUN_ID"]
        parent_process = os.environ["FALA_PARENT_PROCESS_ID"]
        identity = hashlib.sha256(
            f"{parent_run}\0{parent_process}".encode("utf-8")
        ).hexdigest()[:24]
        child_run = f"child-{identity}"
        root = Path(spec["journal_root"]).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        child_db = root / f"{identity}.sqlite"
        if child_db == parent_db:
            raise ValueError("child_path journal must differ from parent journal")
        parent_input = manifest.get("input", {})
        inputs = {
            target: _value_at(parent_input, source)
            for target, source in spec["input_mapping"].items()
        }
        result = host_run_package(
            db_path=child_db,
            package_path=spec["package_ref"],
            path_id=spec["path_id"],
            run_id=child_run,
            inputs=inputs,
        )
        path_result = result.get("path_result")
        if not isinstance(path_result, dict):
            raise RuntimeError("child_path did not return a typed path terminal")
        terminal = path_result.get("terminal")
        if terminal not in spec["terminal_mapping"]:
            raise RuntimeError(f"child_path returned unmapped terminal: {terminal}")
        values = dict(path_result.get("values") or {})
        values["child_ref"] = {
            "journal": str(child_db),
            "run_id": child_run,
            "path_digest": path_result.get("path_digest"),
            "terminal": terminal,
        }
        values["terminal"] = spec["terminal_mapping"][terminal]
        write_result(
            {
                "values": values,
                "reactions": list(path_result.get("evidence") or []),
                "metadata": {"child_ref": values["child_ref"]},
            }
        )
        if spec["retention"] == "delete_on_success":
            # Deliberately leave deletion to the existing retention operation;
            # this process never writes the parent journal.
            pass
        return 0
    except Exception as exc:
        print(str(exc), file=__import__("sys").stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
