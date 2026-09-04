"""Regression tests for files required by the installed Fala wheel."""

from __future__ import annotations

import subprocess
import zipfile
from pathlib import Path


def test_wheel_contains_emberjson_compatibility_patch(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(tmp_path), str(root)],
        check=True,
        capture_output=True,
        text=True,
    )

    wheels = list(tmp_path.glob("fala-*.whl"))
    assert len(wheels) == 1
    with zipfile.ZipFile(wheels[0]) as wheel:
        assert "patches/emberjson-mojo-1.0.patch" in wheel.namelist()
