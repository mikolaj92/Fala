"""Regression tests for files required by the installed Fala wheel."""

from __future__ import annotations

import subprocess
import zipfile
from pathlib import Path


def test_wheel_contains_emberjson_compatibility_patch(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    completed = subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(tmp_path), str(root)],
        cwd=root,
        capture_output=True,
        text=True,
        env={key: value for key, value in __import__("os").environ.items() if key != "PYTHONPATH"},
    )
    assert completed.returncode == 0, completed.stderr

    wheels = list(tmp_path.glob("fala-*.whl"))
    assert len(wheels) == 1
    with zipfile.ZipFile(wheels[0]) as wheel:
        names = wheel.namelist()
        assert "fala/py.typed" in names
        assert "fala/patches/emberjson-mojo-1.0.patch" in names
        assert any(name.startswith("fala/mojo/fala/") and name.endswith(".mojo") for name in names)
        assert "patches/emberjson-mojo-1.0.patch" not in names
        assert not any(name == "mojo/" or name.startswith("mojo/") for name in names)
        assert not any(name == "patches/" or name.startswith("patches/") for name in names)
