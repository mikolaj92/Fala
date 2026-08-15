#!/usr/bin/env python3
"""Fail closed if product stamps drift or src/README denies python/fala."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _search(path: Path, pattern: str) -> str:
    text = path.read_text(encoding="utf-8")
    match = re.search(pattern, text, flags=re.MULTILINE)
    if match is None:
        raise SystemExit(f"{path.relative_to(ROOT)}: missing /{pattern}/")
    return match.group(1)


def main() -> None:
    version = _search(ROOT / "pyproject.toml", r'^version = "([^"]+)"')
    stamps = {
        "pixi.toml": _search(ROOT / "pixi.toml", r'^version = "([^"]+)"'),
        "README.md": _search(ROOT / "README.md", r"\*\*Version ([0-9][0-9.]*)\*\*"),
        "mojo/README.md": _search(
            ROOT / "mojo/README.md", r"\*\*Product version: ([0-9][0-9.]*)\*\*"
        ),
        "docs/FALA_ARCHITECTURE_STATUS.md": _search(
            ROOT / "docs/FALA_ARCHITECTURE_STATUS.md",
            r"\*\*Product: ([0-9][0-9.]*)\*\*",
        ),
        "python/fala/__init__.py": _search(
            ROOT / "python/fala/__init__.py", r'^__version__ = "([^"]+)"'
        ),
        "python/fala/_native.mojo": _search(
            ROOT / "python/fala/_native.mojo",
            r'comptime FALA_RUNTIME_VERSION: String = "([^"]+)"',
        ),
    }
    src_readme = (ROOT / "src/README.md").read_text(encoding="utf-8")
    if "python/fala" not in src_readme:
        raise SystemExit("src/README.md must point at python/fala")
    if re.search(r"no optional Python|There is no historical CPython tree", src_readme):
        raise SystemExit("src/README.md still denies the Python host binding")
    if version not in src_readme:
        raise SystemExit(f"src/README.md must name package version {version}")
    drifted = [f"{name}={found}" for name, found in stamps.items() if found != version]
    if drifted:
        raise SystemExit(
            f"version drift vs pyproject {version}: " + ", ".join(drifted)
        )
    print(version)


if __name__ == "__main__":
    main()
