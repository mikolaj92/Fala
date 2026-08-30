"""Pin the durable host to this checkout before any test loads ``fala._native``.

``fala._build.repo_root`` prefers ``FALA_HOME``. An ambient value from another
checkout would publish/load a stale ``__mojocache__`` artifact, then
``sys.modules['fala._native']`` would keep that module for the rest of the
session. The argv child used by ``subprocess_one.fala-package.toml`` is the
same binary Mojo smoke compiles via ``tools/mojo_sql_run.sh``.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
os.environ["FALA_HOME"] = str(_ROOT)

_NATIVE_SUBPROCESS_FIXTURE = Path("/tmp/fala-native-subprocess-fixture")
_FIXTURE_SRC = _ROOT / "mojo" / "smoke" / "native_effector_fixture.c"


def pytest_sessionstart(session) -> None:  # noqa: ARG001
    dest = _NATIVE_SUBPROCESS_FIXTURE
    src = _FIXTURE_SRC
    if (
        dest.is_file()
        and os.access(dest, os.X_OK)
        and dest.stat().st_mtime >= src.stat().st_mtime
    ):
        return
    subprocess.run(
        ["cc", "-std=c11", "-Wall", "-Wextra", "-o", str(dest), str(src)],
        check=True,
    )
