from pathlib import Path
import re
import tomllib

import fala


ROOT = Path(__file__).parents[2]


def test_public_and_runtime_versions_match_project_version():
    project_version = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["version"]
    native_source = (ROOT / "python/fala/_native.mojo").read_text()
    runtime_version = re.search(
        r'FALA_RUNTIME_VERSION: String = "([^"]+)"', native_source
    )

    assert runtime_version is not None
    assert fala.__version__ == project_version
    assert runtime_version.group(1) == project_version
