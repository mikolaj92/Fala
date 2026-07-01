from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from fala.runtime_backend import SQLiteRuntimeBackend

from tests.runtime_backend_conformance import assert_runtime_backend_conformance


class SQLiteRuntimeBackendConformanceTests(unittest.TestCase):
    def test_sqlite_runtime_backend_satisfies_conformance_suite(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                backend = SQLiteRuntimeBackend(Path(tmp_dir) / "runtime.sqlite")
                await assert_runtime_backend_conformance(backend)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
