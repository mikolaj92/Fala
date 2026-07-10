import asyncio
import tempfile
import unittest
from pathlib import Path

from fala import (
    AutonomousCorrelator,
    RunStatus,
    Impulse,
    Run,
)


class RunScopeTests(unittest.TestCase):
    def test_scope_injects_run_id_and_rejects_foreign_payloads(self) -> None:
        async def scenario(root: Path) -> None:
            runtime = AutonomousCorrelator.sqlite(root / "runtime.db")
            await runtime.create_run(
                Run(id="run_scope", status=RunStatus.active),
                idempotency_key="run.create",
            )
            scope = runtime.scope("run_scope")

            accepted, _ = await scope.accept_impulse(
                Impulse(
                    id="impulse_scope",
                    run_id="run_scope",
                    impulse_type="case",
                    payload={"case_id": "S-1"},
                ),
                idempotency_key="impulse.accept:impulse_scope",
            )
            self.assertEqual(accepted.run_id, "run_scope")

            events = await scope.list_events()
            self.assertIn(
                "impulse.accepted", [event.event_type for event in events]
            )

            with self.assertRaises(ValueError):
                await scope.accept_impulse(
                    Impulse(
                        id="impulse_foreign",
                        run_id="run_other",
                        impulse_type="case",
                        payload={},
                    ),
                    idempotency_key="impulse.accept:impulse_foreign",
                )

            with self.assertRaises(ValueError):
                await scope.list_events(run_id="run_other")

        with tempfile.TemporaryDirectory() as tmp_dir:
            asyncio.run(scenario(Path(tmp_dir)))


if __name__ == "__main__":
    unittest.main()
