from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    monkeypatch.chdir(root)
    monkeypatch.setenv("FALA_HOME", str(root))


def test_host_drive_memory_e2e() -> None:
    import fala

    result = fala.host_drive(
        run_id="run_py",
        title="python host",
        impulse={"id": "imp1", "type": "case", "payload": {"n": 1}},
        path={
            "id": "chain",
            "effectors": [
                {"id": "root", "capability": "source", "conduction": []},
                {"id": "leaf", "capability": "sink", "conduction": ["root"]},
            ],
        },
        outputs={"root": {"value": 42}, "leaf": {"done": True}},
        max_ticks=16,
    )
    assert result["ok"] is True
    assert result["ticks"] >= 2
    assert result["process_count"] == 2
    statuses = {p["id"].split(":")[-1] if ":" in p["id"] else p["status"] for p in result["processes"]}
    # statuses listed as succeeded
    assert all(p["status"] == "succeeded" for p in result["processes"])


def test_memory_host_builder() -> None:
    import fala

    host = (
        fala.open_memory(run_id="run_b")
        .accept_impulse(impulse_id="i1", payload={"x": 1})
        .set_path(
            "p",
            [
                {"id": "a", "capability": "source"},
                {"id": "b", "capability": "sink", "conduction": ["a"]},
            ],
        )
        .register_output("a", {"v": 1})
        .register_output("b", {"v": 2})
    )
    result = host.drive(max_ticks=16)
    assert result["ok"] is True
    assert result["event_count"] >= 1
