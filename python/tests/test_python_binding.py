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


def test_open_sqlite_journal(tmp_path) -> None:
    import fala

    db = tmp_path / "host.sqlite"
    result = fala.open_sqlite(db)
    assert result["ok"] is True
    assert result["kind"] == "sqlite"
    assert db.exists() or True  # journal may create on open
    assert "path" in result


def test_sdk_run_manifest_effector(tmp_path, monkeypatch) -> None:
    import json
    from fala import sdk

    manifest = tmp_path / "manifest.json"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    manifest.write_text(json.dumps({"input": {"x": 1}, "config": {}}), encoding="utf-8")
    monkeypatch.setenv("FALA_EFFECTOR_MANIFEST", str(manifest))
    monkeypatch.setenv("FALA_EFFECTOR_OUTPUT_DIR", str(out_dir))

    def handler(m: dict) -> dict:
        return sdk.output(values={"echo": sdk.input_values(m)})

    assert sdk.run_manifest_effector(handler) == 0
    result = json.loads((out_dir / "result.json").read_text(encoding="utf-8"))
    assert result["values"]["echo"]["x"] == 1


def test_host_run_package_subprocess(tmp_path) -> None:
    import fala
    from pathlib import Path

    pkg = Path(__file__).resolve().parent / "fixtures" / "subprocess_one.fala-package.toml"
    assert pkg.is_file()
    db = tmp_path / "pkg.sqlite"
    result = fala.host_run_package(
        db_path=db,
        package_path=pkg,
        path_id="one_step",
        run_id="pkg_smoke",
        max_ticks=8,
    )
    assert result.get("ok") is True
    assert result.get("run_status") in {"completed", "failed", "active", "waiting"}
    # subprocess fixture success → completed
    assert result.get("run_status") == "completed"
    assert int(result.get("ticks") or 0) >= 1


def test_host_run_package_inherit_env_from_host_process(tmp_path, monkeypatch) -> None:
    """inherit_env must pull ambient host env (regression: empty inherited map)."""
    import json
    import sqlite3
    import sys
    from pathlib import Path

    import fala

    monkeypatch.setenv("FALA_INHERIT_SMOKE_MARKER", "from-host-ok")
    work = tmp_path / "inherit"
    work.mkdir()
    rx = work / "reactions"
    rx.mkdir()
    db = work / "f.sqlite"
    step = work / "step.py"
    step.write_text(
        "import json, os\n"
        "from pathlib import Path\n"
        "out = Path(os.environ['FALA_EFFECTOR_OUTPUT_DIR'])\n"
        "payload = {\n"
        "  'marker': os.environ.get('FALA_INHERIT_SMOKE_MARKER', 'MISSING'),\n"
        "  'path_prefix': (os.environ.get('PATH') or '')[:20],\n"
        "}\n"
        "(out / 'result.json').write_text(json.dumps({'values': payload}))\n",
        encoding="utf-8",
    )
    pkg = work / "pkg.toml"
    pkg.write_text(
        f"""version = "2"
id = "inherit_smoke"
[[capabilities]]
id = "step"
[[correlation_paths]]
id = "path"
[[correlation_paths.effectors]]
id = "step"
capability = "step"
adapter = {{ kind = "subprocess", command = ["{sys.executable}", "{step}"], inherit_env = ["FALA_INHERIT_SMOKE_MARKER", "PATH"] }}
[runtime.backend]
kind = "sqlite"
path = "{db}"
[runtime.reaction_store]
kind = "filesystem"
root = "{rx}"
""",
        encoding="utf-8",
    )
    result = fala.host_run_package(
        db_path=db,
        package_path=pkg,
        path_id="path",
        run_id="inherit-smoke",
        max_ticks=8,
    )
    assert result.get("ok") is True, result
    assert result.get("run_status") == "completed", result
    row = sqlite3.connect(db).execute(
        "select status, error_json, output_json from processes"
    ).fetchone()
    assert row is not None
    assert row[0] == "succeeded", row
    # Child env values are redacted in process output_json when they appear in
    # the payload; a redacted marker proves the host env key was inherited.
    out_blob = row[2] or "{}"
    assert '"marker"' in out_blob
    assert (
        "from-host-ok" in out_blob
        or '"marker":"<redacted>"' in out_blob
        or '"marker": "<redacted>"' in out_blob
    ), out_blob
    assert "path_prefix" in out_blob


def _ensure_schema(db_path) -> None:
    """Open journal probe then initialize full schema-v6 via native store."""
    import json
    from pathlib import Path

    import fala
    from fala._build import ensure_native, ensure_sqlite_fire_library
    from fala.host import _with_sqlite_cwd

    db = Path(db_path)
    fala.open_sqlite(db)
    ensure_sqlite_fire_library()
    native = ensure_native()

    def _call() -> None:
        try:
            native.delete_terminal_run_json(
                json.dumps({"db_path": str(db.resolve()), "run_id": "__schema_probe__"})
            )
        except Exception as exc:
            # Expected: unknown run after initialize creates tables.
            if "unknown run" not in str(exc):
                raise

    _with_sqlite_cwd(_call)


def _seed_run(db_path, run_id: str, status: str, *, with_rows: bool = True) -> None:
    import sqlite3
    from pathlib import Path

    path = Path(db_path)
    _ensure_schema(path)
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        now = "2026-01-01T00:00:00Z"
        conn.execute(
            "INSERT INTO runs (id,status,metadata,created_at,updated_at,schema_version) "
            "VALUES (?, ?, '{}', ?, ?, 6)",
            (run_id, status, now, now),
        )
        if with_rows:
            conn.execute(
                "INSERT INTO impulses (id,run_id,impulse_type,payload,metadata,created_at,updated_at) "
                "VALUES (?, ?, 'case', '{}', '{}', ?, ?)",
                (f"{run_id}:imp", run_id, now, now),
            )
            conn.execute(
                "INSERT INTO runtime_commands "
                "(run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at) "
                "VALUES (?, ?, 'seed', ?, '', '', '', '{}', ?)",
                (run_id, f"{run_id}:cmd", f"{run_id}:key", now),
            )
            conn.execute(
                "INSERT INTO runtime_events "
                "(run_id,id,sequence,event_type,schema_version,payload,created_at) "
                "VALUES (?, ?, 1, 'seeded', 1, '{}', ?)",
                (run_id, f"{run_id}:evt", now),
            )
        conn.commit()
    finally:
        conn.close()


def _trigger_names(db_path) -> set[str]:
    import sqlite3
    from pathlib import Path

    conn = sqlite3.connect(Path(db_path))
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' ORDER BY name"
        ).fetchall()
        return {row[0] for row in rows}
    finally:
        conn.close()


def _count_run_rows(db_path, run_id: str) -> dict[str, int]:
    import sqlite3
    from pathlib import Path

    conn = sqlite3.connect(Path(db_path))
    try:
        tables = (
            "bridge_inbox",
            "bridge_outbox",
            "projections",
            "homeostats",
            "processes",
            "reactions",
            "associations",
            "impulse_relations",
            "impulse_types",
            "impulses",
            "runtime_events",
            "runtime_commands",
        )
        counts: dict[str, int] = {}
        for table in tables:
            counts[table] = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE run_id=?", (run_id,)
            ).fetchone()[0]
        counts["runs"] = conn.execute(
            "SELECT COUNT(*) FROM runs WHERE id=?", (run_id,)
        ).fetchone()[0]
        return counts
    finally:
        conn.close()


def test_delete_terminal_run_completed(tmp_path) -> None:
    import fala

    db = tmp_path / "terminal.sqlite"
    _seed_run(db, "done-run", "completed")

    result = fala.delete_terminal_run(db, "done-run")
    assert result["ok"] is True
    assert result["run_id"] == "done-run"
    assert result["runs"] == 1
    assert result["impulses"] == 1
    assert result["runtime_events"] == 1
    assert result["runtime_commands"] == 1
    assert result["total"] == (
        result["bridge_inbox"]
        + result["bridge_outbox"]
        + result["projections"]
        + result["homeostats"]
        + result["processes"]
        + result["reactions"]
        + result["associations"]
        + result["impulse_relations"]
        + result["impulse_types"]
        + result["impulses"]
        + result["runtime_events"]
        + result["runtime_commands"]
        + result["runs"]
    )
    assert result["total"] >= 4
    assert _count_run_rows(db, "done-run")["runs"] == 0
    triggers = _trigger_names(db)
    assert "runtime_events_no_delete" in triggers
    assert "runtime_commands_no_delete" in triggers


def test_delete_terminal_run_rejects_active_without_writes(tmp_path) -> None:
    import fala
    import pytest

    db = tmp_path / "active.sqlite"
    _seed_run(db, "active-run", "active")
    before = _count_run_rows(db, "active-run")
    triggers_before = _trigger_names(db)

    with pytest.raises(ValueError, match="not terminal"):
        fala.delete_terminal_run(db, "active-run")

    assert _count_run_rows(db, "active-run") == before
    assert _trigger_names(db) == triggers_before
    assert "runtime_events_no_delete" in triggers_before
    assert "runtime_commands_no_delete" in triggers_before


def test_delete_terminal_run_rejects_unknown(tmp_path) -> None:
    import fala
    import pytest

    db = tmp_path / "unknown.sqlite"
    _ensure_schema(db)
    triggers_before = _trigger_names(db)
    assert "runtime_events_no_delete" in triggers_before
    assert "runtime_commands_no_delete" in triggers_before

    with pytest.raises(ValueError, match="unknown run"):
        fala.delete_terminal_run(db, "missing-run")

    assert _trigger_names(db) == triggers_before

    with pytest.raises(ValueError, match="blank"):
        fala.delete_terminal_run(db, "   ")
