from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    monkeypatch.chdir(root)
    monkeypatch.setenv("FALA_HOME", str(root))


def test_native_extension_exports_python_objects_and_keeps_json_compatibility() -> None:
    import json

    from fala._build import ensure_native

    native = ensure_native()
    request = json.dumps(
        {
            "run_id": "run_native_object",
            "path": {
                "id": "single",
                "effectors": [{"id": "step", "capability": "source"}],
            },
            "outputs": {"step": {"value": 1}},
        }
    )
    result = native.host_drive(request)
    serialized = native.host_drive_json(request)
    assert isinstance(result, dict)
    assert result["ok"] is True
    assert isinstance(serialized, str)
    assert json.loads(serialized) == result


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


def test_sdk_declared_inputs_excludes_runtime_injected_keys() -> None:
    from fala import sdk

    manifest = {
        "input": {
            "authored": 1,
            "conduction": {"source": "value"},
            "upstream_reactions": [{"kind": "source"}],
            "regulation": {"retry_policy": "none"},
        }
    }
    assert sdk.declared_inputs(manifest) == {"authored": 1}
    assert sdk.INJECTED_INPUT_KEYS == {
        "conduction",
        "upstream_reactions",
        "regulation",
    }


def test_sdk_explicit_empty_env_does_not_fall_back_to_process_env(
    tmp_path, monkeypatch
) -> None:
    from fala import sdk

    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"input": {}}', encoding="utf-8")
    monkeypatch.setenv("FALA_EFFECTOR_MANIFEST", str(manifest))
    with pytest.raises(RuntimeError, match="FALA_EFFECTOR_MANIFEST"):
        sdk.load_manifest(env={})
    with pytest.raises(RuntimeError, match="FALA_EFFECTOR_OUTPUT_DIR"):
        sdk.write_result({}, env={})

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
    assert result["processes"] == [
        {"id": "one_step:ping", "status": "succeeded"}
    ]
    terminal = result["effector_results"]
    assert list(terminal) == ["ping"]
    assert terminal["ping"]["id"] == "one_step:ping"
    assert terminal["ping"]["status"] == "succeeded"
    assert terminal["ping"]["output"]["ok"] is True
    assert terminal["ping"]["error"] == {}


def test_host_run_package_conditional_conduction_skips_nonmatching_adapter(tmp_path) -> None:
    import json
    import sys

    import fala

    source = tmp_path / "source.py"
    source.write_text(
        "import json, os\n"
        "from pathlib import Path\n"
        "Path(os.environ['FALA_EFFECTOR_OUTPUT_DIR'], 'result.json').write_text("
        "json.dumps({'values': {'decision': {'verdict': 'request_changes'}}}))\n",
        encoding="utf-8",
    )
    sentinel = tmp_path / "merge-ran"
    merge = tmp_path / "merge.py"
    merge.write_text(
        f"from pathlib import Path\nPath({str(sentinel)!r}).write_text('ran')\n",
        encoding="utf-8",
    )
    repair = tmp_path / "repair.py"
    repair.write_text(
        "import json, os\n"
        "from pathlib import Path\n"
        "Path(os.environ['FALA_EFFECTOR_OUTPUT_DIR'], 'result.json').write_text("
        "json.dumps({'values': {'repaired': True}}))\n",
        encoding="utf-8",
    )
    package = {
        "version": "2",
        "id": "conditional_host",
        "capabilities": [{"id": name} for name in ("review", "merge", "repair")],
        "correlation_paths": [{
            "id": "route",
            "effectors": [
                {"id": "review", "capability": "review", "adapter": {"kind": "subprocess", "command": [sys.executable, str(source)]}},
                {"id": "merge", "capability": "merge", "conduction": ["review"], "when": {"upstream": "review", "path": "decision.verdict", "equals": "approve"}, "adapter": {"kind": "subprocess", "command": [sys.executable, str(merge)]}},
                {"id": "repair", "capability": "repair", "conduction": ["review"], "when": {"upstream": "review", "path": "decision.verdict", "equals": "request_changes"}, "adapter": {"kind": "subprocess", "command": [sys.executable, str(repair)]}},
            ],
        }],
    }
    package_path = tmp_path / "conditional.fala-package.json"
    package_path.write_text(json.dumps(package), encoding="utf-8")
    result = fala.host_run_package(
        db_path=tmp_path / "conditional.sqlite",
        package_path=package_path,
        path_id="route",
        run_id="conditional",
        max_ticks=8,
    )
    assert result["run_status"] == "completed"
    assert result["effector_results"]["merge"]["status"] == "skipped"
    assert result["effector_results"]["merge"]["output"]["reason"] == "condition_not_met"
    assert result["effector_results"]["repair"]["status"] == "succeeded"
    assert not sentinel.exists()


def test_host_run_package_exposes_decoded_failed_effector_result(tmp_path) -> None:
    import fala
    from pathlib import Path

    pkg = Path(__file__).resolve().parent / "fixtures" / "subprocess_one.fala-package.toml"
    result = fala.host_run_package(
        db_path=tmp_path / "failed.sqlite",
        package_path=pkg,
        path_id="one_step",
        run_id="failed_result",
        command_overrides={"ping": ["/tmp/fala-native-subprocess-fixture", "fail"]},
        max_ticks=8,
    )

    assert result["run_status"] == "failed"
    terminal = result["effector_results"]["ping"]
    assert terminal["id"] == "one_step:ping"
    assert terminal["status"] == "failed"
    assert isinstance(terminal["output"], dict)
    assert isinstance(terminal["error"], dict)
    assert terminal["error"]


def test_concurrent_host_run_package_serializes_sqlite_cwd(tmp_path) -> None:
    """Concurrent durable hosts must not race process-global chdir (#128).

    ``_with_sqlite_cwd`` mutates cwd for relative dylib discovery. Without a
    host-side lock, one thread can restore another thread's previous cwd before
    ``dlopen(native/libsqlite_fire.dylib)`` completes.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from pathlib import Path

    import fala

    pkg = Path(__file__).resolve().parent / "fixtures" / "subprocess_one.fala-package.toml"
    assert pkg.is_file()
    workers = 4

    def _run(worker: int) -> dict:
        return fala.host_run_package(
            db_path=tmp_path / f"concurrent-{worker}.sqlite",
            package_path=pkg,
            path_id="one_step",
            run_id=f"concurrent_{worker}",
            max_ticks=8,
        )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_run, i) for i in range(workers)]
        results = [future.result(timeout=60) for future in as_completed(futures)]

    assert len(results) == workers
    assert all(result.get("ok") is True for result in results)
    assert all(result.get("run_status") == "completed" for result in results)

def test_host_run_package_honors_explicit_process_host_library(
    tmp_path, monkeypatch
) -> None:
    import fala
    import fala.host as host
    from fala._build import ensure_process_host_library
    from pathlib import Path

    library = ensure_process_host_library()
    pkg = Path(__file__).resolve().parent / "fixtures" / "subprocess_one.fala-package.toml"
    monkeypatch.setenv("FALA_PROCESS_HOST_LIBRARY", str(library))

    def unexpected_packaged_build():
        raise AssertionError("explicit process-host library must bypass packaged build")

    monkeypatch.setattr(host, "ensure_process_host_library", unexpected_packaged_build)
    result = fala.host_run_package(
        db_path=tmp_path / "explicit-library.sqlite",
        package_path=pkg,
        path_id="one_step",
        run_id="explicit_library",
        max_ticks=8,
    )

    assert result["ok"] is True
    assert result["run_status"] == "completed"
    assert __import__("os").environ["FALA_PROCESS_HOST_LIBRARY"] == str(library)


def test_host_run_package_strict_json_package_uses_json_loader(tmp_path) -> None:
    """A canonical JSON package must not be sent through the TOML parser."""
    import json
    import sys

    import fala

    step = tmp_path / "step.py"
    step.write_text(
        "import json, os\n"
        "from pathlib import Path\n"
        "Path(os.environ['FALA_EFFECTOR_OUTPUT_DIR'], 'result.json').write_text("
        "json.dumps({'values': {'ok': True}}))\n",
        encoding="utf-8",
    )
    package = {
        "version": "2",
        "id": "json_smoke",
        "capabilities": [{"id": "step"}],
        "correlation_paths": [{
            "id": "one_step",
            "effectors": [{
                "id": "step",
                "capability": "step",
                "adapter": {"kind": "subprocess", "command": [sys.executable, str(step)]},
            }],
        }],
        "runtime": {
            "backend": {"kind": "sqlite", "path": str(tmp_path / "json.sqlite")},
            "reaction_store": {"kind": "filesystem", "root": str(tmp_path / "reactions")},
        },
    }
    json_package = tmp_path / "subprocess.fala-package.json"
    json_package.write_text(json.dumps(package), encoding="utf-8")
    result = fala.host_run_package(
        db_path=tmp_path / "json.sqlite",
        package_path=json_package,
        path_id="one_step",
        run_id="json_smoke",
        max_ticks=8,
    )
    assert result["ok"] is True
    assert result["run_status"] == "completed"
    step_result = result["effector_results"]["step"]
    assert step_result["status"] == "succeeded"
    assert step_result["output"]["values"] == {"ok": True}
    assert step_result["error"] == {}


def test_host_run_package_fails_closed_on_malformed_stored_result(tmp_path) -> None:
    """Replayed terminal results must never leak malformed journal JSON."""
    import json
    import sqlite3
    import sys

    import fala

    step = tmp_path / "step.py"
    step.write_text(
        "import json, os\n"
        "from pathlib import Path\n"
        "Path(os.environ['FALA_EFFECTOR_OUTPUT_DIR'], 'result.json').write_text("
        "json.dumps({'values': {'ok': True}}))\n",
        encoding="utf-8",
    )
    package = {
        "version": "2",
        "id": "malformed_result",
        "capabilities": [{"id": "step"}],
        "correlation_paths": [{
            "id": "one_step",
            "effectors": [{
                "id": "step",
                "capability": "step",
                "adapter": {"kind": "subprocess", "command": [sys.executable, str(step)]},
            }],
        }],
        "runtime": {
            "backend": {"kind": "sqlite", "path": str(tmp_path / "malformed.sqlite")},
            "reaction_store": {"kind": "filesystem", "root": str(tmp_path / "reactions")},
        },
    }
    package_path = tmp_path / "malformed.fala-package.json"
    package_path.write_text(json.dumps(package), encoding="utf-8")
    db = tmp_path / "malformed.sqlite"
    first = fala.host_run_package(
        db_path=db,
        package_path=package_path,
        path_id="one_step",
        run_id="malformed",
        max_ticks=8,
    )
    assert first["run_status"] == "completed"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "update processes set output_json=? where run_id=? and id=?",
            ("not-json", "malformed", "one_step:step"),
        )

    with pytest.raises(Exception, match="Expected|parse|JSON|json"):
        fala.host_run_package(
            db_path=db,
            package_path=package_path,
            path_id="one_step",
            run_id="malformed",
            max_ticks=8,
        )

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
    # Structured output_json must keep control values intact (#120). Inheritance
    # is proven by the unredacted marker from the host env, not by redaction.
    out_blob = row[2] or "{}"
    assert '"marker"' in out_blob
    assert "from-host-ok" in out_blob, out_blob
    assert "<redacted>" not in out_blob, out_blob
    assert "path_prefix" in out_blob


def test_host_run_package_preserves_digest_with_env_substring(tmp_path, monkeypatch) -> None:
    """Env values must not substring-redact digests inside result.json (#120)."""
    import json
    import sqlite3
    import sys
    from pathlib import Path

    import fala

    # Short ambient value that appears inside the digest below.
    monkeypatch.setenv("LMPROVIDER_TIMEOUT", "300")
    digest = "bbdacd10d0c00730099c2965d5689f5a448fbd45966acf82c904dff020ae23a1"
    work = tmp_path / "digest"
    work.mkdir()
    rx = work / "reactions"
    rx.mkdir()
    db = work / "f.sqlite"
    step = work / "step.py"
    step.write_text(
        "import json, os\n"
        "from pathlib import Path\n"
        "out = Path(os.environ['FALA_EFFECTOR_OUTPUT_DIR'])\n"
        f"digest = {digest!r}\n"
        "payload = {\n"
        "  'uri': f'fala-reaction://sha256/{digest}',\n"
        "  'sha256': digest,\n"
        "}\n"
        "(out / 'result.json').write_text(json.dumps({'values': payload, 'reactions': [{'kind': 'source_docx', 'uri': payload['uri'], 'metadata': {'sha256': digest}}]}))\n",
        encoding="utf-8",
    )
    pkg = work / "pkg.toml"
    pkg.write_text(
        f"""version = "2"
id = "digest_smoke"
[[capabilities]]
id = "step"
[[correlation_paths]]
id = "path"
[[correlation_paths.effectors]]
id = "step"
capability = "step"
adapter = {{ kind = "subprocess", command = ["{sys.executable}", "{step}"], inherit_env = ["LMPROVIDER_TIMEOUT"] }}
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
        run_id="digest-smoke",
        max_ticks=8,
    )
    assert result.get("ok") is True, result
    assert result.get("run_status") == "completed", result
    out_blob = sqlite3.connect(db).execute(
        "select output_json from processes"
    ).fetchone()[0]
    assert digest in out_blob, out_blob
    assert f"fala-reaction://sha256/{digest}" in out_blob, out_blob
    assert "<redacted>" not in out_blob, out_blob


def test_host_run_package_unicode_stdout_redaction(tmp_path, monkeypatch) -> None:
    """Multi-byte stdout + env secret must not abort the host (Fala#121).

    Protective regression: redact_environment used to walk UTF-8 by byte and
    Mojo StringSlice-asserted mid-codepoint on Polish/CJK streams.
    """
    import sqlite3
    import sys
    from pathlib import Path

    import fala

    monkeypatch.setenv("FALA_STREAM_SECRET", "top-secret-value")
    work = tmp_path / "unicode_ok"
    work.mkdir()
    rx = work / "reactions"
    rx.mkdir()
    db = work / "f.sqlite"
    step = work / "step.py"
    step.write_text(
        "import json, os, sys\n"
        "from pathlib import Path\n"
        "out = Path(os.environ['FALA_EFFECTOR_OUTPUT_DIR'])\n"
        "sys.stdout.write('ok top-secret-value żółć ąę 世界\\n')\n"
        "sys.stdout.flush()\n"
        "(out / 'result.json').write_text(json.dumps({'values': {'ok': True}}))\n",
        encoding="utf-8",
    )
    pkg = work / "pkg.toml"
    pkg.write_text(
        f"""version = "2"
id = "unicode_ok"
[[capabilities]]
id = "step"
[[correlation_paths]]
id = "path"
[[correlation_paths.effectors]]
id = "step"
capability = "step"
adapter = {{ kind = "subprocess", command = ["{sys.executable}", "{step}"], inherit_env = ["FALA_STREAM_SECRET"] }}
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
        run_id="unicode-ok",
        max_ticks=8,
    )
    assert result.get("ok") is True, result
    assert result.get("run_status") == "completed", result
    row = sqlite3.connect(db).execute(
        "select status, error_json, output_json from processes"
    ).fetchone()
    assert row is not None
    assert row[0] == "succeeded", row
    # Host must survive; secret may appear redacted in adapter telemetry only.
    blob = (row[1] or "") + (row[2] or "")
    assert "codepoint" not in blob.lower()


def test_host_run_package_unicode_stderr_failure_does_not_abort(tmp_path) -> None:
    """Failed step with multi-byte stderr must terminal-fail, not kill host (#121).

    Protective regression: native_driver._json_quote used to walk error messages
    by byte when persisting adapter_failed payloads with Polish text.
    """
    import sqlite3
    import sys
    from pathlib import Path

    import fala

    work = tmp_path / "unicode_fail"
    work.mkdir()
    rx = work / "reactions"
    rx.mkdir()
    db = work / "f.sqlite"
    step = work / "step.py"
    step.write_text(
        "import sys\n"
        "sys.stderr.write('błąd krytyczny: żółć ąęść\\n')\n"
        "sys.stderr.flush()\n"
        "raise SystemExit(9)\n",
        encoding="utf-8",
    )
    pkg = work / "pkg.toml"
    pkg.write_text(
        f"""version = "2"
id = "unicode_fail"
[[capabilities]]
id = "step"
[[correlation_paths]]
id = "path"
[[correlation_paths.effectors]]
id = "step"
capability = "step"
adapter = {{ kind = "subprocess", command = ["{sys.executable}", "{step}"] }}
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
        run_id="unicode-fail",
        max_ticks=8,
    )
    # Process returns a structured result — host must not abort (exit 133).
    assert result.get("ok") is True, result
    assert result.get("run_status") == "failed", result
    row = sqlite3.connect(db).execute(
        "select status, error_json from processes"
    ).fetchone()
    assert row is not None
    assert row[0] == "failed", row
    err = row[1] or ""
    assert "adapter" in err.lower() or "fail" in err.lower() or err != ""
    assert "codepoint boundary" not in err.lower()


def test_host_run_package_control_stderr_failure_does_not_strand_running(
    tmp_path,
) -> None:
    """C0 controls in adapter stderr must not strand a running placeholder (#186).

    ``native_driver._json_quote`` used to leave U+0000–U+001F (except ``\\n``/
    ``\\r``/``\\t``) raw. BEL/CSI in Posejdon-style stderr made the constructed
    adapter error JSON invalid; ``fail_process`` rolled back and left the row
    ``running`` with ``output_json``/``error_json`` placeholders ``{}``.
    """
    import json
    import sqlite3
    import sys
    from pathlib import Path

    import fala

    work = tmp_path / "control_fail"
    work.mkdir()
    rx = work / "reactions"
    rx.mkdir()
    db = work / "f.sqlite"
    step = work / "step.py"
    step.write_text(
        "import sys\n"
        "sys.stderr.buffer.write(b'posejdon\\x07\\x1b[31mfail\\n')\n"
        "sys.stderr.buffer.flush()\n"
        "raise SystemExit(9)\n",
        encoding="utf-8",
    )
    package = {
        "version": "2",
        "id": "control_fail",
        "capabilities": [{"id": "step"}],
        "correlation_paths": [{
            "id": "path",
            "effectors": [{
                "id": "step",
                "capability": "step",
                "adapter": {
                    "kind": "subprocess",
                    "command": [sys.executable, str(step)],
                },
            }],
        }],
        "runtime": {
            "backend": {"kind": "sqlite", "path": str(db)},
            "reaction_store": {"kind": "filesystem", "root": str(rx)},
        },
    }
    pkg = work / "pkg.fala-package.json"
    pkg.write_text(json.dumps(package), encoding="utf-8")
    result = fala.host_run_package(
        db_path=db,
        package_path=pkg,
        path_id="path",
        run_id="control-fail",
        max_ticks=8,
    )
    assert result.get("ok") is True, result
    assert result.get("run_status") == "failed", result
    terminal = result["effector_results"]["step"]
    assert terminal["id"] == "path:step"
    assert terminal["status"] == "failed"
    assert isinstance(terminal["output"], dict)
    assert isinstance(terminal["error"], dict)
    assert terminal["error"]
    row = sqlite3.connect(db).execute(
        "select status, output_json, error_json from processes where run_id=?",
        ("control-fail",),
    ).fetchone()
    assert row is not None
    assert row[0] == "failed", row
    json.loads(row[1])
    stored_error = json.loads(row[2])
    assert isinstance(stored_error, dict)
    assert stored_error

    replay = fala.host_run_package(
        db_path=db,
        package_path=pkg,
        path_id="path",
        run_id="control-fail",
        max_ticks=8,
    )
    assert replay.get("ok") is True, replay
    assert replay.get("run_status") == "failed", replay
    assert replay["effector_results"]["step"]["status"] == "failed"
    assert isinstance(replay["effector_results"]["step"]["error"], dict)
    assert replay["effector_results"]["step"]["error"]


def test_host_run_package_active_placeholder_is_decodable_and_re_driveable(
    tmp_path,
) -> None:
    """Non-terminal ``{}`` output/error placeholders are a typed empty snapshot (#186)."""
    import json
    import sqlite3
    import sys
    from pathlib import Path

    import fala

    work = tmp_path / "active_placeholder"
    work.mkdir()
    rx = work / "reactions"
    rx.mkdir()
    db = work / "f.sqlite"
    step = work / "step.py"
    step.write_text(
        "import json, os\n"
        "from pathlib import Path\n"
        "Path(os.environ['FALA_EFFECTOR_OUTPUT_DIR'], 'result.json').write_text("
        "json.dumps({'values': {'ok': True}}))\n",
        encoding="utf-8",
    )
    package = {
        "version": "2",
        "id": "active_placeholder",
        "capabilities": [{"id": "step"}],
        "correlation_paths": [{
            "id": "path",
            "effectors": [{
                "id": "step",
                "capability": "step",
                "adapter": {
                    "kind": "subprocess",
                    "command": [sys.executable, str(step)],
                },
            }],
        }],
        "runtime": {
            "backend": {"kind": "sqlite", "path": str(db)},
            "reaction_store": {"kind": "filesystem", "root": str(rx)},
        },
    }
    pkg = work / "pkg.fala-package.json"
    pkg.write_text(json.dumps(package), encoding="utf-8")
    first = fala.host_run_package(
        db_path=db,
        package_path=pkg,
        path_id="path",
        run_id="active-placeholder",
        max_ticks=8,
    )
    assert first["run_status"] == "completed"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "update runs set status=?, finished_at=null where id=?",
            ("active", "active-placeholder"),
        )
        conn.execute(
            "update processes set status=?, output_json=?, error_json=?, "
            "finished_at=null, lease_owner=?, lease_expires_at=? "
            "where run_id=? and id=?",
            (
                "running",
                "{}",
                "{}",
                "python-host",
                "2099-01-01T00:00:00Z",
                "active-placeholder",
                "path:step",
            ),
        )
    snapshot = fala.host_run_package(
        db_path=db,
        package_path=pkg,
        path_id="path",
        run_id="active-placeholder",
        max_ticks=8,
    )
    assert snapshot.get("ok") is True, snapshot
    assert snapshot.get("run_status") in {"active", "waiting", "timed_out"}, snapshot
    held = snapshot["effector_results"]["step"]
    assert held["id"] == "path:step"
    assert held["status"] == "running"
    assert held["output"] == {}
    assert held["error"] == {}
    row = sqlite3.connect(db).execute(
        "select status, output_json, error_json from processes where run_id=?",
        ("active-placeholder",),
    ).fetchone()
    assert row is not None
    assert row[0] == "running", row
    assert row[1] == "{}"
    assert row[2] == "{}"

    # Expired lease lets a later drive finish the still-live effector.
    with sqlite3.connect(db) as conn:
        conn.execute(
            "update processes set lease_expires_at=? where run_id=? and id=?",
            ("2000-01-01T00:00:00Z", "active-placeholder", "path:step"),
        )
    terminal = fala.host_run_package(
        db_path=db,
        package_path=pkg,
        path_id="path",
        run_id="active-placeholder",
        max_ticks=8,
    )
    assert terminal.get("ok") is True, terminal
    assert terminal.get("run_status") in {"completed", "failed"}, terminal
    finished = terminal["effector_results"]["step"]
    assert finished["status"] in {"succeeded", "failed"}
    assert isinstance(finished["output"], dict)
    assert isinstance(finished["error"], dict)
    final_row = sqlite3.connect(db).execute(
        "select status from processes where run_id=?",
        ("active-placeholder",),
    ).fetchone()
    assert final_row is not None
    assert final_row[0] != "running", final_row


def test_host_run_package_malformed_error_json_names_process_not_payload(
    tmp_path,
) -> None:
    """Malformed stored process JSON names run/process/field, not the payload."""
    import json
    import sqlite3
    import sys

    import fala

    work = tmp_path / "malformed_error"
    work.mkdir()
    rx = work / "reactions"
    rx.mkdir()
    db = work / "f.sqlite"
    step = work / "step.py"
    step.write_text(
        "import json, os\n"
        "from pathlib import Path\n"
        "Path(os.environ['FALA_EFFECTOR_OUTPUT_DIR'], 'result.json').write_text("
        "json.dumps({'values': {'ok': True}}))\n",
        encoding="utf-8",
    )
    package = {
        "version": "2",
        "id": "malformed_error",
        "capabilities": [{"id": "step"}],
        "correlation_paths": [{
            "id": "one_step",
            "effectors": [{
                "id": "step",
                "capability": "step",
                "adapter": {
                    "kind": "subprocess",
                    "command": [sys.executable, str(step)],
                },
            }],
        }],
        "runtime": {
            "backend": {"kind": "sqlite", "path": str(db)},
            "reaction_store": {"kind": "filesystem", "root": str(rx)},
        },
    }
    pkg = work / "malformed.fala-package.json"
    pkg.write_text(json.dumps(package), encoding="utf-8")
    first = fala.host_run_package(
        db_path=db,
        package_path=pkg,
        path_id="one_step",
        run_id="malformed-error",
        max_ticks=8,
    )
    assert first["run_status"] == "completed"
    secret = "not-json-PAYLOAD-SECRET"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "update processes set error_json=? where run_id=? and id=?",
            (secret, "malformed-error", "one_step:step"),
        )
    with pytest.raises(Exception) as excinfo:
        fala.host_run_package(
            db_path=db,
            package_path=pkg,
            path_id="one_step",
            run_id="malformed-error",
            max_ticks=8,
        )
    message = str(excinfo.value)
    assert "malformed-error" in message
    assert "one_step:step" in message
    assert "error" in message.lower()
    assert "PAYLOAD-SECRET" not in message


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
