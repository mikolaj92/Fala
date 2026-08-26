"""Regression tests for concurrent ensure_native publish (#119).

These tests intentionally avoid loading a real Mojo extension. They exercise the
artifact publication path with a controlled fake builder so the lock/atomic
replace contract is verified without a Mojo toolchain.
"""

from __future__ import annotations

import multiprocessing
import os
import subprocess
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


def _worker_build_once(payload: dict[str, Any]) -> dict[str, Any]:
    """Child process entrypoint for concurrent ensure_native builds."""
    import fala._build as build

    package_dir = Path(payload["package_dir"])
    cache_dir = package_dir / build._CACHE_DIR_NAME
    counter_path = Path(payload["counter_path"])
    sleep_s = float(payload["sleep_s"])
    root = Path(payload["root"])

    def fake_build(active_root: Path, so_path: Path) -> None:
        del active_root
        with counter_path.open("a+", encoding="utf-8") as handle:
            handle.seek(0)
            raw = handle.read().strip() or "0"
            count = int(raw) + 1
            handle.seek(0)
            handle.truncate()
            handle.write(str(count))
            handle.flush()
        time.sleep(sleep_s)
        so_path.parent.mkdir(parents=True, exist_ok=True)
        so_path.write_bytes(b"native-artifact")

    build._PACKAGE_DIR = package_dir
    build._NATIVE_MOJO = package_dir / "_native.mojo"
    build._build_native_extension = fake_build  # type: ignore[method-assign]
    build._source_hash = lambda _root: "deadbeefcafebabe"  # type: ignore[method-assign]
    build.repo_root = lambda: root  # type: ignore[method-assign]
    build._ensure_ember_json_sources = lambda _root: None  # type: ignore[method-assign]
    build._ensure_sqlite_fire_sources = lambda _root: None  # type: ignore[method-assign]

    path = build._ensure_native_artifact(root)
    return {
        "pid": os.getpid(),
        "path": str(path),
        "exists": path.is_file(),
        "size": path.stat().st_size if path.is_file() else 0,
        "cache": sorted(p.name for p in cache_dir.glob("*")),
    }


def _worker_failed_build_preserves(payload: dict[str, Any]) -> dict[str, Any]:
    import fala._build as build

    package_dir = Path(payload["package_dir"])
    root = Path(payload["root"])
    existing = Path(payload["existing"])

    def boom(_root: Path, _so_path: Path) -> None:
        raise RuntimeError("forced native build failure")

    build._PACKAGE_DIR = package_dir
    build._NATIVE_MOJO = package_dir / "_native.mojo"
    build._build_native_extension = boom  # type: ignore[method-assign]
    build._source_hash = lambda _root: "newdigest0000000"  # type: ignore[method-assign]
    build.repo_root = lambda: root  # type: ignore[method-assign]
    build._ensure_ember_json_sources = lambda _root: None  # type: ignore[method-assign]
    build._ensure_sqlite_fire_sources = lambda _root: None  # type: ignore[method-assign]

    try:
        build._ensure_native_artifact(root)
        raised = False
        error = ""
    except RuntimeError as exc:
        raised = True
        error = str(exc)
    return {
        "raised": raised,
        "error": error,
        "existing_still_present": existing.is_file(),
        "existing_bytes": existing.read_bytes() if existing.is_file() else b"",
        "cache": sorted(p.name for p in (package_dir / build._CACHE_DIR_NAME).glob("*")),
    }


@pytest.fixture()
def isolated_package(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    package = tmp_path / "fala_pkg"
    package.mkdir()
    (package / "_native.mojo").write_text("fn main(): pass\n", encoding="utf-8")
    root = tmp_path / "root"
    (root / "mojo" / "fala").mkdir(parents=True)
    (root / "vendor" / "EmberJson").mkdir(parents=True)
    (root / "vendor" / "EmberJson" / "emberjson").mkdir()
    (root / "vendor" / "EmberJson" / "emberjson" / "__init__.mojo").write_text("")
    (root / "vendor" / "sqlite.fire" / "src").mkdir(parents=True)
    (root / "vendor" / "sqlite.fire" / "src" / "sqlite_fire").mkdir()
    (root / "vendor" / "sqlite.fire" / "src" / "sqlite_fire" / "sqlite.mojo").write_text("")
    monkeypatch.setenv("FALA_HOME", str(root))
    return package


def _checkout_home(tmp_path: Path) -> Path:
    """Minimal FALA_HOME tree with a checkout host package."""
    root = tmp_path / "fala-home"
    checkout = root / "python" / "fala"
    checkout.mkdir(parents=True)
    (root / "mojo" / "fala").mkdir(parents=True)
    (root / "vendor" / "EmberJson" / "emberjson").mkdir(parents=True)
    (root / "vendor" / "sqlite.fire" / "src").mkdir(parents=True)
    (checkout / "_native.mojo").write_text("fn checkout(): pass\n", encoding="utf-8")
    (root / "mojo" / "fala" / "core.mojo").write_text("fn core(): pass\n", encoding="utf-8")
    (root / "vendor" / "EmberJson" / "emberjson" / "__init__.mojo").write_text(
        "", encoding="utf-8"
    )
    return root


def test_source_hash_ignores_effector_leftovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fala._build import _source_hash

    root = tmp_path / "root"
    mojo_dir = root / "mojo" / "fala"
    vendor = root / "vendor" / "sqlite.fire"
    leftover = vendor / ".fala-effector-deadbeef" / "input"
    leftover.mkdir(parents=True)
    mojo_dir.mkdir(parents=True)
    (root / "vendor" / "EmberJson").mkdir(parents=True)
    (mojo_dir / "a.mojo").write_text("fn a(): pass\n", encoding="utf-8")
    (vendor / "src").mkdir(parents=True)
    (vendor / "src" / "real.mojo").write_text("fn real(): pass\n", encoding="utf-8")
    (leftover / "noise.mojo").write_text("fn noise(): pass\n", encoding="utf-8")
    monkeypatch.setenv("FALA_HOME", str(root))

    first = _source_hash(root)
    (leftover / "more.mojo").write_text("fn more(): pass\n", encoding="utf-8")
    second = _source_hash(root)
    assert first == second


def test_source_hash_follows_checkout_native_not_installed_wheel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Installed-wheel _native.mojo must not change the FALA_HOME source hash (#178)."""
    from fala import _build as build

    root = _checkout_home(tmp_path)
    wheel = tmp_path / "site-packages" / "fala"
    wheel.mkdir(parents=True)
    (wheel / "_native.mojo").write_text(
        "fn wheel(): pass\nwhen_json\n", encoding="utf-8"
    )
    monkeypatch.setattr(build, "_PACKAGE_DIR", wheel)
    monkeypatch.setattr(build, "_NATIVE_MOJO", wheel / "_native.mojo")
    monkeypatch.setenv("FALA_HOME", str(root))

    digest = build._source_hash(root)
    (wheel / "_native.mojo").write_text("fn wheel_changed(): pass\n", encoding="utf-8")
    assert build._source_hash(root) == digest

    checkout = root / "python" / "fala"
    monkeypatch.setattr(build, "_PACKAGE_DIR", checkout)
    monkeypatch.setattr(build, "_NATIVE_MOJO", checkout / "_native.mojo")
    assert build._source_hash(root) == digest


def test_ensure_native_loads_matching_checkout_cache_without_rebuild(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Consumer venv + FALA_HOME must load checkout __mojocache__ as-is (#178)."""
    from fala import _build as build

    root = _checkout_home(tmp_path)
    wheel = tmp_path / "site-packages" / "fala"
    wheel.mkdir(parents=True)
    (wheel / "_native.mojo").write_text(
        "fn wheel(): pass\nwhen_json\n", encoding="utf-8"
    )
    monkeypatch.setattr(build, "_PACKAGE_DIR", wheel)
    monkeypatch.setattr(build, "_NATIVE_MOJO", wheel / "_native.mojo")
    monkeypatch.setenv("FALA_HOME", str(root))

    digest = build._source_hash(root)
    cache = root / "python" / "fala" / build._CACHE_DIR_NAME
    cache.mkdir()
    artifact = cache / f"_native.hash-{digest}.so"
    artifact.write_bytes(b"checkout-native")

    def boom_vendor(_root: Path) -> None:
        raise AssertionError("must not fetch vendor when checkout cache matches")

    def boom_build(_root: Path, _so_path: Path) -> None:
        raise AssertionError("must not rebuild wheel native against FALA_HOME")

    monkeypatch.setattr(build, "_ensure_ember_json_sources", boom_vendor)
    monkeypatch.setattr(build, "_ensure_sqlite_fire_sources", boom_vendor)
    monkeypatch.setattr(build, "_build_native_extension", boom_build)

    path = build._ensure_native_artifact(root)
    assert path == artifact
    assert path.read_bytes() == b"checkout-native"
    assert list((wheel / build._CACHE_DIR_NAME).glob("*")) == []


def test_native_rebuild_publishes_checkout_native_not_wheel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A miss still compiles checkout _native.mojo, not the installed wheel (#178)."""
    from fala import _build as build

    root = _checkout_home(tmp_path)
    wheel = tmp_path / "site-packages" / "fala"
    wheel.mkdir(parents=True)
    (wheel / "_native.mojo").write_text("fn wheel(): pass\n", encoding="utf-8")
    monkeypatch.setattr(build, "_PACKAGE_DIR", wheel)
    monkeypatch.setattr(build, "_NATIVE_MOJO", wheel / "_native.mojo")
    monkeypatch.setattr(build, "_mojo_env", lambda _root: {"PATH": os.environ.get("PATH", "")})
    monkeypatch.setattr(build, "_mojo_bin", lambda _env: "fake-mojo")
    monkeypatch.setattr(build, "_ensure_ember_json_sources", lambda _root: None)
    monkeypatch.setattr(build, "_ensure_sqlite_fire_sources", lambda _root: None)
    monkeypatch.setenv("FALA_HOME", str(root))

    seen: list[list[str]] = []

    def fake_run(cmd: list[str], **_kwargs: Any) -> SimpleNamespace:
        seen.append(cmd)
        Path(cmd[cmd.index("-o") + 1]).write_bytes(b"from-checkout")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    path = build._ensure_native_artifact(root)
    checkout_native = root / "python" / "fala" / "_native.mojo"
    assert seen
    assert str(checkout_native) in seen[0]
    assert str(wheel / "_native.mojo") not in seen[0]
    assert path.is_relative_to(root / "python" / "fala" / build._CACHE_DIR_NAME)
    assert path.read_bytes() == b"from-checkout"
    assert list((wheel / build._CACHE_DIR_NAME).glob("*.so")) == []


def test_build_native_extension_publishes_atomically(
    isolated_package: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import fala._build as build

    root = Path(os.environ["FALA_HOME"])
    cache = isolated_package / build._CACHE_DIR_NAME
    cache.mkdir()
    so_path = cache / "_native.hash-deadbeefcafebabe.so"
    monkeypatch.setattr(build, "_PACKAGE_DIR", isolated_package)
    monkeypatch.setattr(build, "_NATIVE_MOJO", isolated_package / "_native.mojo")
    monkeypatch.setattr(build, "_mojo_env", lambda _root: {"PATH": os.environ.get("PATH", "")})
    monkeypatch.setattr(build, "_mojo_bin", lambda _env: "fake-mojo")

    def fake_run(cmd: list[str], **_kwargs: Any) -> SimpleNamespace:
        out = Path(cmd[cmd.index("-o") + 1])
        # Write only to the temp -o path, never the final destination.
        assert out != so_path
        out.write_bytes(b"from-builder")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    build._build_native_extension(root, so_path)

    assert so_path.is_file()
    assert so_path.read_bytes() == b"from-builder"
    assert list(cache.glob("*.tmp.so")) == []


def test_build_native_extension_failure_keeps_existing_artifact(
    isolated_package: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import fala._build as build

    root = Path(os.environ["FALA_HOME"])
    cache = isolated_package / build._CACHE_DIR_NAME
    cache.mkdir()
    so_path = cache / "_native.hash-deadbeefcafebabe.so"
    so_path.write_bytes(b"previous-good")
    monkeypatch.setattr(build, "_PACKAGE_DIR", isolated_package)
    monkeypatch.setattr(build, "_NATIVE_MOJO", isolated_package / "_native.mojo")
    monkeypatch.setattr(build, "_mojo_env", lambda _root: {"PATH": os.environ.get("PATH", "")})
    monkeypatch.setattr(build, "_mojo_bin", lambda _env: "fake-mojo")

    def boom_run(cmd: list[str], **_kwargs: Any) -> SimpleNamespace:
        del cmd
        return SimpleNamespace(returncode=1, stdout="", stderr="compile failed")

    monkeypatch.setattr(subprocess, "run", boom_run)

    with pytest.raises(RuntimeError, match="fala native build failed"):
        build._build_native_extension(root, so_path)

    assert so_path.read_bytes() == b"previous-good"
    assert list(cache.glob("*.tmp.so")) == []


def test_failed_native_build_does_not_delete_previous_artifact(
    isolated_package: Path,
) -> None:
    import fala._build as build

    cache = isolated_package / build._CACHE_DIR_NAME
    cache.mkdir()
    previous = cache / "_native.hash-olddigest000000.so"
    previous.write_bytes(b"still-good")
    root = Path(os.environ["FALA_HOME"])

    ctx = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=1, mp_context=ctx) as pool:
        result = pool.submit(
            _worker_failed_build_preserves,
            {
                "package_dir": str(isolated_package),
                "root": str(root),
                "existing": str(previous),
            },
        ).result(timeout=30)

    assert result["raised"] is True
    assert "forced native build failure" in result["error"]
    assert result["existing_still_present"] is True
    assert result["existing_bytes"] == b"still-good"
    assert previous.name in result["cache"]


@pytest.mark.parametrize("cached", [True, False], ids=["cached", "fresh"])
def test_ensure_native_preserves_caller_library_paths(
    isolated_package: Path,
    monkeypatch: pytest.MonkeyPatch,
    cached: bool,
) -> None:
    import fala._build as build

    root = Path(os.environ["FALA_HOME"])
    toolchain = root / ".pixi" / "envs" / "default"
    (toolchain / "bin").mkdir(parents=True)
    (toolchain / "bin" / "mojo").write_text("", encoding="utf-8")
    (toolchain / "lib").mkdir()
    suffix = ".dylib" if build.sys.platform == "darwin" else ".so"
    runtime_libraries = [
        toolchain / "lib" / f"libKGENCompilerRTShared{suffix}",
        toolchain / "lib" / f"libAsyncRTMojoBindings{suffix}",
    ]
    for library in runtime_libraries:
        library.write_bytes(b"")
    (root / "vendor" / "sqlite.fire" / "native").mkdir(parents=True)

    cache = isolated_package / build._CACHE_DIR_NAME
    cache.mkdir()
    artifact = cache / "_native.hash-deadbeefcafebabe.so"
    if cached:
        artifact.write_bytes(b"cached-native")
    builds: list[Path] = []

    def fake_build(_root: Path, so_path: Path) -> None:
        builds.append(so_path)
        so_path.write_bytes(b"fresh-native")

    loaded: list[str] = []
    native = SimpleNamespace()
    monkeypatch.setattr(build, "_PACKAGE_DIR", isolated_package)
    monkeypatch.setattr(build, "_NATIVE_MOJO", isolated_package / "_native.mojo")
    monkeypatch.setattr(build, "repo_root", lambda: root)
    monkeypatch.setattr(build, "_source_hash", lambda _root: "deadbeefcafebabe")
    monkeypatch.setattr(build, "_ensure_ember_json_sources", lambda _root: None)
    monkeypatch.setattr(build, "_ensure_sqlite_fire_sources", lambda _root: None)
    monkeypatch.setattr(build, "_build_native_extension", fake_build)
    monkeypatch.setattr(
        build.ctypes,
        "CDLL",
        lambda path, *, mode: loaded.append(path),
    )
    monkeypatch.setitem(build.sys.modules, "fala._native", native)
    monkeypatch.setenv("DYLD_LIBRARY_PATH", "/caller/dyld")
    monkeypatch.setenv("LD_LIBRARY_PATH", "/caller/ld")

    result = build.ensure_native()

    assert result is native
    assert builds == ([] if cached else [artifact])
    assert loaded == [str(library) for library in runtime_libraries]
    assert os.environ["DYLD_LIBRARY_PATH"] == "/caller/dyld"
    assert os.environ["LD_LIBRARY_PATH"] == "/caller/ld"


def test_ensure_native_checkout_cache_preserves_caller_library_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Loading FALA_HOME's cached artifact must not guess caller library dirs (#177/#178)."""
    from fala import _build as build

    root = _checkout_home(tmp_path)
    toolchain = root / ".pixi" / "envs" / "default"
    (toolchain / "bin").mkdir(parents=True)
    (toolchain / "bin" / "mojo").write_text("", encoding="utf-8")
    (toolchain / "lib").mkdir()
    suffix = ".dylib" if build.sys.platform == "darwin" else ".so"
    runtime_libraries = [
        toolchain / "lib" / f"libKGENCompilerRTShared{suffix}",
        toolchain / "lib" / f"libAsyncRTMojoBindings{suffix}",
    ]
    for library in runtime_libraries:
        library.write_bytes(b"")

    wheel = tmp_path / "site-packages" / "fala"
    wheel.mkdir(parents=True)
    (wheel / "_native.mojo").write_text("fn wheel(): pass\nwhen_json\n", encoding="utf-8")
    monkeypatch.setattr(build, "_PACKAGE_DIR", wheel)
    monkeypatch.setattr(build, "_NATIVE_MOJO", wheel / "_native.mojo")
    monkeypatch.setattr(build, "repo_root", lambda: root)
    monkeypatch.setenv("FALA_HOME", str(root))

    digest = build._source_hash(root)
    cache = root / "python" / "fala" / build._CACHE_DIR_NAME
    cache.mkdir()
    artifact = cache / f"_native.hash-{digest}.so"
    artifact.write_bytes(b"checkout-native")

    def boom_build(_root: Path, _so_path: Path) -> None:
        raise AssertionError("must not rebuild when checkout cache matches")

    loaded: list[str] = []
    native = SimpleNamespace()
    monkeypatch.setattr(build, "_ensure_ember_json_sources", lambda _root: None)
    monkeypatch.setattr(build, "_ensure_sqlite_fire_sources", lambda _root: None)
    monkeypatch.setattr(build, "_build_native_extension", boom_build)
    monkeypatch.setattr(
        build.ctypes,
        "CDLL",
        lambda path, *, mode: loaded.append(path),
    )
    monkeypatch.setitem(build.sys.modules, "fala._native", native)
    monkeypatch.setenv("DYLD_LIBRARY_PATH", "/caller/dyld")
    monkeypatch.setenv("LD_LIBRARY_PATH", "/caller/ld")

    result = build.ensure_native()

    assert result is native
    assert loaded == [str(library) for library in runtime_libraries]
    assert os.environ["DYLD_LIBRARY_PATH"] == "/caller/dyld"
    assert os.environ["LD_LIBRARY_PATH"] == "/caller/ld"


def test_concurrent_ensure_native_builds_once(
    isolated_package: Path, tmp_path: Path
) -> None:
    import fala._build as build

    root = Path(os.environ["FALA_HOME"])
    counter = tmp_path / "build-count.txt"
    counter.write_text("0", encoding="utf-8")
    ctx = multiprocessing.get_context("spawn")
    workers = 6
    payload = {
        "package_dir": str(isolated_package),
        "counter_path": str(counter),
        "sleep_s": 0.2,
        "root": str(root),
    }
    with ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as pool:
        futures = [pool.submit(_worker_build_once, payload) for _ in range(workers)]
        results = [future.result(timeout=60) for future in futures]

    assert counter.read_text(encoding="utf-8").strip() == "1"
    paths = {result["path"] for result in results}
    assert len(paths) == 1
    so_path = Path(next(iter(paths)))
    assert so_path.is_file()
    assert so_path.read_bytes() == b"native-artifact"
    assert all(result["exists"] for result in results)
    leftovers = list((isolated_package / build._CACHE_DIR_NAME).glob("*.tmp.so"))
    assert leftovers == []


def test_with_sqlite_cwd_uses_owned_temp_effector_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fala import host

    root = tmp_path / "root"
    sqlite = root / "vendor" / "sqlite.fire"
    sqlite.mkdir(parents=True)
    (root / "mojo" / "fala").mkdir(parents=True)
    (root / "vendor" / "EmberJson").mkdir(parents=True)
    monkeypatch.setenv("FALA_HOME", str(root))
    monkeypatch.delenv("FALA_EFFECTOR_ROOT", raising=False)

    seen: dict[str, str] = {}

    def probe() -> str:
        seen["cwd"] = os.getcwd()
        seen["effector_root"] = os.environ["FALA_EFFECTOR_ROOT"]
        marker = Path(seen["effector_root"]) / "marker"
        marker.write_text("x", encoding="utf-8")
        seen["marker"] = str(marker)
        return "ok"

    assert host._with_sqlite_cwd(probe) == "ok"
    assert seen["cwd"] == str(sqlite.resolve())
    assert "FALA_EFFECTOR_ROOT" not in os.environ
    assert not Path(seen["marker"]).exists()
    assert not Path(seen["effector_root"]).exists()
    assert os.path.commonpath(
        [seen["effector_root"], tempfile.gettempdir()]
    ) == tempfile.gettempdir()
    assert not Path(seen["effector_root"]).is_relative_to(sqlite)


def test_with_sqlite_cwd_preserves_configured_effector_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fala import host

    root = tmp_path / "root"
    sqlite = root / "vendor" / "sqlite.fire"
    sqlite.mkdir(parents=True)
    (root / "mojo" / "fala").mkdir(parents=True)
    (root / "vendor" / "EmberJson").mkdir(parents=True)
    configured = tmp_path / "configured-effectors"
    configured.mkdir()
    monkeypatch.setenv("FALA_HOME", str(root))
    monkeypatch.setenv("FALA_EFFECTOR_ROOT", str(configured))

    def probe() -> str:
        assert os.environ["FALA_EFFECTOR_ROOT"] == str(configured)
        return "ok"

    assert host._with_sqlite_cwd(probe) == "ok"
    assert os.environ["FALA_EFFECTOR_ROOT"] == str(configured)
    assert configured.is_dir()


def test_with_sqlite_cwd_serializes_concurrent_callers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Concurrent ``_with_sqlite_cwd`` callers must not race chdir restore (#128)."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    from fala import host

    root = tmp_path / "root"
    sqlite = root / "vendor" / "sqlite.fire"
    sqlite.mkdir(parents=True)
    (root / "mojo" / "fala").mkdir(parents=True)
    (root / "vendor" / "EmberJson").mkdir(parents=True)
    monkeypatch.setenv("FALA_HOME", str(root))
    monkeypatch.delenv("FALA_EFFECTOR_ROOT", raising=False)

    baseline = os.getcwd()
    seen: list[str] = []
    errors: list[str] = []

    def probe(worker: int) -> str:
        def body() -> str:
            cwd = os.getcwd()
            if Path(cwd).resolve() != sqlite.resolve():
                errors.append(f"worker {worker}: cwd={cwd!r}")
            time.sleep(0.05)
            if os.getcwd() != cwd:
                errors.append(f"worker {worker}: cwd changed under lock")
            seen.append(cwd)
            return "ok"

        return host._with_sqlite_cwd(body)

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(probe, i) for i in range(8)]
        results = [future.result(timeout=30) for future in as_completed(futures)]

    assert results == ["ok"] * 8
    assert errors == []
    assert len(seen) == 8
    assert all(Path(cwd).resolve() == sqlite.resolve() for cwd in seen)
    assert os.getcwd() == baseline
