"""Unit tests for first-use native library builds — no Mojo required."""

from __future__ import annotations

import ctypes
import os
import shutil
from pathlib import Path

import pytest


@pytest.fixture()
def fake_root(tmp_path: Path) -> Path:
    """Minimal tree that looks like an installed Fala root with sqlite.fire sources."""
    native = tmp_path / "vendor" / "sqlite.fire" / "native"
    native.mkdir(parents=True)
    (tmp_path / "mojo" / "fala").mkdir(parents=True)
    (tmp_path / "vendor" / "EmberJson").mkdir(parents=True)
    (tmp_path / "mojo" / "fala" / "placeholder.mojo").write_text("", encoding="utf-8")
    # Makefile that creates a platform-correct shared lib name without a real C compile.
    makefile = """
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  SHARED = libsqlite_fire.dylib
else
  SHARED = libsqlite_fire.so
endif

all: $(SHARED)

$(SHARED):
	@echo stub > $@
"""
    (native / "Makefile").write_text(makefile, encoding="utf-8")
    (native / "sqlite_fire.c").write_text("/* stub */\n", encoding="utf-8")
    (native / "sqlite_fire.h").write_text("/* stub */\n", encoding="utf-8")
    return tmp_path


@pytest.fixture()
def fake_process_host_root(tmp_path: Path) -> Path:
    source_dir = tmp_path / "mojo" / "fala"
    source_dir.mkdir(parents=True)
    (tmp_path / "vendor" / "EmberJson").mkdir(parents=True)
    (source_dir / "native_process_host.c").write_text(
        "int fala_process_host_stub(void) { return 0; }\n", encoding="utf-8"
    )
    (source_dir / "native_process_host.h").write_text("/* stub */\n", encoding="utf-8")
    return tmp_path


def test_ensure_sqlite_fire_builds_when_missing(
    fake_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fala._build import ensure_sqlite_fire_library, sqlite_fire_library_path

    monkeypatch.delenv("FALA_SKIP_NATIVE_BUILD", raising=False)
    lib = ensure_sqlite_fire_library(fake_root)
    assert lib == sqlite_fire_library_path(fake_root)
    assert lib.is_file()
    # Second call is a no-op (already present).
    again = ensure_sqlite_fire_library(fake_root)
    assert again == lib


@pytest.mark.parametrize("cached", [False, True])
def test_ensure_sqlite_fire_preserves_caller_library_paths(
    fake_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    cached: bool,
) -> None:
    from fala import _build

    library = _build.sqlite_fire_library_path(fake_root)
    if cached:
        library.write_bytes(b"cached")
    loaded: list[tuple[str, int]] = []
    monkeypatch.setattr(
        _build.ctypes,
        "CDLL",
        lambda path, *, mode: loaded.append((path, mode)),
    )
    monkeypatch.setenv("DYLD_LIBRARY_PATH", "/caller/dyld")
    monkeypatch.setenv("LD_LIBRARY_PATH", "/caller/ld")

    result = _build.ensure_sqlite_fire_library(fake_root)

    assert result == library
    assert os.environ["DYLD_LIBRARY_PATH"] == "/caller/dyld"
    assert os.environ["LD_LIBRARY_PATH"] == "/caller/ld"
    assert loaded == [(str(library), ctypes.RTLD_GLOBAL)]


def test_ensure_sqlite_fire_skip_env_fails_closed(
    fake_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fala._build import ensure_sqlite_fire_library

    monkeypatch.setenv("FALA_SKIP_NATIVE_BUILD", "1")
    with pytest.raises(RuntimeError) as excinfo:
        ensure_sqlite_fire_library(fake_root)
    msg = str(excinfo.value)
    assert "FALA_SKIP_NATIVE_BUILD" in msg
    assert "make -C" in msg


def test_ensure_sqlite_fire_missing_sources_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fala._build import ensure_sqlite_fire_library

    monkeypatch.delenv("FALA_SKIP_NATIVE_BUILD", raising=False)
    monkeypatch.setattr(shutil, "which", lambda x: None if x == "git" else "/usr/bin/make")
    (tmp_path / "mojo" / "fala").mkdir(parents=True)
    (tmp_path / "vendor" / "EmberJson").mkdir(parents=True)
    with pytest.raises(RuntimeError) as excinfo:
        ensure_sqlite_fire_library(tmp_path)
    assert "source dependency" in str(excinfo.value)


def test_source_dependencies_checkout_pinned_revisions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fala import _build

    calls: list[list[str]] = []

    def fake_run(command: list[str], **_kwargs: object) -> None:
        calls.append(command)
        if command[1] == "clone":
            Path(command[-1]).mkdir(parents=True)

    monkeypatch.setattr(_build.shutil, "which", lambda command: f"/usr/bin/{command}")
    monkeypatch.setattr(_build.subprocess, "run", fake_run)

    _build._ensure_ember_json_sources(tmp_path)
    _build._ensure_sqlite_fire_sources(tmp_path)

    assert [call[-1] for call in calls if "checkout" in call] == [
        _build._EMBER_JSON_REV,
        _build._SQLITE_FIRE_REV,
    ]


def test_incomplete_source_dependency_is_replaced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fala import _build

    incomplete = tmp_path / "vendor" / "sqlite.fire"
    incomplete.mkdir(parents=True)
    (incomplete / "stale").write_text("floating checkout", encoding="utf-8")

    def fake_clone(*, url: str, revision: str, destination: Path) -> None:
        assert url == "https://github.com/mikolaj92/sqlite.fire.git"
        assert revision == _build._SQLITE_FIRE_REV
        assert not (destination / "stale").exists()

    monkeypatch.setattr(_build, "_clone_pinned_source", fake_clone)

    _build._ensure_sqlite_fire_sources(tmp_path)


def test_complete_wrong_source_revision_is_replaced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fala import _build

    source = tmp_path / "vendor" / "sqlite.fire"
    (source / ".git").mkdir(parents=True)
    expected = source / "src" / "sqlite_fire" / "sqlite.mojo"
    expected.parent.mkdir(parents=True)
    expected.write_text("complete but stale", encoding="utf-8")
    cloned: list[tuple[str, str, Path]] = []

    monkeypatch.setattr(
        _build.subprocess,
        "run",
        lambda *_args, **_kwargs: _build.subprocess.CompletedProcess([], 0, "wrong-revision\n", ""),
    )
    monkeypatch.setattr(
        _build,
        "_clone_pinned_source",
        lambda *, url, revision, destination: cloned.append((url, revision, destination)),
    )

    _build._ensure_sqlite_fire_sources(tmp_path)

    assert cloned == [
        (
            "https://github.com/mikolaj92/sqlite.fire.git",
            _build._SQLITE_FIRE_REV,
            source,
        )
    ]
    assert not expected.exists()


def test_memory_path_does_not_require_sqlite_fire_env(
    fake_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Documented contract: skip env must not break pure ensure_native path wiring."""
    from fala._build import _skip_native_build

    monkeypatch.setenv("FALA_SKIP_NATIVE_BUILD", "true")
    assert _skip_native_build() is True
    monkeypatch.delenv("FALA_SKIP_NATIVE_BUILD", raising=False)
    assert _skip_native_build() is False


def test_ensure_process_host_builds_packaged_sibling(
    fake_process_host_root: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from fala._build import ensure_process_host_library, process_host_library_path

    monkeypatch.delenv("FALA_SKIP_NATIVE_BUILD", raising=False)
    monkeypatch.chdir(tmp_path)
    lib = ensure_process_host_library(fake_process_host_root)
    assert lib == process_host_library_path(fake_process_host_root)
    assert lib.is_file()
    assert lib.parent == fake_process_host_root / "mojo" / "fala" / "native"
    assert ensure_process_host_library(fake_process_host_root) == lib


def test_ensure_process_host_rebuilds_when_source_is_newer(
    fake_process_host_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fala._build import ensure_process_host_library

    monkeypatch.delenv("FALA_SKIP_NATIVE_BUILD", raising=False)
    lib = ensure_process_host_library(fake_process_host_root)
    before = lib.stat().st_mtime_ns
    source = fake_process_host_root / "mojo" / "fala" / "native_process_host.c"
    source.write_text("int fala_process_host_stub(void) { return 1; }\n", encoding="utf-8")
    source.touch()

    rebuilt = ensure_process_host_library(fake_process_host_root)

    assert rebuilt == lib
    assert lib.stat().st_mtime_ns > before


def test_process_host_source_changes_native_cache_hash(
    fake_process_host_root: Path,
) -> None:
    from fala._build import _source_hash

    before = _source_hash(fake_process_host_root)
    source = fake_process_host_root / "mojo" / "fala" / "native_process_host.c"
    source.write_text("int fala_process_host_stub(void) { return 1; }\n", encoding="utf-8")
    assert _source_hash(fake_process_host_root) != before


def test_mojo_env_discovers_repo_pixi_without_caller_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fala._build import _mojo_env

    toolchain = tmp_path / ".pixi" / "envs" / "default"
    (toolchain / "bin").mkdir(parents=True)
    (toolchain / "bin" / "mojo").write_text("", encoding="utf-8")
    (toolchain / "lib" / "mojo").mkdir(parents=True)
    monkeypatch.delenv("CONDA_PREFIX", raising=False)
    monkeypatch.delenv("MODULAR_MOJO_MAX_DRIVER_PATH", raising=False)
    monkeypatch.delenv("MOJO", raising=False)

    env = _mojo_env(tmp_path)

    assert env["MODULAR_MOJO_MAX_DRIVER_PATH"] == str(toolchain / "bin" / "mojo")
    assert env["PATH"].split(":", maxsplit=1)[0] == str(toolchain / "bin")
    assert env["DYLD_LIBRARY_PATH"].split(":", maxsplit=1)[0] == str(toolchain / "lib")


def test_preload_mojo_runtime_loads_cached_extension_dependencies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fala import _build

    lib_dir = tmp_path / "lib"
    lib_dir.mkdir()
    suffix = ".dylib" if _build.sys.platform == "darwin" else ".so"
    libraries = [
        lib_dir / f"libKGENCompilerRTShared{suffix}",
        lib_dir / f"libAsyncRTMojoBindings{suffix}",
    ]
    for library in libraries:
        library.write_bytes(b"")
    loaded: list[str] = []
    monkeypatch.setattr(
        _build.ctypes,
        "CDLL",
        lambda path, *, mode: loaded.append(path),
    )

    _build._preload_mojo_runtime(tmp_path)

    assert loaded == [str(library) for library in libraries]


def test_sqlite_fire_has_no_local_mojo_1_patch() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    patch = root / "patches" / "sqlite-fire-mojo-1.0.patch"
    assert not patch.is_file()
    assert "3d482362c863e769d018443045b27ca5db645b3c" not in (
        (root / "python" / "fala" / "_build.py").read_text()
    )
    assert "3d482362c863e769d018443045b27ca5db645b3c" not in (
        (root / "tools" / "setup_sqlite_fire.sh").read_text()
    )
    assert "2cb4da921f590f170f6431ab873cd8200384f09a" in (
        (root / "python" / "fala" / "_build.py").read_text()
    )
