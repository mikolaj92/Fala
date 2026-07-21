"""Unit tests for sqlite.fire first-use build (#106) — no Mojo required."""

from __future__ import annotations

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


def test_ensure_sqlite_fire_builds_when_missing(fake_root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from fala._build import ensure_sqlite_fire_library, sqlite_fire_library_path

    monkeypatch.delenv("FALA_SKIP_NATIVE_BUILD", raising=False)
    lib = ensure_sqlite_fire_library(fake_root)
    assert lib == sqlite_fire_library_path(fake_root)
    assert lib.is_file()
    # Second call is a no-op (already present).
    again = ensure_sqlite_fire_library(fake_root)
    assert again == lib


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
    (tmp_path / "mojo" / "fala").mkdir(parents=True)
    (tmp_path / "vendor" / "EmberJson").mkdir(parents=True)
    with pytest.raises(RuntimeError) as excinfo:
        ensure_sqlite_fire_library(tmp_path)
    assert "native sources missing" in str(excinfo.value)


def test_memory_path_does_not_require_sqlite_fire_env(
    fake_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Documented contract: skip env must not break pure ensure_native path wiring."""
    from fala._build import _skip_native_build

    monkeypatch.setenv("FALA_SKIP_NATIVE_BUILD", "true")
    assert _skip_native_build() is True
    monkeypatch.delenv("FALA_SKIP_NATIVE_BUILD", raising=False)
    assert _skip_native_build() is False
