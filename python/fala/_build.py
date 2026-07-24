"""Compile Mojo ``_native`` and ensure the optional sqlite.fire shared library (#106).

Memory path needs only the Mojo extension. Durable path (``open_sqlite`` /
``host_run_package``) additionally needs ``libsqlite_fire`` under
``vendor/sqlite.fire/native`` — hatch force-includes sources; this module builds
the shared library on first use when it is missing.
"""

from __future__ import annotations

import ctypes
import hashlib
import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path
from types import ModuleType

_PACKAGE_DIR = Path(__file__).resolve().parent
_NATIVE_MOJO = _PACKAGE_DIR / "_native.mojo"
_CACHE_DIR_NAME = "__mojocache__"
_SKIP_NATIVE_BUILD_ENV = "FALA_SKIP_NATIVE_BUILD"
_TRUTHY = frozenset({"1", "true", "yes", "on"})


def repo_root() -> Path:
    env = os.environ.get("FALA_HOME")
    if env:
        return Path(env).expanduser().resolve()
    for candidate in (_PACKAGE_DIR.parents[2], _PACKAGE_DIR.parent, Path.cwd()):
        if (candidate / "mojo" / "fala").is_dir() and (
            candidate / "vendor" / "EmberJson"
        ).is_dir():
            return candidate.resolve()
    raise RuntimeError(
        "Cannot locate Fala Mojo sources. Set FALA_HOME to the Fala checkout."
    )


def sqlite_fire_native_dir(root: Path | None = None) -> Path:
    """Directory that holds Makefile + libsqlite_fire shared library."""
    return (root or repo_root()) / "vendor" / "sqlite.fire" / "native"


def sqlite_fire_library_name() -> str:
    if sys.platform == "darwin":
        return "libsqlite_fire.dylib"
    if sys.platform.startswith("linux"):
        return "libsqlite_fire.so"
    raise RuntimeError(
        f"fala durable SQLite host is POSIX-only (macOS/Linux); unsupported platform: {sys.platform}"
    )


def sqlite_fire_library_path(root: Path | None = None) -> Path:
    return sqlite_fire_native_dir(root) / sqlite_fire_library_name()


def _skip_native_build() -> bool:
    return os.environ.get(_SKIP_NATIVE_BUILD_ENV, "").strip().lower() in _TRUTHY


def _prepend_library_path(native_lib: Path) -> None:
    if not native_lib.is_dir():
        return
    prefix = str(native_lib)
    for key in ("DYLD_LIBRARY_PATH", "LD_LIBRARY_PATH"):
        cur = os.environ.get(key, "")
        parts = [p for p in cur.split(os.pathsep) if p]
        if prefix not in parts:
            os.environ[key] = prefix + (os.pathsep + cur if cur else "")


def ensure_sqlite_fire_library(root: Path | None = None) -> Path:
    """Ensure ``libsqlite_fire`` exists for the durable SQLite journal sink (#106).

    When the shared library is missing, runs ``make -C vendor/sqlite.fire/native``
    once (sources are hatch force-included). Set ``FALA_SKIP_NATIVE_BUILD=1`` to
    skip the build attempt (memory-path-only machines); durable APIs then fail
    closed with an actionable error.

    Returns the absolute path to the shared library.
    """
    root = root or repo_root()
    native_dir = sqlite_fire_native_dir(root)
    lib_path = sqlite_fire_library_path(root)

    if lib_path.is_file():
        _prepend_library_path(native_dir)
        return lib_path

    if _skip_native_build():
        raise RuntimeError(
            f"fala durable path requires {lib_path.name} at {lib_path}, but it is "
            f"missing and {_SKIP_NATIVE_BUILD_ENV} is set. Unset the env var and "
            f"retry, or build manually: make -C {native_dir}"
        )

    if not native_dir.is_dir():
        raise RuntimeError(
            f"sqlite.fire native sources missing at {native_dir}. "
            "Fala install is incomplete (expected hatch force-include of "
            "vendor/sqlite.fire)."
        )
    makefile = native_dir / "Makefile"
    if not makefile.is_file():
        raise RuntimeError(
            f"sqlite.fire Makefile missing at {makefile}. Fala install is incomplete."
        )
    if not (native_dir / "sqlite_fire.c").is_file():
        raise RuntimeError(
            f"sqlite.fire C sources missing under {native_dir}. "
            "Fala install is incomplete."
        )

    make = shutil.which("make")
    if make is None:
        raise RuntimeError(
            "fala: cannot build sqlite.fire native library: `make` not found on PATH. "
            "Install a C toolchain (and libsqlite3), then retry, or run: "
            f"make -C {native_dir}"
        )
    cc = (
        shutil.which(os.environ.get("CC", "cc"))
        or shutil.which("clang")
        or shutil.which("gcc")
    )
    if cc is None:
        raise RuntimeError(
            "fala: cannot build sqlite.fire native library: no C compiler (`cc`/`clang`/`gcc`) "
            f"on PATH. Install a C toolchain and libsqlite3, then: make -C {native_dir}"
        )

    proc = subprocess.run(
        [make, "-C", str(native_dir)],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0 or not lib_path.is_file():
        raise RuntimeError(
            "fala: failed to build sqlite.fire native library "
            f"({lib_path.name}) under {native_dir}.\n"
            "Durable Python host requires a C compiler and libsqlite3 "
            f"(optional sink; memory path does not need this).\n"
            f"Command: make -C {native_dir}\n"
            f"exit={proc.returncode}\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )

    _prepend_library_path(native_dir)
    return lib_path


def _source_hash(root: Path) -> str:
    paths = sorted(
        list(_PACKAGE_DIR.glob("*.mojo"))
        + list((root / "mojo" / "fala").rglob("*.mojo"))
        + list((root / "vendor" / "EmberJson").rglob("*.mojo"))
        + list((root / "vendor" / "sqlite.fire").rglob("*.mojo"))
    )
    h = hashlib.sha256()
    for p in paths:
        try:
            rel = str(p.relative_to(root))
        except ValueError:
            rel = p.name
        h.update(rel.encode())
        h.update(p.read_bytes())
    return h.hexdigest()[:16]


def _mojo_toolchain_root(env: dict[str, str], root: Path) -> Path | None:
    """Resolve the Mojo toolchain without requiring caller-owned env setup."""
    driver = env.get("MODULAR_MOJO_MAX_DRIVER_PATH") or env.get("MOJO")
    package_root = env.get("MODULAR_MOJO_MAX_PACKAGE_ROOT")
    candidates = [
        Path(driver).expanduser().resolve().parent.parent if driver else None,
        Path(package_root).expanduser().resolve() if package_root else None,
        Path(env["CONDA_PREFIX"]).expanduser().resolve()
        if env.get("CONDA_PREFIX")
        else None,
        root / ".pixi" / "envs" / "default",
    ]
    for candidate in candidates:
        if candidate is not None and (candidate / "bin" / "mojo").is_file():
            return candidate
    return None


def _preload_mojo_runtime(toolchain_root: Path) -> None:
    """Make Mojo runtime dylibs available before importing a cached extension."""
    lib_dir = toolchain_root / "lib"
    _prepend_library_path(lib_dir)
    suffix = ".dylib" if sys.platform == "darwin" else ".so"
    for stem in ("libKGENCompilerRTShared", "libAsyncRTMojoBindings"):
        library = lib_dir / f"{stem}{suffix}"
        if library.is_file():
            ctypes.CDLL(str(library), mode=ctypes.RTLD_GLOBAL)


def _mojo_env(root: Path) -> dict[str, str]:
    env = dict(os.environ)
    try:
        from mojo._package_root import (  # type: ignore[import-not-found]
            get_package_root,
        )
        from mojo.run import _sdk_default_env  # type: ignore[import-not-found]

        package_root = get_package_root()
        if package_root is not None:
            sdk_env = _sdk_default_env()
            sdk_env.update(env)
            sdk_env.setdefault("MODULAR_MOJO_MAX_PACKAGE_ROOT", str(package_root))
            return sdk_env
    except (AttributeError, ImportError, OSError, TypeError) as exc:
        if os.environ.get("FALA_DEBUG_TOOLCHAIN"):
            print(
                f"fala: optional Mojo package discovery failed: {exc}", file=sys.stderr
            )
    toolchain_root = _mojo_toolchain_root(env, root)
    if toolchain_root is not None:
        mojo_bin = toolchain_root / "bin" / "mojo"
        import_path = toolchain_root / "lib" / "mojo"
        env.setdefault("MODULAR_MAX_PACKAGE_ROOT", str(toolchain_root))
        env.setdefault("MODULAR_MOJO_MAX_PACKAGE_ROOT", str(toolchain_root))
        env.setdefault("MODULAR_MOJO_MAX_DRIVER_PATH", str(mojo_bin))
        if import_path.is_dir():
            env.setdefault("MODULAR_MOJO_MAX_IMPORT_PATH", str(import_path))
        env["PATH"] = str(toolchain_root / "bin") + os.pathsep + env.get("PATH", "")
        _prepend_env_path(env, toolchain_root / "lib")
    return env


def _prepend_env_path(env: dict[str, str], path: Path) -> None:
    if not path.is_dir():
        return
    prefix = str(path)
    for key in ("DYLD_LIBRARY_PATH", "LD_LIBRARY_PATH"):
        current = env.get(key, "")
        parts = [part for part in current.split(os.pathsep) if part]
        if prefix not in parts:
            env[key] = prefix + (os.pathsep + current if current else "")


def _mojo_bin(env: dict[str, str]) -> str:
    for key in ("MODULAR_MOJO_MAX_DRIVER_PATH", "MOJO"):
        p = env.get(key)
        if p and Path(p).is_file():
            return p
    found = shutil.which("mojo", path=env.get("PATH"))
    if found:
        return found
    raise RuntimeError(
        "mojo executable not found; set FALA_HOME to a Fala checkout and run "
        "`mise exec -- pixi install` there"
    )


def ensure_native() -> ModuleType:
    """Build/load the Mojo Python extension (memory + durable host entrypoints).

    Does **not** compile sqlite.fire by itself — call
    :func:`ensure_sqlite_fire_library` from durable APIs so pure memory-path
    users never require a C toolchain (#106).
    """
    if not _NATIVE_MOJO.is_file():
        raise RuntimeError(f"missing {_NATIVE_MOJO}")
    root = repo_root()
    digest = _source_hash(root)
    cache_dir = _PACKAGE_DIR / _CACHE_DIR_NAME
    cache_dir.mkdir(exist_ok=True)
    so_path = cache_dir / f"_native.hash-{digest}.so"
    if not so_path.is_file():
        for old in cache_dir.glob("_native.hash-*.so"):
            old.unlink(missing_ok=True)
        env = _mojo_env(root)
        mojo = _mojo_bin(env)
        cmd = [
            mojo,
            "build",
            str(_NATIVE_MOJO),
            "--emit",
            "shared-lib",
            "-I",
            str(root / "mojo"),
            "-I",
            str(root / "vendor" / "EmberJson"),
            "-I",
            str(root / "vendor" / "sqlite.fire" / "src"),
            "-o",
            str(so_path),
        ]
        proc = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            raise RuntimeError(
                "fala native build failed:\n" + (proc.stderr or proc.stdout or "")
            )
    env = _mojo_env(root)
    toolchain_root = _mojo_toolchain_root(env, root)
    if toolchain_root is not None:
        _preload_mojo_runtime(toolchain_root)
    native_lib = sqlite_fire_native_dir(root)
    if native_lib.is_dir():
        _prepend_library_path(native_lib)

    # Reuse a loaded module if the same .so is already mapped.
    existing = sys.modules.get("fala._native")
    if existing is not None:
        return existing

    spec = importlib.util.spec_from_file_location("fala._native", so_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {so_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["fala._native"] = mod
    spec.loader.exec_module(mod)
    return mod
