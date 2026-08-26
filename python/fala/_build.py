"""Build the native libraries required by the optional Mojo Python host.

Memory paths need only the Mojo extension. Durable package execution additionally
needs ``libsqlite_fire`` and the direct-argv process host. Mojo extension build
dynamically clones sqlite.fire sources from GitHub (gitignored in vendor/);
this module builds missing shared libraries on first use.
"""

from __future__ import annotations

import ctypes
import fcntl
import hashlib
import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType

_PACKAGE_DIR = Path(__file__).resolve().parent
_NATIVE_MOJO = _PACKAGE_DIR / "_native.mojo"
_CACHE_DIR_NAME = "__mojocache__"
_SKIP_NATIVE_BUILD_ENV = "FALA_SKIP_NATIVE_BUILD"
_TRUTHY = frozenset({"1", "true", "yes", "on"})
_EMBER_JSON_REV = "951f4ef28d0c2748a30b2c5e43e139411ccca5ef"
_SQLITE_FIRE_REV = "2cb4da921f590f170f6431ab873cd8200384f09a"
_NATIVE_BUILD_LOCK = threading.Lock()


def _is_source_hash_path(path: Path) -> bool:
    """Skip temp effector workdirs and other non-source junk under vendor trees."""
    return not any(part.startswith(".fala-effector-") for part in path.parts)


def repo_root() -> Path:
    env = os.environ.get("FALA_HOME")
    if env:
        return Path(env).expanduser().resolve()
    for candidate in (_PACKAGE_DIR.parents[2], _PACKAGE_DIR.parent, Path.cwd()):
        if (candidate / "mojo" / "fala").is_dir():
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


def process_host_native_dir(root: Path | None = None) -> Path:
    return (root or repo_root()) / "mojo" / "fala" / "native"


def process_host_library_name() -> str:
    if sys.platform == "darwin":
        return "libfala_process_host.dylib"
    if sys.platform.startswith("linux"):
        return "libfala_process_host.so"
    raise RuntimeError(
        f"fala subprocess host is POSIX-only (macOS/Linux); unsupported platform: {sys.platform}"
    )


def process_host_library_path(root: Path | None = None) -> Path:
    return process_host_native_dir(root) / process_host_library_name()


def _skip_native_build() -> bool:
    return os.environ.get(_SKIP_NATIVE_BUILD_ENV, "").strip().lower() in _TRUTHY


def ensure_sqlite_fire_library(root: Path | None = None) -> Path:
    """Ensure ``libsqlite_fire`` exists for the durable SQLite journal sink (#106).

    When the shared library is missing, runs ``make -C vendor/sqlite.fire/native``
    once (sources are dynamically cloned if missing). Set ``FALA_SKIP_NATIVE_BUILD=1``
    to skip the build attempt (memory-path-only machines); durable APIs then fail
    closed with an actionable error.

    Returns the absolute path to the shared library.
    """
    root = (root or repo_root()).resolve()
    native_dir = sqlite_fire_native_dir(root)
    lib_path = sqlite_fire_library_path(root)

    if lib_path.is_file():
        ctypes.CDLL(str(lib_path), mode=ctypes.RTLD_GLOBAL)
        return lib_path

    if _skip_native_build():
        raise RuntimeError(
            f"fala durable path requires {lib_path.name} at {lib_path}, but it is "
            f"missing and {_SKIP_NATIVE_BUILD_ENV} is set. Unset the env var and "
            f"retry, or build manually: make -C {native_dir}"
        )

    _ensure_sqlite_fire_sources(root)
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

    ctypes.CDLL(str(lib_path), mode=ctypes.RTLD_GLOBAL)
    return lib_path


def ensure_process_host_library(root: Path | None = None) -> Path:
    """Build or refresh the packaged direct-argv process-host library."""
    root = root or repo_root()
    native_dir = process_host_native_dir(root)
    lib_path = process_host_library_path(root)
    source_dir = root / "mojo" / "fala"
    source = source_dir / "native_process_host.c"
    header = source_dir / "native_process_host.h"
    if not source.is_file() or not header.is_file():
        raise RuntimeError(
            f"Fala process-host sources missing under {source_dir}. Installation is incomplete."
        )
    newest_source = max(source.stat().st_mtime_ns, header.stat().st_mtime_ns)
    if lib_path.is_file() and lib_path.stat().st_mtime_ns >= newest_source:
        return lib_path
    if _skip_native_build():
        raise RuntimeError(
            f"fala subprocess path requires a current {lib_path.name} at {lib_path}, "
            f"but it is missing or stale and {_SKIP_NATIVE_BUILD_ENV} is set"
        )
    cc = (
        shutil.which(os.environ.get("CC", "cc"))
        or shutil.which("clang")
        or shutil.which("gcc")
    )
    if cc is None:
        raise RuntimeError(
            "fala: cannot build the subprocess host: no C compiler (`cc`/`clang`/`gcc`) on PATH"
        )
    native_dir.mkdir(parents=True, exist_ok=True)
    temp_path = lib_path.with_suffix(lib_path.suffix + ".tmp")
    temp_path.unlink(missing_ok=True)
    command = [cc, "-std=c11", "-Wall", "-Wextra"]
    if sys.platform == "darwin":
        command.append("-dynamiclib")
    else:
        command.extend(("-fPIC", "-shared"))
    command.extend(("-o", str(temp_path), str(source)))
    proc = subprocess.run(command, capture_output=True, text=True, check=False)
    if proc.returncode != 0 or not temp_path.is_file():
        temp_path.unlink(missing_ok=True)
        raise RuntimeError(
            "fala: failed to build the subprocess host "
            f"({lib_path.name})\nCommand: {' '.join(command)}\n"
            f"exit={proc.returncode}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    os.replace(temp_path, lib_path)
    return lib_path


def _checkout_host_package(root: Path) -> Path | None:
    """Return ``FALA_HOME/python/fala`` when it is a complete host package.

    Installed wheels live in site-packages. Hashing and cache lookup must follow
    the checkout so a consumer venv can load a matching ``__mojocache__``
    artifact instead of compiling the wheel against a foreign Mojo (#178).
    """
    candidate = root / "python" / "fala"
    if (candidate / "_native.mojo").is_file():
        return candidate
    return None


def _native_source_package(root: Path) -> Path:
    checkout = _checkout_host_package(root)
    return checkout if checkout is not None else _PACKAGE_DIR


def _native_mojo_path(root: Path) -> Path:
    return _native_source_package(root) / "_native.mojo"


def _native_cache_dirs(root: Path) -> list[Path]:
    """Prefer the FALA_HOME checkout cache, then the imported package cache."""
    dirs: list[Path] = []
    seen: set[Path] = set()
    checkout = _checkout_host_package(root)
    if checkout is not None:
        cache = checkout / _CACHE_DIR_NAME
        dirs.append(cache)
        seen.add(cache.resolve())
    package_cache = _PACKAGE_DIR / _CACHE_DIR_NAME
    if package_cache.resolve() not in seen:
        dirs.append(package_cache)
    return dirs


def _find_native_artifact(root: Path, digest: str) -> Path | None:
    name = f"_native.hash-{digest}.so"
    for cache_dir in _native_cache_dirs(root):
        candidate = cache_dir / name
        if candidate.is_file():
            return candidate
    return None


def _source_hash(root: Path) -> str:
    package_dir = _native_source_package(root)
    paths = sorted(
        path
        for path in (
            list(package_dir.glob("*.mojo"))
            + list((root / "mojo" / "fala").rglob("*.mojo"))
            + list((root / "vendor" / "EmberJson").rglob("*.mojo"))
            + list((root / "vendor" / "sqlite.fire").rglob("*.mojo"))
            + list((root / "mojo" / "fala").glob("native_process_host.[ch]"))
        )
        if _is_source_hash_path(path)
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


@contextmanager
def _cross_process_lock(lock_path: Path) -> Iterator[None]:
    """Serialize native builds across processes and threads (#119)."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with _NATIVE_BUILD_LOCK, lock_path.open("a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

def _clone_pinned_source(*, url: str, revision: str, destination: Path) -> None:
    git = shutil.which("git")
    if git is None:
        raise RuntimeError(
            f"Fala source dependency {url}@{revision} is missing and `git` is not on PATH"
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        subprocess.run(
            [git, "clone", "--filter=blob:none", "--no-checkout", url, str(destination)],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [git, "-C", str(destination), "checkout", "--detach", revision],
            check=True,
            capture_output=True,
        )
    except Exception as exc:
        shutil.rmtree(destination, ignore_errors=True)
        raise RuntimeError(
            f"Failed to fetch Fala source dependency {url}@{revision}: {exc}"
        ) from exc


def _source_is_pinned(source_dir: Path, revision: str) -> bool:
    git = shutil.which("git")
    if git is None or not (source_dir / ".git").exists():
        return False
    result = subprocess.run(
        [git, "-C", str(source_dir), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0 and result.stdout.strip() == revision


def _ensure_ember_json_sources(root: Path) -> None:
    source_dir = root / "vendor" / "EmberJson"
    patch = root / "patches" / "emberjson-mojo-1.0.patch"
    if not _source_is_pinned(source_dir, _EMBER_JSON_REV):
        shutil.rmtree(source_dir, ignore_errors=True)
        _clone_pinned_source(
            url="https://github.com/bgreni/EmberJson.git",
            revision=_EMBER_JSON_REV,
            destination=source_dir,
        )
    reverse_check = subprocess.run(
        ["git", "-C", str(source_dir), "apply", "--check", "--reverse", str(patch)],
        capture_output=True,
        check=False,
    )
    if reverse_check is None or reverse_check.returncode != 0:
        subprocess.run(
            ["git", "-C", str(source_dir), "apply", str(patch)],
            check=True,
            capture_output=True,
        )


def _ensure_sqlite_fire_sources(root: Path) -> None:
    source_dir = root / "vendor" / "sqlite.fire"
    if not _source_is_pinned(source_dir, _SQLITE_FIRE_REV):
        shutil.rmtree(source_dir, ignore_errors=True)
        _clone_pinned_source(
            url="https://github.com/mikolaj92/sqlite.fire.git",
            revision=_SQLITE_FIRE_REV,
            destination=source_dir,
        )


def _build_native_extension(root: Path, so_path: Path) -> None:
    """Build the Mojo extension to a unique temp path and publish atomically."""
    env = _mojo_env(root)
    mojo = _mojo_bin(env)
    so_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f"{so_path.stem}.",
        suffix=".tmp.so",
        dir=so_path.parent,
    )
    os.close(fd)
    temp_path = Path(temp_name)
    try:
        cmd = [
            mojo,
            "build",
            str(_native_mojo_path(root)),
            "--emit",
            "shared-lib",
            "-I",
            str(root / "mojo"),
            "-I",
            str(root / "vendor" / "EmberJson"),
            "-I",
            str(root / "vendor" / "sqlite.fire" / "src"),
            "-o",
            str(temp_path),
        ]
        proc = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
        if proc.returncode != 0 or not temp_path.is_file() or temp_path.stat().st_size == 0:
            raise RuntimeError(
                "fala native build failed:\n" + (proc.stderr or proc.stdout or "")
            )
        os.replace(temp_path, so_path)
    finally:
        temp_path.unlink(missing_ok=True)


def _prune_stale_native_artifacts(cache_dir: Path, keep: Path) -> None:
    """Drop superseded cache entries only after the current artifact is published."""
    for old in cache_dir.glob("_native.hash-*.so"):
        if old.resolve() == keep.resolve():
            continue
        if old.name.endswith(".tmp.so"):
            continue
        old.unlink(missing_ok=True)
    for old in cache_dir.glob("_native.hash-*.tmp.so"):
        # Leftover crash debris; never touch the live artifact.
        if old.resolve() == keep.resolve():
            continue
        old.unlink(missing_ok=True)


def _ensure_native_artifact(root: Path | None = None) -> Path:
    """Return the published native extension path, building once under lock (#119).

    Look for a matching FALA_HOME checkout artifact before fetching vendor
    sources or compiling. A consumer wheel must not rebuild ``_native.mojo``
    against checkout vendor + pixi Mojo when the checkout already has a
    matching ``__mojocache__`` (#178).
    """
    active_root = root if root is not None else repo_root()
    native_mojo = _native_mojo_path(active_root)
    if not native_mojo.is_file():
        raise RuntimeError(f"missing {native_mojo}")

    digest = _source_hash(active_root)
    existing = _find_native_artifact(active_root, digest)
    if existing is not None:
        return existing

    _ensure_ember_json_sources(active_root)
    _ensure_sqlite_fire_sources(active_root)
    digest = _source_hash(active_root)
    existing = _find_native_artifact(active_root, digest)
    if existing is not None:
        return existing

    cache_dir = _native_cache_dirs(active_root)[0]
    cache_dir.mkdir(parents=True, exist_ok=True)
    so_path = cache_dir / f"_native.hash-{digest}.so"
    lock_path = cache_dir / "ensure_native.lock"

    if not so_path.is_file():
        with _cross_process_lock(lock_path):
            # Re-check under the lock so losers never rebuild.
            if not so_path.is_file():
                _build_native_extension(active_root, so_path)
            _prune_stale_native_artifacts(cache_dir, so_path)
    return so_path


def ensure_native() -> ModuleType:
    """Build/load the Mojo Python extension (memory + durable host entrypoints).

    Does **not** compile sqlite.fire by itself — call
    :func:`ensure_sqlite_fire_library` from durable APIs so pure memory-path
    users never require a C toolchain (#106).

    Concurrent callers share one cross-process lock and only the winner builds.
    Failed builds leave any previously published artifact intact (#119).

    When ``FALA_HOME`` has a matching checkout ``__mojocache__`` artifact, that
    file is loaded by absolute path. Vendor fetch and rebuild are skipped so a
    pinned consumer wheel does not compile against a foreign Mojo (#178).
    Caller ``DYLD_LIBRARY_PATH`` / ``LD_LIBRARY_PATH`` stay unchanged (#177).
    """
    root = repo_root()
    so_path = _ensure_native_artifact(root)

    env = _mojo_env(root)
    toolchain_root = _mojo_toolchain_root(env, root)
    if toolchain_root is not None:
        _preload_mojo_runtime(toolchain_root)

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
    suffix = ".dylib" if sys.platform == "darwin" else ".so"
    for stem in ("libKGENCompilerRTShared", "libAsyncRTMojoBindings"):
        library = lib_dir / f"{stem}{suffix}"
        if library.is_file():
            ctypes.CDLL(str(library), mode=ctypes.RTLD_GLOBAL)


def _mojo_env(root: Path) -> dict[str, str]:
    env = dict(os.environ)
    # A toolchain under the requested root is authoritative. Package discovery
    # may otherwise leak the caller's installed SDK into tests or another Fala
    # checkout and compile against the wrong Mojo version.
    root_toolchain = root / ".pixi" / "envs" / "default"
    if (root_toolchain / "bin" / "mojo").is_file():
        mojo_bin = root_toolchain / "bin" / "mojo"
        import_path = root_toolchain / "lib" / "mojo"
        env["MODULAR_MAX_PACKAGE_ROOT"] = str(root_toolchain)
        env["MODULAR_MOJO_MAX_PACKAGE_ROOT"] = str(root_toolchain)
        env["MODULAR_MOJO_MAX_DRIVER_PATH"] = str(mojo_bin)
        if import_path.is_dir():
            env["MODULAR_MOJO_MAX_IMPORT_PATH"] = str(import_path)
        env["PATH"] = str(root_toolchain / "bin") + os.pathsep + env.get("PATH", "")
        _prepend_env_path(env, root_toolchain / "lib")
        return env
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
