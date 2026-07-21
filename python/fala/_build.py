"""Compile ``_native.mojo`` against the Mojo Fala core (memory host path)."""

from __future__ import annotations

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


def _source_hash(root: Path) -> str:
    paths = sorted(
        list(_PACKAGE_DIR.glob("*.mojo"))
        + list((root / "mojo" / "fala").rglob("*.mojo"))
        + list((root / "vendor" / "EmberJson").rglob("*.mojo"))
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


def _mojo_env() -> dict[str, str]:
    env = dict(os.environ)
    try:
        from mojo._package_root import get_package_root  # type: ignore[import-not-found]
        from mojo.run import _sdk_default_env  # type: ignore[import-not-found]

        root = get_package_root()
        if root is not None:
            return {**_sdk_default_env(), **env}
    except Exception:
        pass
    candidates = [
        Path(env["CONDA_PREFIX"]) if env.get("CONDA_PREFIX") else None,
        Path.home() / "Developer" / "OSS" / "Fala" / ".pixi" / "envs" / "default",
    ]
    for root in candidates:
        if root is None:
            continue
        mojo_bin = root / "bin" / "mojo"
        import_path = root / "lib" / "mojo"
        if mojo_bin.is_file() and import_path.is_dir():
            env.setdefault("MODULAR_MAX_PACKAGE_ROOT", str(root))
            env.setdefault("MODULAR_MOJO_MAX_PACKAGE_ROOT", str(root))
            env.setdefault("MODULAR_MOJO_MAX_DRIVER_PATH", str(mojo_bin))
            env.setdefault("MODULAR_MOJO_MAX_IMPORT_PATH", str(import_path))
            env["PATH"] = str(root / "bin") + os.pathsep + env.get("PATH", "")
            break
    return env


def _mojo_bin(env: dict[str, str]) -> str:
    for key in ("MODULAR_MOJO_MAX_DRIVER_PATH", "MOJO"):
        p = env.get(key)
        if p and Path(p).is_file():
            return p
    found = shutil.which("mojo", path=env.get("PATH"))
    if found:
        return found
    fala = Path.home() / "Developer" / "OSS" / "Fala" / ".pixi" / "envs" / "default" / "bin" / "mojo"
    if fala.is_file():
        return str(fala)
    raise RuntimeError("mojo executable not found")


def ensure_native() -> ModuleType:
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
        env = _mojo_env()
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
            "-o",
            str(so_path),
        ]
        proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(
                "fala native build failed:\n" + (proc.stderr or proc.stdout or "")
            )
    spec = importlib.util.spec_from_file_location("fala._native", so_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {so_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["fala._native"] = mod
    spec.loader.exec_module(mod)
    return mod
