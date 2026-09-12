#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output="${1:?usage: build_native_effector_fixture.sh OUTPUT_PATH}"
compiler="${CC:-cc}"
compiler_args=(-std=c11 -Wall -Wextra)
target_linux=0

if [[ "$(uname -s)" == "Linux" || "${FALA_FORCE_LINUX:-0}" == "1" ]]; then
    target_linux=1
fi

if [[ "$target_linux" == "1" ]]; then
    if [[ "$(uname -s)" == "Darwin" ]]; then
        compiler_args+=(-U__APPLE__ -D__linux__)
    fi
    if [[ -n "${CONDA_PREFIX:-}" ]]; then
        compiler_args+=(-I"$CONDA_PREFIX/include")
    fi
fi

compiler_args+=(-o "$output" "$root/mojo/smoke/native_effector_fixture.c")
if [[ "$target_linux" == "1" ]]; then
    if [[ -n "${CONDA_PREFIX:-}" ]]; then
        compiler_args+=(-L"$CONDA_PREFIX/lib" "-Wl,-rpath,$CONDA_PREFIX/lib")
    fi
    compiler_args+=(-lcrypto)
fi

"$compiler" "${compiler_args[@]}"
