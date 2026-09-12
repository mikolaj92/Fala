#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compiler="${CC:-cc}"
platform_flags=()

# Exercise the Linux preprocessor branch on macOS too. Its POSIX headers expose
# the same *_np declaration, so this catches a branch that only compiles on
# Apple while the Linux ARM64 CI job validates the real glibc feature macros.
if [[ "$(uname -s)" == "Darwin" ]]; then
    platform_flags=(-U__APPLE__ -D__linux__ -D_DARWIN_C_SOURCE)
fi

"$compiler" -std=c11 -Wall -Wextra -Werror=implicit-function-declaration \
    "${platform_flags[@]}" -fsyntax-only "$root/mojo/fala/native_process_host.c"
printf '%s\n' "native process host Linux compile check ok"
