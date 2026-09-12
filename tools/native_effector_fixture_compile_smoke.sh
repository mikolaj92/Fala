#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/fala-effector-fixture.XXXXXX")"
trap 'rm -rf "$tmpdir"' EXIT

"$root/tools/build_native_effector_fixture.sh" "$tmpdir/native-effector-fixture"
if [[ "$(uname -s)" == "Darwin" ]]; then
    FALA_FORCE_LINUX=1 "$root/tools/build_native_effector_fixture.sh" \
        "$tmpdir/native-effector-fixture-linux"
fi
printf '%s\n' "native effector fixture compile smoke ok"
