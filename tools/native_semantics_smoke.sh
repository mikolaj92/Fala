#!/bin/sh
set -eu

export LC_ALL=C
command -v mojo >/dev/null || {
    echo "native-semantics unavailable: missing prerequisite mojo" >&2
    exit 2
}

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
host="$root/mojo/fala/native/libfala_process_host.dylib"
tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/fala-native-semantics.XXXXXX")
original="$tmpdir/libfala_process_host.dylib"
had_host=0

restore_host() {
    rm -f "$host"
    if test "$had_host" -eq 1; then
        mv "$original" "$host"
    fi
    rm -rf "$tmpdir"
}
trap restore_host EXIT
trap 'exit 1' HUP INT TERM

if test -e "$host"; then
    had_host=1
    mv "$host" "$original"
fi
mkdir -p "$(dirname -- "$host")"
# A regular file at the expected path proves loader-based availability checks do
# not mistake a stale artifact for a usable process-host ABI.
printf '%s\n' "native-semantics sentinel" >"$host"

"$root/tools/setup_ember_json.sh"

"$root/tools/setup_sqlite_fire.sh"

cd "$root/vendor/sqlite.fire"
export DYLD_LIBRARY_PATH="$root/vendor/sqlite.fire/native${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
mojo run -I "$root/mojo" -I "$root/vendor/EmberJson" -I "$root/vendor/sqlite.fire/src" "$root/mojo/smoke/native_semantics.mojo"

