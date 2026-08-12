#!/usr/bin/env bash
# Run a Mojo smoke that needs sqlite.fire (+ optional process host).
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
smoke="${1:?smoke path under mojo/smoke or absolute}"
need_host="${2:-0}"

"$root/tools/setup_ember_json.sh"

"$root/tools/setup_sqlite_fire.sh"

# sqlite.fire native (platform-specific name from its Makefile)
if [[ "$(uname -s)" == "Darwin" ]]; then
  sqlite_lib="$root/vendor/sqlite.fire/native/libsqlite_fire.dylib"
else
  sqlite_lib="$root/vendor/sqlite.fire/native/libsqlite_fire.so"
fi
test -s "$sqlite_lib" || make -C "$root/vendor/sqlite.fire/native"
if [[ "$need_host" == "1" ]]; then
  mkdir -p "$root/mojo/fala/native"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    host_lib="$root/mojo/fala/native/libfala_process_host.dylib"
    host_src="$root/mojo/fala/native_process_host.c"
    if [[ ! -s "$host_lib" || "$host_src" -nt "$host_lib" ]]; then
      cc -std=c11 -Wall -Wextra -dynamiclib \
        -o "$host_lib" \
        "$host_src"
    fi
  else
    host_lib="$root/mojo/fala/native/libfala_process_host.so"
    host_src="$root/mojo/fala/native_process_host.c"
    if [[ ! -s "$host_lib" || "$host_src" -nt "$host_lib" ]]; then
      cc -std=c11 -Wall -Wextra -fPIC -shared \
        -o "$host_lib" \
        "$host_src"
    fi
  fi
  export FALA_PROCESS_HOST_LIBRARY="$host_lib"
  # Effector fixture binary used by native_subprocess smoke (argv child).
  fixture="/tmp/fala-native-subprocess-fixture"
  if [[ ! -x "$fixture" ]]; then
    cc -std=c11 -Wall -Wextra \
      -o "$fixture" \
      "$root/mojo/smoke/native_effector_fixture.c"
  fi
fi

case "$smoke" in
  /*) target="$smoke" ;;
  *) target="$root/$smoke" ;;
esac

cd "$root/vendor/sqlite.fire"
exec mojo run -I ../../mojo -I ../EmberJson -I src "$target"
