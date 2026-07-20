#!/usr/bin/env bash
# Run a Mojo smoke that needs sqlite.fire (+ optional process host).
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
smoke="${1:?smoke path under mojo/smoke or absolute}"
need_host="${2:-0}"

test -s "$root/vendor/sqlite.fire/native/libsqlite_fire.dylib" \
  || make -C "$root/vendor/sqlite.fire/native"

if [[ "$need_host" == "1" ]]; then
  mkdir -p "$root/mojo/fala/native"
  test -s "$root/mojo/fala/native/libfala_process_host.dylib" \
    || cc -std=c11 -Wall -Wextra -dynamiclib \
         -o "$root/mojo/fala/native/libfala_process_host.dylib" \
         "$root/mojo/fala/native_process_host.c"
  # Effector fixture binary used by native_subprocess smoke (argv child).
  test -x /tmp/fala-native-subprocess-fixture \
    || cc -std=c11 -Wall -Wextra \
         -o /tmp/fala-native-subprocess-fixture \
         "$root/mojo/smoke/native_effector_fixture.c"
fi

case "$smoke" in
  /*) target="$smoke" ;;
  *) target="$root/$smoke" ;;
esac

cd "$root/vendor/sqlite.fire"
exec mojo run -I ../../mojo -I ../EmberJson -I src "$target"
