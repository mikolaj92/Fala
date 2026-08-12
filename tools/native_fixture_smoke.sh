#!/bin/sh
set -eu

export LC_ALL=C
command -v mojo >/dev/null || {
    echo "native-fixture-smoke unavailable: missing prerequisite mojo" >&2
    exit 2
}

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
"$root/tools/setup_ember_json.sh"

"$root/tools/setup_sqlite_fire.sh"

tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/fala-native-fixtures.XXXXXX")
trap 'rm -rf "$tmpdir"' EXIT HUP INT TERM
for fixture in schema-impulse schema-model run-lifecycle correlation-conduction retry-backoff cli-parser manifest-validation runtime-policy reaction-accumulation wait-diagnosis bridge-persistence; do
    case "$fixture" in
        schema-impulse) input='{"operation":"schema","model":"impulse"}' ;;
        schema-model) input='{"operation":"schema","model":"model"}' ;;
        run-lifecycle) input='{"operation":"journal","scenario":"persisted-lifecycle"}' ;;
        correlation-conduction) input='{"operation":"journal","scenario":"correlation-chain"}' ;;
        retry-backoff) input='{"operation":"journal","scenario":"retry-backoff"}' ;;
        cli-parser) input='{"operation":"cli","scenario":"event-filters"}' ;;
        manifest-validation) input='{"operation":"manifest","scenario":"strict-json"}' ;;
        runtime-policy) input='{"operation":"runtime","scenario":"least-busy-selection"}' ;;
        reaction-accumulation) input='{"operation":"reaction","scenario":"ancestor-order"}' ;;
        wait-diagnosis) input='{"operation":"journal","scenario":"waiting-process"}' ;;
        bridge-persistence) input='{"operation":"bridge","scenario":"outbox-idempotency"}' ;;
    esac
    input_path="$tmpdir/$fixture.json"
    printf '%s' "$input" >"$input_path"
    output=$(mojo run -I "$root/mojo" -I "$root/vendor/EmberJson" -I "$root/vendor/sqlite.fire/src" "$root/tests/native_fixture_producer.mojo" "$fixture" "$input_path")
    case "$output" in
        \{*\}) ;;
        *)
            echo "native fixture did not emit exactly one JSON object: $fixture" >&2
            exit 1
            ;;
    esac
done

printf '%s\n' "native fixture smoke ok: deterministic fixtures emitted one JSON object each"
