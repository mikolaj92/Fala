#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
destination="$root/vendor/EmberJson"
revision=951f4ef28d0c2748a30b2c5e43e139411ccca5ef
patch="$root/patches/emberjson-mojo-1.0.patch"

if [ ! -d "$destination/.git" ] ||
   [ "$(git -C "$destination" rev-parse HEAD 2>/dev/null || true)" != "$revision" ]; then
    rm -rf "$destination"
    mkdir -p "$(dirname -- "$destination")"
    git clone --filter=blob:none --no-checkout https://github.com/bgreni/EmberJson.git "$destination"
    git -C "$destination" fetch --depth 1 origin "$revision"
    git -C "$destination" checkout --detach "$revision"
fi

git -C "$destination" apply --check --reverse "$patch" 2>/dev/null ||
    git -C "$destination" apply "$patch"
