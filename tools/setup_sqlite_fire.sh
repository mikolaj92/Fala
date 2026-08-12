#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
source_dir="$root/vendor/sqlite.fire"
revision="3d482362c863e769d018443045b27ca5db645b3c"
patch="$root/patches/sqlite-fire-mojo-1.0.patch"

if [ ! -d "$source_dir/.git" ] || [ "$(git -C "$source_dir" rev-parse HEAD 2>/dev/null || true)" != "$revision" ]; then
    rm -rf "$source_dir"
    git clone --filter=blob:none --no-checkout https://github.com/mikolaj92/sqlite.fire.git "$source_dir"
    git -C "$source_dir" checkout --detach "$revision"
fi

if git -C "$source_dir" apply --check --reverse "$patch" >/dev/null 2>&1; then
    exit 0
fi
git -C "$source_dir" apply --check "$patch"
git -C "$source_dir" apply "$patch"
