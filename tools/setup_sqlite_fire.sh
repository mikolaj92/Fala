#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
source_dir="$root/vendor/sqlite.fire"
revision="2cb4da921f590f170f6431ab873cd8200384f09a"

if [ ! -d "$source_dir/.git" ] || [ "$(git -C "$source_dir" rev-parse HEAD 2>/dev/null || true)" != "$revision" ]; then
    rm -rf "$source_dir"
    git clone --filter=blob:none --no-checkout https://github.com/mikolaj92/sqlite.fire.git "$source_dir"
    git -C "$source_dir" checkout --detach "$revision"
fi

mojo_minor=$(mojo --version 2>/dev/null | sed -n 's/.*Mojo \([0-9][0-9]*\)\.\([0-9][0-9]*\).*/\1.\2/p')
case "$mojo_minor" in
    1.[2-9]|[2-9].*)
        patch="$root/patches/sqlite-fire-mojo-1.2.patch"
        git -C "$source_dir" apply --check --reverse "$patch" 2>/dev/null ||
            git -C "$source_dir" apply "$patch"
        ;;
esac
