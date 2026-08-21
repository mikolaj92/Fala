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
