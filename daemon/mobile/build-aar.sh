#!/usr/bin/env bash
set -euo pipefail

mobile_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$mobile_dir/../.." && pwd)"
output="${GOWORKER_AAR:-$repo_root/build/goworker.aar}"
if [[ "$output" != /* ]]; then output="$repo_root/$output"; fi

if [[ -n "${GOMOBILE:-}" ]]; then
    gomobile="$GOMOBILE"
elif command -v gomobile >/dev/null 2>&1; then
    gomobile="$(command -v gomobile)"
else
    gomobile="${GOPATH:-$(go env GOPATH)}/bin/gomobile"
fi

if [[ ! -x "$gomobile" ]]; then
    printf 'gomobile not found: %s\n' "$gomobile" >&2
    printf 'install it with: go install golang.org/x/mobile/cmd/gomobile@latest\n' >&2
    exit 1
fi

export PATH="$(dirname -- "$gomobile"):$PATH"
mkdir -p "$(dirname -- "$output")"
cd -- "$repo_root/daemon"
exec "$gomobile" bind -target=android -o "$output" ./mobile
