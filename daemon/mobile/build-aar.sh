#!/usr/bin/env bash
set -euo pipefail

mobile_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$mobile_dir/../.." && pwd)"
output="${GOWORKER_AAR:-$repo_root/build/goworker-agent.aar}"
if [[ "$output" != /* ]]; then output="$repo_root/$output"; fi
go_tags="${GOWORKER_MOBILE_GO_TAGS:-goworker_no_dispatcher}"

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
deps="$(go list -tags="$go_tags" -deps ./mobile)"
agent_package='github.com/tinguo/goworker/daemon/internal/plugins/agent'
dispatcher_package='github.com/tinguo/goworker/daemon/internal/plugins/dispatcher'
if ! grep -Fxq "$agent_package" <<< "$deps"; then
    printf 'mobile build must include the Agent plugin (tags: %s)\n' "$go_tags" >&2
    exit 1
fi
if grep -Fxq "$dispatcher_package" <<< "$deps"; then
    printf 'mobile Agent-only build unexpectedly includes the Dispatcher plugin (tags: %s)\n' "$go_tags" >&2
    exit 1
fi
exec "$gomobile" bind -target=android -tags="$go_tags" -o "$output" ./mobile
