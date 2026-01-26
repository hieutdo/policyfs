#!/usr/bin/env bash
set -euo pipefail

if [[ ! -d /workspace/cmd || ! -f /workspace/go.mod ]]; then
  echo "ERROR: /workspace is not mounted to the repo." >&2
  exit 2
fi

_term() {
  /workspace/scripts/virtual_disks.sh down || true
  kill -TERM "$child_pid" 2>/dev/null || true
  wait "$child_pid" 2>/dev/null || true
}

/workspace/scripts/virtual_disks.sh up

# Run child in background so PID1 can trap and cleanup on stop
"$@" &
child_pid=$!

trap _term TERM INT HUP
trap '/workspace/scripts/virtual_disks.sh down || true' EXIT

wait "$child_pid"
