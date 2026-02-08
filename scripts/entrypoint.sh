#!/usr/bin/env bash
set -euo pipefail

if [[ ! -d /workspace/cmd || ! -f /workspace/go.mod ]]; then
  echo "ERROR: /workspace is not mounted to the repo." >&2
  exit 2
fi

if [[ -n "${PFS_STATE_DIR:-}" ]]; then
  mkdir -p "${PFS_STATE_DIR}"
fi

if [[ -n "${PFS_RUNTIME_DIR:-}" ]]; then
  mkdir -p "${PFS_RUNTIME_DIR}"
fi

if [[ -n "${PFS_LOG_FILE:-}" ]]; then
  mkdir -p "$(dirname "${PFS_LOG_FILE}")"
  touch "${PFS_LOG_FILE}" || true
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
