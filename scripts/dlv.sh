#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-code}"
PFS_CFG="${PFS_CFG:-/workspace/configs/dev.yaml}"
PFS_LOG_FILE="${PFS_LOG_FILE:-/workspace/tmp/pfs.log}"
PFS_MOUNT_NAME="${PFS_MOUNT_NAME:-media}"

usage() {
  cat <<USAGE >&2
usage: $0 [code|unit|integration]

  code         build ./cmd/pfs and start dlv (default)
  unit         build ./internal/cli tests with go test -c
  integration  build ./tests/integration tests with go test -c -tags=integration

env:
  PFS_CFG            config path (default: /workspace/configs/dev.yaml)
  PFS_LOG_FILE       log file path (default: /workspace/tmp/pfs.log)
  PFS_MOUNT_NAME     mount name (default: media)
USAGE
  exit 2
}

case "$MODE" in
  code) ;;
  unit) ;;
  integration) ;;
  *)
    usage
    ;;
esac

kill_dlv() {
  pkill -f "/go/bin/dlv dap --listen=0.0.0.0:${1}" 2>/dev/null || true
  sleep 0.1
}

build_code() {
  go build -gcflags="all=-N -l" -o "/workspace/bin/pfs" "/workspace/cmd/pfs"
}

build_unit() {
  go test -c -gcflags="all=-N -l" -o "/workspace/bin/pfs.unit.cli" "/workspace/internal/cli"
}

build_integration() {
  go test -tags=integration -c -gcflags="all=-N -l" -o "/workspace/bin/pfs.integration" "/workspace/tests/integration"
}

start_dlv_dap() {
  kill_dlv "$1"
  /workspace/scripts/fuse.sh umount || true
  exec /go/bin/dlv dap --listen="0.0.0.0:${1}" --log=false
}

case "$MODE" in
  code)
    build_code
    start_dlv_dap 40000
    ;;
  unit)
    build_unit
    start_dlv_dap 40010
    ;;
  integration)
    build_integration
    start_dlv_dap 40020
    ;;
esac
