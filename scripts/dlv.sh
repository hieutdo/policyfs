#!/usr/bin/env bash
set -euo pipefail

DLV_PORT="${DLV_PORT:-40000}"
MODE="${1:-code}"

usage() {
  cat <<USAGE >&2
usage: $0 [code|unit|integration]

  code         build ./cmd/pfs and start dlv (default)
  unit         build ./internal/... tests with go test -c
  integration  build ./tests/integration/... tests with go test -c -tags=integration
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
  pkill -f '/go/bin/dlv' 2>/dev/null || true
  sleep 0.1
}

build_code() {
  local out="/workspace/bin/pfs"
  /workspace/scripts/fuse.sh umount || true
  /usr/local/go/bin/go build -gcflags="all=-N -l" -o "$out" ./cmd/pfs
}

build_unit() {
  local out="/workspace/bin/pfs.test.unit"
  /usr/local/go/bin/go test -c -gcflags="all=-N -l" -o "$out" ./internal/...
}

build_integration() {
  local out="/workspace/bin/pfs.test.integration"
  /usr/local/go/bin/go test -tags=integration -c -gcflags="all=-N -l" -o "$out" ./tests/integration/...
}

start_dlv() {
  exec /go/bin/dlv dap --listen="0.0.0.0:${DLV_PORT}" --log=false
}

kill_dlv

case "$MODE" in
  code)
    build_code
    ;;
  unit)
    build_unit
    ;;
  integration)
    build_integration
    ;;
esac

start_dlv
