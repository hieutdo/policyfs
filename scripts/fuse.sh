#!/usr/bin/env bash
set -euo pipefail

PFS_BIN="/workspace/bin/pfs"
PFS_CFG="/workspace/configs/dev.yaml"
MP="/mnt/pfs/media"

usage() {
  echo "usage: $0 {mount|umount}" >&2
  exit 2
}

_umount() {
  # Normal unmount first
  fusermount3 -u "$MP" 2>/dev/null && return 0

  # If stale ("Transport endpoint is not connected"), force/lazy it
  fusermount3 -uz "$MP" 2>/dev/null && return 0

  # Fallbacks
  umount "$MP" 2>/dev/null && return 0
  umount -l "$MP" 2>/dev/null && return 0

  return 0
}

_mount() {
  _umount

  go build -o "$PFS_BIN" ./cmd/pfs
  exec "$PFS_BIN" mount media --config "$PFS_CFG"
}

cmd="${1:-}"
case "$cmd" in
  mount) _mount ;;
  umount) _umount ;;
  *) usage ;;
esac
