#!/usr/bin/env bash
set -euo pipefail

DISK_DIR="/var/lib/pfs-disks"
DISK_ORDER=(ssd1 ssd2 hdd1 hdd2 hdd3)

# Format: fstype:size_mb:mountpoint
declare -A DISKS=(
  ["ssd1"]="ext4:512:/mnt/ssd1"
  ["ssd2"]="xfs:512:/mnt/ssd2"
  ["hdd1"]="ext4:1024:/mnt/hdd1"
  ["hdd2"]="xfs:2048:/mnt/hdd2"
  ["hdd3"]="xfs:2048:/mnt/hdd3"
)

_cleanup_mounts() {
  local name fstype size mp
  for name in "${DISK_ORDER[@]}"; do
    IFS=':' read -r fstype size mp <<<"${DISKS[$name]}"
    if mountpoint -q "$mp" 2>/dev/null; then
      umount "$mp" >/dev/null 2>&1 || umount -l "$mp" >/dev/null 2>&1 || true
    fi
  done
}

_cleanup_loops() {
  # Detach any loop devices backed by our disk images
  shopt -s nullglob
  local img loop
  for img in "$DISK_DIR"/*.img; do
    while IFS= read -r loop; do
      [[ -n "$loop" ]] && losetup -d "$loop" 2>/dev/null || true
    done < <(losetup -j "$img" 2>/dev/null | awk -F: '{print $1}')
  done
}

cleanup() {
  _cleanup_mounts
  _cleanup_loops
}

_setup_disk() {
  local name="$1"
  local fstype="$2"
  local size_mb="$3"
  local mountpoint="$4"

  local img_file="${DISK_DIR}/${name}.img"
  local loop_dev=""
  local cur_fs=""

  mkdir -p "$mountpoint"

  if [[ ! -f "$img_file" ]]; then
    fallocate -l "${size_mb}M" "$img_file" 2>/dev/null \
      || dd if=/dev/zero of="$img_file" bs=1M count="$size_mb" status=none
  fi

  # Reuse existing loop if already attached (keep the first, detach the rest)
  loop_dev="$(losetup -j "$img_file" 2>/dev/null | awk -F: 'NR==1{print $1}')"
  losetup -j "$img_file" 2>/dev/null | awk -F: 'NR>1{print $1}' | xargs -r losetup -d 2>/dev/null || true

  if [[ -z "$loop_dev" ]]; then
    loop_dev="$(losetup --find --show "$img_file" 2>/dev/null || true)"
  fi
  if [[ -z "$loop_dev" ]]; then
    echo "no free loop devices for $img_file" >&2
    return 1
  fi

  cur_fs="$(blkid -o value -s TYPE "$loop_dev" 2>/dev/null || true)"
  if [[ "$cur_fs" != "$fstype" ]]; then
    case "$fstype" in
      ext4) mkfs.ext4 -F -m 0 -L "$name" "$loop_dev" >/dev/null ;;
      xfs) mkfs.xfs -f -L "$name" "$loop_dev" >/dev/null ;;
      *)
        echo "unsupported fstype: $fstype" >&2
        return 1
        ;;
    esac
  fi

  if mountpoint -q "$mountpoint"; then
    local src
    src="$(findmnt -n -o SOURCE --target "$mountpoint" 2>/dev/null || true)"
    if [[ -n "$src" && "$src" != "$loop_dev" ]]; then
      umount "$mountpoint" >/dev/null 2>&1 || umount -l "$mountpoint" >/dev/null 2>&1 || true
    fi
  fi

  if ! mountpoint -q "$mountpoint"; then
    mount "$loop_dev" "$mountpoint"
  fi

  mkdir -p "${mountpoint}/media"
  chmod 777 "${mountpoint}/media"
}

up() {
  modprobe loop >/dev/null 2>&1 || true
  if [[ ! -e /dev/loop-control ]]; then
    mknod -m 0660 /dev/loop-control c 10 237 >/dev/null 2>&1 || true
  fi
  mkdir -p "$DISK_DIR"

  # Cleanup first to avoid loop leaks across runs
  cleanup

  local name fstype size mountpoint
  for name in "${DISK_ORDER[@]}"; do
    IFS=':' read -r fstype size mountpoint <<<"${DISKS[$name]}"
    _setup_disk "$name" "$fstype" "$size" "$mountpoint"
  done
}

down() {
  cleanup
}

case "${1:-up}" in
  up) up ;;
  down) down ;;
  *)
    echo "usage: $0 [up|down]" >&2
    exit 2
    ;;
esac
