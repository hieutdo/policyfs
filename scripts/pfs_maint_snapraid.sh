#!/usr/bin/env bash
set -euo pipefail

# maint_snapraid_hook.sh runs `pfs maint` and (optionally) `snapraid sync` with simple notifications.
#
# Usage:
#   ./scripts/maint_snapraid_hook.sh <mount> [--pfs-maint-flags...]
#
# Environment:
#   - PFS_BIN: path to `pfs` (default: pfs)
#   - RUN_SNAPRAID: set to 0 to skip snapraid (default: 1)
#   - SNAPRAID_BIN: path to `snapraid` (default: snapraid)
#   - SNAPRAID_CONF: snapraid config file (default: /etc/snapraid.conf if exists)
#   - SNAPRAID_EXTRA_ARGS: extra args appended to `snapraid sync` (space-separated)
#   - NOTIFY_CMD: if set, will be executed as: $NOTIFY_CMD <stage> <message>
#
# systemd note:
#   If you want "busy" (exit 75) and "no changes" (exit 3) to be considered success:
#     SuccessExitStatus=75 3

# usage prints a short usage hint.
usage() {
  echo "usage: $0 <mount> [--pfs-maint-flags...]" >&2
  exit 2
}

# log writes a timestamped line to stderr.
log() {
  local msg="$1"
  printf '%s %s\n' "$(date -Is)" "$msg" >&2
}

# notify emits a notification. Failures are ignored by design.
notify() {
  local stage="$1"
  local msg="$2"

  if [[ -n "${NOTIFY_CMD:-}" ]]; then
    "${NOTIFY_CMD}" "$stage" "$msg" >/dev/null 2>&1 || true
    return 0
  fi

  if command -v logger >/dev/null 2>&1; then
    logger -t "pfs-maint-snapraid" -- "stage=${stage} ${msg}" || true
    return 0
  fi

  log "stage=${stage} ${msg}"
}

mount="${1:-}"
if [[ -z "$mount" || "$mount" == "-h" || "$mount" == "--help" ]]; then
  usage
fi
shift || true

pfs_bin="${PFS_BIN:-pfs}"
run_snapraid="${RUN_SNAPRAID:-1}"
snapraid_bin="${SNAPRAID_BIN:-snapraid}"
snapraid_conf="${SNAPRAID_CONF:-}"
if [[ -z "$snapraid_conf" && -f /etc/snapraid.conf ]]; then
  snapraid_conf="/etc/snapraid.conf"
fi
snapraid_extra_args="${SNAPRAID_EXTRA_ARGS:-}"

notify "maint_start" "mount=${mount}"

set +e
"${pfs_bin}" maint "${mount}" "$@"
maint_rc=$?
set -e

if [[ "$maint_rc" -eq 75 ]]; then
  notify "maint_busy" "mount=${mount} rc=75 (skipping snapraid)"
  exit 75
fi

if [[ "$maint_rc" -eq 3 ]]; then
  notify "maint_no_changes" "mount=${mount} rc=3 (skipping snapraid)"
  exit 3
fi

if [[ "$maint_rc" -ne 0 ]]; then
  notify "maint_failed" "mount=${mount} rc=${maint_rc}"
  exit "$maint_rc"
fi

notify "maint_ok" "mount=${mount}"

if [[ "$run_snapraid" == "0" ]]; then
  notify "snapraid_skipped" "mount=${mount} RUN_SNAPRAID=0"
  exit 0
fi

if [[ -z "$snapraid_conf" ]]; then
  notify "snapraid_skipped" "mount=${mount} SNAPRAID_CONF not set"
  exit 0
fi

if [[ ! -f "$snapraid_conf" ]]; then
  notify "snapraid_failed" "mount=${mount} config_not_found=${snapraid_conf}"
  exit 2
fi

snapraid_args=()
if [[ -n "$snapraid_extra_args" ]]; then
  read -r -a snapraid_args <<<"$snapraid_extra_args"
fi

notify "snapraid_start" "mount=${mount}"

set +e
snapraid_log_dir="${SNAPRAID_LOG_DIR:-/var/log/pfs}"
snapraid_log_file="${snapraid_log_dir}/snapraid-sync-${mount}.log"

snapraid_tmp=""
if mkdir -p "${snapraid_log_dir}" >/dev/null 2>&1 && [[ -w "${snapraid_log_dir}" ]]; then
  snapraid_tmp="${snapraid_log_file}.$$.$RANDOM.tmp"
else
  snapraid_tmp="$(mktemp -t "snapraid-sync-${mount}.XXXXXX" 2>/dev/null)" || snapraid_tmp="/tmp/snapraid-sync-${mount}.$$.$RANDOM.tmp"
  snapraid_log_file="${snapraid_tmp}"
fi

snapraid_rc=0
if command -v script >/dev/null 2>&1; then
  snapraid_cmd_str="$(printf '%q ' "${snapraid_bin}" -c "${snapraid_conf}" sync "${snapraid_args[@]}")"
  script -q -e -c "${snapraid_cmd_str}" "${snapraid_tmp}" >/dev/null 2>&1
  snapraid_rc=$?
else
  "${snapraid_bin}" -c "${snapraid_conf}" sync "${snapraid_args[@]}" >"${snapraid_tmp}" 2>&1
  snapraid_rc=$?
fi
set -e

if [[ "$snapraid_rc" -ne 0 ]]; then
  mv -f "${snapraid_tmp}" "${snapraid_log_file}" >/dev/null 2>&1 || true
  notify "snapraid_failed" "mount=${mount} rc=${snapraid_rc} log=${snapraid_log_file}"
  tail -n 200 "${snapraid_log_file}" >&2 2>/dev/null || true
  exit "$snapraid_rc"
fi

snapraid_success_lines="${SNAPRAID_SUCCESS_LOG_LINES:-200}"
if [[ "${snapraid_log_file}" != "${snapraid_tmp}" ]]; then
  tail -n "${snapraid_success_lines}" "${snapraid_tmp}" >"${snapraid_log_file}" 2>/dev/null || true
  rm -f "${snapraid_tmp}" >/dev/null 2>&1 || true
else
  snapraid_tail_tmp="${snapraid_tmp}.tail"
  if tail -n "${snapraid_success_lines}" "${snapraid_tmp}" >"${snapraid_tail_tmp}" 2>/dev/null; then
    mv -f "${snapraid_tail_tmp}" "${snapraid_tmp}" >/dev/null 2>&1 || true
  else
    rm -f "${snapraid_tail_tmp}" >/dev/null 2>&1 || true
  fi
fi

notify "snapraid_ok" "mount=${mount} log=${snapraid_log_file}"
exit 0
