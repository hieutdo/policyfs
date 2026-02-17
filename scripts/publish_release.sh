#!/usr/bin/env bash
set -euo pipefail

# Publish a built .deb to GitHub Releases using gh (manual fallback).
# Usage: scripts/publish_release.sh <tag> <deb_path>

tag="${1:-}"
deb_path="${2:-}"

if [[ -z "${tag}" || -z "${deb_path}" ]]; then
  echo "usage: scripts/publish_release.sh <tag> <deb_path>" >&2
  exit 2
fi

if [[ ! -f "${deb_path}" ]]; then
  echo "pfs: error: deb not found: ${deb_path}" >&2
  exit 2
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "pfs: hint: gh not found; upload ${deb_path} to GitHub Releases manually" >&2
  exit 2
fi

gh release create "${tag}" "${deb_path}" "${deb_path}.sha256" --notes "pfs ${tag}"
