#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Coverage directories
COVER_DIR="${COVER_DIR:-/workspace/coverage}"
INTEGRATION_COVER_DIR="${COVER_DIR}/covdata-integration"

# Coverage files
UNIT_PROFILE="${COVER_DIR}/unit.out"
INTEGRATION_PROFILE="${COVER_DIR}/integration.out"
MERGED_PROFILE="${COVER_DIR}/coverage.out"

# Report files
ENTRY_HTML="${COVER_DIR}/index.html"
LCOV_INFO="${COVER_DIR}/lcov.info"
LCOV_REPORT_DIR="${COVER_DIR}/lcov-report"

cd "${ROOT_DIR}"
rm -rf "${COVER_DIR}"
mkdir -p "${COVER_DIR}"

rm -rf "${INTEGRATION_COVER_DIR}"
mkdir -p "${INTEGRATION_COVER_DIR}"

echo "[coverage] running unit tests"
go test -count=1 -covermode=atomic -coverpkg=./cmd/...,./internal/... -coverprofile="${UNIT_PROFILE}" ./cmd/... ./internal/...

if [[ -d ./tests/integration ]]; then
  echo "[coverage] running integration tests"
  PFS_INTEGRATION_COVER=1 GOCOVERDIR="${INTEGRATION_COVER_DIR}" go test -count=1 -tags=integration ./tests/integration/...

  echo "[coverage] converting integration covdata to coverprofile"
  if [[ -n "$(ls -A "${INTEGRATION_COVER_DIR}" 2>/dev/null || true)" ]]; then
    go tool covdata textfmt -i="${INTEGRATION_COVER_DIR}" -o "${INTEGRATION_PROFILE}"
  else
    printf 'mode: atomic\n' >"${INTEGRATION_PROFILE}"
  fi
else
  echo "[coverage] no ./tests/integration directory; skipping integration tests" >&2
  printf 'mode: atomic\n' >"${INTEGRATION_PROFILE}"
fi

echo "[coverage] merging coverprofiles"
gocovmerge "${UNIT_PROFILE}" "${INTEGRATION_PROFILE}" >"${MERGED_PROFILE}"

echo "[coverage] generating html report"
if command -v genhtml >/dev/null 2>&1; then
  gcov2lcov -infile "${MERGED_PROFILE}" -outfile "${LCOV_INFO}"
  genhtml --ignore-errors empty -o "${LCOV_REPORT_DIR}" "${LCOV_INFO}" >/dev/null
  cat >"${ENTRY_HTML}" <<'EOF'
<!doctype html>
<meta http-equiv="refresh" content="0; url=lcov-report/index.html">
EOF
  echo "[coverage] report: ${LCOV_REPORT_DIR}/index.html"
else
  echo "[coverage] genhtml not found; falling back to go tool cover" >&2
  go tool cover -html="${MERGED_PROFILE}" -o "${ENTRY_HTML}"
  echo "[coverage] report: ${ENTRY_HTML}"
fi

go tool cover -func="${MERGED_PROFILE}" | tail -n 1
