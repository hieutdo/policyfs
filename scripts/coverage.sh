#!/usr/bin/env bash
set -euo pipefail

COVER_DIR="${COVER_DIR:-/workspace/coverage}"
UNIT_PROFILE="${COVER_DIR}/unit.out"
INTEGRATION_PROFILE="${COVER_DIR}/integration.out"
MERGED_PROFILE="${COVER_DIR}/coverage.out"
ENTRY_HTML="${COVER_DIR}/index.html"
LCOV_INFO="${COVER_DIR}/lcov.info"
LCOV_REPORT_DIR="${COVER_DIR}/lcov-report"

rm -rf "${COVER_DIR}"
mkdir -p "${COVER_DIR}"

echo "[coverage] running unit tests"
go test -count=1 -covermode=atomic -coverpkg=./... -coverprofile="${UNIT_PROFILE}" ./...

if [[ -d ./tests/integration ]]; then
  echo "[coverage] running integration tests"
  go test -count=1 -tags=integration -covermode=atomic -coverpkg=./... -coverprofile="${INTEGRATION_PROFILE}" ./tests/integration/...
else
  echo "[coverage] no ./tests/integration directory; skipping integration tests" >&2
  printf 'mode: atomic\n' >"${INTEGRATION_PROFILE}"
fi

echo "[coverage] merging coverprofiles"
gocovmerge "${UNIT_PROFILE}" "${INTEGRATION_PROFILE}" >"${MERGED_PROFILE}"

echo "[coverage] generating html report"
if command -v genhtml >/dev/null 2>&1; then
  rm -rf "${LCOV_REPORT_DIR}"
  gcov2lcov -infile "${MERGED_PROFILE}" -outfile "${LCOV_INFO}"
  genhtml -o "${LCOV_REPORT_DIR}" "${LCOV_INFO}" >/dev/null
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
