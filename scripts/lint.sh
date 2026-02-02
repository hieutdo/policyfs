#!/usr/bin/env bash
set -euo pipefail

mode="${1:-all}"
if [[ "$mode" != "all" && "$mode" != "staged" ]]; then
  echo "usage: $0 [all|staged]" >&2
  exit 2
fi

if [[ "$mode" == "all" ]]; then
  golangci-lint run ./...
  if [[ -d ./tests/integration ]]; then
    GOFLAGS="-tags=integration" golangci-lint run --tests ./tests/integration/...
  fi
  exit 0
fi

staged_go_files=()
while IFS= read -r f; do
  [[ -n "$f" ]] && staged_go_files+=("$f")
done < <(git diff --name-only --cached --diff-filter=ACMR -- '*.go')

if ((${#staged_go_files[@]} == 0)); then
  exit 0
fi

pkg_dirs=()
integration_needed=false
for f in "${staged_go_files[@]}"; do
  d="$(dirname "$f")"
  if [[ "$d" == "." ]]; then
    d="./"
  elif [[ "$d" != ./* ]]; then
    d="./$d"
  fi
  case "$d" in
    ./tests/integration*)
      integration_needed=true
      ;;
    *)
      pkg_dirs+=("$d")
      ;;
  esac
done

normal_dirs=()
if ((${#pkg_dirs[@]} > 0)); then
  readarray -t normal_dirs < <(printf '%s\n' "${pkg_dirs[@]}" | sort -u)
fi

if ((${#normal_dirs[@]} > 0)); then
  golangci-lint run -- "${normal_dirs[@]}"
fi

if [[ "$integration_needed" == true ]]; then
  GOFLAGS="-tags=integration" golangci-lint run --tests ./tests/integration/...
fi
