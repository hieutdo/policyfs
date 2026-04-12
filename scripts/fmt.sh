#!/usr/bin/env bash
set -euo pipefail

mode="${1:-all}"
if [[ "$mode" != "all" && "$mode" != "staged" ]]; then
  echo "usage: $0 [all|staged]" >&2
  exit 2
fi

list_files() {
  if [[ "$mode" == "staged" ]]; then
    git diff --name-only --cached --diff-filter=ACMR
  else
    git ls-files
  fi
}

files=()
while IFS= read -r f; do
  [[ -n "$f" && -f "$f" ]] && files+=("$f")
done < <(list_files)

unicode_dash_re=$'\xE2\x80\x94|\xE2\x80\x93'

dash_files=()
if [[ "$mode" == "all" ]]; then
  readarray -t dash_files < <(git grep -I -l -E "$unicode_dash_re" -- . || true)
else
  if ((${#files[@]} > 0)); then
    readarray -t dash_files < <(git grep -I -l -E "$unicode_dash_re" -- "${files[@]}" || true)
  fi
fi

if ((${#dash_files[@]} > 0)); then
  emdash=$'\xE2\x80\x94'
  endash=$'\xE2\x80\x93'
  for f in "${dash_files[@]}"; do
    sed -i "s/${emdash}/-/g; s/${endash}/-/g" "$f"
  done
fi

go_files=()
sh_files=()
md_files=()

for f in "${files[@]}"; do
  case "$f" in
    *.go)
      go_files+=("$f")
      ;;
    *.md)
      md_files+=("$f")
      ;;
    *.sh | scripts/* | .githooks/*)
      sh_files+=("$f")
      ;;
  esac
done

if ((${#go_files[@]} > 0)); then
  printf '%s\n' "${go_files[@]}" | xargs -r goimports -w
fi

if ((${#sh_files[@]} > 0)); then
  printf '%s\n' "${sh_files[@]}" | xargs -r shfmt -w -i 2 -ci -bn
fi

if ((${#md_files[@]} > 0)); then
  prettier -w -- "${md_files[@]}"
fi
