#!/usr/bin/env bash
set -euo pipefail

# Build the docs site with a versioned prefix (/v1/).
#
# Output layout:
#   public/index.html        (redirect to /v1/)
#   public/_redirects        (Cloudflare Pages redirects)
#   public/v1/*              (MkDocs site)

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

out_dir="${OUT_DIR:-${root_dir}/public}"
rm -rf "${out_dir}"
mkdir -p "${out_dir}"

# Build MkDocs into /v1.
python_bin="${PYTHON_BIN:-python3}"
"${python_bin}" -m mkdocs build -f "${root_dir}/mkdocs.yml" -d "${out_dir}/v1"

# Add root redirect for the docs domain.
cp "${root_dir}/docsroot/index.html" "${out_dir}/index.html"
cp "${root_dir}/docsroot/_redirects" "${out_dir}/_redirects"
