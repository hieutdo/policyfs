#!/usr/bin/env bash
set -euo pipefail

# Build a Debian package for PolicyFS (Debian/Ubuntu amd64).
# Usage: scripts/package_deb.sh

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! command -v dpkg-deb >/dev/null 2>&1; then
  echo "pfs: error: dpkg-deb not found (run inside Debian/Ubuntu container)" >&2
  exit 1
fi

version="${VERSION:-$(git -C "${root_dir}" describe --tags --always --dirty 2>/dev/null || echo "dev")}"
commit="${COMMIT:-$(git -C "${root_dir}" rev-parse --short HEAD 2>/dev/null || echo "unknown")}"
date="${DATE:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
arch="${ARCH:-amd64}"

ldflags="${LDFLAGS:--X github.com/hieutdo/policyfs/internal/cli.Version=${version} -X github.com/hieutdo/policyfs/internal/cli.Commit=${commit} -X github.com/hieutdo/policyfs/internal/cli.BuildDate=${date}}"

build_dir="${BUILD_DIR:-${root_dir}/packaging/build}"
pkg_dir="${build_dir}/pfs"
debian_dir="${pkg_dir}/DEBIAN"
out_dir="${OUT_DIR:-${root_dir}/dist}"

rm -rf "${pkg_dir}"
mkdir -p "${debian_dir}" \
  "${pkg_dir}/usr/bin" \
  "${pkg_dir}/etc/pfs" \
  "${pkg_dir}/etc/systemd/system" \
  "${pkg_dir}/var/lib/pfs" \
  "${out_dir}"

sed "s/@VERSION@/${version}/g; s/@ARCH@/${arch}/g" "${root_dir}/packaging/deb/control.in" >"${debian_dir}/control"

install -m 0755 "${root_dir}/packaging/deb/postinst" "${debian_dir}/postinst"
install -m 0755 "${root_dir}/packaging/deb/prerm" "${debian_dir}/prerm"
install -m 0755 "${root_dir}/packaging/deb/postrm" "${debian_dir}/postrm"

install -m 0644 "${root_dir}/packaging/systemd/pfs@.service" "${pkg_dir}/etc/systemd/system/pfs@.service"
install -m 0644 "${root_dir}/packaging/systemd/pfs-index@.service" "${pkg_dir}/etc/systemd/system/pfs-index@.service"
install -m 0644 "${root_dir}/packaging/systemd/pfs-index@.timer" "${pkg_dir}/etc/systemd/system/pfs-index@.timer"
install -m 0644 "${root_dir}/packaging/config/pfs.example.yaml" "${pkg_dir}/etc/pfs/pfs.yaml.example"

(cd "${root_dir}" && GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go build -ldflags "${ldflags}" -o "${pkg_dir}/usr/bin/pfs" ./cmd/pfs)

pkg_name="pfs_${version}_${arch}.deb"
dpkg-deb --build --root-owner-group "${pkg_dir}" "${out_dir}/${pkg_name}" >/dev/null

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "${out_dir}/${pkg_name}" >"${out_dir}/${pkg_name}.sha256"
elif command -v shasum >/dev/null 2>&1; then
  shasum -a 256 "${out_dir}/${pkg_name}" >"${out_dir}/${pkg_name}.sha256"
else
  echo "pfs: hint: sha256sum not found; skip checksum" >&2
fi

echo "pfs: built ${out_dir}/${pkg_name}" >&2
