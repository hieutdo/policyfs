#!/usr/bin/env bash
set -euo pipefail

# Build a Debian package for PolicyFS (Debian/Ubuntu amd64).
# Usage: scripts/package_deb.sh

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_dir="${BUILD_DIR:-${root_dir}/packaging/build}"
binary_path="${BINARY_PATH:-${root_dir}/bin/pfs}"

if ! command -v dpkg-deb >/dev/null 2>&1; then
  echo "pfs: error: dpkg-deb not found (run inside Debian/Ubuntu container)" >&2
  exit 1
fi
if [[ -z "${VERSION:-}" ]]; then
  echo "pfs: error: VERSION is required" >&2
  exit 1
fi
if [[ ! -x "${binary_path}" ]]; then
  echo "pfs: error: binary not found: ${binary_path}" >&2
  exit 1
fi

version="${VERSION:-}"
if [[ "${version}" == v[0-9]* ]]; then
  version="${version#v}"
fi
if [[ ! "${version}" =~ ^[0-9] ]]; then
  version="0.0.0+${version}"
fi

arch="${ARCH:-amd64}"
if [[ "${arch}" != "amd64" ]]; then
  echo "pfs: error: ARCH must be amd64" >&2
  exit 1
fi
pkg_dir="${build_dir}/pfs"
systemd_unit_dir="${pkg_dir}/lib/systemd/system"
debian_dir="${pkg_dir}/DEBIAN"
out_dir="${OUT_DIR:-${root_dir}/dist}"

rm -rf "${pkg_dir}"
mkdir -p "${debian_dir}" \
  "${pkg_dir}/usr/bin" \
  "${pkg_dir}/etc/pfs" \
  "${pkg_dir}/etc/logrotate.d" \
  "${systemd_unit_dir}" \
  "${pkg_dir}/var/lib/pfs" \
  "${out_dir}"

sed "s/@VERSION@/${version}/g; s/@ARCH@/${arch}/g" "${root_dir}/packaging/deb/control.in" >"${debian_dir}/control"

install -m 0755 "${root_dir}/packaging/deb/preinst" "${debian_dir}/preinst"
install -m 0755 "${root_dir}/packaging/deb/postinst" "${debian_dir}/postinst"
install -m 0755 "${root_dir}/packaging/deb/prerm" "${debian_dir}/prerm"
install -m 0755 "${root_dir}/packaging/deb/postrm" "${debian_dir}/postrm"

install -m 0644 "${root_dir}/packaging/systemd/pfs@.service" "${systemd_unit_dir}/pfs@.service"
install -m 0644 "${root_dir}/packaging/systemd/pfs-index@.service" "${systemd_unit_dir}/pfs-index@.service"
install -m 0644 "${root_dir}/packaging/systemd/pfs-index@.timer" "${systemd_unit_dir}/pfs-index@.timer"
install -m 0644 "${root_dir}/packaging/systemd/pfs-move@.service" "${systemd_unit_dir}/pfs-move@.service"
install -m 0644 "${root_dir}/packaging/systemd/pfs-move@.timer" "${systemd_unit_dir}/pfs-move@.timer"
install -m 0644 "${root_dir}/packaging/systemd/pfs-prune@.service" "${systemd_unit_dir}/pfs-prune@.service"
install -m 0644 "${root_dir}/packaging/systemd/pfs-prune@.timer" "${systemd_unit_dir}/pfs-prune@.timer"
install -m 0644 "${root_dir}/packaging/systemd/pfs-maint@.service" "${systemd_unit_dir}/pfs-maint@.service"
install -m 0644 "${root_dir}/packaging/systemd/pfs-maint@.timer" "${systemd_unit_dir}/pfs-maint@.timer"
install -m 0644 "${root_dir}/packaging/config/pfs.example.yaml" "${pkg_dir}/etc/pfs/pfs.yaml.example"
install -m 0644 "${root_dir}/packaging/logrotate/pfs" "${pkg_dir}/etc/logrotate.d/pfs"

install -m 0755 "${binary_path}" "${pkg_dir}/usr/bin/pfs"

pkg_name="policyfs_${version}_${arch}.deb"
dpkg-deb --build --root-owner-group "${pkg_dir}" "${out_dir}/${pkg_name}" >/dev/null

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "${out_dir}/${pkg_name}" >"${out_dir}/${pkg_name}.sha256"
elif command -v shasum >/dev/null 2>&1; then
  shasum -a 256 "${out_dir}/${pkg_name}" >"${out_dir}/${pkg_name}.sha256"
else
  echo "pfs: hint: sha256sum not found; skip checksum" >&2
fi

echo "pfs: built ${out_dir}/${pkg_name}" >&2
