#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_dir="${BUILD_DIR:-${root_dir}/packaging/build}"
binary_path="${root_dir}/bin/pfs"

if ! command -v rpmbuild >/dev/null 2>&1; then
  echo "pfs: error: rpmbuild not found (run inside Fedora/EL container)" >&2
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

version="${VERSION}"
if [[ "${version}" == v[0-9]* ]]; then
  version="${version#v}"
fi

arch="${ARCH:-x86_64}"
if [[ "${arch}" != "x86_64" ]]; then
  echo "pfs: error: ARCH must be x86_64" >&2
  exit 1
fi

release="${RELEASE:-1}"
out_dir="${OUT_DIR:-${root_dir}/dist/rpm}"

work_dir="${build_dir}/rpm"
src_dir="${work_dir}/src/policyfs-${version}"
topdir="${work_dir}/rpmbuild"

rm -rf "${work_dir}"
mkdir -p \
  "${src_dir}/usr/bin" \
  "${src_dir}/usr/lib/systemd/system" \
  "${src_dir}/etc/pfs" \
  "${src_dir}/etc/logrotate.d" \
  "${src_dir}/var/lib/pfs" \
  "${topdir}/SOURCES" \
  "${topdir}/SPECS" \
  "${out_dir}"

install -m 0755 "${binary_path}" "${src_dir}/usr/bin/pfs"

install -m 0644 "${root_dir}/packaging/systemd/pfs@.service" "${src_dir}/usr/lib/systemd/system/pfs@.service"
install -m 0644 "${root_dir}/packaging/systemd/pfs-index@.service" "${src_dir}/usr/lib/systemd/system/pfs-index@.service"
install -m 0644 "${root_dir}/packaging/systemd/pfs-index@.timer" "${src_dir}/usr/lib/systemd/system/pfs-index@.timer"
install -m 0644 "${root_dir}/packaging/systemd/pfs-move@.service" "${src_dir}/usr/lib/systemd/system/pfs-move@.service"
install -m 0644 "${root_dir}/packaging/systemd/pfs-move@.timer" "${src_dir}/usr/lib/systemd/system/pfs-move@.timer"
install -m 0644 "${root_dir}/packaging/systemd/pfs-prune@.service" "${src_dir}/usr/lib/systemd/system/pfs-prune@.service"
install -m 0644 "${root_dir}/packaging/systemd/pfs-prune@.timer" "${src_dir}/usr/lib/systemd/system/pfs-prune@.timer"
install -m 0644 "${root_dir}/packaging/systemd/pfs-maint@.service" "${src_dir}/usr/lib/systemd/system/pfs-maint@.service"
install -m 0644 "${root_dir}/packaging/systemd/pfs-maint@.timer" "${src_dir}/usr/lib/systemd/system/pfs-maint@.timer"

install -m 0644 "${root_dir}/packaging/config/pfs.example.yaml" "${src_dir}/etc/pfs/pfs.yaml.example"
install -m 0644 "${root_dir}/packaging/logrotate/pfs" "${src_dir}/etc/logrotate.d/pfs"

install -m 0644 "${root_dir}/LICENSE" "${src_dir}/LICENSE"
install -m 0644 "${root_dir}/README.md" "${src_dir}/README.md"

tarball="${topdir}/SOURCES/policyfs-${version}.tar.gz"
tar -C "${work_dir}/src" -czf "${tarball}" "policyfs-${version}"

cp "${root_dir}/packaging/rpm/policyfs.spec" "${topdir}/SPECS/policyfs.spec"

rpmbuild \
  --define "_topdir ${topdir}" \
  --define "pfs_version ${version}" \
  --define "pfs_release ${release}" \
  -ba "${topdir}/SPECS/policyfs.spec" >/dev/null

rpm_path="$(find "${topdir}/RPMS" -type f -name '*.rpm' | head -n 1)"
if [[ -z "${rpm_path}" || ! -f "${rpm_path}" ]]; then
  echo "pfs: error: rpm not found after rpmbuild" >&2
  exit 1
fi

rpm_base="$(basename "${rpm_path}")"
cp "${rpm_path}" "${out_dir}/${rpm_base}"
cp "${rpm_path}" "${out_dir}/policyfs_${arch}.rpm"

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "${out_dir}/${rpm_base}" >"${out_dir}/${rpm_base}.sha256"
  sha256sum "${out_dir}/policyfs_${arch}.rpm" >"${out_dir}/policyfs_${arch}.rpm.sha256"
elif command -v shasum >/dev/null 2>&1; then
  shasum -a 256 "${out_dir}/${rpm_base}" >"${out_dir}/${rpm_base}.sha256"
  shasum -a 256 "${out_dir}/policyfs_${arch}.rpm" >"${out_dir}/policyfs_${arch}.rpm.sha256"
else
  echo "pfs: hint: sha256sum not found; skip checksum" >&2
fi

echo "pfs: built ${out_dir}/${rpm_base}" >&2
