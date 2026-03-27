%global pfs_version 0.0.0
%global pfs_release 1

Name:           policyfs
Version:        %{pfs_version}
Release:        %{pfs_release}%{?dist}
Summary:        Linux FUSE storage daemon with routing rules and indexing

License:        Apache-2.0
URL:            https://github.com/hieutdo/policyfs
Source0:        %{name}-%{version}.tar.gz

ExclusiveArch:  x86_64

Provides:       pfs
Obsoletes:      pfs

Requires:       fuse3
Requires:       sqlite-libs

Requires(post):   systemd
Requires(preun):  systemd
Requires(postun): systemd

%description
PolicyFS unifies multiple storage paths under one mountpoint with explicit read/write routing rules, an optional SQLite metadata index, and built-in maintenance jobs.

%prep
%setup -q

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}

cp -a usr etc %{buildroot}/

mkdir -p %{buildroot}/var/lib/pfs

%post
if [ -f /etc/pfs/pfs.yaml.example ] && [ ! -f /etc/pfs/pfs.yaml ]; then
  cp /etc/pfs/pfs.yaml.example /etc/pfs/pfs.yaml || :
fi

if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload || :
fi

%preun
if [ "$1" -eq 0 ] && command -v systemctl >/dev/null 2>&1; then
  systemctl stop 'pfs@*.service' >/dev/null 2>&1 || :
  systemctl stop 'pfs-index@*.service' >/dev/null 2>&1 || :
  systemctl stop 'pfs-index@*.timer' >/dev/null 2>&1 || :
  systemctl stop 'pfs-move@*.service' >/dev/null 2>&1 || :
  systemctl stop 'pfs-move@*.timer' >/dev/null 2>&1 || :
  systemctl stop 'pfs-prune@*.service' >/dev/null 2>&1 || :
  systemctl stop 'pfs-prune@*.timer' >/dev/null 2>&1 || :
  systemctl stop 'pfs-maint@*.service' >/dev/null 2>&1 || :
  systemctl stop 'pfs-maint@*.timer' >/dev/null 2>&1 || :
fi

%postun
if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload || :
fi

%files
%license LICENSE
%doc README.md

/usr/bin/pfs

%dir /etc/pfs
%config(noreplace) /etc/pfs/pfs.yaml.example

%config(noreplace) /etc/logrotate.d/pfs

/usr/lib/systemd/system/pfs@.service
/usr/lib/systemd/system/pfs-index@.service
/usr/lib/systemd/system/pfs-index@.timer
/usr/lib/systemd/system/pfs-move@.service
/usr/lib/systemd/system/pfs-move@.timer
/usr/lib/systemd/system/pfs-prune@.service
/usr/lib/systemd/system/pfs-prune@.timer
/usr/lib/systemd/system/pfs-maint@.service
/usr/lib/systemd/system/pfs-maint@.timer

%dir /var/lib/pfs

%changelog
* Thu Mar 26 2026 PolicyFS <hieutdo2@gmail.com>
- Initial RPM packaging
