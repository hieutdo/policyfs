# Getting started

## Prerequisites

- Linux
- systemd
- FUSE3 userspace tools

## Installation

### Debian/Ubuntu

Install FUSE3:

```bash
sudo apt-get update
sudo apt-get install -y fuse3
```

Install from the APT repo:

```bash
sudo curl -fsSL -o /usr/share/keyrings/policyfs-archive-keyring.gpg \
  "https://repo.policyfs.org/apt/policyfs.gpg"

echo "deb [arch=amd64 signed-by=/usr/share/keyrings/policyfs-archive-keyring.gpg] https://repo.policyfs.org/apt stable main" \
  | sudo tee /etc/apt/sources.list.d/policyfs.list >/dev/null

sudo apt-get update
sudo apt-get install -y policyfs
```

Or download the `.deb` from GitHub Releases and install it:

```bash
curl -L -o "policyfs_amd64.deb" "https://github.com/hieutdo/policyfs/releases/latest/download/policyfs_amd64.deb"
sudo dpkg -i "./policyfs_amd64.deb"
```

### RedHat/Fedora (DNF/YUM)

Install FUSE3:

```bash
sudo dnf install -y fuse3
```

Install from the RPM repo:

```bash
sudo curl -fsSL -o /etc/pki/rpm-gpg/RPM-GPG-KEY-policyfs \
  "https://repo.policyfs.org/rpm/policyfs.gpg"

cat <<'EOF' | sudo tee /etc/yum.repos.d/policyfs.repo >/dev/null
[policyfs]
name=PolicyFS
baseurl=https://repo.policyfs.org/rpm/fedora/x86_64
enabled=1
gpgcheck=1
repo_gpgcheck=0
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-policyfs
EOF

sudo dnf makecache
sudo dnf install -y policyfs
```

EL9 (Rocky/Alma/CentOS Stream 9) uses:

```text
baseurl=https://repo.policyfs.org/rpm/el/9/x86_64
```

### Build from source

Packages are recommended (they install systemd units and a default config), but you can build PolicyFS from source.

You will need Go installed (see `go.mod` for the required version).

Install build dependencies:

#### Debian/Ubuntu

```bash
sudo apt-get update
sudo apt-get install -y --no-install-recommends \
  git \
  build-essential \
  pkg-config \
  libsqlite3-dev \
  libfuse3-dev
```

#### Fedora/EL9

```bash
sudo dnf install -y \
  git \
  gcc \
  make \
  pkgconf-pkg-config \
  sqlite-devel \
  fuse3-devel
```

Build and install:

```bash
git clone https://github.com/hieutdo/policyfs.git
cd policyfs

CGO_ENABLED=1 go build -o ./bin/pfs ./cmd/pfs

sudo install -m 0755 ./bin/pfs /usr/local/bin/pfs
```

Optional validation before building:

```bash
# Full test suite (recommended when your environment is ready)
go test ./...
```

Create an example config:

```bash
sudo install -d /etc/pfs
if [ ! -f /etc/pfs/pfs.yaml ]; then
  sudo install -m 0644 packaging/config/pfs.example.yaml /etc/pfs/pfs.yaml
fi
```

Then continue with the configuration steps below.

If you want to manage PolicyFS via systemd, the unit files live under `packaging/systemd/` (see `systemd.md`).

Note: packaged unit files reference `/usr/bin/pfs`. For source installs, choose one approach:

- Install your binary to `/usr/bin/pfs`.
- Or copy unit files to `/etc/systemd/system/` and update `ExecStart`/`ExecStop` to `/usr/local/bin/pfs`.

## Configure

### Allow other users to access the mount

If apps like Plex or Jellyfin run as a different user (e.g. `plex`, `jellyfin`), they cannot access a FUSE mount started by root unless `allow_other` is enabled. This is required for most media server setups.

Enable it in `/etc/fuse.conf`:

```bash
sudo nano /etc/fuse.conf
```

Uncomment:

```text
user_allow_other
```

Then set `fuse.allow_other: true` in `/etc/pfs/pfs.yaml`.

### Configure a minimal mount

On first install, the package creates:

- `/etc/pfs/pfs.yaml` (copied from the example)

Before enabling any systemd units, edit `/etc/pfs/pfs.yaml`:

- Pick your mount name under `mounts:` (it does not have to be `media`).
- Set `mountpoint` to a real path on your machine.
- Set `storage_paths[].path` to your actual disk paths.

Here is an example configuration:

```yaml
mounts:
  media:
    mountpoint: /mnt/pfs/media

    storage_paths:
      - { id: d1, path: /mnt/disk1/media, indexed: false }
      - { id: d2, path: /mnt/disk2/media, indexed: false }

    storage_groups:
      disks: [d1, d2]

    routing_rules:
      - match: '**'
        read_targets: [disks]
        write_targets: [disks]
        write_policy: most_free
        path_preserving: true
```

### Create directories

Create the mountpoint and storage path directories:

```bash
sudo mkdir -p /mnt/pfs/media
sudo mkdir -p /mnt/disk1/media /mnt/disk2/media
```

If these paths do not exist, `pfs@<mount>.service` will fail to start.

!!! note "Indexed storage expectations"
`indexed: true` is optional and only affects metadata operations (like directory listing). File content reads still come from the owning disk, and maintenance jobs will spin disks by design.

!!! tip "Not sure what config to use?"
See [Use cases](use-cases.md) - pick the pattern closest to your setup and copy it as your starting point.

### Enable a mount

Example: assuming your mount is called `media`:

```bash
sudo systemctl enable --now pfs@media.service
```

Verify it started:

```bash
sudo systemctl status pfs@media.service
sudo pfs doctor media
ls /mnt/pfs/media
```

### (Optional) Enable indexing for archive disks

Indexing is optional. It reduces metadata-driven disk touches for storage paths with `indexed: true`.

- Metadata operations (like directory listing) can be served from SQLite after an index run.
- File content reads still come from the owning disk.

To use it:

1. Set `indexed: true` on the storage paths you want indexed.
2. Run:

   ```bash
   sudo systemctl start pfs-index@media.service
   ```

### Run maintenance jobs

Test the jobs manually before scheduling them:

```bash
sudo systemctl start pfs-move@media.service
sudo systemctl start pfs-prune@media.service
sudo systemctl start pfs-index@media.service
```

If you want scheduling, enable the batched maintenance timer:

```bash
sudo systemctl enable --now pfs-maint@media.timer
```

This runs `move → prune → index` under one schedule (default: 00:30 nightly). See [systemd](systemd.md) for default schedules and how to override them.
