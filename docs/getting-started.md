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

Coming soon.

### Build from source

Coming soon.

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
See [Use cases](use-cases.md) — pick the pattern closest to your setup and copy it as your starting point.

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
