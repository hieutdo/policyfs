# Install (Debian/Ubuntu)

If you just want a working mount quickly, start with [Quickstart](../quickstart.md).

## Prerequisites

- Linux
- systemd
- FUSE3 userspace tools

Install FUSE3:

```bash
sudo apt-get update
sudo apt-get install -y fuse3
```

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

## Install PolicyFS

Download the `.deb` from GitHub Releases and install it:

```bash
curl -L -o "pfs_amd64.deb" "https://github.com/hieutdo/policyfs/releases/latest/download/pfs_amd64.deb"
sudo dpkg -i "./pfs_amd64.deb"
```

On first install, the package creates:

- `/etc/pfs/pfs.yaml` (copied from the example)

Before enabling any systemd units, edit `/etc/pfs/pfs.yaml`:

- Pick your mount name under `mounts:` (it does not have to be `media`).
- Set `mountpoint` to a real path on your machine.
- Set `storage_paths[].path` to your actual disk paths.

!!! note "Indexed storage expectations"
`indexed: true` is optional and only affects metadata operations (like directory listing). File content reads still come from the owning disk, and maintenance jobs will spin disks by design.

!!! tip "Not sure what config to use?"
See [Use cases](../use-cases.md) — pick the pattern closest to your setup and copy it as your starting point.

Create the mountpoint and storage path directories before starting the service:

```bash
sudo mkdir -p /mnt/pfs/media
sudo mkdir -p /mnt/ssd1/media /mnt/hdd1/media /mnt/hdd2/media
```

If these paths do not exist, `pfs@<mount>.service` will fail to start.

## Enable a mount

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

## Run maintenance jobs

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

This runs `move → prune → index` under one schedule (default: 00:30 nightly). See [systemd](../systemd.md) for default schedules and how to override them.
