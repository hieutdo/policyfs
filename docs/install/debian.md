# Install (Debian/Ubuntu)

## Prerequisites

- Linux
- systemd
- FUSE3 userspace tools

Install FUSE3:

```bash
sudo apt-get update
sudo apt-get install -y fuse3
```

If you plan to set `fuse.allow_other: true`, make sure `/etc/fuse.conf` allows it:

```bash
sudo nano /etc/fuse.conf
```

Uncomment:

```text
user_allow_other
```

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

If these paths do not exist, `pfs@<mount>.service` will fail to start.

## Enable a mount

Example: assuming your mount is called `media`:

```bash
sudo systemctl enable --now pfs@media.service
```

## Run maintenance jobs

Examples:

```bash
sudo systemctl start pfs-index@media.service
sudo systemctl start pfs-move@media.service
sudo systemctl start pfs-prune@media.service
```

If you want scheduling, enable timers:

Before enabling timers, see [systemd](../systemd.md) for default schedules and how to override them.

```bash
sudo systemctl enable --now pfs-index@media.timer
sudo systemctl enable --now pfs-move@media.timer
sudo systemctl enable --now pfs-prune@media.timer
```
