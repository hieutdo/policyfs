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

## Install PolicyFS

Download the `.deb` from GitHub Releases and install it:

```bash
sudo dpkg -i ./pfs_<version>_amd64.deb
```

On first install, the package creates:

- `/etc/pfs/pfs.yaml` (copied from the example)

## Enable a mount

Assuming your mount is called `media`:

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

```bash
sudo systemctl enable --now pfs-index@media.timer
sudo systemctl enable --now pfs-move@media.timer
sudo systemctl enable --now pfs-prune@media.timer
```
