# Quickstart (Debian/Ubuntu)

This page gets you to a working PolicyFS mount in about 10 minutes.

## 1) Install prerequisites

Install FUSE3 userspace tools:

```bash
sudo apt-get update
sudo apt-get install -y fuse3
```

If your apps run as a different user (for example `plex` or `jellyfin`), enable `allow_other`:

1. Edit `/etc/fuse.conf` and uncomment:

   ```text
   user_allow_other
   ```

2. In `/etc/pfs/pfs.yaml`, set:

   ```yaml
   fuse:
     allow_other: true
   ```

## 2) Install PolicyFS

Download the `.deb` from GitHub Releases and install it:

```bash
curl -L -o "pfs_amd64.deb" "https://github.com/hieutdo/policyfs/releases/latest/download/pfs_amd64.deb"
sudo dpkg -i "./pfs_amd64.deb"
```

On first install, the package creates `/etc/pfs/pfs.yaml`.

## 3) Configure a minimal mount

Edit `/etc/pfs/pfs.yaml` and paste a minimal `mounts:` block like this (adjust disk paths):

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

## 4) Create directories

Create the mountpoint and storage path directories:

```bash
sudo mkdir -p /mnt/pfs/media
sudo mkdir -p /mnt/disk1/media /mnt/disk2/media
```

## 5) Start the service

Enable and start the mount:

```bash
sudo systemctl enable --now pfs@media.service
```

Verify it started:

```bash
sudo systemctl status pfs@media.service
sudo pfs doctor media
ls /mnt/pfs/media
```

## 6) (Optional) Enable indexing for archive disks

Indexing is optional. It reduces metadata-driven disk touches for storage paths with `indexed: true`.

- Metadata operations (like directory listing) can be served from SQLite after an index run.
- File content reads still come from the owning disk.

To use it:

1. Set `indexed: true` on the storage paths you want indexed.
2. Run:

   ```bash
   sudo systemctl start pfs-index@media.service
   ```

See also: [Concepts](concepts.md) and [Disk spindown (power saving)](spindown.md).

## Next

- For more realistic patterns (SSD ingest + HDD archive, NVR retention), see [Use cases](use-cases.md).
- For scheduling (`pfs maint`) and timers, see [systemd](systemd.md).
