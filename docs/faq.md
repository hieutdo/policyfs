# FAQ

## What workloads is PolicyFS a good fit for?

PolicyFS is designed for mostly-static files where explicit placement and predictable reads/writes matter:

- Media libraries
- NVR/CCTV retention stores
- Photo/document archives

## Is indexing required?

No. Indexing is optional and configured per storage path (`indexed: true/false`).

- If you enable indexing, metadata-heavy operations can avoid touching slower disks.
- If you disable indexing, PolicyFS serves metadata directly from the underlying filesystem.

## Known limitations

### Not a good fit for databases and VM images

PolicyFS is a FUSE filesystem. It adds per-call overhead and does not target low-latency random I/O workloads.

### File locking behavior

Some applications rely on advisory file locks across multiple processes and mount boundaries.
FUSE filesystems can have surprising locking semantics. If your app requires strict lock behavior, test carefully.

### NFS re-exports

FUSE mounts generally cannot be reliably re-exported over NFS.

### Maximum POSIX edge-case compatibility

PolicyFS is "POSIX-ish" by design. It aims for boring and predictable behavior, not perfect coverage of every filesystem edge case.

## Is PolicyFS a backup solution?

No. PolicyFS is a placement/routing filesystem layer, not a backup solution.

Use your normal backup strategy (snapshots, replication, offsite backups) for data protection.

## What platforms are supported?

- Linux only
- systemd is required for the supported/recommended setup
- Packaging is currently provided for Debian/Ubuntu and Fedora/EL9 (linux/amd64)

## Does PolicyFS work on Unraid or TrueNAS?

PolicyFS requires Linux and systemd. Unraid is Linux-based but uses a non-standard storage stack and does not use systemd; PolicyFS is not tested or supported there. TrueNAS SCALE is Linux-based and uses systemd, but it is not an officially supported platform — proceed with caution and test thoroughly.

## How do I know the index is working?

Run `pfs doctor <mount>` — it reports index stats, including how many files are indexed per storage path. You can also check the last run of the indexer:

```bash
journalctl -u pfs-index@media.service -n 50
```

## Where should I start?

- [Getting started](getting-started.md)
- [Use cases](use-cases.md)
- [Configuration reference](config.md)
- [Systemd integration](systemd.md)
- [Troubleshooting](troubleshooting.md)
