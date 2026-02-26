# PolicyFS (pfs)

PolicyFS is a policy-routed FUSE filesystem plus maintenance jobs (`index`, `move`, `prune`) for managing multi-disk storage.

## When you would use it

- You want a single mountpoint over multiple storage paths.
- You want explicit read/write routing rules.
- You want HDD-friendly directory listings using an index (SQLite) to avoid spinning up disks.

## Next steps

- [Install on Debian/Ubuntu](install/debian.md)
- [Configure mounts and routing](config.md)
- [Run as a systemd-managed service](systemd.md)
- [CLI overview](cli.md)
