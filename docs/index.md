# PolicyFS (pfs)

PolicyFS is a Linux FUSE storage daemon that unifies multiple storage paths under one mountpoint with explicit read/write routing rules, an optional SQLite metadata index, and built-in maintenance jobs.

## When you would use it

- You want a single mountpoint over multiple storage paths.
- You want explicit read/write routing rules.
- You want HDD-friendly directory listings using an index (SQLite) to avoid spinning up disks.

## Why PolicyFS?

While tools like `mergerfs` are great for pooling drives, PolicyFS is focused on making storage behavior explicit:

- How reads and writes are routed.
- How metadata can be served without touching slow disks.
- How and when data should be moved to the archive tier.

### Key Advantages

- **Policy-based routing:**
  Route reads and writes based on path patterns, with clear write target selection (`first_found`, `most_free`, `least_free`).

- **SQLite-backed metadata index (optional):**
  For storage paths with `indexed: true`, metadata operations (e.g., `readdir`, `getattr`) are served from SQLite.
  This reduces disk touches for metadata-heavy workloads.

- **Built-in maintenance jobs:**
  Indexing, moving, and applying deferred mutations are first-class CLI commands (`pfs index`, `pfs move`, `pfs prune`, `pfs maint`) with systemd units.

- **Portable data layout:**
  Your data stays on normal Linux filesystems as plain files and directories.

## Next steps

If you want practical configuration examples, see [Use cases](use-cases.md). If you care about power saving (HDD standby), see [Disk spindown (power saving)](spindown.md).

- [Quickstart (Debian/Ubuntu)](quickstart.md)
- [Concepts](concepts.md)
- [Disk spindown (power saving)](spindown.md)
- [Use cases](use-cases.md)
- [Configure mounts and routing](config.md)
- [Install on Debian/Ubuntu](install/debian.md)
- [Run as a systemd-managed service](systemd.md)
- [CLI overview](cli.md)
