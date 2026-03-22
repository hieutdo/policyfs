# How pfs works

PolicyFS is a FUSE daemon. Applications mount it like a normal filesystem and issue standard POSIX
calls. pfs intercepts each call, routes it based on explicit rules, and forwards it to the
appropriate physical path. For archive disks marked `indexed: true`, a local SQLite database
(`index.db`) serves `readdir` and `getattr` calls so the disk can stay spun down.

Three maintenance jobs handle housekeeping on a schedule — typically a systemd timer.

---

## Key terms

- **Indexed storage**: `indexed: true` storage paths serve metadata from `index.db` (disk can stay asleep).
- **Deferred mutations**: on indexed storage, metadata changes are recorded in `events.ndjson` and applied later by `pfs prune`.
- **Maintenance window**: a scheduled time (often nightly) when running maintenance jobs can wake archive disks.

See [Concepts reference](concepts.md) for the full details.

---

## Maintenance cycle

The three jobs run in order — either individually or via `pfs maint` which runs all three under one
lock:

```
pfs move  →  pfs prune  →  pfs index
```

| Job         | What it does                                                                                                                                                                 |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pfs move`  | Copies files from the fast tier (SSD) to archive storage (HDD) based on age, size, and disk usage conditions. Skips files currently open, if the daemon socket is available. |
| `pfs prune` | Reads `events.ndjson` and applies deferred operations (DELETE, RENAME, SETATTR) to physical storage. Frees disk space from deleted files.                                    |
| `pfs index` | Walks indexed storage paths and upserts file metadata into `index.db`. Must run after `pfs move` and `pfs prune` so the index reflects the final state.                      |

Visual diagrams for each job: [policyfs.org/how-it-works](https://policyfs.org/how-it-works.html)

---

## Not a good fit for

- **Databases and VM images** — FUSE adds per-call latency; `O_DIRECT` is not supported.
- **File-locking-dependent apps** — advisory locks do not cross FUSE mount boundaries reliably (affects some torrent clients and sync tools).
- **NFS re-exports** — FUSE filesystems cannot be reliably re-exported over NFS.
- **Maximum POSIX coverage** — [mergerfs](https://github.com/trapexit/mergerfs) covers more edge cases and has a longer track record.

---

## Further reading

- [Use cases](use-cases.md) — ready-to-use config patterns for media servers, NVR, and seedboxes
- [Install on Debian/Ubuntu](install/debian.md) — step-by-step install and service setup
- [Concepts reference](concepts.md) — indexed storage invariants, routing semantics, event log format
- [Configuration](config.md) — full config schema with annotated examples
- [Systemd integration](systemd.md) — timer units for the maintenance cycle
- [Disk spindown guide](spindown.md) — tuning HDD sleep alongside pfs
