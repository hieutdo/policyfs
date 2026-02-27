# Core Concepts

PolicyFS presents a single FUSE mountpoint over multiple physical storage paths.
Routing rules control which storage handles reads and writes for each virtual path.
An optional SQLite index lets metadata operations skip spinning disks entirely.

## Storage paths

A storage path is a physical directory root.
Each storage path has:

| Field         | Description                                                                                                                                  |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`          | Stable identifier used in config, logs, and the index database.                                                                              |
| `path`        | Absolute filesystem path (e.g. `/mnt/hdd1/media`).                                                                                           |
| `indexed`     | When `true`, metadata is served from the SQLite index instead of the physical disk. Mutations are deferred and applied later by `pfs prune`. |
| `min_free_gb` | Minimum free space (GiB) required before PolicyFS will write new files to this storage.                                                      |

## Storage groups

A storage group is a named list of storage path IDs.
Groups simplify routing rules and mover configuration — you can reference `ssds` instead of listing `ssd1, ssd2` everywhere.

```yaml
storage_groups:
  ssds: [ssd1, ssd2]
  hdds: [hdd1, hdd2, hdd3]
```

Groups are expanded when routing rules and mover jobs are evaluated.

## Routing rules

Routing rules map virtual paths to storage targets. Rules are evaluated **top-to-bottom, first match wins**.

A rule defines:

| Field             | Description                                                                  |
| ----------------- | ---------------------------------------------------------------------------- |
| `match`           | Glob pattern matched against the virtual path. `**` matches any depth.       |
| `read_targets`    | Storage IDs or group names used for reads and directory listings.            |
| `write_targets`   | Storage IDs or group names used for writes (create, link, rename).           |
| `write_policy`    | How to pick one write target from the candidates — see below.                |
| `path_preserving` | When `true`, prefer write targets where the parent directory already exists. |

Shorthand: `targets` sets both `read_targets` and `write_targets` at once.

!!! warning "Catch-all rule required"
The last routing rule **must** be the catch-all pattern `**`. PolicyFS rejects configs without it.

### Write policies

When a write operation needs a single target, the write policy selects one from the resolved candidates (after `min_free_gb` filtering):

| Policy        | Behavior                                                              |
| ------------- | --------------------------------------------------------------------- |
| `first_found` | Use the first eligible target in list order. This is the **default**. |
| `most_free`   | Use the target with the most free space.                              |
| `least_free`  | Use the target with the least free space.                             |

### Path-preserving writes

When `path_preserving: true`, PolicyFS prefers targets where the parent directory of the new file already exists on disk.
If no target has the parent directory, all candidates remain eligible and the write policy decides.

This keeps related files together — for example, all files in `library/movies/MovieA/` will land on the same disk.

## Directory listings

Unlike reads and writes (first match wins), directory listings consider **all** routing rules whose pattern could match descendants of the listed directory.
The result is the union of entries across all matching storage targets, deduplicated by name.

## Indexed storage and deferred mutations

Storage paths with `indexed: true` are backed by a per-mount SQLite database.

**Reads:** Metadata operations (lookup, getattr, readdir) are served from the index, so the physical disk does not need to spin up.

**Writes:** Mutations on indexed paths are recorded in an event log (`events.ndjson`) instead of being applied immediately. The supported deferred event types are:

- **DELETE** — unlink or rmdir
- **RENAME** — file or directory rename
- **SETATTR** — permission, ownership, or timestamp changes

These deferred operations are applied to the physical disk later by [`pfs prune`](commands/prune.md).

## Locks

PolicyFS uses file locks to prevent conflicts:

| Lock          | Location                  | Purpose                                                                                |
| ------------- | ------------------------- | -------------------------------------------------------------------------------------- |
| `daemon.lock` | `/run/pfs/<mount>/locks/` | Ensures only one FUSE daemon runs per mount.                                           |
| `job.lock`    | `/run/pfs/<mount>/locks/` | Ensures only one maintenance job (index, move, prune, maint) runs at a time per mount. |

If a lock is already held, the command exits with code **75** (busy).

## State and runtime directories

| Directory | Default                 | Contents                                            |
| --------- | ----------------------- | --------------------------------------------------- |
| Config    | `/etc/pfs/pfs.yaml`     | Main configuration file.                            |
| State     | `/var/lib/pfs/<mount>/` | Persistent data: SQLite index database, event logs. |
| Runtime   | `/run/pfs/<mount>/`     | Ephemeral data: lock files, daemon control socket.  |
