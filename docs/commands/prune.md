# `pfs prune`

Process deferred filesystem operations recorded in `events.ndjson` for indexed storage paths.

When files on indexed storage are deleted, renamed, or have attributes changed through the FUSE mount, the operations are recorded in an event log instead of being applied immediately. This avoids spinning up HDDs during normal filesystem use. `pfs prune` applies these deferred operations to the physical disk.

## Usage

```bash
pfs prune <mount> [flags]
pfs prune --all [flags]
```

## Flags

| Flag          | Default        | Description                                                                     |
| ------------- | -------------- | ------------------------------------------------------------------------------- |
| `--all`       | `false`        | Process all mounts defined in the config. Cannot be used with a mount argument. |
| `--dry-run`   | `false`        | Show what would be executed without making changes.                             |
| `--limit <n>` | `0` (no limit) | Limit the number of events processed.                                           |
| `--quiet`     | `false`        | Suppress success output.                                                        |

## Deferred event types

| Type      | Physical operation                                |
| --------- | ------------------------------------------------- |
| `DELETE`  | `unlink` (file) or `rmdir` (directory)            |
| `RENAME`  | Move/rename on the physical storage               |
| `SETATTR` | `chmod`, `chown`, or `utimens` (timestamp change) |

## Behavior

- Reads `events.ndjson` from each indexed storage's state directory.
- Applies each event to the physical disk in order.
- Truncates the event log after successful processing.
- Acquires `job.lock` per mount.

## Exit codes

| Code | Meaning                                                       |
| ---- | ------------------------------------------------------------- |
| 0    | Events were processed.                                        |
| 3    | Nothing to prune (no pending events).                         |
| 75   | Another maintenance job is already running (`job.lock` held). |

## Examples

```bash
# Process deferred operations for the "media" mount
pfs prune media

# Preview without applying changes
pfs prune media --dry-run

# Process at most 10 events
pfs prune media --limit 10

# Process all mounts at once
pfs prune --all
```

## systemd

- `pfs-prune@<mount>.service`
- `pfs-prune@<mount>.timer`
