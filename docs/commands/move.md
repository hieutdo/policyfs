# `pfs move`

Move files between storage paths based on the mount's [mover configuration](../config.md#mover).

This command evaluates mover jobs, discovers candidates from source storages, and copies them to destination storages. It is typically scheduled via systemd timers.

## Usage

```bash
pfs move <mount> [flags]
```

## Flags

| Flag                | Default        | Description                                                       |
| ------------------- | -------------- | ----------------------------------------------------------------- |
| `--job <name>`      | all jobs       | Run only the named mover job.                                     |
| `--dry-run`         | `false`        | Show what would be moved without making changes.                  |
| `--force`           | `false`        | Ignore trigger conditions (usage thresholds, allowed window).     |
| `--limit <n>`       | `0` (no limit) | Limit the number of files moved.                                  |
| `--debug`           | `false`        | Print debug info about skipped entries and destination selection. |
| `--debug-max <n>`   | `20`           | Maximum number of debug entries to print.                         |
| `--quiet`           | `false`        | Suppress all output.                                              |
| `--progress <mode>` | `auto`         | Progress output mode: `auto`, `tty`, `plain`, or `off`.           |

## Trigger behavior

Each mover job has a trigger that controls when it runs:

**Usage trigger** (`type: usage`):
The job runs only when disk usage on any source storage exceeds `threshold_start` (default 80%).
Moving stops when usage drops below `threshold_stop` (default 70%).

**Allowed window**:
When `allowed_window` is configured, the job only starts during the specified time window.
If `finish_current: true` (default), a file being copied when the window closes is allowed to finish.

**Manual trigger** (`type: manual`):
The job always runs when you invoke `pfs move` (unless `mover.enabled: false`).

Use `--force` to bypass all trigger conditions.

## Open file skipping

When the FUSE daemon is running, the mover communicates with it via a control socket to discover which files are currently open.
Open files are skipped to avoid moving files that are being read or written.

If the daemon control socket is unavailable, open-file checks are skipped (best-effort) and the mover proceeds.

## Exit codes

| Code | Meaning                                                                           |
| ---- | --------------------------------------------------------------------------------- |
| 0    | Files were moved successfully.                                                    |
| 2    | Usage error (e.g., unknown job specified with `--job`).                           |
| 3    | No changes (nothing to move, triggers not met, mover disabled, or no mover jobs). |
| 75   | Another maintenance job is already running (`job.lock` held).                     |

## Examples

```bash
# Move files for the "media" mount
pfs move media

# Preview what would be moved
pfs move media --dry-run --force

# Run a specific job, limited to 5 files, with debug output
pfs move media --job archive --limit 5 --debug

# Silent mode for cron/scripts
pfs move media --quiet

# Force plain-text progress (no TTY bar)
pfs move media --progress=plain
```

## systemd

- `pfs-move@<mount>.service`
- `pfs-move@<mount>.timer`
