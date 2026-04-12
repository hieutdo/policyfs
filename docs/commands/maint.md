# `pfs maint`

Run mover, prune, and index for a mount in a single `job.lock` session.

This command batches maintenance work to reduce disk wake-ups. It is the recommended way to schedule maintenance via systemd timers.

## Execution order

1. **Move** - run mover jobs (same as `pfs move`).
2. **Prune** - apply deferred mutations (same as `pfs prune`). Skipped if the mover did no work.
3. **Index** - re-index touched storages (same as `pfs index`). Skipped if the mover did no work.

!!! note "Prune and index are skipped when the mover finds nothing to do"
If the mover's trigger conditions are not met (e.g. SSD usage is below `threshold_start`) or no files match the job conditions, the mover exits early and prune and index phases are skipped. If you have pending deferred events (from deletes/renames on indexed storage) but don't want to trigger the mover, run `pfs prune <mount>` directly.

## Usage

```bash
pfs maint <mount> [flags]
```

## Flags

| Flag             | Default        | Description                              |
| ---------------- | -------------- | ---------------------------------------- |
| `--job <name>`   | all jobs       | Run only the named mover job.            |
| `--force`        | `false`        | Ignore mover trigger conditions.         |
| `--limit <n>`    | `0` (no limit) | Limit number of files moved.             |
| `--index <mode>` | `touch`        | Index mode after move+prune (see below). |
| `--quiet`        | `false`        | Suppress success output.                 |

## Index modes

| Mode    | Behavior                                                                                         |
| ------- | ------------------------------------------------------------------------------------------------ |
| `touch` | Re-index only indexed storages that were touched during the move phase. This is the **default**. |
| `all`   | Re-index all indexed storages regardless of whether they were touched.                           |
| `off`   | Skip the index phase entirely.                                                                   |

## Exit codes

| Code | Meaning                                                       |
| ---- | ------------------------------------------------------------- |
| 0    | Maintenance completed with changes.                           |
| 3    | No work done across all phases.                               |
| 75   | Another maintenance job is already running (`job.lock` held). |

## Examples

```bash
# Standard maintenance run
pfs maint media

# Force mover, run a specific job, limit to 10 files
pfs maint media --job archive --force --limit 10

# Re-index all indexed storages (not just touched ones)
pfs maint media --index=all

# Skip index phase
pfs maint media --index=off

# Silent mode
pfs maint media --quiet
```

## systemd

- `pfs-maint@<mount>.service`
- `pfs-maint@<mount>.timer`
