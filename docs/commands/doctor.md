# `pfs doctor`

Run health checks to validate configuration and runtime state.

## Usage

```bash
pfs doctor              # check all mounts
pfs doctor <mount>      # check a specific mount
pfs doctor <mount> <path>  # inspect a specific file
```

## Scope levels

**Global** (`pfs doctor`):
Validates the config file and runs health checks for every mount defined in the config.

**Mount** (`pfs doctor <mount>`):
Runs checks for a single mount, including config validation, storage path accessibility, daemon status, index stats, pending events, and disk access analysis.

When a JSON log file is configured (via `log.file` or `PFS_LOG_FILE`), doctor also scans recent log entries for FUSE permission errors.
This requires JSON logs (`log.format: json`). To avoid stale one-off errors, only recent entries (last ~15 minutes, and after the most recent `mount ready` for that mount when present) are reported.

**File inspect** (`pfs doctor <mount> <path>`):
Inspects a specific virtual path across all storages, showing index metadata, pending events, and (optionally) physical disk state.

By default, file inspect avoids touching disk for storages where the path is present in the index database (to prevent spinning up HDDs).
To force an on-disk stat across storages, pass `--disk`.

## Exit codes

| Code | Meaning                                        |
| ---- | ---------------------------------------------- |
| 0    | All checks passed (or file inspect succeeded). |
| 78   | Validation errors or issues found.             |

## Examples

```bash
# Check everything
pfs doctor

# Check a specific mount
pfs doctor media

# Inspect a specific file across all storages
pfs doctor media library/movies/MovieA/MovieA.mkv
```

## Shell completion

`pfs doctor` supports tab completion for mount names and virtual paths.
Virtual path completion uses the index database, with a fallback to non-indexed storage for paths not in the index.
