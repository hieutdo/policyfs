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

**File inspect** (`pfs doctor <mount> <path>`):
Inspects a specific virtual path across all storages, showing index metadata, pending events, and physical disk state.

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
