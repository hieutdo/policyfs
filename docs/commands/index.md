# `pfs index`

Build a metadata index for storage paths with `indexed: true` and write it to the per-mount SQLite database.

This enables metadata operations (lookup, readdir, getattr) to be served from the index instead of touching physical disks.

## Usage

```bash
pfs index <mount> [flags]
```

## Flags

| Flag                | Default     | Description                                                                    |
| ------------------- | ----------- | ------------------------------------------------------------------------------ |
| `--storage <id>`    | all indexed | Index only a specific storage path ID.                                         |
| `--rebuild`         | `false`     | Delete the index database before indexing (requires the daemon to be stopped). |
| `-v, --verbose`     | `false`     | Print every file as it is indexed.                                             |
| `--quiet`           | `false`     | Suppress progress output.                                                      |
| `--progress <mode>` | `auto`      | Progress output mode: `auto`, `tty`, `plain`, or `off`.                        |

## Behavior

- Scans all storage paths with `indexed: true` (or just the one specified by `--storage`).
- Upserts file and directory metadata into the SQLite database.
- Removes stale entries that no longer exist on disk.
- Respects `indexer.ignore` glob patterns from the config.
- Acquires `job.lock` — only one maintenance job can run per mount at a time.

!!! note "`--rebuild` requires the daemon to be stopped"
The `--rebuild` flag deletes and recreates the index database. It acquires `daemon.lock` to ensure the FUSE daemon is not running, since the daemon holds a connection to the database.

## Exit codes

| Code | Meaning                                                         |
| ---- | --------------------------------------------------------------- |
| 0    | Index updated with changes.                                     |
| 3    | Nothing to index (no changes detected, or no indexed storages). |
| 75   | Another maintenance job is already running (`job.lock` held).   |

## Examples

```bash
# Index all indexed storages for the "media" mount
pfs index media

# Index a specific storage
pfs index media --storage hdd1

# Rebuild from scratch
pfs index media --rebuild

# Verbose output showing each file
pfs index media -v

# Silent mode
pfs index media --quiet
```

## systemd

- `pfs-index@<mount>.service`
- `pfs-index@<mount>.timer`
