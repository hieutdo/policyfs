# Configuration

PolicyFS reads its config from `/etc/pfs/pfs.yaml` by default.
Override the path with the `PFS_CONFIG_FILE` environment variable or the `--config` flag.

The Debian package ships an example at `/etc/pfs/pfs.yaml.example`.

## Full example

```yaml
fuse:
  allow_other: true

log:
  level: info
  format: json
  file: /var/log/pfs/pfs.log

mounts:
  media:
    mountpoint: /mnt/pfs/media
    storage_paths:
      - id: ssd1
        path: /mnt/ssd1/media
        indexed: false
      - id: hdd1
        path: /mnt/hdd1/media
        indexed: true

    storage_groups:
      ssds: [ssd1]
      hdds: [hdd1]

    routing_rules:
      - match: '**'
        read_targets: [ssds, hdds]
        write_targets: [ssds]
        write_policy: first_found
        path_preserving: true

    indexer:
      ignore: ['**/.DS_Store', '**/Thumbs.db']

    mover:
      enabled: true
      jobs:
        - name: archive
          trigger:
            type: usage
            threshold_start: 80
            threshold_stop: 70
            allowed_window:
              start: '23:00'
              end: '06:00'
              finish_current: true
          source:
            groups: [ssds]
            patterns: ['library/**']
            ignore: ['**/.DS_Store']
          destination:
            groups: [hdds]
            policy: most_free
            path_preserving: true
          conditions:
            min_age: 7d
            min_size: 100MB
          delete_source: true
          delete_empty_dir: true
          verify: false
```

## Top-level keys

### `fuse`

| Field         | Type | Default | Description                                                                                                                         |
| ------------- | ---- | ------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `allow_other` | bool | `false` | Pass the `allow_other` FUSE mount option, letting non-root users access the mount. Requires `user_allow_other` in `/etc/fuse.conf`. |

### `log`

| Field    | Type   | Default | Description                                                                                          |
| -------- | ------ | ------- | ---------------------------------------------------------------------------------------------------- |
| `level`  | string | `info`  | Log level: `debug`, `info`, `warn`, `error`, or `off`.                                               |
| `format` | string | `json`  | Log format: `json` or `text`.                                                                        |
| `file`   | string | (empty) | If set, structured logs are also written to this file. Override with `--log-file` or `PFS_LOG_FILE`. |

The Debian package example config sets `log.file` to `/var/log/pfs/pfs.log`.

### `mounts`

A map of mount name to mount configuration. The mount name is a short identifier used in systemd unit names and CLI commands (e.g. `media`).

## Mount configuration

Each mount under `mounts.<name>` has:

### `mountpoint`

Absolute path where the FUSE filesystem is mounted (e.g. `/mnt/pfs/media`).

### `log`

Optional mount-scoped logging overrides.

| Field   | Type   | Default | Description                                                                   |
| ------- | ------ | ------- | ----------------------------------------------------------------------------- |
| `level` | string | (empty) | If set, overrides top-level `log.level` for this mount only (empty inherits). |

Changes to `mounts.<name>.log.level` can be applied to a running daemon with `pfs reload <mount>`.

### `storage_paths`

List of physical storage roots. Each entry:

| Field         | Type   | Required | Default | Description                                                                       |
| ------------- | ------ | -------- | ------- | --------------------------------------------------------------------------------- |
| `id`          | string | yes      | —       | Stable identifier used in logs, database, and config references.                  |
| `path`        | string | yes      | —       | Absolute filesystem path.                                                         |
| `indexed`     | bool   | no       | `false` | When `true`, metadata is served from the SQLite index and mutations are deferred. |
| `min_free_gb` | float  | no       | `0`     | Minimum free space in GiB. Write targets below this threshold are skipped.        |

### `storage_groups`

Map of group name to list of storage path IDs. Groups are expanded wherever storage references appear (routing rules, mover source/destination).

```yaml
storage_groups:
  ssds: [ssd1, ssd2]
  hdds: [hdd1, hdd2, hdd3]
```

### `routing_rules`

List of routing rules evaluated top-to-bottom. First match wins.

| Field             | Type   | Required | Default       | Description                                                     |
| ----------------- | ------ | -------- | ------------- | --------------------------------------------------------------- |
| `match`           | string | yes      | —             | Glob pattern for virtual paths. `**` matches any depth.         |
| `targets`         | list   | no       | —             | Shorthand: sets both `read_targets` and `write_targets`.        |
| `read_targets`    | list   | no       | —             | Storage IDs or group names for reads.                           |
| `write_targets`   | list   | no       | —             | Storage IDs or group names for writes.                          |
| `write_policy`    | string | no       | `first_found` | Target selection: `first_found`, `most_free`, or `least_free`.  |
| `path_preserving` | bool   | no       | `false`       | Prefer write targets where the parent directory already exists. |

!!! warning "Catch-all rule required"
The last rule **must** be `match: '**'`. PolicyFS rejects configs without a catch-all rule, and only one catch-all is allowed.

Changes to `mounts.<name>.routing_rules` can be applied to a running daemon with `pfs reload <mount>`.

### `indexer`

| Field    | Type | Default | Description                                                  |
| -------- | ---- | ------- | ------------------------------------------------------------ |
| `ignore` | list | `[]`    | Glob patterns for files/directories to skip during indexing. |

### `mover`

Controls file movement jobs for this mount.

| Field     | Type | Default | Description                       |
| --------- | ---- | ------- | --------------------------------- |
| `enabled` | bool | `true`  | Master switch for all mover jobs. |
| `jobs`    | list | `[]`    | List of mover job definitions.    |

!!! warning "delete_source is true by default"
The mover deletes the source file after a successful copy. Before running `pfs move` for the first time, use `pfs move <mount> --dry-run` to see what would be moved.

    Add `--force` if you want to bypass job triggers/conditions.

#### Mover job

Each job under `mover.jobs[]`:

| Field              | Type   | Default | Description                                                                                                   |
| ------------------ | ------ | ------- | ------------------------------------------------------------------------------------------------------------- |
| `name`             | string | —       | Unique job name (used in `--job` flag and logs).                                                              |
| `description`      | string | —       | Optional human-readable description.                                                                          |
| `trigger`          | object | —       | When the job should run.                                                                                      |
| `source`           | object | —       | Where candidates come from.                                                                                   |
| `destination`      | object | —       | Where files are moved to.                                                                                     |
| `conditions`       | object | —       | Filters applied to candidates.                                                                                |
| `delete_source`    | bool   | `true`  | Delete the source file after a successful move. Use `--dry-run` to preview before running for the first time. |
| `delete_empty_dir` | bool   | `true`  | Remove empty parent directories after moving.                                                                 |
| `verify`           | bool   | `false` | Re-read the destination file after copying to verify integrity.                                               |

#### `trigger`

| Field             | Type   | Default | Description                                                      |
| ----------------- | ------ | ------- | ---------------------------------------------------------------- |
| `type`            | string | —       | Trigger type: `usage` or `manual`.                               |
| `threshold_start` | int    | `80`    | Start moving when any source storage usage exceeds this percent. |
| `threshold_stop`  | int    | `70`    | Stop moving when source storage usage drops below this percent.  |
| `allowed_window`  | object | —       | Optional time window restriction (only valid for `type: usage`). |

#### `trigger.allowed_window`

| Field            | Type   | Default | Description                                                                 |
| ---------------- | ------ | ------- | --------------------------------------------------------------------------- |
| `start`          | string | —       | Window start time in `HH:MM` format (e.g. `23:00`).                         |
| `end`            | string | —       | Window end time in `HH:MM` format (e.g. `06:00`). Wraps past midnight.      |
| `finish_current` | bool   | `true`  | If `true`, a file being copied when the window closes is allowed to finish. |

#### `source`

| Field          | Type   | Description                                                                                                              |
| -------------- | ------ | ------------------------------------------------------------------------------------------------------------------------ |
| `paths`        | list   | Storage path IDs to scan.                                                                                                |
| `groups`       | list   | Storage group names to scan (expanded to IDs).                                                                           |
| `patterns`     | list   | Glob patterns to filter candidate files (e.g. `library/**`).                                                             |
| `ignore`       | list   | Glob patterns to exclude (e.g. `**/.DS_Store`).                                                                          |
| `include_file` | string | Optional newline-delimited list file of paths/globs to include as candidates. If the file cannot be read, the job fails. |
| `ignore_file`  | string | Optional newline-delimited list file of paths/globs to exclude (ignore wins). If the file cannot be read, the job fails. |

At least one of `paths` or `groups` must be provided.

**`include_file` / `ignore_file` semantics:** entries in these files are matched against the
virtual relative path (e.g. `library/movies/A.mkv` — forward slashes, no leading `/`), the same
as `patterns` and `ignore`. Lines starting with `#` and blank lines are ignored. Ignore always
wins: if a path matches `ignore` or `ignore_file`, it is skipped regardless of `patterns` or
`include_file`. At least one of `patterns` or `include_file` must be provided.

#### `destination`

| Field                | Type   | Default     | Description                                                                                                                                                                                                                   |
| -------------------- | ------ | ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `paths`              | list   | —           | Storage path IDs as destinations.                                                                                                                                                                                             |
| `groups`             | list   | —           | Storage group names as destinations (expanded to IDs).                                                                                                                                                                        |
| `policy`             | string | `most_free` | Target selection: `most_free`, `least_free`, or `first_found`.                                                                                                                                                                |
| `skip_if_exists_any` | bool   | `false`     | If `true`, skip a candidate when the destination path already exists on any destination storage (avoids duplicates; may increase disk I/O). When a file is skipped, `delete_source` does not apply — the source file is kept. |
| `path_preserving`    | bool   | `false`     | Prefer destinations where the parent directory already exists.                                                                                                                                                                |

#### `conditions`

| Field      | Type   | Description                                 |
| ---------- | ------ | ------------------------------------------- |
| `min_age`  | string | Minimum file age (e.g. `7d`, `24h`, `30m`). |
| `min_size` | string | Minimum file size (e.g. `100MB`, `1GB`).    |
| `max_size` | string | Maximum file size.                          |

## Environment variables

| Variable              | Default             | Description                                                                    |
| --------------------- | ------------------- | ------------------------------------------------------------------------------ |
| `PFS_CONFIG_FILE`     | `/etc/pfs/pfs.yaml` | Override the config file path.                                                 |
| `PFS_LOG_FILE`        | (unset)             | If set, enables structured log duplication to this file. Overrides `log.file`. |
| `PFS_STATE_DIR`       | `/var/lib/pfs`      | Base directory for persistent state (index DB, event logs).                    |
| `PFS_RUNTIME_DIR`     | `/run/pfs`          | Base directory for runtime data (locks, sockets).                              |
| `PFS_LOG_DISK_ACCESS` | —                   | Set to `1` to enable disk access logging for indexed storage.                  |
| `TZ`                  | system              | Timezone for `allowed_window` evaluation.                                      |
