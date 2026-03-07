# `pfs mount`

Start a PolicyFS FUSE daemon for the specified mount configuration.

The daemon runs in the foreground until terminated (SIGTERM/SIGINT).
It must be run as root.

## Usage

```bash
pfs mount <mount> [flags]
```

## Flags

| Flag                          | Default      | Description                                                                           |
| ----------------------------- | ------------ | ------------------------------------------------------------------------------------- |
| `--fuse-debug`                | `false`      | Enable go-fuse internal debug logging (raw FUSE request/response dump).               |
| `--log-file <path>`           | config value | Override the log file path (takes precedence over `PFS_LOG_FILE`).                    |
| `--log-disk-access`           | `false`      | Enable disk access logging for indexed storage. Can also set `PFS_LOG_DISK_ACCESS=1`. |
| `--dedup-ttl <sec>`           | `60`         | Disk access log dedup TTL in seconds. `0` disables dedup.                             |
| `--disk-access-summary <sec>` | `60`         | Disk access summary interval in seconds. `0` disables summaries.                      |

## Behavior

- Acquires `daemon.lock` — only one daemon per mount can run at a time (exit code 75 if busy).
- Opens the SQLite index database if any storage path has `indexed: true`.
- Starts a daemon control socket at `/run/pfs/<mount>/daemon.sock` for open-file tracking and config reload.
- Mounts the FUSE filesystem with `default_permissions` and optionally `allow_other`.

## Exit codes

| Code | Meaning                                           |
| ---- | ------------------------------------------------- |
| 0    | Clean shutdown.                                   |
| 75   | Another daemon is already running for this mount. |

## Examples

```bash
# Start the daemon for the "media" mount
pfs mount media

# Start with FUSE debug logging
pfs mount media --fuse-debug

# Start with a custom config
pfs mount media -c /path/to/config.yaml
```

## systemd

In production, use the systemd service:

```bash
sudo systemctl enable --now pfs@media.service
```
