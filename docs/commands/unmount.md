# `pfs unmount`

Request unmount and clean shutdown of a PolicyFS mount.

This command is designed for systemd `ExecStop` and for manual recovery when a mount is stuck.

## Usage

```bash
pfs unmount <mount>
```

## Behavior

- Checks if the mountpoint is currently mounted (via `/proc/self/mountinfo` or `mountpoint` command).
- If not mounted, exits successfully (idempotent - safe to run when the daemon is not running).
- Attempts unmount using `fusermount3 -u -z`, `fusermount -u -z`, or `umount -l` (first available tool).
- Times out after 8 seconds if no unmount tool succeeds.

## Exit codes

| Code | Meaning                                                  |
| ---- | -------------------------------------------------------- |
| 0    | Unmount succeeded (or mountpoint was already unmounted). |
| 1    | Unmount failed.                                          |

## Examples

```bash
# Unmount the "media" mount
pfs unmount media

# With explicit config path
pfs unmount media --config /etc/pfs/pfs.yaml
```
