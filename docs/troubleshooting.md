# Troubleshooting

## Before you start

Replace `media` with your mount name.

## FUSE permission issues (apps can’t access the mount)

Symptoms:

- Plex/Jellyfin (or another app user) cannot read `/mnt/pfs/<mount>`.

Checklist:

- Ensure `fuse3` is installed.
- If you use `allow_other`, ensure `/etc/fuse.conf` contains:

  ```text
  user_allow_other
  ```

- Ensure your config enables it:

  ```yaml
  fuse:
    allow_other: true
  ```

## systemd service won’t start

- Check status:

  ```bash
  sudo systemctl status pfs@media.service
  ```

- Check logs:

  ```bash
  sudo journalctl -u pfs@media.service -n 200 --no-pager
  ```

- Validate config:

  ```bash
  sudo pfs doctor media
  ```

- Ensure paths exist (common first-run failure):

  ```bash
  sudo ls -ld /mnt/pfs/media
  sudo ls -ld /mnt/ssd1/media /mnt/hdd1/media
  ```

## Maintenance jobs do nothing

Many maintenance commands intentionally exit `3` (no changes) when there is nothing to do. systemd timers treat exit code `3` as success.

Useful checks:

- `pfs doctor media`
- `sudo journalctl -u pfs-move@media.service -n 200 --no-pager`
- `sudo journalctl -u pfs-prune@media.service -n 200 --no-pager`
- `sudo journalctl -u pfs-index@media.service -n 200 --no-pager`

## Lock held (exit code 75)

Exit code `75` means another PolicyFS process is holding a lock (usually because another job is running).

Checklist:

- Check for running units:

  ```bash
  systemctl list-units 'pfs-*' --state=running
  ```

- Check the daemon:

  ```bash
  sudo systemctl status pfs@media.service
  ```
