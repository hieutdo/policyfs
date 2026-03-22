# Troubleshooting

## Before you start

Replace `media` with your mount name.

## FUSE permission issues (apps can’t access the mount)

Symptoms:

- Plex/Jellyfin (or another app user) cannot read `/mnt/pfs/<mount>`.

Checklist:

1. Ensure `fuse3` is installed.
2. If you use `allow_other`, ensure `/etc/fuse.conf` contains:

   ```text
   user_allow_other
   ```

3. Ensure your config enables it:

   ```yaml
   fuse:
     allow_other: true
   ```

## systemd service won’t start

1. Check status:

   ```bash
   sudo systemctl status pfs@media.service
   ```

2. Check logs:

   ```bash
   sudo journalctl -u pfs@media.service -n 200 --no-pager
   ```

3. Validate config:

   ```bash
   sudo pfs doctor media
   ```

4. Ensure paths exist (common first-run failure):

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

1. Check for running units:

   ```bash
   systemctl list-units 'pfs-*' --state=running
   ```

2. Check the daemon:

   ```bash
   sudo systemctl status pfs@media.service
   ```
