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

## Files still appear after deletion

**Symptom:** You deleted a file (or folder) through the mount but it still shows up in directory listings or still occupies space on the physical disk.

This is expected behavior for storage paths with `indexed: true`. Deletes on indexed storage are recorded in the event log (`events.ndjson`) and are not applied to the physical disk immediately - they are deferred to the next `pfs prune` run.

Run prune manually to apply them now:

```bash
sudo systemctl start pfs-prune@media.service
```

Or check how many events are pending:

```bash
sudo pfs doctor media
```

## Archive disks still spinning up after enabling indexed storage

**Symptom:** You set `indexed: true` on your HDDs but they still spin up during scans.

Two common reasons:

1. **The index hasn't been populated yet.** Metadata can't be served from SQLite until `pfs index` has run. Run it once first:

   ```bash
   sudo systemctl start pfs-index@media.service
   ```

2. **File data reads always touch the physical disk.** `indexed: true` only helps with metadata operations (directory listings, file attributes). Whenever an app reads actual file content, the disk spins up - this is unavoidable.

Use `pfs doctor <mount>` to confirm the index is populated and check how many files are indexed per storage path.

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
