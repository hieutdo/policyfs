# Troubleshooting

## FUSE permission issues

- Ensure `fuse3` is installed.
- Check `/etc/fuse.conf` if you use `allow_other`.

## systemd service won't start

- Check logs:

  ```bash
  journalctl -u pfs@media.service -n 200 --no-pager
  ```

- Validate config:

  ```bash
  pfs doctor media
  ```

## Index/move/prune does nothing

- Many maintenance commands intentionally exit `3` (no changes) when there is nothing to do.
- Timers treat exit code `3` as success.
