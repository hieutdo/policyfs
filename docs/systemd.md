# systemd

PolicyFS is designed to be operated via systemd:

- `pfs@<mount>.service` is the long-running daemon (FUSE mount).
- `pfs-index@<mount>.timer` runs indexing periodically.
- `pfs-move@<mount>.timer` runs mover periodically.
- `pfs-prune@<mount>.timer` runs prune periodically.
- `pfs-maint@<mount>.timer` runs a batched maintenance flow.

## Directory layout

PolicyFS uses standard Linux locations:

- Config: `/etc/pfs/pfs.yaml`
- State: `/var/lib/pfs/<mount>/`
- Runtime: `/run/pfs/<mount>/`
- Logs: journald by default; optional file logging can be enabled via config.

## Common operations

Start/restart a mount:

```bash
sudo systemctl restart pfs@media.service
```

Disable all scheduled jobs for a mount:

```bash
sudo systemctl disable --now pfs-index@media.timer
sudo systemctl disable --now pfs-move@media.timer
sudo systemctl disable --now pfs-prune@media.timer
sudo systemctl disable --now pfs-maint@media.timer
```

## Overrides

Use drop-ins instead of editing vendor unit files:

```bash
sudo systemctl edit pfs@media.service
```
