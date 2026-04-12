# systemd

PolicyFS is designed to be operated via systemd:

- `pfs@<mount>.service` is the long-running daemon (FUSE mount).
- `pfs-index@<mount>.timer` runs indexing periodically.
- `pfs-move@<mount>.timer` runs mover periodically.
- `pfs-prune@<mount>.timer` runs prune periodically.
- `pfs-maint@<mount>.timer` runs a batched maintenance flow.

## Timer schedules

The Debian package ships these default schedules:

| Unit                      | Schedule                    | Notes                                       |
| ------------------------- | --------------------------- | ------------------------------------------- |
| `pfs-move@<mount>.timer`  | `OnCalendar=*-*-* 00:30:00` | `RandomizedDelaySec=5m`, `Persistent=true`  |
| `pfs-maint@<mount>.timer` | `OnCalendar=*-*-* 00:30:00` | `RandomizedDelaySec=10m`, `Persistent=true` |
| `pfs-prune@<mount>.timer` | `OnCalendar=*-*-* 04:15:00` | `RandomizedDelaySec=10m`, `Persistent=true` |
| `pfs-index@<mount>.timer` | `OnCalendar=*-*-* 05:30:00` | `RandomizedDelaySec=2m`, `Persistent=true`  |

You typically enable either:

- **The maint timer** (`pfs-maint`) if you want one schedule and a single maintenance window. It runs a batched flow.
- **Individual timers** (`pfs-index`, `pfs-move`, `pfs-prune`) if you want to stagger work across the day or tune each phase separately.

I recommend starting with **`pfs-maint`** unless you have a reason to separate the phases.

Avoid enabling `pfs-maint@<mount>.timer` at the same time as the individual timers.

## Inspect timer settings

Show the effective unit (including drop-ins):

```bash
sudo systemctl cat pfs-index@media.timer
```

List next runs:

```bash
systemctl list-timers 'pfs-*@media.timer'
```

## Directory layout

PolicyFS uses standard Linux locations:

- Config: `/etc/pfs/pfs.yaml`
- State: `/var/lib/pfs/<mount>/`
- Runtime: `/run/pfs/<mount>/`
- Logs: journald by default; optional file logging can be enabled via config.

## Check job output

View the last run of a maintenance job:

```bash
journalctl -u pfs-maint@media.service -n 50
```

Example output:

```
Mar 22 00:30:12 host pfs[1234]: move: moved 3 files (2.1 GB) from ssd1 → hdd1
Mar 22 00:30:15 host pfs[1234]: prune: applied 7 events (4 deletes, 3 renames)
Mar 22 00:30:18 host pfs[1234]: index: indexed 12483 files across hdd1, hdd2
```

Exit code 3 means the job ran but found nothing to do - this is normal when disks are not above the threshold or there are no deferred events.

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

To change a timer schedule, edit the timer unit:

```bash
sudo systemctl edit pfs-index@media.timer
```

Example override:

```ini
[Timer]
OnCalendar=
OnCalendar=*-*-* 03:00:00
RandomizedDelaySec=0
```

Then reload and restart the timer:

```bash
sudo systemctl daemon-reload
sudo systemctl restart pfs-index@media.timer
```
