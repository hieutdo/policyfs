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

- **Option A: individual timers** (`pfs-index`, `pfs-move`, `pfs-prune`)
  - Pros: easy to spread work across the day.
  - Cons: more units to manage.
- **Option B: maint timer** (`pfs-maint`)
  - Pros: one schedule, runs a batched flow.
  - Cons: less control over per-phase timing.

I recommend starting with **Option B (`pfs-maint`)** unless you have a reason to separate the phases.

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
