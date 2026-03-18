# Disk spindown (power saving)

PolicyFS can reduce disk wake-ups caused by metadata operations by using `indexed: true`, but it does not control drive power states.
If you want disks to actually spin down and save power, you must configure OS-level disk power management.

## What PolicyFS does (and does not) do

- PolicyFS can avoid touching slow disks for metadata-heavy workloads by serving `readdir/getattr` from the SQLite index.
- PolicyFS does not spin down disks.
- Maintenance jobs (`pfs index`, `pfs move`, `pfs prune`, `pfs maint`) will wake archive disks by design. Schedule them off-hours.

## Recommended approach: hd-idle

`hd-idle` is a small daemon that spins down disks after an idle timeout.

### Pros/cons

**Pros**

- Simple and explicit.
- Works well with “keep HDD asleep most of the day, wake it during maintenance windows”.

**Cons**

- Too-aggressive timeouts can increase load/unload cycles and reduce drive lifespan.
- Some USB/SATA bridges and some drives ignore standby requests.

### Setup (Debian/Ubuntu)

**Install**

```bash
sudo apt-get install hd-idle
```

**Configure**

Packaging varies by distro, so follow your distro docs or the upstream guide:
https://github.com/adelolmo/hd-idle?tab=readme-ov-file#configuration

If you are not sure where the options are set, check the effective systemd unit:

```bash
systemctl cat hd-idle
```

Prefer stable device paths like `/dev/disk/by-id/...` instead of `/dev/sdX`, and do not apply spindown to SSDs.

**Enable and start**

```bash
sudo systemctl enable --now hd-idle
```

If you change `hd-idle` configuration, reload and restart:

```bash
sudo systemctl daemon-reload
sudo systemctl restart hd-idle
```

## Alternative: hdparm

`hdparm` can set a drive standby timer.

### Pros/cons

**Pros**

- No daemon process.
- Works fine on many direct-attached SATA disks.

**Cons**

- Easy to misconfigure.
- Some devices ignore it (especially behind USB bridges).
- Some queries can wake the drive.

If you go this route, test on one disk first, and keep the setting conservative.

## Verify whether disks are spinning down

There is no universal method that works across every enclosure/bridge.
A few common tools:

- `smartctl -n standby -a <device>` (tries to avoid waking the drive)
- `hdparm -C <device>` (may or may not wake the drive depending on the transport)

If your disks never go idle, check for background activity.

## Common reasons disks never reach standby

- SMART polling too frequently.
- RAID/mdadm periodic checks.
- Filesystems mounted with `atime` causing metadata writes on reads.
- Your media app continuously scanning.

The goal with PolicyFS is: reduce unnecessary metadata I/O so that OS-level spindown tools have a chance to keep disks asleep most of the time.
