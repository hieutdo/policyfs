# Disk spindown (power saving)

PolicyFS can reduce disk wake-ups caused by metadata operations by using `indexed: true`, but it does not control drive power states.
If you want disks to actually spin down and save power, you must configure OS-level disk power management.

## What PolicyFS does (and does not) do

- PolicyFS can avoid touching slow disks for metadata-heavy workloads by serving `readdir/getattr` from the SQLite index.
- PolicyFS does not spin down disks.
- Maintenance jobs (`pfs index`, `pfs move`, `pfs prune`, `pfs maint`) will wake archive disks by design. Schedule them off-hours.

## Recommended approach: hd-idle

`hd-idle` is a small daemon that spins down disks after an idle timeout.

### Setup (Debian/Ubuntu)

**Install**

```bash
sudo apt-get install hd-idle
```

**Configure**

Edit the hd-idle configuration. The location varies by distro; check the effective unit first:

```bash
systemctl cat hd-idle
```

A common pattern is to override `ExecStart`. Use a drop-in:

```bash
sudo systemctl edit hd-idle
```

Example override (10-minute spindown for two HDDs):

```ini
[Service]
ExecStart=
ExecStart=/usr/sbin/hd-idle \
  -i 0 \
  -a /dev/disk/by-id/ata-WDC_WD40EFRX-EXAMPLE1 -i 600 \
  -a /dev/disk/by-id/ata-WDC_WD40EFRX-EXAMPLE2 -i 600
```

Key points:

- Use `/dev/disk/by-id/...` (stable paths) instead of `/dev/sdX` (changes on reboot).
- `-i 0` disables the global default so only explicitly listed disks are managed.
- `-i 600` = 600 seconds (10 minutes) idle before spindown.
- Do not include SSDs

For the full option reference, see the [upstream hd-idle docs](https://github.com/adelolmo/hd-idle?tab=readme-ov-file#configuration).

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

If you go this route, test on one disk first, and keep the setting conservative.

## Verify whether disks are spinning down

There is no universal method that works across every enclosure/bridge.
A few common tools:

- `smartctl -n standby -a <device>` (tries to avoid waking the drive)
- `hdparm -C <device>` (may or may not wake the drive depending on the transport)

If your disks never go idle, check for background activity.

## Mount HDDs with noatime

By default, Linux writes an access timestamp (`atime`) every time a file is read. On HDDs, this wakes a sleeping disk even when the read was served from PolicyFS's metadata index.

Add `noatime` (or `relatime`) to the HDD mount options in `/etc/fstab`:

```fstab
/dev/disk/by-id/ata-... /mnt/hdd1  ext4  defaults,noatime  0 2
```

Then remount:

```bash
sudo mount -o remount,noatime /mnt/hdd1
```

## Common reasons disks never reach standby

- HDDs mounted without `noatime` — reads trigger atime writes.
- SMART polling too frequently.
- RAID/mdadm periodic checks.
- Your media app continuously scanning.

The goal with PolicyFS is: reduce unnecessary metadata I/O so that OS-level spindown tools have a chance to keep disks asleep most of the time.
