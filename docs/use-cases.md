# Use cases

This page collects practical configuration patterns.
Each snippet is intentionally partial — merge it into your mount config and adjust paths/IDs.

## 1) Mergerfs alternative (single mount over many disks)

**Goal:** Present multiple disks as a single mountpoint, while distributing writes.

**Suggested config:**

```yaml
mounts:
  media:
    storage_paths:
      - { id: d1, path: /mnt/disk1/media, indexed: false }
      - { id: d2, path: /mnt/disk2/media, indexed: false }
      - { id: d3, path: /mnt/disk3/media, indexed: false }
    storage_groups:
      disks: [d1, d2, d3]
    routing_rules:
      - match: '**'
        read_targets: [disks]
        write_targets: [disks]
        write_policy: most_free
        path_preserving: true
```

- **Pros**
  - Simple mental model (one mount).
  - Easy to expand capacity by adding another `storage_path`.
- **Cons**
  - Writes are still “one target per file” (not striping).

**Recommendation:** Use `write_policy: most_free` + `path_preserving: true` for the usual “fill the emptiest disk but keep directories together” behavior.

## 2) Media library (SSD for hot writes, HDD for capacity)

**Goal:** Put new writes on SSD, keep reads/listings unified, then move older files to HDD.

**Recommended folder layout**

Keep the same relative layout on every disk:

```text
/mnt/ssd1/media/library/{movies,tv,music}/...
/mnt/ssd2/media/library/{movies,tv,music}/...
/mnt/hdd1/media/library/{movies,tv,music}/...
/mnt/hdd2/media/library/{movies,tv,music}/...
/mnt/hdd3/media/library/{movies,tv,music}/...
```

This pairs well with `path_preserving: true`.

**Suggested config:**

```yaml
mounts:
  media:
    storage_paths:
      - { id: ssd1, path: /mnt/ssd1/media, indexed: false }
      - { id: ssd2, path: /mnt/ssd2/media, indexed: false }
      - { id: hdd1, path: /mnt/hdd1/media, indexed: true }
      - { id: hdd2, path: /mnt/hdd2/media, indexed: true }
      - { id: hdd3, path: /mnt/hdd3/media, indexed: true }
    storage_groups:
      ssds: [ssd1, ssd2]
      hdds: [hdd1, hdd2, hdd3]
    routing_rules:
      - match: 'library/**'
        read_targets: [ssds, hdds]
        write_targets: [ssds]
        write_policy: most_free
        path_preserving: true
      - match: '**'
        targets: [ssds, hdds]
        write_policy: most_free
        path_preserving: true
    mover:
      enabled: true
      jobs:
        - name: ssd-to-hdd
          trigger:
            type: usage
            threshold_start: 80
            threshold_stop: 70
          source:
            groups: [ssds]
            patterns: ['library/{movies,tv,music}/**']
          destination:
            groups: [hdds]
            policy: most_free
            path_preserving: true
          conditions:
            min_age: 24h
```

- **Pros**
  - SSD absorbs “churn” (new files, renames, lots of small metadata operations).
  - HDD can stay idle longer because listings come from the index.
- **Cons**
  - PolicyFS does not spin down disks by itself; you still need OS-level HDD power management.

**Recommendation:** Mark HDDs as `indexed: true` to reduce metadata disk touches. Use mover thresholds so the SSDs do not fill up.

## 3) NVR / CCTV storage (tiered retention)

**Goal:** Write new footage to SSD (IOPS), then archive to HDD after a minimum age.

**Suggested config:**

```yaml
mounts:
  nvr:
    storage_paths:
      - { id: ssd1, path: /mnt/ssd1/nvr, indexed: false }
      - { id: ssd2, path: /mnt/ssd2/nvr, indexed: false }
      - { id: hdd1, path: /mnt/hdd1/nvr, indexed: false }
      - { id: hdd2, path: /mnt/hdd2/nvr, indexed: false }
    storage_groups:
      hot: [ssd1, ssd2]
      archive: [hdd1, hdd2]
    routing_rules:
      - match: '**'
        read_targets: [hot, archive]
        write_targets: [hot]
        write_policy: most_free
        path_preserving: true
    mover:
      enabled: true
      jobs:
        - name: archive-footage
          trigger:
            type: manual
          source:
            groups: [hot]
            patterns: ['**']
          destination:
            groups: [archive]
            policy: most_free
            path_preserving: true
          conditions:
            min_age: 24h
```

- **Pros**
  - Keeps HDD from being hammered by small, frequent writes.
  - Retention policy is explicit (age threshold in config).
- **Cons**
  - The “when to run” is your responsibility (timer/scheduler).

**Recommendation:** Start with `trigger.type: manual` and run `pfs move <mount> --job archive-footage` from a timer you control.

## 4) Photo management & preview (fast browsing)

**Goal:** Make directory listings and metadata browsing fast without touching archival disks for metadata.

**Suggested config:**

```yaml
mounts:
  photos:
    storage_paths:
      - { id: hdd1, path: /mnt/hdd1/photos, indexed: true }
      - { id: hdd2, path: /mnt/hdd2/photos, indexed: true }
      - { id: hdd3, path: /mnt/hdd3/photos, indexed: true }
    storage_groups:
      archive: [hdd1, hdd2, hdd3]
    routing_rules:
      - match: '**'
        targets: [archive]
        write_policy: most_free
        path_preserving: true
```

- **Pros**
  - `readdir/getattr` comes from SQLite (fast, no metadata spin-ups).
  - Works well for “browse first, open occasionally”.
- **Cons**
  - Opening the actual photo file still requires disk access.

**Recommendation:** Keep archival storages `indexed: true` and run `pfs index` (or `pfs maint`) on a schedule.

## 5) Seedbox hybrid (download + archive)

**Goal:** Download and seed from SSD, then archive to HDD later without breaking paths.

**Suggested config:**

```yaml
mounts:
  seedbox:
    storage_paths:
      - { id: ssd1, path: /mnt/ssd1/seedbox, indexed: false }
      - { id: ssd2, path: /mnt/ssd2/seedbox, indexed: false }
      - { id: hdd1, path: /mnt/hdd1/seedbox, indexed: true }
      - { id: hdd2, path: /mnt/hdd2/seedbox, indexed: true }
      - { id: hdd3, path: /mnt/hdd3/seedbox, indexed: true }
    storage_groups:
      hot: [ssd1, ssd2]
      archive: [hdd1, hdd2, hdd3]
    routing_rules:
      - match: 'downloads/**'
        read_targets: [hot, archive]
        write_targets: [hot]
        write_policy: most_free
        path_preserving: true
      - match: '**'
        targets: [hot, archive]
        write_policy: most_free
        path_preserving: true
    mover:
      enabled: true
      jobs:
        - name: archive-downloads
          trigger:
            type: manual
          source:
            groups: [hot]
            patterns: ['downloads/**']
          destination:
            groups: [archive]
            policy: most_free
            path_preserving: true
          conditions:
            min_age: 7d
```

- **Pros**
  - High write and read performance for active torrents.
  - Stable paths for apps (no bind-mount gymnastics).
- **Cons**
  - Ratio-based policies are app-specific; PolicyFS only sees filesystem state.

**Recommendation:** Trigger the move job from your seedbox tooling (when it decides a torrent is “cold”). Keep `trigger.type: manual` and call `pfs move seedbox --job archive-downloads`.
