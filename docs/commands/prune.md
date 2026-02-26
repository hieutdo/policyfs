# `pfs prune`

Applies deferred physical operations (e.g. DELETE/RENAME) that were recorded for indexed paths.

## Usage

```bash
pfs prune <mount>
```

## systemd

- `pfs-prune@<mount>.service`
- `pfs-prune@<mount>.timer`
