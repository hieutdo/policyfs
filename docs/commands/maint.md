# `pfs maint`

Runs a batched maintenance flow (move + prune + optional index).

## Usage

```bash
pfs maint <mount> [--index touch|all|off]
```

## Index modes

- `touch` (default): only index storages touched by the same maint run
- `all`: index all indexed storages
- `off`: skip indexing

## systemd

- `pfs-maint@<mount>.service`
- `pfs-maint@<mount>.timer`
