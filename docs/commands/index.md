# `pfs index`

Indexes `indexed=true` storage paths so metadata operations can be served without waking HDDs.

## Usage

```bash
pfs index <mount>
```

## systemd

- `pfs-index@<mount>.service`
- `pfs-index@<mount>.timer`
