# `pfs mount`

Starts the PolicyFS FUSE mount daemon instance for a mount.

## Usage

```bash
pfs mount <mount>
```

## Notes

- In production, you typically run this via systemd: `pfs@<mount>.service`.
- The mount name is positional.
