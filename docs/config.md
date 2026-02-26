# Configuration

PolicyFS reads its config from:

- `/etc/pfs/pfs.yaml` (default)

The Debian package ships an example at:

- `/etc/pfs/pfs.yaml.example`

## Minimal example

The shipped example config is a good starting point. Key fields:

- `mounts.<name>.mountpoint`: where the FUSE mount lives
- `mounts.<name>.storage_paths`: physical storage roots
- `mounts.<name>.storage_groups`: logical grouping
- `mounts.<name>.routing_rules`: read/write routing rules

## Storage paths

Each storage path has:

- `id`: stable identifier (used in logs and DB)
- `path`: absolute filesystem path
- `indexed`: when `true`, metadata operations use the index to avoid spinning up disks

## Routing rules

Rules are evaluated top-to-bottom. First match wins.

A rule defines:

- `match`: glob-like pattern
- `read_targets`: list of groups or storage IDs
- `write_targets`: list of groups or storage IDs
- `write_policy`: how to select a write target

## Mover

Mover is configured per mount under `mounts.<name>.mover`.

See also:

- `packaging/config/pfs.example.yaml` in the repo
