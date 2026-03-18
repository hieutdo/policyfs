# PolicyFS

[![codecov](https://codecov.io/gh/hieutdo/policyfs/branch/main/graph/badge.svg)](https://codecov.io/gh/hieutdo/policyfs)

PolicyFS (pfs) unifies multiple disks into one mount and uses rules to control where files go, with maintenance jobs to index, move, and prune data.

## What it does

- **One mountpoint, many disks**: merge multiple storage paths into a single POSIX-ish view.
- **Routing rules**: decide where reads/writes go based on path patterns.
- **Spin-down friendly listings**: keep HDDs asleep by serving metadata from an index (SQLite) for "indexed" paths.
- **Deferred physical operations**: for indexed paths, record DELETE/RENAME events and apply them later.

## Status

- Supported packaging today: Debian/Ubuntu `.deb` (linux/amd64).

## Docs

- User documentation: https://docs.policyfs.org/v1/

## Quickstart (Debian/Ubuntu)

1. Install runtime dependencies:

   ```bash
   sudo apt-get update
   sudo apt-get install -y fuse3
   ```

2. Download and install the `.deb` from GitHub Releases.

3. Edit the config:
   - `/etc/pfs/pfs.yaml` (created from the example on first install)

4. Start a mount:

   ```bash
   sudo systemctl enable --now pfs@media.service
   ```

5. Run the indexer (optional, but recommended for indexed paths):

   ```bash
   sudo systemctl start pfs-index@media.service
   ```

## Development

The development environment uses Docker Compose (so you can work on macOS/Windows while running Linux FUSE inside the container).

- Start dev environment:

  ```bash
  make dev
  ```

- Run tests:

  ```bash
  make test-unit
  make test-integration
  ```

## License

Apache-2.0. See `LICENSE`.
