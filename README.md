# PolicyFS

[![codecov](https://codecov.io/gh/hieutdo/policyfs/branch/main/graph/badge.svg)](https://codecov.io/gh/hieutdo/policyfs)
[![license](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

PolicyFS is a Linux FUSE storage daemon that unifies multiple storage paths under one mountpoint with explicit read/write routing rules, an optional SQLite metadata index, and built-in maintenance jobs.

## What it does

- **One mountpoint, many disks**: merge multiple storage paths into a single POSIX-ish view.
- **Routing rules**: decide where reads/writes go based on path patterns.
- **Spin-down friendly listings**: keep HDDs asleep by serving metadata from an index (SQLite) for "indexed" paths.
- **Deferred physical operations**: for indexed paths, record DELETE/RENAME events and apply them later.

## Status

- Supported packaging today: Debian/Ubuntu + Fedora/EL9 (linux/amd64).

## Docs

- User documentation: https://docs.policyfs.org/

## Getting started

For the full step-by-step guide, see: https://docs.policyfs.org/getting-started/

1. Install runtime dependencies:

   ```bash
   sudo apt-get update
   sudo apt-get install -y fuse3
   ```

2. Install from the APT repo or download a `.deb` from GitHub Releases.

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

## Contributing

If you want to contribute, keep PRs small and behavior-focused.

The development environment uses Docker Compose so you can work on macOS/Windows while running Linux FUSE inside the container.

Start dev environment:

```bash
make dev # Start the dev container
```

Run tests:

```bash
make test-unit # Run unit tests in the dev container
make test-integration # Run integration tests in the dev container
make coverage # Generate coverage report for both unit and integration tests
```

Useful commands:

```bash
make dev-fresh # Start the dev container with a fresh state
make dev-shell # Open a shell in the dev container
make fmt # Format code
make lint # Lint code

make docs # Generate documentation
make docs-serve # Serve documentation
```

Before opening a PR:

- Ensure `make lint` is clean.
- Run the relevant tests (`make test-unit` at minimum; add `make test-integration` for filesystem behavior changes).
- Run `make coverage` to generate a coverage report for both unit and integration tests.
- If you changed behavior or configuration, update the nearest relevant docs in `docs/`.

## License

Apache-2.0. See `LICENSE`.
