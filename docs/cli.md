# CLI overview

PolicyFS exposes a single `pfs` binary.

## Command families

- Daemon/lifecycle:
  - `pfs mount <mount>`
  - `pfs unmount <mount>`
- Maintenance jobs:
  - `pfs index <mount>`
  - `pfs move <mount>`
  - `pfs prune <mount>`
  - `pfs maint <mount>`
- Diagnostics:
  - `pfs doctor [<mount>] [<file/dir>]`
- Build info:
  - `pfs version`

## Exit codes

Common exit codes:

- `0`: success
- `3`: no changes (clean no-op)
- `75`: busy / lock held
- `78`: `doctor` found issues

See also:

- `pfs --help`
- `pfs <command> --help`
