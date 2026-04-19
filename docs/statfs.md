# Statfs reporting

PolicyFS can route writes to different storage targets depending on the path.
That makes `statfs` reporting a trade-off between:

- A stable, mount-wide view that works well with `df -h` and SMB clients.
- A per-path view that reflects the actual write targets for a specific directory.

This page explains what the `mounts.<name>.statfs.reporting` modes mean and when to use each.

## What `statfs` is (and what it is not)

`statfs` reports filesystem capacity and free space. Traditional filesystems return values that are effectively constant across the whole mount (unless quotas are involved).

PolicyFS is different: it presents a single mountpoint backed by multiple underlying filesystems and a routing policy. PolicyFS therefore has to choose which underlying roots to report.

## Reporting modes

### `mount_pooled_targets` (default)

Mount-wide reporting. PolicyFS returns pooled stats for the union of all write targets referenced by the mount's routing rules.

This mode is designed to be:

- Stable for the mountpoint (so `df -h /mnt/pfs/<mount>` stays intuitive).
- Conservative about "what this mount can write to" (it does not blindly include storage paths that are never used for writes).

**Trade-off:** the numbers may not reflect the write capacity of a specific subdirectory. A particular subtree might only write to a subset of these targets.

### `path_pooled_targets`

Path-aware reporting. PolicyFS returns pooled stats for the write targets resolved for the path being queried.

This mode is designed to help applications that call `statfs` on a specific directory and want the result to reflect "where writes to this directory will land".

**Trade-off:** different directories can return different totals. In particular, it is possible for a subdirectory to report more free space than the mount root if routing rules differ.

## Why results can differ by directory

Consider a mount with the following intent:

- `library/**` should write to HDDs.
- Everything else should write to a single SSD.

In `path_pooled_targets` mode:

- `statfs(/mnt/pfs/media)` follows the catch-all rule and reports the SSD.
- `statfs(/mnt/pfs/media/library)` follows the `library/**` rule and reports the HDDs.

This is correct for "capacity for writes at this path", but it can surprise users who expect mount-wide `df` output to be constant.

If you want `df` to look stable, keep the default `mount_pooled_targets`.

## Error handling (`statfs.on_error`)

When pooling stats across multiple roots, some roots may fail `statfs` (e.g. disk unplugged, permission issues).

`mounts.<name>.statfs.on_error` controls how PolicyFS behaves:

- `ignore_failed`: pool what succeeds; if everything fails, fall back to loopback reporting.
- `fail_eio`: return `EIO` if any root fails.
- `fallback_effective_target`: if pooling cannot complete, fall back to reporting the selected write target for the requested path; if that also fails, fall back to loopback.
- `fallback_loopback`: if pooling cannot complete, fall back to loopback reporting.

## Recommendation

- Use `mount_pooled_targets` unless you have a specific application that needs per-directory `statfs` routing semantics.
- Use `path_pooled_targets` only when you understand and accept that different directories may report different totals.
