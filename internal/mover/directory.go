package mover

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// mkdiratFunc is a test seam for concurrent directory creation races.
var mkdiratFunc = unix.Mkdirat

// unlinkatFunc is a test seam for rollback failures.
var unlinkatFunc = unix.Unlinkat

// directoryMetadata contains ownership and mode for one source or destination directory.
type directoryMetadata struct {
	mode uint32
	uid  uint32
	gid  uint32
}

// createdDirectory identifies one directory relative to its stable parent descriptor.
type createdDirectory struct {
	parentFD int
	name     string
}

// createdDirectories tracks directories created by one copy attempt for safe rollback.
type createdDirectories struct {
	entries []createdDirectory
}

// duplicateDescriptor duplicates an FD while preserving close-on-exec semantics.
func duplicateDescriptor(fd int) (int, error) {
	dupFD, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("failed to duplicate descriptor: %w", err)
	}
	return dupFD, nil
}

// record retains a stable parent descriptor for later descriptor-relative cleanup.
func (d *createdDirectories) record(parentFD int, name string) error {
	dupFD, err := duplicateDescriptor(parentFD)
	if err != nil {
		return fmt.Errorf("failed to duplicate destination parent descriptor: %w", err)
	}
	d.entries = append(d.entries, createdDirectory{parentFD: dupFD, name: name})
	return nil
}

// cleanup removes only empty directories created by this attempt and reports unexpected failures.
func (d *createdDirectories) cleanup() error {
	var cleanupErr error
	for i := len(d.entries) - 1; i >= 0; i-- {
		entry := d.entries[i]
		err := unlinkatFunc(entry.parentFD, entry.name, unix.AT_REMOVEDIR)
		if err == nil || errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ENOTEMPTY) {
			continue
		}
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("failed to remove destination directory: %w", err))
	}
	d.close()
	return cleanupErr
}

// close releases retained parent descriptors after a committed copy.
func (d *createdDirectories) close() {
	for _, entry := range d.entries {
		_ = unix.Close(entry.parentFD)
	}
	d.entries = nil
}

// destinationDirectoryMode preserves source metadata while ensuring group write and traversal.
func destinationDirectoryMode(sourceMode uint32, parentMode uint32) uint32 {
	mode := sourceMode & 0o7777
	mode |= 0o030
	if parentMode&0o2000 != 0 {
		mode |= 0o2000
	}
	return mode
}

// relativeDirectoryParts resolves a file location's parent into safe root-relative components.
func relativeDirectoryParts(location copyLocation) ([]string, error) {
	root := filepath.Clean(strings.TrimSpace(location.root))
	if root == "." {
		return nil, fmt.Errorf("invalid storage root")
	}
	rel, err := filepath.Rel(root, filepath.Dir(location.physicalPath))
	if err != nil {
		return nil, fmt.Errorf("failed to compute relative directory path: %w", err)
	}
	rel = filepath.Clean(filepath.FromSlash(rel))
	if rel == "." {
		return nil, nil
	}
	if filepath.IsAbs(rel) || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("invalid relative directory path")
	}
	return strings.Split(rel, string(filepath.Separator)), nil
}

// openStorageRoot opens a stable descriptor for one configured storage root.
func openStorageRoot(root string) (int, error) {
	fd, err := unix.Open(filepath.Clean(strings.TrimSpace(root)), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("failed to open storage root: %w", err)
	}
	return fd, nil
}

// openChildDirectory opens one child without following a final symlink.
func openChildDirectory(parentFD int, name string) (int, error) {
	fd, err := unix.Openat(parentFD, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, fmt.Errorf("failed to open child directory: %w", err)
	}
	return fd, nil
}

// directoryMetadataFromFD reads directory ownership and mode from a stable descriptor.
func directoryMetadataFromFD(fd int) (directoryMetadata, error) {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return directoryMetadata{}, fmt.Errorf("failed to stat directory descriptor: %w", err)
	}
	if uint32(st.Mode)&unix.S_IFMT != unix.S_IFDIR {
		return directoryMetadata{}, fmt.Errorf("path is not a directory")
	}
	return directoryMetadata{mode: uint32(st.Mode), uid: st.Uid, gid: st.Gid}, nil
}

// openOrCreateDestinationDirectory tolerates EEXIST races and never follows child symlinks.
func openOrCreateDestinationDirectory(parentFD int, name string, created *createdDirectories) (int, bool, error) {
	fd, err := openChildDirectory(parentFD, name)
	if err == nil {
		return fd, false, nil
	}
	if !errors.Is(err, unix.ENOENT) {
		return -1, false, fmt.Errorf("failed to open destination directory: %w", err)
	}

	createdByUs := false
	if err := mkdiratFunc(parentFD, name, 0o700); err != nil {
		if !errors.Is(err, unix.EEXIST) {
			return -1, false, fmt.Errorf("failed to create destination directory: %w", err)
		}
	} else {
		createdByUs = true
		if err := created.record(parentFD, name); err != nil {
			_ = unix.Unlinkat(parentFD, name, unix.AT_REMOVEDIR)
			return -1, false, err
		}
	}

	fd, err = openChildDirectory(parentFD, name)
	if err != nil {
		return -1, false, fmt.Errorf("failed to open created destination directory: %w", err)
	}
	return fd, createdByUs, nil
}

// ensureDestinationParent creates missing destination components through stable no-follow descriptors.
func ensureDestinationParent(src copyLocation, dst copyLocation) (_ *createdDirectories, retErr error) {
	srcParts, err := relativeDirectoryParts(src)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve source directory: %w", err)
	}
	dstParts, err := relativeDirectoryParts(dst)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve destination directory: %w", err)
	}
	if len(srcParts) != len(dstParts) {
		return nil, fmt.Errorf("source and destination directory depths differ")
	}

	created := &createdDirectories{}
	committed := false
	defer func() {
		if !committed {
			retErr = errors.Join(retErr, created.cleanup())
		}
	}()

	srcFD, err := openStorageRoot(src.root)
	if err != nil {
		return nil, err
	}
	defer func() { _ = unix.Close(srcFD) }()
	dstFD, err := openStorageRoot(dst.root)
	if err != nil {
		return nil, err
	}
	defer func() { _ = unix.Close(dstFD) }()

	for i, dstName := range dstParts {
		srcNextFD, err := openChildDirectory(srcFD, srcParts[i])
		if err != nil {
			return nil, fmt.Errorf("failed to open source directory: %w", err)
		}
		dstNextFD, createdByUs, err := openOrCreateDestinationDirectory(dstFD, dstName, created)
		if err != nil {
			_ = unix.Close(srcNextFD)
			return nil, err
		}
		if createdByUs {
			srcMeta, err := directoryMetadataFromFD(srcNextFD)
			if err != nil {
				_ = unix.Close(srcNextFD)
				_ = unix.Close(dstNextFD)
				return nil, fmt.Errorf("failed to read source directory metadata: %w", err)
			}
			parentMeta, err := directoryMetadataFromFD(dstFD)
			if err != nil {
				_ = unix.Close(srcNextFD)
				_ = unix.Close(dstNextFD)
				return nil, fmt.Errorf("failed to read destination parent metadata: %w", err)
			}
			if err := unix.Fchown(dstNextFD, int(srcMeta.uid), int(srcMeta.gid)); err != nil {
				_ = unix.Close(srcNextFD)
				_ = unix.Close(dstNextFD)
				return nil, fmt.Errorf("failed to set destination directory ownership: %w", err)
			}
			if err := unix.Fchmod(dstNextFD, destinationDirectoryMode(srcMeta.mode, parentMeta.mode)); err != nil {
				_ = unix.Close(srcNextFD)
				_ = unix.Close(dstNextFD)
				return nil, fmt.Errorf("failed to set destination directory mode: %w", err)
			}
		}

		_ = unix.Close(srcFD)
		srcFD = srcNextFD
		_ = unix.Close(dstFD)
		dstFD = dstNextFD
	}

	committed = true
	return created, nil
}
