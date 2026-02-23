package cli

import (
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/hieutdo/policyfs/internal/doctor"
	"github.com/hieutdo/policyfs/internal/eventlog"
)

// runDoctorFileInspect handles the `pfs doctor <mount> <path>` file inspect mode.
// It loads config, resolves the mount, builds the inspect report, and prints output.
func runDoctorFileInspect(w io.Writer, mountName string, filePath string, cfgPath string) error {
	rootCfg, loadErr := loadRootConfig(cfgPath)
	if loadErr != nil {
		return newConfigLoadCLIError("doctor", cfgPath, loadErr)
	}

	mountCfg, ok := rootCfg.Mounts[mountName]
	if !ok {
		err := fmt.Errorf("mount %q not found", mountName)
		return &CLIError{
			Code:     ExitUsage,
			Cmd:      "doctor",
			Headline: "invalid arguments",
			Cause:    err,
			Hint:     "run 'pfs doctor --help'",
		}
	}

	report, err := doctor.InspectFile(mountName, filePath, mountCfg)
	if err != nil {
		return &CLIError{
			Code:     ExitFail,
			Cmd:      "doctor",
			Headline: "file inspect failed",
			Cause:    err,
		}
	}

	printFileInspect(w, *report)
	return nil
}

// printFileInspect renders the file inspect report to w.
func printFileInspect(w io.Writer, r doctor.FileInspectReport) {
	fmt.Fprintf(w, "virtual path: %s\n", r.Path)

	// Check if any storage has the file.
	found := false
	for _, s := range r.Storages {
		if s.InIndex || (s.DiskExists != nil && *s.DiskExists) {
			found = true
			break
		}
	}

	if !found {
		fmt.Fprintln(w, "not found in any storage")
		if len(r.Pending) > 0 {
			printFileInspectPending(w, r.Pending)
		}
		return
	}

	multiStorage := len(r.Storages) > 1
	for _, s := range r.Storages {
		if !s.InIndex && (s.DiskExists == nil || !*s.DiskExists) {
			continue // skip storages where file doesn't exist at all
		}
		if multiStorage {
			fmt.Fprintln(w)
		}
		printFileInspectStorage(w, s)
	}

	if len(r.Pending) > 0 {
		printFileInspectPending(w, r.Pending)
	}
}

// printFileInspectStorage prints one storage's file info.
func printFileInspectStorage(w io.Writer, s doctor.FileInspectStorage) {
	indexed := ""
	if s.Indexed {
		indexed = " (indexed)"
	}
	fmt.Fprintf(w, "storage: %s%s\n", s.StorageID, indexed)
	fmt.Fprintf(w, "real path: %s\n", s.PhysicalPath)

	isDir := false
	if s.InIndex {
		isDir = s.IsDir
	} else if s.DiskMode != nil {
		isDir = (*s.DiskMode&syscall.S_IFMT == syscall.S_IFDIR)
	}

	if s.InIndex {
		// Size.
		if !isDir && s.Size != nil {
			fmt.Fprintf(w, "size: %s\n", humanize.IBytes(uint64(*s.Size)))
		}

		// Indexed status.
		if s.LastSeenRunID != nil {
			fmt.Fprintf(w, "indexed: yes (run_id=%d)\n", *s.LastSeenRunID)
		} else {
			fmt.Fprintln(w, "indexed: yes")
		}

		// Mtime.
		if s.MTimeSec != nil {
			t := time.Unix(*s.MTimeSec, 0)
			fmt.Fprintf(w, "mtime: %s\n", t.Format("2006-01-02 15:04:05"))
		}

		// Mode + owner.
		if s.Mode != nil {
			m := *s.Mode
			if m&syscall.S_IFMT == 0 {
				if isDir {
					m |= syscall.S_IFDIR
				} else {
					m |= syscall.S_IFREG
				}
			}
			fmt.Fprintf(w, "mode: %s\n", formatFileMode(m))
			if s.UID != nil && s.GID != nil {
				fmt.Fprintf(w, "owner: %d:%d\n", *s.UID, *s.GID)
			}
		}

		// Deleted status.
		if s.Deleted != nil {
			fmt.Fprintf(w, "deleted: %s\n", deletedLabel(*s.Deleted))
		}

		// Pending rename (real_path != path).
		if s.RenamePending {
			fmt.Fprintf(w, "index real_path: %s (rename pending)\n", s.RealPath)
		}

		// file_meta overrides.
		printMetaOverrides(w, s)
	} else {
		fmt.Fprintln(w, "indexed: no")

		// Show disk stat info for non-indexed storages where the file exists.
		if s.DiskExists != nil && *s.DiskExists {
			if !isDir && s.DiskSize != nil {
				fmt.Fprintf(w, "size: %s\n", humanize.IBytes(uint64(*s.DiskSize)))
			}
			if s.DiskMTime != nil {
				t := time.Unix(*s.DiskMTime, 0)
				fmt.Fprintf(w, "mtime: %s\n", t.Format("2006-01-02 15:04:05"))
			}
			if s.DiskMode != nil {
				fmt.Fprintf(w, "mode: %s\n", formatFileMode(*s.DiskMode))
			}
			if s.DiskUID != nil && s.DiskGID != nil {
				fmt.Fprintf(w, "owner: %d:%d\n", *s.DiskUID, *s.DiskGID)
			}
		}
	}

	// Disk status.
	if s.DiskExists != nil && !*s.DiskExists {
		if s.DiskError != "" {
			fmt.Fprintf(w, "disk: error: %s\n", s.DiskError)
		} else {
			fmt.Fprintln(w, "disk: not found")
		}
	}
}

// printMetaOverrides prints file_meta overrides if any are active.
func printMetaOverrides(w io.Writer, s doctor.FileInspectStorage) {
	var parts []string
	if s.MetaMTime != nil {
		t := time.Unix(*s.MetaMTime, 0)
		parts = append(parts, fmt.Sprintf("mtime=%s", t.Format("2006-01-02 15:04:05")))
	}
	if s.MetaMode != nil {
		parts = append(parts, fmt.Sprintf("mode=%s", formatFileMode(*s.MetaMode)))
	}
	if s.MetaUID != nil {
		parts = append(parts, fmt.Sprintf("uid=%d", *s.MetaUID))
	}
	if s.MetaGID != nil {
		parts = append(parts, fmt.Sprintf("gid=%d", *s.MetaGID))
	}
	if len(parts) > 0 {
		fmt.Fprintf(w, "overrides: %s\n", joinParts(parts))
	}
}

// printFileInspectPending prints pending events for the file.
func printFileInspectPending(w io.Writer, events []doctor.FileInspectEvent) {
	fmt.Fprintf(w, "Pending Events (%d)\n", len(events))
	for _, e := range events {
		ts := e.TS.Format("2006-01-02 15:04:05")
		switch e.Type {
		case eventlog.TypeRename:
			fmt.Fprintf(w, "%-8s storage_id=%-8s old=%s new=%s  %s\n", string(e.Type), e.StorageID, e.OldPath, e.NewPath, ts)
		case eventlog.TypeSetattr:
			fmt.Fprintf(w, "%-8s storage_id=%-8s path=%s  %s\n", string(e.Type), e.StorageID, e.Path, ts)
		default:
			fmt.Fprintf(w, "%-8s storage_id=%-8s path=%s  %s\n", string(e.Type), e.StorageID, e.Path, ts)
		}
	}
}

// formatFileMode converts a raw uint32 mode to a human-readable string like "-rw-r--r--".
func formatFileMode(mode uint32) string {
	fm := os.FileMode(mode & 0o777)
	if mode&syscall.S_ISUID != 0 {
		fm |= os.ModeSetuid
	}
	if mode&syscall.S_ISGID != 0 {
		fm |= os.ModeSetgid
	}
	if mode&syscall.S_ISVTX != 0 {
		fm |= os.ModeSticky
	}

	switch mode & syscall.S_IFMT {
	case syscall.S_IFDIR:
		fm |= os.ModeDir
	case syscall.S_IFLNK:
		fm |= os.ModeSymlink
	case syscall.S_IFCHR:
		fm |= os.ModeDevice | os.ModeCharDevice
	case syscall.S_IFBLK:
		fm |= os.ModeDevice
	case syscall.S_IFIFO:
		fm |= os.ModeNamedPipe
	case syscall.S_IFSOCK:
		fm |= os.ModeSocket
	}

	return fm.String()
}

// deletedLabel returns a human label for the deleted field value.
func deletedLabel(d int) string {
	switch d {
	case 0:
		return "no"
	case 1:
		return "pending delete"
	case 2:
		return "stale"
	default:
		return fmt.Sprintf("unknown (%d)", d)
	}
}
