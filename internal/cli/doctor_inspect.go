package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/eventlog"
	"github.com/hieutdo/policyfs/internal/indexdb"
)

// fileInspectReport holds the full file inspect result.
type fileInspectReport struct {
	Mount    string               `json:"mount"`
	Path     string               `json:"path"` // normalized virtual path
	Storages []fileInspectStorage `json:"storages"`
	Pending  []fileInspectEvent   `json:"pending"`
}

// fileInspectStorage describes a file's state on one storage.
type fileInspectStorage struct {
	StorageID    string `json:"storage_id"`
	PhysicalPath string `json:"physical_path"`
	Indexed      bool   `json:"indexed"` // from config (storage has indexed=true)

	// From index DB (zero values when InIndex is false).
	InIndex       bool       `json:"in_index"`
	IsDir         bool       `json:"is_dir"`
	Size          *int64     `json:"size,omitempty"`
	MTimeSec      *int64     `json:"mtime_sec,omitempty"`
	Mode          *uint32    `json:"mode,omitempty"`
	UID           *uint32    `json:"uid,omitempty"`
	GID           *uint32    `json:"gid,omitempty"`
	Deleted       *int       `json:"deleted,omitempty"` // 0=live, 1=pending-delete, 2=stale
	LastSeenRunID *int64     `json:"last_seen_run_id,omitempty"`
	CurrentRunID  *int64     `json:"current_run_id,omitempty"`
	LastCompleted *time.Time `json:"last_completed,omitempty"` // last completed index run for this storage
	RealPath      string     `json:"real_path,omitempty"`      // if != Path → pending physical rename
	RenamePending bool       `json:"rename_pending"`

	// file_meta overrides (nil = no override).
	MetaMTime *int64  `json:"meta_mtime,omitempty"`
	MetaMode  *uint32 `json:"meta_mode,omitempty"`
	MetaUID   *uint32 `json:"meta_uid,omitempty"`
	MetaGID   *uint32 `json:"meta_gid,omitempty"`

	// On-disk stat.
	DiskExists *bool   `json:"disk_exists,omitempty"`
	DiskSize   *int64  `json:"disk_size,omitempty"`
	DiskMTime  *int64  `json:"disk_mtime,omitempty"`
	DiskMode   *uint32 `json:"disk_mode,omitempty"`
	DiskUID    *uint32 `json:"disk_uid,omitempty"`
	DiskGID    *uint32 `json:"disk_gid,omitempty"`
	DiskError  string  `json:"disk_error,omitempty"`
}

// fileInspectEvent describes one pending event relevant to this path.
type fileInspectEvent struct {
	Type      eventlog.Type `json:"type"`
	StorageID string        `json:"storage_id"`
	Path      string        `json:"path,omitempty"`     // DELETE/SETATTR
	OldPath   string        `json:"old_path,omitempty"` // RENAME
	NewPath   string        `json:"new_path,omitempty"` // RENAME
	TS        time.Time     `json:"ts"`
}

// runDoctorFileInspect handles the `pfs doctor <mount> <path>` file inspect mode.
// It loads config, resolves the mount, builds the inspect report, and prints output.
func runDoctorFileInspect(w io.Writer, jsonOut bool, mountName string, filePath string, cfgPath string) error {
	rootCfg, loadErr := loadRootConfig(cfgPath)
	if loadErr != nil {
		if jsonOut {
			scope := &JSONScope{Mount: &mountName, Config: &cfgPath}
			env := JSONEnvelope{
				Command:  "doctor",
				OK:       false,
				Scope:    scope,
				Warnings: []JSONIssue{},
				Errors:   []JSONIssue{jsonIssueFromConfigLoadError(cfgPath, loadErr)},
			}
			return emitJSONAndExit(ExitFail, env)
		}
		return newConfigLoadCLIError("doctor", cfgPath, loadErr)
	}

	mountCfg, ok := rootCfg.Mounts[mountName]
	if !ok {
		err := fmt.Errorf("mount %q not found", mountName)
		if jsonOut {
			scope := &JSONScope{Mount: &mountName, Config: &cfgPath}
			env := JSONEnvelope{
				Command:  "doctor",
				OK:       false,
				Scope:    scope,
				Warnings: []JSONIssue{},
				Errors:   []JSONIssue{jsonIssueFromError("ARG_MOUNT", err, "run 'pfs doctor --help'")},
			}
			return emitJSONAndExit(ExitUsage, env)
		}
		return &CLIError{
			Code:     ExitUsage,
			Cmd:      "doctor",
			Headline: "invalid arguments",
			Cause:    err,
			Hint:     "run 'pfs doctor --help'",
		}
	}

	report, err := runFileInspect(mountName, filePath, mountCfg)
	if err != nil {
		if jsonOut {
			scope := &JSONScope{Mount: &mountName, Config: &cfgPath}
			env := JSONEnvelope{
				Command:  "doctor",
				OK:       false,
				Scope:    scope,
				Warnings: []JSONIssue{},
				Errors:   []JSONIssue{jsonIssueFromError("INSPECT", err, "")},
			}
			return emitJSONAndExit(ExitFail, env)
		}
		return &CLIError{
			Code:     ExitFail,
			Cmd:      "doctor",
			Headline: "file inspect failed",
			Cause:    err,
		}
	}

	if jsonOut {
		scope := &JSONScope{Mount: &mountName, Config: &cfgPath}
		env := JSONEnvelope{
			Command:  "doctor",
			OK:       true,
			Scope:    scope,
			Inspect:  report,
			Warnings: []JSONIssue{},
			Errors:   []JSONIssue{},
		}
		if err := writeJSON(env); err != nil {
			return &CLIError{
				Code:     ExitFail,
				Cmd:      "doctor",
				Headline: "failed to write json",
				Cause:    rootCause(err),
			}
		}
		return nil
	}

	printFileInspect(w, *report)
	return nil
}

// runFileInspect builds a fileInspectReport for a single file across all storages.
func runFileInspect(mountName string, filePath string, mountCfg config.MountConfig) (*fileInspectReport, error) {
	normalizedPath := indexdb.NormalizeVirtualPath(filePath)
	if normalizedPath == "" {
		return nil, fmt.Errorf("path resolves to root (empty after normalization)")
	}

	report := &fileInspectReport{
		Mount: mountName,
		Path:  normalizedPath,
	}

	// Query index DB for this path across all storages.
	dbRows, err := indexdb.QueryFileInspect(mountName, normalizedPath)
	if err != nil {
		return nil, fmt.Errorf("query index: %w", err)
	}

	// Index results by storage_id for fast lookup.
	dbByStorage := map[string]indexdb.InspectFileRow{}
	for _, row := range dbRows {
		dbByStorage[row.StorageID] = row
	}

	// Build per-storage results.
	for _, sp := range mountCfg.StoragePaths {
		physicalRel := normalizedPath
		s := fileInspectStorage{
			StorageID: sp.ID,
			Indexed:   sp.Indexed,
		}

		// Merge index DB row if found.
		if row, ok := dbByStorage[sp.ID]; ok {
			s.InIndex = true
			s.IsDir = row.IsDir
			s.Size = row.Size
			s.MTimeSec = &row.MTimeSec
			s.Mode = &row.Mode
			s.UID = &row.UID
			s.GID = &row.GID
			s.Deleted = &row.Deleted
			s.LastSeenRunID = row.LastSeenRunID
			s.CurrentRunID = &row.CurrentRunID
			if row.LastCompleted != nil {
				t := time.Unix(*row.LastCompleted, 0)
				s.LastCompleted = &t
			}
			s.RealPath = row.RealPath
			s.RenamePending = row.RealPath != row.Path
			physicalRel = row.RealPath

			// file_meta overrides.
			s.MetaMTime = row.MetaMTime
			s.MetaMode = row.MetaMode
			s.MetaUID = row.MetaUID
			s.MetaGID = row.MetaGID
		}

		s.PhysicalPath = filepath.Join(sp.Path, physicalRel)

		// Stat physical file on disk.
		s.statDisk()

		report.Storages = append(report.Storages, s)
	}

	// Scan pending events for this path.
	report.Pending = []fileInspectEvent{}
	pending, err := findPendingEvents(mountName, normalizedPath)
	if err == nil && pending != nil {
		report.Pending = pending
	}

	return report, nil
}

// statDisk performs os.Stat on the physical path and populates disk fields.
func (s *fileInspectStorage) statDisk() {
	fi, err := os.Stat(s.PhysicalPath)
	if err != nil {
		exists := false
		s.DiskExists = &exists
		if !os.IsNotExist(err) {
			s.DiskError = err.Error()
		}
		return
	}

	exists := true
	s.DiskExists = &exists
	sz := fi.Size()
	s.DiskSize = &sz
	mt := fi.ModTime().Unix()
	s.DiskMTime = &mt

	// Prefer unix mode bits from syscall.Stat_t for consistency with indexed mode.
	if stat, ok := fi.Sys().(*syscall.Stat_t); ok {
		mode := uint32(stat.Mode)
		s.DiskMode = &mode
		uid := stat.Uid
		s.DiskUID = &uid
		gid := stat.Gid
		s.DiskGID = &gid
		return
	}

	mode := uint32(fi.Mode().Perm())
	if fi.Mode()&os.ModeSetuid != 0 {
		mode |= syscall.S_ISUID
	}
	if fi.Mode()&os.ModeSetgid != 0 {
		mode |= syscall.S_ISGID
	}
	if fi.Mode()&os.ModeSticky != 0 {
		mode |= syscall.S_ISVTX
	}
	if fi.IsDir() {
		mode |= syscall.S_IFDIR
	} else {
		mode |= syscall.S_IFREG
	}
	s.DiskMode = &mode
}

// pendingEventReader is the minimal interface we need from eventlog.Reader.
// It exists to make findPendingEvents testable.
type pendingEventReader interface {
	Next() (line []byte, nextOffset int64, err error)
	Close() error
}

// findPendingEventsFromReader scans pending events from reader and returns those
// matching the given path. Caps at maxScan events to avoid slowness.
func findPendingEventsFromReader(reader pendingEventReader, path string) ([]fileInspectEvent, error) {
	const maxScan = 10_000
	var results []fileInspectEvent
	count := 0

	for count < maxScan {
		line, _, err := reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("failed to read pending events: %w", err)
		}
		count++

		evt, err := eventlog.Parse(line)
		if err != nil {
			continue
		}

		switch e := evt.(type) {
		case eventlog.DeleteEvent:
			if e.Path == path {
				results = append(results, fileInspectEvent{
					Type: eventlog.TypeDelete, StorageID: e.StorageID,
					Path: e.Path, TS: time.Unix(e.TS, 0),
				})
			}
		case eventlog.RenameEvent:
			if e.OldPath == path || e.NewPath == path {
				results = append(results, fileInspectEvent{
					Type: eventlog.TypeRename, StorageID: e.StorageID,
					OldPath: e.OldPath, NewPath: e.NewPath, TS: time.Unix(e.TS, 0),
				})
			}
		case eventlog.SetattrEvent:
			if e.Path == path {
				results = append(results, fileInspectEvent{
					Type: eventlog.TypeSetattr, StorageID: e.StorageID,
					Path: e.Path, TS: time.Unix(e.TS, 0),
				})
			}
		}
	}

	return results, nil
}

// findPendingEvents scans pending events from the prune offset and returns those
// matching the given path. Caps at maxScan events to avoid slowness.
func findPendingEvents(mountName string, path string) ([]fileInspectEvent, error) {
	offset, err := eventlog.ReadOffset(mountName)
	if err != nil {
		return nil, fmt.Errorf("read offset: %w", err)
	}

	reader, err := eventlog.OpenReader(mountName, offset)
	if err != nil {
		return nil, fmt.Errorf("open reader: %w", err)
	}
	defer func() { _ = reader.Close() }()

	return findPendingEventsFromReader(reader, path)
}

// printFileInspect renders the file inspect report to w.
func printFileInspect(w io.Writer, r fileInspectReport) {
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
func printFileInspectStorage(w io.Writer, s fileInspectStorage) {
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
func printMetaOverrides(w io.Writer, s fileInspectStorage) {
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
func printFileInspectPending(w io.Writer, events []fileInspectEvent) {
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
