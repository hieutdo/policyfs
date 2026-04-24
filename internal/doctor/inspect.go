package doctor

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/eventlog"
	"github.com/hieutdo/policyfs/internal/indexdb"
)

// InspectFile inspects a single virtual file/dir path across all storages.
func InspectFile(mountName string, filePath string, mountCfg config.MountConfig, statDisk bool) (*FileInspectReport, error) {
	normalizedPath := indexdb.NormalizeVirtualPath(filePath)
	if normalizedPath == "" {
		return nil, fmt.Errorf("path resolves to root (empty after normalization)")
	}

	report := &FileInspectReport{Mount: mountName, Path: normalizedPath}

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
		s := FileInspectStorage{StorageID: sp.ID, Indexed: sp.Indexed}

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
		if !statDisk && s.InIndex {
			s.DiskStatSkipped = true
		} else {
			s.statDisk()
		}

		report.Storages = append(report.Storages, s)
	}

	// Scan pending events for this path.
	report.Pending = []FileInspectEvent{}
	pending, err := findPendingEvents(mountName, normalizedPath)
	if err == nil && pending != nil {
		report.Pending = pending
	}

	return report, nil
}

// statDisk performs os.Stat on the physical path and populates disk fields.
func (s *FileInspectStorage) statDisk() {
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

// findPendingEventsFromReader scans pending events from reader and returns those
// matching the given path. Caps at maxScan events to avoid slowness.
func findPendingEventsFromReader(reader pendingEventReader, path string) ([]FileInspectEvent, error) {
	const maxScan = 10_000
	var results []FileInspectEvent
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
				results = append(results, FileInspectEvent{Type: eventlog.TypeDelete, StorageID: e.StorageID, Path: e.Path, TS: time.Unix(e.TS, 0)})
			}
		case eventlog.RenameEvent:
			if e.OldPath == path || e.NewPath == path {
				results = append(results, FileInspectEvent{Type: eventlog.TypeRename, StorageID: e.StorageID, OldPath: e.OldPath, NewPath: e.NewPath, TS: time.Unix(e.TS, 0)})
			}
		case eventlog.SetattrEvent:
			if e.Path == path {
				results = append(results, FileInspectEvent{Type: eventlog.TypeSetattr, StorageID: e.StorageID, Path: e.Path, TS: time.Unix(e.TS, 0)})
			}
		}
	}

	return results, nil
}

// findPendingEvents scans pending events from the prune offset and returns those
// matching the given path. Caps at maxScan events to avoid slowness.
func findPendingEvents(mountName string, path string) ([]FileInspectEvent, error) {
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
