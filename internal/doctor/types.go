package doctor

import (
	"time"

	"github.com/hieutdo/policyfs/internal/eventlog"
)

// CheckResult represents one pass/fail check in the doctor report.
type CheckResult struct {
	Name   string // human label, e.g. `Mount "media": mountpoint set`
	Pass   bool
	Detail string // extra info on failure (optional)
}

// StorageReport describes accessibility and free space for one storage path.
type StorageReport struct {
	ID         string
	Path       string
	Indexed    bool
	Accessible bool
	Error      string // non-empty when !Accessible
	FreeBytes  uint64
	TotalBytes uint64
	UsedPct    int // 0..100

	// Usage trigger thresholds, populated from the first mover job with trigger.type=usage.
	// Only set for storages that are sources of that usage-triggered job.
	ThresholdStartPct *int
	ThresholdStopPct  *int
}

// IndexStatsReport holds per-storage index stats from indexer_state.
type IndexStatsReport struct {
	StorageID      string
	LastCompleted  *time.Time
	LastDurationMS *int64
	FileCount      *int64
	TotalBytes     *int64
	StaleFiles     *int64
}

// PendingEvents describes deferred events awaiting prune.
type PendingEvents struct {
	Total  int
	ByType map[eventlog.Type]int
	Recent []EventSummary
}

// EventSummary is one recent deferred event for display.
type EventSummary struct {
	Type      eventlog.Type
	StorageID string
	Path      string
	OldPath   string // RENAME only
	NewPath   string // RENAME only
	TS        time.Time
}

// DiskAccessReport aggregates disk access stats from the log file.
type DiskAccessReport struct {
	LogPath      string
	LinesScanned int
	TopProcesses []DiskAccessRank
	TopStorages  []DiskAccessRank
}

// DiskAccessRank is a ranked entry in disk access analysis.
type DiskAccessRank struct {
	Label string
	Count int
}

// FileReport describes one file path with best-effort size.
type FileReport struct {
	Path      string
	Missing   bool
	SizeBytes *int64
	StatError string
}

// SystemdTimerReport describes one systemd timer unit instance.
type SystemdTimerReport struct {
	Unit          string
	UnitFileState string
	ActiveState   string
	OnCalendar    string
	Next          string
	Last          string
	Error         string
}

// SystemdTimersReport aggregates systemd timer state for one mount.
type SystemdTimersReport struct {
	Supported bool
	Error     string
	Timers    []SystemdTimerReport
	Redundant []string
}

// MountReport aggregates all doctor checks for one mount.
type MountReport struct {
	Name                 string
	ConfigValid          bool
	Daemon               CheckResult
	Mountpoint           CheckResult
	JobLock              CheckResult
	FusePermissionErrors CheckResult

	// PoolSizeBytes is the sum of TotalBytes across accessible storages.
	// Nil means pool size is unknown.
	PoolSizeBytes *uint64
	IndexDB       FileReport
	LogFile       FileReport
	SystemdTimers *SystemdTimersReport
	Storages      []StorageReport
	IndexStats    []IndexStatsReport
	PendingEvents *PendingEvents
	DiskAccess    *DiskAccessReport
}

// Report aggregates the full doctor output.
type Report struct {
	ConfigPath   string
	ConfigChecks []CheckResult
	Mounts       []MountReport
	Suggestions  []string
	IssueCount   int
}

// FileInspectReport holds the full file inspect result.
type FileInspectReport struct {
	Mount    string
	Path     string // normalized virtual path
	Storages []FileInspectStorage
	Pending  []FileInspectEvent
}

// FileInspectStorage describes a file's state on one storage.
type FileInspectStorage struct {
	StorageID    string
	PhysicalPath string
	Indexed      bool // from config (storage has indexed=true)

	// From index DB (zero values when InIndex is false).
	InIndex       bool
	IsDir         bool
	Size          *int64
	MTimeSec      *int64
	Mode          *uint32
	UID           *uint32
	GID           *uint32
	Deleted       *int // 0=live, 1=pending-delete, 2=stale
	LastSeenRunID *int64
	CurrentRunID  *int64
	LastCompleted *time.Time // last completed index run for this storage
	RealPath      string     // if != Path → pending physical rename
	RenamePending bool

	// file_meta overrides (nil = no override).
	MetaMTime *int64
	MetaMode  *uint32
	MetaUID   *uint32
	MetaGID   *uint32

	// On-disk stat.
	DiskStatSkipped bool
	DiskExists      *bool
	DiskSize        *int64
	DiskMTime       *int64
	DiskMode        *uint32
	DiskUID         *uint32
	DiskGID         *uint32
	DiskError       string
}

// FileInspectEvent describes one pending event relevant to this path.
type FileInspectEvent struct {
	Type      eventlog.Type
	StorageID string
	Path      string // DELETE/SETATTR
	OldPath   string // RENAME
	NewPath   string // RENAME
	TS        time.Time
}

// pendingEventReader is the minimal interface we need from eventlog.Reader.
// It exists to make findPendingEvents testable.
type pendingEventReader interface {
	Next() (line []byte, nextOffset int64, err error)
	Close() error
}
