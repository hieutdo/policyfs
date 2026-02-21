package cli

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/eventlog"
	"github.com/hieutdo/policyfs/internal/indexdb"
	"github.com/hieutdo/policyfs/internal/lock"
)

// checkResult represents one pass/fail check in the doctor report.
type checkResult struct {
	Name   string // human label, e.g. `Mount "media": mountpoint set`
	Pass   bool
	Detail string // extra info on failure (optional)
}

// storageReport describes accessibility and free space for one storage path.
type storageReport struct {
	ID         string
	Path       string
	Indexed    bool
	Accessible bool
	Error      string // non-empty when !Accessible
	FreeBytes  uint64
	TotalBytes uint64
	UsedPct    int // 0..100
}

// indexStatsReport holds per-storage index stats from indexer_state.
type indexStatsReport struct {
	StorageID      string
	LastCompleted  *time.Time
	LastDurationMS *int64
	FileCount      *int64
	TotalBytes     *int64
	StaleFiles     *int64
}

// pendingEvents describes deferred events awaiting prune.
type pendingEvents struct {
	Total  int
	ByType map[eventlog.Type]int
	Recent []eventSummary
}

// eventSummary is one recent deferred event for display.
type eventSummary struct {
	Type      eventlog.Type
	StorageID string
	Path      string
	OldPath   string // RENAME only
	NewPath   string // RENAME only
	TS        time.Time
}

// diskAccessReport aggregates disk access stats from the log file.
type diskAccessReport struct {
	LogPath      string
	LinesScanned int
	TopProcesses []diskAccessRank
	TopStorages  []diskAccessRank
}

// diskAccessRank is a ranked entry in disk access analysis.
type diskAccessRank struct {
	Label string
	Count int
}

// fileReport describes one file path with best-effort size.
type fileReport struct {
	Path      string
	Missing   bool
	SizeBytes *int64
	StatError string
}

// systemdTimerReport describes one systemd timer unit instance.
type systemdTimerReport struct {
	Unit          string
	UnitFileState string
	ActiveState   string
	OnCalendar    string
	Next          string
	Last          string
	Error         string
}

// systemdTimersReport aggregates systemd timer state for one mount.
type systemdTimersReport struct {
	Supported bool
	Error     string
	Timers    []systemdTimerReport
	Redundant []string
}

// mountReport aggregates all doctor checks for one mount.
type mountReport struct {
	Name          string
	ConfigValid   bool
	Daemon        checkResult
	Mountpoint    checkResult
	JobLock       checkResult
	IndexDB       fileReport
	LogFile       fileReport
	SystemdTimers *systemdTimersReport
	Storages      []storageReport
	IndexStats    []indexStatsReport
	PendingEvents *pendingEvents
	DiskAccess    *diskAccessReport
}

// doctorReport aggregates the full doctor output.
type doctorReport struct {
	ConfigPath   string
	ConfigChecks []checkResult
	Mounts       []mountReport
	Suggestions  []string
	IssueCount   int
}

// --- Config checks ---

// checkConfigLoaded returns a check for whether the config file loaded successfully.
func checkConfigLoaded(configPath string, err error) checkResult {
	if err != nil {
		return checkResult{Name: fmt.Sprintf("Config loaded: %s", configPath), Pass: false, Detail: rootCause(err).Error()}
	}
	return checkResult{Name: fmt.Sprintf("Config loaded: %s", configPath), Pass: true}
}

// configChecksForMount converts validateConfigAll errors into per-mount check results.
// When a mount has no validation errors, it emits ✓ checks for each aspect.
func configChecksForMount(mountName string, allErrs []error) []checkResult {
	mountErrs := make([]*mountConfigError, 0)
	for _, e := range allErrs {
		var me *mountConfigError
		if errors.As(e, &me) && me != nil && me.Mount == mountName {
			mountErrs = append(mountErrs, me)
		}
	}

	if len(mountErrs) == 0 {
		return []checkResult{
			{Name: fmt.Sprintf("Mount %q: config valid", mountName), Pass: true},
		}
	}

	checks := make([]checkResult, 0, len(mountErrs))
	for _, e := range mountErrs {
		checks = append(checks, checkResult{
			Name: fmt.Sprintf("Mount %q: %s", mountName, e.Msg),
			Pass: false,
		})
	}
	return checks
}

// --- Mount status checks ---

// checkDaemonLock probes whether the daemon lock is held.
func checkDaemonLock(mountName string) checkResult {
	busy, pid, err := lock.ProbeMountLock(mountName, config.DefaultDaemonLockFile)
	if err != nil {
		return checkResult{Name: "daemon", Pass: true, Detail: fmt.Sprintf("probe error: %s", err)}
	}
	if busy {
		detail := "running"
		if pid > 0 {
			uptime := getProcessUptime(pid)
			if uptime > 0 {
				detail = fmt.Sprintf("running (pid %d, uptime %s)", pid, humanizeDuration(uptime))
			} else {
				detail = fmt.Sprintf("running (pid %d)", pid)
			}
		}
		return checkResult{Name: "daemon", Pass: true, Detail: detail}
	}
	return checkResult{Name: "daemon", Pass: true, Detail: "not running"}
}

// getProcessUptime returns how long a process has been running.
// Linux only — reads /proc/<pid>/stat and /proc/uptime. Returns 0 on non-Linux or error.
func getProcessUptime(pid int) time.Duration {
	if runtime.GOOS != "linux" || pid <= 0 {
		return 0
	}

	// Read process starttime (field 22, 0-indexed 21) from /proc/<pid>/stat.
	statPath := fmt.Sprintf("/proc/%d/stat", pid)
	statBytes, err := os.ReadFile(statPath)
	if err != nil {
		return 0
	}

	// The comm field (field 2) may contain spaces/parens, so find the last ')' first.
	statStr := string(statBytes)
	closeParen := strings.LastIndex(statStr, ")")
	if closeParen < 0 || closeParen+2 >= len(statStr) {
		return 0
	}
	// Fields after ')' start at field 3 (state). Field 22 is index 19 relative to after ')'.
	fields := strings.Fields(statStr[closeParen+2:])
	if len(fields) < 20 {
		return 0
	}
	startTimeTicks, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil {
		return 0
	}

	// Read system uptime from /proc/uptime.
	uptimeBytes, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0
	}
	uptimeFields := strings.Fields(string(uptimeBytes))
	if len(uptimeFields) < 1 {
		return 0
	}
	systemUptime, err := strconv.ParseFloat(uptimeFields[0], 64)
	if err != nil {
		return 0
	}

	const clockTicksPerSec = 100
	processStartSec := float64(startTimeTicks) / float64(clockTicksPerSec)
	processUptime := systemUptime - processStartSec

	if processUptime < 0 {
		return 0
	}
	return time.Duration(processUptime * float64(time.Second))
}

// checkJobLock probes whether the job lock is held.
func checkJobLock(mountName string) checkResult {
	busy, pid, err := lock.ProbeMountLock(mountName, config.DefaultJobLockFile)
	if err != nil {
		return checkResult{Name: "job lock", Pass: true, Detail: fmt.Sprintf("probe error: %s", err)}
	}
	if busy {
		detail := "busy"
		if pid > 0 {
			detail = fmt.Sprintf("busy (pid %d)", pid)
		}
		return checkResult{Name: "job lock", Pass: true, Detail: detail}
	}
	return checkResult{Name: "job lock", Pass: true, Detail: "free"}
}

// checkMountpointAccessible checks if a mountpoint path exists and is a directory.
func checkMountpointAccessible(mountpoint string) checkResult {
	if mountpoint == "" {
		return checkResult{Name: "mountpoint", Pass: false, Detail: "not configured"}
	}
	fi, err := os.Stat(mountpoint)
	if err != nil {
		return checkResult{Name: "mountpoint", Pass: false, Detail: fmt.Sprintf("%s (not found)", mountpoint)}
	}
	if !fi.IsDir() {
		return checkResult{Name: "mountpoint", Pass: false, Detail: fmt.Sprintf("%s (not a directory)", mountpoint)}
	}
	return checkResult{Name: "mountpoint", Pass: true, Detail: fmt.Sprintf("%s (exists)", mountpoint)}
}

// checkFile returns best-effort existence + size information for a file.
func checkFile(path string) fileReport {
	r := fileReport{Path: path}
	if strings.TrimSpace(path) == "" {
		r.Missing = true
		return r
	}
	fi, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			r.Missing = true
			return r
		}
		r.StatError = err.Error()
		return r
	}
	sz := fi.Size()
	r.SizeBytes = &sz
	return r
}

// checkIndexDBFile returns existence + size for the mount index DB file.
func checkIndexDBFile(mountName string) fileReport {
	path := filepath.Join(config.MountStateDir(mountName), "index.db")
	return checkFile(path)
}

// checkLogFile returns existence + size for the PolicyFS log file.
func checkLogFile(path string) fileReport {
	return checkFile(path)
}

// resolveDoctorLogFilePath resolves the log file path doctor should use for a given mount.
//
// It prefers systemd overrides (daemon runtime), then falls back to config/env/default.
func resolveDoctorLogFilePath(mountName string, cfg config.LogConfig) string {
	if p, err := querySystemdDaemonLogFilePath(mountName); err == nil {
		p = strings.TrimSpace(p)
		if p != "" {
			return p
		}
	}

	// Fallback order is intentionally simple: env, YAML, default.
	if v := os.Getenv(config.EnvLogFile); strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	if strings.TrimSpace(cfg.File) != "" {
		return strings.TrimSpace(cfg.File)
	}
	return config.DefaultLogFile
}

// querySystemdDaemonLogFilePath returns the best-effort daemon log file path as configured for the systemd service.
//
// It checks (in order): Environment (PFS_LOG_FILE), then ExecStart (--log-file).
func querySystemdDaemonLogFilePath(mountName string) (string, error) {
	if runtime.GOOS != "linux" {
		return "", nil
	}
	if _, err := exec.LookPath("systemctl"); err != nil {
		return "", fmt.Errorf("systemctl not found: %w", err)
	}

	unit := fmt.Sprintf("pfs@%s.service", mountName)
	out, err := exec.Command(
		"systemctl",
		"show",
		unit,
		"--no-pager",
		"-p",
		"Environment",
		"-p",
		"ExecStart",
	).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("systemctl show %s: %w", unit, err)
	}

	props := parseSystemctlShow(out)
	if v := parseSystemdEnvironmentValue(props["Environment"], config.EnvLogFile); strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v), nil
	}
	if v := parseSystemdExecStartFlagValue(props["ExecStart"], "--log-file"); strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v), nil
	}

	return "", nil
}

// parseSystemdEnvironmentValue extracts one variable value from the systemd Environment property.
func parseSystemdEnvironmentValue(env string, key string) string {
	needle := key + "="
	i := strings.Index(env, needle)
	if i < 0 {
		return ""
	}
	j := i + len(needle)
	if j >= len(env) {
		return ""
	}
	return parseSystemdValueToken(env[j:])
}

// parseSystemdExecStartFlagValue extracts a flag value from the systemd ExecStart property.
func parseSystemdExecStartFlagValue(execStart string, flag string) string {
	i := strings.Index(execStart, flag)
	if i < 0 {
		return ""
	}
	j := i + len(flag)
	if j >= len(execStart) {
		return ""
	}
	if execStart[j] == '=' {
		return parseSystemdValueToken(execStart[j+1:])
	}
	if isSpaceByte(execStart[j]) {
		k := j
		for k < len(execStart) && isSpaceByte(execStart[k]) {
			k++
		}
		if k >= len(execStart) {
			return ""
		}
		return parseSystemdValueToken(execStart[k:])
	}
	return ""
}

// parseSystemdValueToken parses a single systemd token value (quoted or unquoted).
func parseSystemdValueToken(s string) string {
	s = strings.TrimLeftFunc(s, func(r rune) bool { return r == ' ' || r == '\t' })
	if s == "" {
		return ""
	}
	q := s[0]
	if q == '\'' || q == '"' {
		// Quoted token.
		end := strings.IndexByte(s[1:], q)
		if end < 0 {
			return ""
		}
		return s[1 : 1+end]
	}
	// Unquoted token.
	end := 0
	for end < len(s) {
		if isSpaceByte(s[end]) || s[end] == ';' || s[end] == '}' {
			break
		}
		end++
	}
	if end == 0 {
		return ""
	}
	return s[:end]
}

// isSpaceByte reports whether b is an ASCII whitespace.
func isSpaceByte(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\r':
		return true
	default:
		return false
	}
}

// querySystemdTimers returns a best-effort systemd timer report for the mount.
func querySystemdTimers(mountName string) *systemdTimersReport {
	if runtime.GOOS != "linux" {
		return nil
	}
	if _, err := exec.LookPath("systemctl"); err != nil {
		return &systemdTimersReport{Supported: false, Error: "systemctl not found"}
	}

	units := []string{
		fmt.Sprintf("pfs-maint@%s.timer", mountName),
		fmt.Sprintf("pfs-move@%s.timer", mountName),
		fmt.Sprintf("pfs-index@%s.timer", mountName),
		fmt.Sprintf("pfs-prune@%s.timer", mountName),
	}

	timers := make([]systemdTimerReport, 0, len(units))
	for _, u := range units {
		timers = append(timers, querySystemdTimer(u))
	}

	redundant := redundantSystemdTimers(timers)
	return &systemdTimersReport{
		Supported: true,
		Timers:    timers,
		Redundant: redundant,
	}
}

// querySystemdTimer reads systemd timer properties via `systemctl show`.
func querySystemdTimer(unit string) systemdTimerReport {
	r := systemdTimerReport{Unit: unit}
	out, err := exec.Command("systemctl",
		"show",
		unit,
		"--no-pager",
		"-p",
		"UnitFileState",
		"-p",
		"ActiveState",
		"-p",
		"OnCalendar",
		"-p",
		"NextElapseUSecRealtime",
		"-p",
		"LastTriggerUSecRealtime",
	).CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg == "" {
			msg = err.Error()
		}
		r.Error = msg
		return r
	}

	props := parseSystemctlShow(out)
	r.UnitFileState = props["UnitFileState"]
	r.ActiveState = props["ActiveState"]
	r.OnCalendar = props["OnCalendar"]
	r.Next = props["NextElapseUSecRealtime"]
	r.Last = props["LastTriggerUSecRealtime"]
	return r
}

// parseSystemctlShow parses `systemctl show` output (key=value lines).
func parseSystemctlShow(out []byte) map[string]string {
	m := map[string]string{}
	for line := range strings.SplitSeq(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		m[k] = v
	}
	return m
}

// redundantSystemdTimers returns the list of redundant timer units enabled alongside an active maint timer.
func redundantSystemdTimers(timers []systemdTimerReport) []string {
	maintActive := false
	enabled := map[string]bool{}
	for _, t := range timers {
		if strings.HasPrefix(t.Unit, "pfs-maint@") && strings.HasSuffix(t.Unit, ".timer") {
			if strings.ToLower(t.ActiveState) == "active" {
				maintActive = true
			}
		}
		if !strings.HasPrefix(t.UnitFileState, "enabled") {
			continue
		}
		enabled[t.Unit] = true
	}
	if !maintActive {
		return nil
	}

	var redundant []string
	for _, prefix := range []string{"pfs-move@", "pfs-index@", "pfs-prune@"} {
		for unit := range enabled {
			if strings.HasPrefix(unit, prefix) && strings.HasSuffix(unit, ".timer") {
				redundant = append(redundant, unit)
			}
		}
	}
	sort.Strings(redundant)
	return redundant
}

// --- Storage checks ---

// checkStorage checks accessibility and free space for one storage path.
func checkStorage(sp config.StoragePath) storageReport {
	r := storageReport{
		ID:      sp.ID,
		Path:    sp.Path,
		Indexed: sp.Indexed,
	}

	fi, err := os.Stat(sp.Path)
	if err != nil {
		r.Accessible = false
		r.Error = "not found"
		return r
	}
	if !fi.IsDir() {
		r.Accessible = false
		r.Error = "not a directory"
		return r
	}

	r.Accessible = true

	var stat syscall.Statfs_t
	if err := syscall.Statfs(sp.Path, &stat); err == nil {
		r.TotalBytes = stat.Blocks * uint64(stat.Bsize)
		r.FreeBytes = stat.Bavail * uint64(stat.Bsize)
		if r.TotalBytes > 0 {
			used := r.TotalBytes - r.FreeBytes
			r.UsedPct = int(used * 100 / r.TotalBytes)
		}
	}

	return r
}

// --- Index stats ---

// queryIndexStats queries indexer_state and stale count for a storage.
func queryIndexStats(mountName string, storageID string) *indexStatsReport {
	row, err := indexdb.QueryIndexerState(mountName, storageID)
	if err != nil || row == nil {
		return nil
	}

	r := &indexStatsReport{StorageID: storageID}
	if row.LastCompleted != nil {
		t := time.Unix(*row.LastCompleted, 0)
		r.LastCompleted = &t
	}
	r.LastDurationMS = row.LastDurationMS
	r.FileCount = row.FileCount
	r.TotalBytes = row.TotalBytes

	if stale, err := indexdb.QueryStaleCount(mountName, storageID); err == nil && stale > 0 {
		r.StaleFiles = &stale
	}
	return r
}

// --- Pending events ---

// countPendingEvents reads deferred events from the offset and counts them.
// It caps at maxEvents to avoid slow doctor on huge logs.
func countPendingEvents(mountName string, maxEvents int) (*pendingEvents, error) {
	offset, err := eventlog.ReadOffset(mountName)
	if err != nil {
		return nil, fmt.Errorf("read offset: %w", err)
	}

	reader, err := eventlog.OpenReader(mountName, offset)
	if err != nil {
		return nil, fmt.Errorf("open reader: %w", err)
	}
	defer func() { _ = reader.Close() }()

	result := &pendingEvents{
		ByType: map[eventlog.Type]int{},
	}

	maxRecent := 20
	count := 0
	for count < maxEvents {
		line, _, err := reader.Next()
		if err != nil {
			break // EOF or error
		}

		evt, err := eventlog.Parse(line)
		if err != nil {
			continue
		}

		count++
		typ := evt.EventType()
		result.ByType[typ]++

		// Keep last N events (overwrite ring-style by appending and trimming later).
		summary := eventSummary{Type: typ}
		switch e := evt.(type) {
		case eventlog.DeleteEvent:
			summary.StorageID = e.StorageID
			summary.Path = e.Path
			summary.TS = time.Unix(e.TS, 0)
		case eventlog.RenameEvent:
			summary.StorageID = e.StorageID
			summary.OldPath = e.OldPath
			summary.NewPath = e.NewPath
			summary.TS = time.Unix(e.TS, 0)
		case eventlog.SetattrEvent:
			summary.StorageID = e.StorageID
			summary.Path = e.Path
			summary.TS = time.Unix(e.TS, 0)
		}
		result.Recent = append(result.Recent, summary)
	}

	result.Total = count

	// Keep only the last maxRecent entries.
	if len(result.Recent) > maxRecent {
		result.Recent = result.Recent[len(result.Recent)-maxRecent:]
	}

	if result.Total == 0 {
		return nil, nil
	}

	return result, nil
}

// --- Disk access analysis ---

// analyzeDiskAccess parses the last tailLines lines from the log file for disk_access entries.
func analyzeDiskAccess(logPath string, tailLines int) (*diskAccessReport, error) {
	if logPath == "" {
		return nil, nil
	}

	f, err := os.Open(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open log: %w", err)
	}
	defer func() { _ = f.Close() }()

	lines := tailFile(f, tailLines)

	type procKey struct {
		pid  int
		name string
	}
	procCounts := map[procKey]int{}
	storageCounts := map[string]int{}
	scanned := 0

	for _, line := range lines {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		msg, _ := entry["message"].(string)
		if msg == "" {
			msg, _ = entry["msg"].(string)
		}
		if msg != "disk_access" {
			continue
		}
		scanned++

		sid, _ := entry["storage_id"].(string)
		pid := 0
		if v, ok := entry["caller_pid"].(float64); ok {
			pid = int(v)
		}
		name, _ := entry["caller_name"].(string)

		if pid > 0 || name != "" {
			procCounts[procKey{pid: pid, name: name}]++
		}
		if sid != "" {
			storageCounts[sid]++
		}
	}

	if scanned == 0 {
		return nil, nil
	}

	report := &diskAccessReport{
		LogPath:      logPath,
		LinesScanned: len(lines),
	}

	// Top processes.
	type ranked struct {
		label string
		count int
	}
	var procs []ranked
	for k, c := range procCounts {
		label := k.name
		if label == "" {
			label = "unknown"
		}
		if k.pid > 0 {
			label = fmt.Sprintf("%s (pid %d)", label, k.pid)
		}
		procs = append(procs, ranked{label: label, count: c})
	}
	sort.Slice(procs, func(i, j int) bool { return procs[i].count > procs[j].count })
	for i, p := range procs {
		if i >= 5 {
			break
		}
		report.TopProcesses = append(report.TopProcesses, diskAccessRank{Label: p.label, Count: p.count})
	}

	// Top storages.
	var storages []ranked
	for sid, c := range storageCounts {
		storages = append(storages, ranked{label: sid, count: c})
	}
	sort.Slice(storages, func(i, j int) bool { return storages[i].count > storages[j].count })
	for i, s := range storages {
		if i >= 5 {
			break
		}
		report.TopStorages = append(report.TopStorages, diskAccessRank{Label: s.label, Count: s.count})
	}

	return report, nil
}

// tailFile reads the last n lines from a file. It reads from the end.
func tailFile(r io.ReadSeeker, n int) []string {
	// Seek to end and read backwards in chunks.
	end, err := r.Seek(0, io.SeekEnd)
	if err != nil || end == 0 {
		return nil
	}

	const chunkSize = 8192
	var buf []byte
	pos := end

	for pos > 0 {
		readSize := min(int64(chunkSize), pos)
		pos -= readSize
		if _, err := r.Seek(pos, io.SeekStart); err != nil {
			break
		}
		chunk := make([]byte, readSize)
		nr, err := r.Read(chunk)
		if err != nil && nr == 0 {
			break
		}
		buf = append(chunk[:nr], buf...)

		// Count newlines — if we have enough, stop.
		nlCount := 0
		for _, b := range buf {
			if b == '\n' {
				nlCount++
			}
		}
		if nlCount > n+1 {
			break
		}
	}

	scanner := bufio.NewScanner(strings.NewReader(string(buf)))
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return lines
}

// --- Suggestions ---

// generateSuggestions produces actionable suggestions from the doctor report.
func generateSuggestions(report *doctorReport) []string {
	var suggestions []string

	for _, m := range report.Mounts {
		if !m.ConfigValid {
			suggestions = append(suggestions, fmt.Sprintf("Mount %q: fix config errors above before running doctor again", m.Name))
			continue
		}
		if m.SystemdTimers != nil && len(m.SystemdTimers.Redundant) > 0 {
			timers := strings.Join(m.SystemdTimers.Redundant, " ")
			suggestions = append(suggestions, fmt.Sprintf("Mount %q: maint timer is active; disable redundant timers: systemctl disable --now %s", m.Name, timers))
		}
		for _, s := range m.Storages {
			if !s.Accessible {
				suggestions = append(suggestions, fmt.Sprintf("Mount %q: %s (%s) is not accessible — check disk/mount", m.Name, s.ID, s.Path))
			}
			if s.Accessible && s.UsedPct >= 90 {
				suggestions = append(suggestions, fmt.Sprintf("Mount %q: %s is %d%% full — consider freeing space or adding storage", m.Name, s.ID, s.UsedPct))
			}
		}
		for _, idx := range m.IndexStats {
			if idx.LastCompleted == nil {
				suggestions = append(suggestions, fmt.Sprintf("Mount %q: storage %q has never been indexed — run 'pfs index %s'", m.Name, idx.StorageID, m.Name))
			}
		}
	}

	return suggestions
}
