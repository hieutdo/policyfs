package doctor

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
	"github.com/hieutdo/policyfs/internal/humanfmt"
	"github.com/hieutdo/policyfs/internal/indexdb"
	"github.com/hieutdo/policyfs/internal/lock"
)

// BuildReport builds the doctor report in text mode. It never fails-fast;
// callers decide how to render and which exit code to return.
func BuildReport(cfgPath string, rootCfg *config.RootConfig, loadErr error, filterMount *string) Report {
	report := Report{ConfigPath: cfgPath}

	// Config loaded check.
	if loadErr != nil {
		report.ConfigChecks = append(report.ConfigChecks, CheckResult{Name: fmt.Sprintf("Config loaded: %s", cfgPath), Pass: false, Detail: rootCause(loadErr).Error()})
		report.IssueCount = 1
		return report
	}
	if rootCfg == nil {
		report.ConfigChecks = append(report.ConfigChecks, CheckResult{Name: fmt.Sprintf("Config loaded: %s", cfgPath), Pass: false, Detail: "config is nil"})
		report.IssueCount = 1
		return report
	}
	report.ConfigChecks = append(report.ConfigChecks, CheckResult{Name: fmt.Sprintf("Config loaded: %s", cfgPath), Pass: true})

	validateErrs := ValidateConfigAll(rootCfg)

	mountNames := make([]string, 0, len(rootCfg.Mounts))
	for name := range rootCfg.Mounts {
		mountNames = append(mountNames, name)
	}
	sort.Strings(mountNames)

	// If a specific mount was requested, the CLI must have validated it exists.
	if filterMount != nil {
		mountNames = []string{*filterMount}
	}

	for _, name := range mountNames {
		checks := configChecksForMount(name, validateErrs)
		report.ConfigChecks = append(report.ConfigChecks, checks...)
	}
	for _, c := range report.ConfigChecks {
		if !c.Pass {
			report.IssueCount++
		}
	}

	mountValid := map[string]bool{}
	for _, name := range mountNames {
		mountValid[name] = true
	}
	for _, e := range validateErrs {
		var me *MountConfigError
		if errors.As(e, &me) && me != nil {
			if _, ok := mountValid[me.Mount]; ok {
				mountValid[me.Mount] = false
			}
		}
	}

	for _, name := range mountNames {
		m := MountReport{Name: name, ConfigValid: mountValid[name]}
		if !m.ConfigValid {
			report.Mounts = append(report.Mounts, m)
			continue
		}

		mountCfg := rootCfg.Mounts[name]
		logPath := resolveDoctorLogFilePath(name, rootCfg.Log)

		m.Daemon = checkDaemonLock(name)
		m.Mountpoint = checkMountpointAccessible(mountCfg.MountPoint)
		m.JobLock = checkJobLock(name)

		m.IndexDB = checkIndexDBFile(name)
		m.LogFile = checkLogFile(logPath)

		m.SystemdTimers = querySystemdTimers(name)

		for _, sp := range mountCfg.StoragePaths {
			m.Storages = append(m.Storages, checkStorage(sp))
		}
		m.PoolSizeBytes = computePoolSizeBytes(m.Storages)
		applyUsageThresholdsForFirstUsageJob(mountCfg, m.Storages)

		for _, sp := range mountCfg.StoragePaths {
			if !sp.Indexed {
				continue
			}
			if stats := queryIndexStats(name, sp.ID); stats != nil {
				m.IndexStats = append(m.IndexStats, *stats)
			}
		}

		if pe, err := countPendingEvents(name, 1000); err == nil && pe != nil {
			m.PendingEvents = pe
		}

		if da, err := analyzeDiskAccess(logPath, 1000); err == nil && da != nil {
			m.DiskAccess = da
		}

		report.Mounts = append(report.Mounts, m)
	}

	report.Suggestions = generateSuggestions(&report)
	return report
}

// rootCause unwraps err until it reaches the innermost error.
func rootCause(err error) error {
	if err == nil {
		return nil
	}
	for {
		u := errors.Unwrap(err)
		if u == nil {
			return err
		}
		err = u
	}
}

// configChecksForMount converts ValidateConfigAll errors into per-mount check results.
// When a mount has no validation errors, it emits ✓ checks for each aspect.
func configChecksForMount(mountName string, allErrs []error) []CheckResult {
	mountErrs := make([]*MountConfigError, 0)
	for _, e := range allErrs {
		var me *MountConfigError
		if errors.As(e, &me) && me != nil && me.Mount == mountName {
			mountErrs = append(mountErrs, me)
		}
	}

	if len(mountErrs) == 0 {
		return []CheckResult{{Name: fmt.Sprintf("Mount %q: config valid", mountName), Pass: true}}
	}

	checks := make([]CheckResult, 0, len(mountErrs))
	for _, e := range mountErrs {
		checks = append(checks, CheckResult{Name: fmt.Sprintf("Mount %q: %s", mountName, e.Msg), Pass: false})
	}
	return checks
}

// checkDaemonLock probes whether the daemon lock is held.
func checkDaemonLock(mountName string) CheckResult {
	busy, pid, err := lock.ProbeMountLock(mountName, config.DefaultDaemonLockFile)
	if err != nil {
		return CheckResult{Name: "daemon", Pass: true, Detail: fmt.Sprintf("probe error: %s", err)}
	}
	if busy {
		detail := "running"
		if pid > 0 {
			uptime := getProcessUptime(pid)
			if uptime > 0 {
				detail = fmt.Sprintf("running (pid %d, uptime %s)", pid, humanfmt.HumanizeDuration(uptime))
			} else {
				detail = fmt.Sprintf("running (pid %d)", pid)
			}
		}
		return CheckResult{Name: "daemon", Pass: true, Detail: detail}
	}
	return CheckResult{Name: "daemon", Pass: true, Detail: "not running"}
}

// getProcessUptime returns how long a process has been running.
// Linux only - reads /proc/<pid>/stat and /proc/uptime. Returns 0 on non-Linux or error.
func getProcessUptime(pid int) time.Duration {
	if runtime.GOOS != "linux" || pid <= 0 {
		return 0
	}

	statPath := fmt.Sprintf("/proc/%d/stat", pid)
	statBytes, err := os.ReadFile(statPath)
	if err != nil {
		return 0
	}

	statStr := string(statBytes)
	closeParen := strings.LastIndex(statStr, ")")
	if closeParen < 0 || closeParen+2 >= len(statStr) {
		return 0
	}
	fields := strings.Fields(statStr[closeParen+2:])
	if len(fields) < 20 {
		return 0
	}
	startTimeTicks, err := strconvParseInt(fields[19])
	if err != nil {
		return 0
	}

	uptimeBytes, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0
	}
	uptimeFields := strings.Fields(string(uptimeBytes))
	if len(uptimeFields) < 1 {
		return 0
	}
	systemUptime, err := strconvParseFloat(uptimeFields[0])
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

// strconvParseInt exists to keep report.go self-contained without pulling in fmt-heavy errors.
func strconvParseInt(s string) (int64, error) {
	// stdlib strconv is fine here; keep errors wrapped for stable callers.
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse int: %w", err)
	}
	return v, nil
}

// strconvParseFloat exists to keep report.go self-contained.
func strconvParseFloat(s string) (float64, error) {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse float: %w", err)
	}
	return v, nil
}

// checkJobLock probes whether the job lock is held.
func checkJobLock(mountName string) CheckResult {
	busy, pid, err := lock.ProbeMountLock(mountName, config.DefaultJobLockFile)
	if err != nil {
		return CheckResult{Name: "job lock", Pass: true, Detail: fmt.Sprintf("probe error: %s", err)}
	}
	if busy {
		detail := "busy"
		if pid > 0 {
			detail = fmt.Sprintf("busy (pid %d)", pid)
		}
		return CheckResult{Name: "job lock", Pass: true, Detail: detail}
	}
	return CheckResult{Name: "job lock", Pass: true, Detail: "free"}
}

// checkMountpointAccessible checks if a mountpoint path exists and is a directory.
func checkMountpointAccessible(mountpoint string) CheckResult {
	if mountpoint == "" {
		return CheckResult{Name: "mountpoint", Pass: false, Detail: "not configured"}
	}
	fi, err := os.Stat(mountpoint)
	if err != nil {
		return CheckResult{Name: "mountpoint", Pass: false, Detail: fmt.Sprintf("%s (not found)", mountpoint)}
	}
	if !fi.IsDir() {
		return CheckResult{Name: "mountpoint", Pass: false, Detail: fmt.Sprintf("%s (not a directory)", mountpoint)}
	}
	return CheckResult{Name: "mountpoint", Pass: true, Detail: fmt.Sprintf("%s (exists)", mountpoint)}
}

// checkFile returns best-effort existence + size information for a file.
func checkFile(path string) FileReport {
	r := FileReport{Path: path}
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
func checkIndexDBFile(mountName string) FileReport {
	path := filepath.Join(config.MountStateDir(mountName), "index.db")
	return checkFile(path)
}

// checkLogFile returns existence + size for the PolicyFS log file.
func checkLogFile(path string) FileReport {
	return checkFile(path)
}

// resolveDoctorLogFilePath resolves the log file path doctor should use for a given mount.
func resolveDoctorLogFilePath(mountName string, cfg config.LogConfig) string {
	if p, err := querySystemdDaemonLogFilePath(mountName); err == nil {
		p = strings.TrimSpace(p)
		if p != "" {
			return p
		}
	}
	if v := os.Getenv(config.EnvLogFile); strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	if strings.TrimSpace(cfg.File) != "" {
		return strings.TrimSpace(cfg.File)
	}
	return config.DefaultLogFile
}

// querySystemdDaemonLogFilePath returns the best-effort daemon log file path as configured for the systemd service.
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
		end := strings.IndexByte(s[1:], q)
		if end < 0 {
			return ""
		}
		return s[1 : 1+end]
	}
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
func querySystemdTimers(mountName string) *SystemdTimersReport {
	if runtime.GOOS != "linux" {
		return nil
	}
	if _, err := exec.LookPath("systemctl"); err != nil {
		return &SystemdTimersReport{Supported: false, Error: "systemctl not found"}
	}

	units := []string{
		fmt.Sprintf("pfs-maint@%s.timer", mountName),
		fmt.Sprintf("pfs-move@%s.timer", mountName),
		fmt.Sprintf("pfs-index@%s.timer", mountName),
		fmt.Sprintf("pfs-prune@%s.timer", mountName),
	}

	timers := make([]SystemdTimerReport, 0, len(units))
	for _, u := range units {
		timers = append(timers, querySystemdTimer(u))
	}

	redundant := redundantSystemdTimers(timers)
	return &SystemdTimersReport{Supported: true, Timers: timers, Redundant: redundant}
}

// querySystemdTimer reads systemd timer properties via `systemctl show`.
func querySystemdTimer(unit string) SystemdTimerReport {
	r := SystemdTimerReport{Unit: unit}
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
func redundantSystemdTimers(timers []SystemdTimerReport) []string {
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

// checkStorage checks accessibility and free space for one storage path.
func checkStorage(sp config.StoragePath) StorageReport {
	r := StorageReport{ID: sp.ID, Path: sp.Path, Indexed: sp.Indexed}
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

// computePoolSizeBytes returns the sum of TotalBytes across accessible storages.
// It returns nil when pool size is unknown.
func computePoolSizeBytes(storages []StorageReport) *uint64 {
	var sum uint64
	hasAccessible := false
	for _, s := range storages {
		if !s.Accessible {
			continue
		}
		hasAccessible = true
		if s.TotalBytes == 0 {
			return nil
		}
		sum += s.TotalBytes
	}
	if !hasAccessible {
		return nil
	}
	return &sum
}

// applyUsageThresholdsForFirstUsageJob annotates storage reports with threshold_start/threshold_stop
// from the first mover job where trigger.type=usage.
//
// Semantics:
//   - only applied when mover is enabled
//   - only for storages that are sources of that job (source.paths + expanded source.groups)
//   - only for storages that are accessible
func applyUsageThresholdsForFirstUsageJob(mountCfg config.MountConfig, storages []StorageReport) {
	enabled := true
	if mountCfg.Mover.Enabled != nil {
		enabled = *mountCfg.Mover.Enabled
	}
	if !enabled {
		return
	}

	var job *config.MoverJobConfig
	for i := range mountCfg.Mover.Jobs {
		j := &mountCfg.Mover.Jobs[i]
		if strings.TrimSpace(j.Trigger.Type) == "usage" {
			job = j
			break
		}
	}
	if job == nil {
		return
	}

	sourceIDs := map[string]struct{}{}
	for _, sid := range job.Source.Paths {
		sid = strings.TrimSpace(sid)
		if sid == "" {
			continue
		}
		sourceIDs[sid] = struct{}{}
	}
	for _, g := range job.Source.Groups {
		g = strings.TrimSpace(g)
		if g == "" {
			continue
		}
		for _, sid := range mountCfg.StorageGroups[g] {
			sid = strings.TrimSpace(sid)
			if sid == "" {
				continue
			}
			sourceIDs[sid] = struct{}{}
		}
	}
	if len(sourceIDs) == 0 {
		return
	}

	start := job.Trigger.ThresholdStart
	stop := job.Trigger.ThresholdStop
	for i := range storages {
		if !storages[i].Accessible {
			continue
		}
		if _, ok := sourceIDs[storages[i].ID]; !ok {
			continue
		}
		storages[i].ThresholdStartPct = &start
		storages[i].ThresholdStopPct = &stop
	}
}

// queryIndexStats queries indexer_state and stale count for a storage.
func queryIndexStats(mountName string, storageID string) *IndexStatsReport {
	row, err := indexdb.QueryIndexerState(mountName, storageID)
	if err != nil || row == nil {
		return nil
	}

	r := &IndexStatsReport{StorageID: storageID}
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

// countPendingEvents reads deferred events from the offset and counts them.
// It caps at maxEvents to avoid slow doctor on huge logs.
func countPendingEvents(mountName string, maxEvents int) (*PendingEvents, error) {
	offset, err := eventlog.ReadOffset(mountName)
	if err != nil {
		return nil, fmt.Errorf("read offset: %w", err)
	}

	reader, err := eventlog.OpenReader(mountName, offset)
	if err != nil {
		return nil, fmt.Errorf("open reader: %w", err)
	}
	defer func() { _ = reader.Close() }()

	result := &PendingEvents{ByType: map[eventlog.Type]int{}}
	maxRecent := 20
	count := 0
	for count < maxEvents {
		line, _, err := reader.Next()
		if err != nil {
			break
		}

		evt, err := eventlog.Parse(line)
		if err != nil {
			continue
		}

		count++
		typ := evt.EventType()
		result.ByType[typ]++

		summary := EventSummary{Type: typ}
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
	if len(result.Recent) > maxRecent {
		result.Recent = result.Recent[len(result.Recent)-maxRecent:]
	}
	if result.Total == 0 {
		return nil, nil
	}
	return result, nil
}

// analyzeDiskAccess parses the last tailLines lines from the log file for disk_access entries.
func analyzeDiskAccess(logPath string, tailLines int) (*DiskAccessReport, error) {
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

	report := &DiskAccessReport{LogPath: logPath, LinesScanned: len(lines)}

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
		report.TopProcesses = append(report.TopProcesses, DiskAccessRank{Label: p.label, Count: p.count})
	}

	var storages []ranked
	for sid, c := range storageCounts {
		storages = append(storages, ranked{label: sid, count: c})
	}
	sort.Slice(storages, func(i, j int) bool { return storages[i].count > storages[j].count })
	for i, s := range storages {
		if i >= 5 {
			break
		}
		report.TopStorages = append(report.TopStorages, DiskAccessRank{Label: s.label, Count: s.count})
	}

	return report, nil
}

// tailFile reads the last n lines from a file. It reads from the end.
func tailFile(r io.ReadSeeker, n int) []string {
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

// generateSuggestions produces actionable suggestions from the doctor report.
func generateSuggestions(report *Report) []string {
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
				suggestions = append(suggestions, fmt.Sprintf("Mount %q: %s (%s) is not accessible - check disk/mount", m.Name, s.ID, s.Path))
			}
			if s.Accessible && s.UsedPct >= 90 {
				suggestions = append(suggestions, fmt.Sprintf("Mount %q: %s is %d%% full - consider freeing space or adding storage", m.Name, s.ID, s.UsedPct))
			}
		}
		for _, idx := range m.IndexStats {
			if idx.LastCompleted == nil {
				suggestions = append(suggestions, fmt.Sprintf("Mount %q: storage %q has never been indexed - run 'pfs index %s'", m.Name, idx.StorageID, m.Name))
			}
		}
	}
	return suggestions
}
