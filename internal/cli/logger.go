package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/rs/zerolog"
)

// effectiveLogFilePath resolves the log file path.
//
// Priority: flag > env > YAML.
func effectiveLogFilePath(cfg config.LogConfig, logFileFlag string) string {
	if strings.TrimSpace(logFileFlag) != "" {
		return strings.TrimSpace(logFileFlag)
	}
	if v := os.Getenv(config.EnvLogFile); strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	if strings.TrimSpace(cfg.File) != "" {
		return strings.TrimSpace(cfg.File)
	}
	return ""
}

// OpenLogFileError indicates the optional log file output could not be enabled.
type OpenLogFileError struct {
	Path  string
	Cause error
}

// Error formats the error message for users.
func (e *OpenLogFileError) Error() string {
	if e == nil {
		return "failed to open log file"
	}
	if strings.TrimSpace(e.Path) != "" {
		return fmt.Sprintf("failed to open log file: %s", e.Path)
	}
	return "failed to open log file"
}

// Unwrap returns the underlying failure.
func (e *OpenLogFileError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// NewLogger builds a CLI logger from config.
//
// Rules:
// - log.format=json: structured logs to stdout.
// - log.format=text: human-friendly logs to stderr.
// - If --log-file, PFS_LOG_FILE, or log.file is set, structured logs are also duplicated to that file.
func NewLogger(cfg config.LogConfig, logFileFlag string) (zerolog.Logger, func() error, error) {
	return newLogger(cfg, logFileFlag, os.Stdout, os.Stderr)
}

// NewJobLogger builds a logger for oneshot maintenance jobs.
//
// It always writes logs to stderr so stdout can be reserved for command output.
func NewJobLogger(cfg config.LogConfig, logFileFlag string) (zerolog.Logger, func() error, error) {
	return newLogger(cfg, logFileFlag, os.Stderr, os.Stderr)
}

// newLogger is the shared logger builder used by NewLogger and NewJobLogger.
func newLogger(cfg config.LogConfig, logFileFlag string, jsonOut io.Writer, textOut io.Writer) (zerolog.Logger, func() error, error) {
	level := zerolog.InfoLevel
	switch cfg.Level {
	case "debug":
		level = zerolog.DebugLevel
	case "info", "":
		// default
	case "warn":
		level = zerolog.WarnLevel
	case "error":
		level = zerolog.ErrorLevel
	case "off":
		level = zerolog.Disabled
	default:
		return zerolog.Logger{}, nil, fmt.Errorf("unsupported log level: %q", cfg.Level)
	}

	if level == zerolog.Disabled {
		log := zerolog.New(io.Discard).Level(zerolog.Disabled)
		return log, nil, nil
	}

	writers := []io.Writer{}
	var closer func() error

	// Primary stream output.
	switch cfg.Format {
	case "text":
		cw := zerolog.ConsoleWriter{Out: textOut, TimeFormat: "2006-01-02 15:04:05"}
		cw.FormatLevel = func(i any) string {
			v := strings.ToLower(fmt.Sprintf("%v", i))
			switch v {
			case "inf":
				return "[info]"
			case "dbg":
				return "[debug]"
			case "wrn":
				return "[warn]"
			case "err":
				return "[error]"
			case "ftl":
				return "[fatal]"
			case "pnk":
				return "[panic]"
			case "trc":
				return "[trace]"
			default:
				return v
			}
		}
		writers = append(writers, cw)
	case "json", "":
		// Default: json
		writers = append(writers, jsonOut)
	default:
		return zerolog.Logger{}, nil, fmt.Errorf("unsupported log format: %q", cfg.Format)
	}

	// Optional structured file output.
	logFilePath := effectiveLogFilePath(cfg, logFileFlag)
	if logFilePath != "" {
		dir := filepath.Dir(logFilePath)
		if dir != "." {
			if _, err := os.Stat(dir); err != nil {
				if os.IsNotExist(err) {
					return zerolog.Logger{}, nil, &OpenLogFileError{Path: logFilePath, Cause: fmt.Errorf("log dir missing: %s", dir)}
				}
				return zerolog.Logger{}, nil, &OpenLogFileError{Path: logFilePath, Cause: err}
			}
		}

		f, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return zerolog.Logger{}, nil, &OpenLogFileError{Path: logFilePath, Cause: err}
		}
		closer = f.Close
		writers = append(writers, f)
	}

	if len(writers) == 0 {
		return zerolog.Logger{}, nil, errors.New("failed to configure logging: no outputs enabled")
	}

	w := zerolog.MultiLevelWriter(writers...)
	log := zerolog.New(w).
		Level(level).
		With().
		Timestamp().
		Logger()
	return log, closer, nil
}
