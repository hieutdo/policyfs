package cli

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

// Build-time variables set via -ldflags.
var (
	// Version is the semantic version (e.g., "1.0.0").
	Version = "dev"
	// Commit is the git commit SHA.
	Commit = "unknown"
	// BuildDate is the build timestamp in RFC3339 format.
	BuildDate = "unknown"
)

// JSONVersionOutput is the JSON output for `pfs version --json`.
type JSONVersionOutput struct {
	Command   string `json:"command"`
	OK        bool   `json:"ok"`
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	GoVersion string `json:"go_version"`
	BuildTime string `json:"build_time"`
}

// newVersionCmd creates `pfs version`.
func newVersionCmd() *cobra.Command {
	var jsonOut bool

	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print version and build information",
		Long: `Print version and build information for pfs.

Displays the semantic version, git commit, build date, Go version,
and platform information.`,
		Example: `  pfs version
  pfs version --json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			version := Version
			commit := Commit
			buildTime := BuildDate
			goVersion := runtime.Version()

			if jsonOut {
				out := JSONVersionOutput{Command: "version", OK: true, Version: version, Commit: commit, GoVersion: goVersion, BuildTime: buildTime}
				if err := writeJSON(out); err != nil {
					return &CLIError{Code: ExitFail, Cmd: "version", Headline: "failed to write json", Cause: rootCause(err)}
				}
				return nil
			}

			fmt.Fprintf(cmd.OutOrStdout(), "pfs %s\n", version)
			fmt.Fprintf(cmd.OutOrStdout(), "  commit:     %s\n", commit)
			fmt.Fprintf(cmd.OutOrStdout(), "  built:      %s\n", buildTime)
			fmt.Fprintf(cmd.OutOrStdout(), "  go version: %s\n", goVersion)
			fmt.Fprintf(cmd.OutOrStdout(), "  platform:   %s/%s\n", runtime.GOOS, runtime.GOARCH)
			return nil
		},
	}

	cmd.Flags().BoolVarP(&jsonOut, "json", "j", false, "output as JSON")

	return cmd
}
