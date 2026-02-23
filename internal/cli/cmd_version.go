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

// newVersionCmd creates `pfs version`.
func newVersionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print version and build information",
		Long: `Print version and build information for pfs.

Displays the semantic version, git commit, build date, Go version,
and platform information.`,
		Example: `  pfs version
  pfs version`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			version := Version
			commit := Commit
			buildTime := BuildDate
			goVersion := runtime.Version()

			fmt.Fprintf(cmd.OutOrStdout(), "pfs %s\n", version)
			fmt.Fprintf(cmd.OutOrStdout(), "  commit:     %s\n", commit)
			fmt.Fprintf(cmd.OutOrStdout(), "  built:      %s\n", buildTime)
			fmt.Fprintf(cmd.OutOrStdout(), "  go version: %s\n", goVersion)
			fmt.Fprintf(cmd.OutOrStdout(), "  platform:   %s/%s\n", runtime.GOOS, runtime.GOARCH)
			return nil
		},
	}

	return cmd
}
