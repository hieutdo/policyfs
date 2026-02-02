package main

import (
	"os"
	"time"

	"github.com/hieutdo/policyfs/internal/cli"
	"github.com/rs/zerolog"
)

// setupLogger configures zerolog global settings once for the whole process.
func setupLogger() {
	loc := time.Local
	if tz := os.Getenv("TZ"); tz != "" {
		if l, err := time.LoadLocation(tz); err == nil {
			loc = l
		}
	}

	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.TimestampFunc = func() time.Time { return time.Now().In(loc) }
	zerolog.MessageFieldName = "msg"
}

// main is the pfs CLI entrypoint.
func main() {
	setupLogger()
	os.Exit(cli.Execute(os.Args[1:]))
}
