package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/hieutdo/policyfs/internal/config"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "mount":
		cmdMount(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: pfs mount --config <path>")
}

func cmdMount(args []string) {
	fsFlags := flag.NewFlagSet("mount", flag.ExitOnError)
	configPath := fsFlags.String("config", "", "path to mount config yaml")
	debug := fsFlags.Bool("debug", false, "enable fuse debug")
	if err := fsFlags.Parse(args); err != nil {
		log.Fatal(err)
	}
	if *configPath == "" {
		log.Fatal("--config is required")
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	if cfg.MountPoint == "" {
		log.Fatal("config: mountpoint is required")
	}

	source, err := cfg.FirstStoragePath()
	if err != nil {
		log.Fatal(err)
	}

	root, err := fs.NewLoopbackRoot(source)
	if err != nil {
		log.Fatal(err)
	}

	server, err := fs.Mount(cfg.MountPoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug:   *debug,
			Name:    "policyfs",
			Options: []string{"allow_other"},
		},
	})
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			log.Fatal("mount failed: permission denied (need SYS_ADMIN/privileged + /dev/fuse)")
		}
		log.Fatal(err)
	}
	server.Wait()
}
