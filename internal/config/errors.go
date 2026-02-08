package config

import "errors"

var (
	// ErrConfigNil is returned when a config receiver is nil.
	ErrConfigNil = errors.New("config is nil")

	// ErrMountNameRequired is returned when a mount name is missing.
	ErrMountNameRequired = errors.New("mount name is required")

	// ErrLockFileRequired is returned when a lock file name is missing.
	ErrLockFileRequired = errors.New("lock file is required")

	// ErrDBNil is returned when a DB handle is nil.
	ErrDBNil = errors.New("db is nil")

	// ErrMountsRequired is returned when mounts are not configured.
	ErrMountsRequired = errors.New("mounts is required")

	// ErrMountNotFound is returned when a mount name does not exist in the config.
	ErrMountNotFound = errors.New("mount not found")

	// ErrMountConfigNil is returned when a mount config receiver is nil.
	ErrMountConfigNil = errors.New("mount config is nil")

	// ErrStoragePathsEmpty is returned when storage_paths is empty.
	ErrStoragePathsEmpty = errors.New("storage_paths must not be empty")

	// ErrRoutingRulesEmpty is returned when routing_rules is empty.
	ErrRoutingRulesEmpty = errors.New("routing_rules must not be empty")

	// ErrStoragePathIDRequired is returned when a storage path id is missing.
	ErrStoragePathIDRequired = errors.New("storage path id is required")

	// ErrStoragePathPathRequired is returned when a storage path path is missing.
	ErrStoragePathPathRequired = errors.New("storage path is required")

	// ErrStoragePath0Required is returned when the first storage path has an empty path.
	ErrStoragePath0Required = errors.New("storage_paths[0].path is required")
)
