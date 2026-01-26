package config

import (
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	MountPoint   string        `yaml:"mountpoint"`
	StoragePaths []StoragePath `yaml:"storage_paths"`
}

type StoragePath struct {
	ID        string  `yaml:"id"`
	Path      string  `yaml:"path"`
	Indexed   bool    `yaml:"indexed"`
	MinFreeGB float64 `yaml:"min_free_gb"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}

	return &cfg, nil
}

func (c *Config) FirstStoragePath() (string, error) {
	if c == nil {
		return "", errors.New("config is nil")
	}
	if len(c.StoragePaths) == 0 {
		return "", errors.New("config: storage_paths must not be empty")
	}
	if c.StoragePaths[0].Path == "" {
		return "", errors.New("config: storage_paths[0].path is required")
	}
	return c.StoragePaths[0].Path, nil
}
