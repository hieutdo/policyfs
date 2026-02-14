package fuse

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/router"
	"github.com/stretchr/testify/require"
)

// Test_openFirst verifies we resolve targets in order and return stable errno values.
func Test_openFirst(t *testing.T) {
	// This test uses real filesystem temp dirs because openFirst is a thin syscall wrapper.
	root1 := t.TempDir()
	root2 := t.TempDir()

	mountCfg := &config.MountConfig{
		MountPoint: "/mnt/unused",
		StoragePaths: []config.StoragePath{
			{ID: "ssd1", Path: root1, Indexed: false},
			{ID: "ssd2", Path: root2, Indexed: false},
		},
		RoutingRules: []config.RoutingRule{{
			Match:        "**",
			Targets:      []string{"ssd1", "ssd2"},
			ReadTargets:  []string{"ssd1", "ssd2"},
			WriteTargets: []string{"ssd1", "ssd2"},
			WritePolicy:  "first_found",
		}},
	}

	rt, err := router.New(mountCfg)
	require.NoError(t, err)

	t.Run("should open first existing read target", func(t *testing.T) {
		// Create the file only on the second target; the first should return ENOENT and be skipped.
		virtualPath := "dir/file.txt"
		physical2 := filepath.Join(root2, virtualPath)
		require.NoError(t, os.MkdirAll(filepath.Dir(physical2), 0o755))
		require.NoError(t, os.WriteFile(physical2, []byte("x"), 0o644))

		fh, _, errno := openFirst(context.Background(), rt, nil, virtualPath, syscall.O_RDONLY, false)
		require.Equal(t, syscall.Errno(0), errno)
		require.NotNil(t, fh)

		h := fh.(*FileHandle)
		require.Equal(t, "ssd2", h.storageID)
		require.Equal(t, filepath.Join(root2, virtualPath), h.physicalPath)
		require.Equal(t, syscall.Errno(0), h.Release(context.Background()))
	})

	t.Run("should return ENOENT when no read target has the path", func(t *testing.T) {
		virtualPath := "dir/missing.txt"
		fh, _, errno := openFirst(context.Background(), rt, nil, virtualPath, syscall.O_RDONLY, false)
		require.Nil(t, fh)
		require.Equal(t, syscall.ENOENT, errno)
	})

	t.Run("should return EROFS when write is only allowed on indexed targets", func(t *testing.T) {
		mountCfgIndexed := &config.MountConfig{
			MountPoint: "/mnt/unused",
			StoragePaths: []config.StoragePath{
				{ID: "ssd1", Path: root1, Indexed: true},
				{ID: "ssd2", Path: root2, Indexed: true},
			},
			RoutingRules: []config.RoutingRule{{
				Match:        "**",
				Targets:      []string{"ssd1", "ssd2"},
				ReadTargets:  []string{"ssd1", "ssd2"},
				WriteTargets: []string{"ssd1", "ssd2"},
				WritePolicy:  "first_found",
			}},
		}
		rtIndexed, err := router.New(mountCfgIndexed)
		require.NoError(t, err)

		virtualPath := "dir/anything.txt"
		fh, _, errno := openFirst(context.Background(), rtIndexed, nil, virtualPath, syscall.O_WRONLY, true)
		require.Nil(t, fh)
		require.Equal(t, syscall.EROFS, errno)
	})
}

// Test_firstExistingPhysical verifies we return the first physical path that exists in read order.
func Test_firstExistingPhysical(t *testing.T) {
	// This test uses real temp dirs to exercise os.Lstat behavior.
	root1 := t.TempDir()
	root2 := t.TempDir()

	mountCfg := &config.MountConfig{
		MountPoint: "/mnt/unused",
		StoragePaths: []config.StoragePath{
			{ID: "ssd1", Path: root1, Indexed: false},
			{ID: "ssd2", Path: root2, Indexed: false},
		},
		RoutingRules: []config.RoutingRule{{
			Match:       "**",
			Targets:     []string{"ssd1", "ssd2"},
			ReadTargets: []string{"ssd1", "ssd2"},
		}},
	}

	rt, err := router.New(mountCfg)
	require.NoError(t, err)

	t.Run("should return the first existing target", func(t *testing.T) {
		virtualPath := "dir/file.txt"
		physical2 := filepath.Join(root2, virtualPath)
		require.NoError(t, os.MkdirAll(filepath.Dir(physical2), 0o755))
		require.NoError(t, os.WriteFile(physical2, []byte("x"), 0o644))

		target, physical, errno := firstExistingPhysical(rt, virtualPath)
		require.Equal(t, syscall.Errno(0), errno)
		require.Equal(t, "ssd2", target.ID)
		require.Equal(t, physical2, physical)
	})

	t.Run("should return ENOENT when none exist", func(t *testing.T) {
		virtualPath := "dir/missing.txt"
		_, _, errno := firstExistingPhysical(rt, virtualPath)
		require.Equal(t, syscall.ENOENT, errno)
	})
}
