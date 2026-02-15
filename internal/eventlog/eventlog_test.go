package eventlog

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// TestAppend_shouldCreateLogWith0600 verifies Append creates the NDJSON log with restricted perms.
func TestAppend_shouldCreateLogWith0600(t *testing.T) {
	base := t.TempDir()
	t.Setenv(config.EnvStateDir, base)

	err := Append(context.Background(), "media", DeleteEvent{Type: TypeDelete, StorageID: "hdd1", Path: "a", IsDir: false, TS: 1})
	require.NoError(t, err)

	p := filepath.Join(base, "media", "events.ndjson")
	st, err := os.Stat(p)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(FileMode), st.Mode().Perm())
}

// TestReader_shouldIgnorePartialLastLine verifies partial trailing lines are not returned.
func TestReader_shouldIgnorePartialLastLine(t *testing.T) {
	base := t.TempDir()
	t.Setenv(config.EnvStateDir, base)

	p := filepath.Join(base, "media", "events.ndjson")
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	content := []byte("{\"type\":\"DELETE\",\"storage_id\":\"hdd1\",\"path\":\"a\",\"is_dir\":false,\"ts\":1}\n{\"type\":")
	require.NoError(t, os.WriteFile(p, content, FileMode))

	r, err := OpenReader("media", 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	line, off, err := r.Next()
	require.NoError(t, err)
	require.Greater(t, off, int64(0))
	require.Contains(t, string(line), "\"type\":\"DELETE\"")

	line, off2, err := r.Next()
	require.True(t, errors.Is(err, io.EOF))
	require.Nil(t, line)
	require.Equal(t, off, off2)
}

// TestParse_shouldRoundTrip verifies Parse returns the correct concrete event type.
func TestParse_shouldRoundTrip(t *testing.T) {
	line := []byte(`{"type":"RENAME","storage_id":"hdd1","old_path":"a","new_path":"b","ts":2}`)
	e, err := Parse(line)
	require.NoError(t, err)

	rn, ok := e.(RenameEvent)
	require.True(t, ok)
	require.Equal(t, TypeRename, rn.Type)
	require.Equal(t, "a", rn.OldPath)
	require.Equal(t, "b", rn.NewPath)
}

// TestOffset_shouldRoundTrip verifies offset read/write uses decimal integers and survives missing files.
func TestOffset_shouldRoundTrip(t *testing.T) {
	base := t.TempDir()
	t.Setenv(config.EnvStateDir, base)

	off, err := ReadOffset("media")
	require.NoError(t, err)
	require.Equal(t, int64(0), off)

	require.NoError(t, WriteOffset("media", 123))
	off, err = ReadOffset("media")
	require.NoError(t, err)
	require.Equal(t, int64(123), off)
}
