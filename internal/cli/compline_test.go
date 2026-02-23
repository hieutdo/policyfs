package cli

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitShellWords(t *testing.T) {
	tests := []struct {
		name string
		line string
		want []string
	}{
		{"empty", "", nil},
		{"simple", "pfs doctor media", []string{"pfs", "doctor", "media"}},
		{"backslash space", `pfs doctor media library/movies/Aladdin\ (1992)/Alad`, []string{"pfs", "doctor", "media", "library/movies/Aladdin (1992)/Alad"}},
		{"backslash parens", `pfs doctor media dir\ with\ spaces/sub\(1\)/file`, []string{"pfs", "doctor", "media", "dir with spaces/sub(1)/file"}},
		{"single quotes", `pfs doctor media 'library/movies/Aladdin (1992)/Alad'`, []string{"pfs", "doctor", "media", "library/movies/Aladdin (1992)/Alad"}},
		{"double quotes", `pfs doctor media "library/movies/Aladdin (1992)/Alad"`, []string{"pfs", "doctor", "media", "library/movies/Aladdin (1992)/Alad"}},
		{"trailing space", "pfs doctor media ", []string{"pfs", "doctor", "media"}},
		{"multiple spaces", "pfs  doctor   media", []string{"pfs", "doctor", "media"}},
		{"brackets", `pfs doctor media library/movies/Movie\ \[Remux\].mkv`, []string{"pfs", "doctor", "media", "library/movies/Movie [Remux].mkv"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitShellWords(tt.line)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCompLineArgs(t *testing.T) {
	tests := []struct {
		name       string
		line       string
		point      int
		cmdName    string
		wantArgs   []string
		wantToComp string
		wantOK     bool
	}{
		{
			name:       "normal path",
			line:       "pfs doctor media lib",
			point:      20,
			cmdName:    "doctor",
			wantArgs:   []string{"media"},
			wantToComp: "lib",
			wantOK:     true,
		},
		{
			name:       "path with spaces",
			line:       `pfs doctor media library/movies/Aladdin\ (1992)/Alad`,
			point:      52,
			cmdName:    "doctor",
			wantArgs:   []string{"media"},
			wantToComp: "library/movies/Aladdin (1992)/Alad",
			wantOK:     true,
		},
		{
			name:       "trailing space means empty toComplete",
			line:       "pfs doctor media ",
			point:      17,
			cmdName:    "doctor",
			wantArgs:   []string{"media"},
			wantToComp: "",
			wantOK:     true,
		},
		{
			name:       "mount name completion",
			line:       "pfs doctor me",
			point:      13,
			cmdName:    "doctor",
			wantArgs:   []string{},
			wantToComp: "me",
			wantOK:     true,
		},
		{
			name:       "no env var",
			line:       "",
			point:      0,
			cmdName:    "doctor",
			wantArgs:   nil,
			wantToComp: "",
			wantOK:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.line != "" {
				t.Setenv("_PFS_COMP_LINE", tt.line)
				t.Setenv("_PFS_COMP_POINT", itoa(tt.point))
			}
			args, toComp, ok := compLineArgs(tt.cmdName)
			require.Equal(t, tt.wantOK, ok)
			if ok {
				require.Equal(t, tt.wantArgs, args)
				require.Equal(t, tt.wantToComp, toComp)
			}
		})
	}
}

func itoa(n int) string {
	return fmt.Sprintf("%d", n)
}
