package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCompletion_Bash_shouldProducePatchedScript verifies `pfs completion bash` outputs a
// bash script with our patches applied.
func TestCompletion_Bash_shouldProducePatchedScript(t *testing.T) {
	code, stdout, stderr := runCLI(t, []string{"completion", "bash"})
	require.Equal(t, ExitOK, code)
	require.Empty(t, stderr)
	require.NotEmpty(t, stdout)

	// Patch 1: COMP_LINE env var + eval arg quoting.
	require.Contains(t, stdout, `_PFS_COMP_LINE=`, "should pass COMP_LINE env var")
	require.Contains(t, stdout, `printf '%q' "$arg"`, "should quote args with printf %%q")
	require.NotContains(t, stdout, `requestComp="${words[0]} __complete ${args[*]}"`,
		"should NOT contain unquoted args expansion")

	// Patch 2: ( excluded from word-breaking.
	require.Contains(t, stdout, `-n "=:()"`, "should exclude ( ) from word-breaking")
	require.NotContains(t, stdout, `-n =: ||`, "should NOT contain original -n =:")

	// Patch 3: handle_special_char for (.
	require.Contains(t, stdout, `__pfs_handle_special_char "$cur" "("`,
		"should handle ( as special char")

	// Patch 4: complete -o filenames.
	require.Contains(t, stdout, `-o filenames`, "should use -o filenames for readline escaping")

	// Patch 5: skip compgen in handle_standard_completion_case.
	require.Contains(t, stdout, `COMPREPLY=("${completions[@]}")`,
		"should populate COMPREPLY directly without compgen")
}

// TestPatchBashCompletion_shouldApplyAllPatches unit-tests the patch function directly.
func TestPatchBashCompletion_shouldApplyAllPatches(t *testing.T) {
	// Simulate cobra's generated script with the relevant patterns.
	// Use the exact formatting cobra produces (indentation matters for patches).
	script := "__pfs_get_completion_results() {\n" +
		"    args=(\"${words[@]:1}\")\n" +
		"    requestComp=\"${words[0]} __complete ${args[*]}\"\n" +
		"    out=$(eval \"${requestComp}\" 2>/dev/null)\n" +
		"}\n" +
		"__pfs_handle_standard_completion_case() {\n" +
		"    local tab=$'\\t'\n" +
		"\n" +
		"    # If there are no completions, we don't need to do anything\n" +
		"    (( ${#completions[@]} == 0 )) && return 0\n" +
		"\n" +
		"    # Short circuit to optimize if we don't have descriptions\n" +
		"    if [[ \"${completions[*]}\" != *$tab* ]]; then\n" +
		"        # First, escape the completions to handle special characters\n" +
		"        IFS=$'\\n' read -ra completions -d '' < <(printf \"%q\\n\" \"${completions[@]}\")\n" +
		"        # Only consider the completions that match what the user typed\n" +
		"        IFS=$'\\n' read -ra COMPREPLY -d '' < <(IFS=$'\\n'; compgen -W \"${completions[*]}\" -- \"${cur}\")\n" +
		"\n" +
		"        # compgen looses the escaping so, if there is only a single completion, we need to\n" +
		"        # escape it again because it will be inserted on the command-line.  If there are multiple\n" +
		"        # completions, we don't want to escape them because they will be printed in a list\n" +
		"        # and we don't want to show escape characters in that list.\n" +
		"        if (( ${#COMPREPLY[@]} == 1 )); then\n" +
		"            COMPREPLY[0]=$(printf \"%q\" \"${COMPREPLY[0]}\")\n" +
		"        fi\n" +
		"        return 0\n" +
		"    fi\n" +
		"}\n" +
		"__start_pfs()\n" +
		"{\n" +
		"    if declare -F _init_completion >/dev/null 2>&1; then\n" +
		"        _init_completion -n =: || return\n" +
		"    else\n" +
		"        __pfs_init_completion -n =: || return\n" +
		"    fi\n" +
		"    __pfs_handle_special_char \"$cur\" :\n" +
		"    __pfs_handle_special_char \"$cur\" =\n" +
		"}\n" +
		"complete -o default -F __start_pfs pfs\n"

	patched := patchBashCompletion(script, "pfs")

	// Patch 1: COMP_LINE env var + eval quoting.
	require.NotContains(t, patched, `${args[*]}`)
	require.Contains(t, patched, `_PFS_COMP_LINE=`)
	require.Contains(t, patched, `printf '%q' "$arg"`)

	// Patch 2: ( in -n list.
	require.NotContains(t, patched, `-n =: ||`)
	require.Contains(t, patched, `-n "=:()" ||`)

	// Patch 3: handle_special_char for (.
	require.Contains(t, patched, `__pfs_handle_special_char "$cur" "("`)

	// Patch 4: -o filenames.
	require.Contains(t, patched, `complete -o default -o filenames -F __start_pfs pfs`)

	// Patch 5: skip compgen.
	require.Contains(t, patched, `COMPREPLY=("${completions[@]}")`)
	require.NotContains(t, patched, `compgen -W`)
}
