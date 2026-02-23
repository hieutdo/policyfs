package cli

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
)

// newCompletionCmd creates `pfs completion` with patched bash output.
//
// Cobra's generated bash script has several issues that break completion for
// paths containing spaces or shell metacharacters like ( ) [ ]:
//
//  1. COMP_WORDS splits at COMP_WORDBREAKS (including space) without respecting
//     backslash escaping, so "dir\ name" becomes two words.
//  2. The eval-based __complete call re-splits args at spaces.
//  3. compgen matching uses the wrong cur value for prefix filtering.
//  4. printf "%q" escaping doesn't interact correctly with readline's
//     word replacement when COMP_WORDBREAKS characters are in the path.
//
// We generate the script via cobra and apply targeted patches.
func newCompletionCmd(root *cobra.Command) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "completion [bash|zsh|fish|powershell]",
		Short: "Generate shell completion script",
		Long: `Generate a shell completion script for the specified shell.

  source <(pfs completion bash)     # bash
  pfs completion zsh > _pfs         # zsh
  pfs completion fish | source      # fish
  pfs completion powershell | Out-String | Invoke-Expression  # powershell`,
	}

	bashCmd := &cobra.Command{
		Use:   "bash",
		Short: "Generate bash completion script",
		Long: `Generate a bash completion script.
To use, add to your ~/.bashrc:

  source <(pfs completion bash)`,
		Args:               cobra.NoArgs,
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return genPatchedBashCompletion(cmd.OutOrStdout(), root)
		},
	}

	zshCmd := &cobra.Command{
		Use:   "zsh",
		Short: "Generate zsh completion script",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return root.GenZshCompletion(cmd.OutOrStdout())
		},
	}

	fishCmd := &cobra.Command{
		Use:   "fish",
		Short: "Generate fish completion script",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return root.GenFishCompletion(cmd.OutOrStdout(), true)
		},
	}

	powershellCmd := &cobra.Command{
		Use:   "powershell",
		Short: "Generate powershell completion script",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return root.GenPowerShellCompletionWithDesc(cmd.OutOrStdout())
		},
	}

	cmd.AddCommand(bashCmd, zshCmd, fishCmd, powershellCmd)
	return cmd
}

// genPatchedBashCompletion generates cobra's bash completion and applies patches
// for proper handling of paths with spaces and shell metacharacters.
func genPatchedBashCompletion(w io.Writer, root *cobra.Command) error {
	var buf bytes.Buffer
	if err := root.GenBashCompletionV2(&buf, true); err != nil {
		return fmt.Errorf("failed to generate bash completion: %w", err)
	}

	script := patchBashCompletion(buf.String(), root.Name())
	_, err := io.WriteString(w, script)
	if err != nil {
		return fmt.Errorf("failed to write bash completion: %w", err)
	}
	return nil
}

// patchBashCompletion applies targeted fixes to cobra's generated bash script.
func patchBashCompletion(script string, name string) string {
	// Patch 1: Pass COMP_LINE/COMP_POINT via env vars AND quote args for eval.
	//
	// Bash's COMP_WORDS splits at COMP_WORDBREAKS characters (including space)
	// WITHOUT respecting backslash escaping. Paths like "dir\ name/file" get
	// split into multiple words, causing cobra to receive wrong arguments.
	//
	// We pass the raw COMP_LINE and COMP_POINT so the Go side can re-parse
	// the command line correctly. We also quote each arg with printf '%q' as
	// a belt-and-suspenders measure for the eval call.
	script = strings.Replace(script,
		`requestComp="${words[0]} __complete ${args[*]}"`,
		`# pfs: pass raw COMP_LINE so Go can re-parse args with proper escaping.
    local quotedArgs=""
    for arg in "${args[@]}"; do
        quotedArgs+=" $(printf '%q' "$arg")"
    done
    requestComp="_PFS_COMP_LINE='${COMP_LINE//\'/\'\\\'\'}' _PFS_COMP_POINT=${COMP_POINT} ${words[0]} __complete${quotedArgs}"`,
		1)

	// Patch 2: Exclude ( and ) from word-breaking in _init_completion.
	script = strings.ReplaceAll(script,
		`_init_completion -n =: || return`,
		`_init_completion -n "=:()" || return`)
	script = strings.ReplaceAll(script,
		`__`+name+`_init_completion -n =: || return`,
		`__`+name+`_init_completion -n "=:()" || return`)

	// Patch 3: Handle ( as special char for COMPREPLY prefix stripping.
	script = strings.Replace(script,
		`__`+name+`_handle_special_char "$cur" =`,
		`__`+name+`_handle_special_char "$cur" =
    __`+name+`_handle_special_char "$cur" "("`,
		1)

	// Patch 4: Use "complete -o filenames" so readline handles escaping of
	// special characters (spaces, parens, brackets) in completions. This
	// replaces cobra's manual printf "%q" escaping which doesn't interact
	// correctly with readline's word replacement logic.
	script = strings.Replace(script,
		`complete -o default -F __start_`+name+` `+name,
		`complete -o default -o filenames -F __start_`+name+` `+name,
		1)

	// Patch 5: Replace __handle_standard_completion_case to skip compgen.
	//
	// Go already filters completions by prefix. Cobra's compgen matching
	// fails when cur is wrong (split at COMP_WORDBREAKS without escaping).
	// With -o filenames, we return unescaped completions and let readline
	// handle the escaping and prefix matching.
	script = strings.Replace(script,
		`__`+name+`_handle_standard_completion_case() {
    local tab=$'\t'

    # If there are no completions, we don't need to do anything
    (( ${#completions[@]} == 0 )) && return 0

    # Short circuit to optimize if we don't have descriptions
    if [[ "${completions[*]}" != *$tab* ]]; then
        # First, escape the completions to handle special characters
        IFS=$'\n' read -ra completions -d '' < <(printf "%q\n" "${completions[@]}")
        # Only consider the completions that match what the user typed
        IFS=$'\n' read -ra COMPREPLY -d '' < <(IFS=$'\n'; compgen -W "${completions[*]}" -- "${cur}")

        # compgen looses the escaping so, if there is only a single completion, we need to
        # escape it again because it will be inserted on the command-line.  If there are multiple
        # completions, we don't want to escape them because they will be printed in a list
        # and we don't want to show escape characters in that list.
        if (( ${#COMPREPLY[@]} == 1 )); then
            COMPREPLY[0]=$(printf "%q" "${COMPREPLY[0]}")
        fi
        return 0
    fi`,
		`__`+name+`_handle_standard_completion_case() {
    local tab=$'\t'

    (( ${#completions[@]} == 0 )) && return 0

    # pfs: Go already filters completions. With complete -o filenames,
    # readline handles escaping. We just populate COMPREPLY directly.
    if [[ "${completions[*]}" != *$tab* ]]; then
        COMPREPLY=("${completions[@]}")
        return 0
    fi`,
		1)

	return script
}
