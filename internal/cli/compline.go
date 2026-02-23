package cli

import (
	"os"
	"strconv"
	"strings"
)

// compLineArgs re-parses the completion line from environment variables
// set by our patched bash completion script. This is necessary because
// bash's COMP_WORDS splits at COMP_WORDBREAKS characters (including space)
// WITHOUT respecting backslash escaping, so paths like "dir\ name/file"
// get split into multiple words.
//
// Returns (args, toComplete, true) if env vars are set and parsing succeeds,
// or (nil, "", false) otherwise. The returned args exclude the program name
// and the command name prefix that cobra already stripped.
func compLineArgs(cmdName string) (args []string, toComplete string, ok bool) {
	line := os.Getenv("_PFS_COMP_LINE")
	if line == "" {
		return nil, "", false
	}
	pointStr := os.Getenv("_PFS_COMP_POINT")
	point, err := strconv.Atoi(pointStr)
	if err != nil || point < 0 {
		return nil, "", false
	}
	if point > len(line) {
		point = len(line)
	}
	line = line[:point]

	words := splitShellWords(line)
	if len(words) == 0 {
		return nil, "", false
	}

	// Strip program name (words[0] = "pfs").
	words = words[1:]

	// Strip the command name(s). For "pfs doctor media path",
	// cobra calls ValidArgsFunction with args=["media"], toComplete="path".
	// We need to strip "doctor" (the cmdName) from the front.
	stripped := false
	for i, w := range words {
		if w == cmdName {
			words = words[i+1:]
			stripped = true
			break
		}
	}
	if !stripped {
		return nil, "", false
	}

	// The last word is toComplete (what the user is currently typing).
	// Everything before it is args.
	// Exception: if the line ends with a space, toComplete is "" and all words are args.
	trimmed := strings.TrimRight(line, " \t")
	endsWithSpace := len(trimmed) < point

	if endsWithSpace {
		return words, "", true
	}
	if len(words) == 0 {
		return nil, "", true
	}
	return words[:len(words)-1], words[len(words)-1], true
}

// splitShellWords splits a command line string into words, respecting
// backslash escaping and single/double quoting. This mirrors how bash
// interprets the command line (minus variable/glob expansion).
func splitShellWords(line string) []string {
	var words []string
	var cur strings.Builder
	inWord := false

	i := 0
	for i < len(line) {
		ch := line[i]
		switch {
		case ch == '\\' && i+1 < len(line):
			// Backslash escapes the next character.
			inWord = true
			cur.WriteByte(line[i+1])
			i += 2

		case ch == '\'':
			// Single quote: everything until closing quote is literal.
			inWord = true
			i++
			for i < len(line) && line[i] != '\'' {
				cur.WriteByte(line[i])
				i++
			}
			if i < len(line) {
				i++ // skip closing quote
			}

		case ch == '"':
			// Double quote: backslash escaping works inside.
			inWord = true
			i++
			for i < len(line) && line[i] != '"' {
				if line[i] == '\\' && i+1 < len(line) {
					cur.WriteByte(line[i+1])
					i += 2
				} else {
					cur.WriteByte(line[i])
					i++
				}
			}
			if i < len(line) {
				i++ // skip closing quote
			}

		case ch == ' ' || ch == '\t':
			if inWord {
				words = append(words, cur.String())
				cur.Reset()
				inWord = false
			}
			i++

		default:
			inWord = true
			cur.WriteByte(ch)
			i++
		}
	}
	if inWord {
		words = append(words, cur.String())
	}
	return words
}
