package sybil

import "strings"

func BuildReplacements(fieldSeparator, replacements string) map[string]StrReplace {
	if replacements == "" {
		return nil
	}
	result := make(map[string]StrReplace)
	parts := strings.Split(replacements, fieldSeparator)
	for _, repl := range parts {
		tokens := strings.Split(repl, ":")
		if len(tokens) > 2 {
			col := tokens[0]
			pattern := tokens[1]
			replacement := tokens[2]
			result[col] = StrReplace{pattern, replacement}
		}
	}
	return result
}
