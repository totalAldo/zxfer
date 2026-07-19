# Extract mutable-global references from POSIX shell source.
# Output: variable<TAB>module.
#
# Assignment targets are included deliberately: the architecture checker
# resolves ownership separately, and a cross-owner assignment must not escape
# the result-channel consumer inventory merely because it is also a write.

function is_shell_token_separator(char) {
	return (char == " " || char == "\t" ||
		char == ";" || char == "|" || char == "&" ||
		char == "(" || char == ")" || char == "<" || char == ">")
}

function emit(variable) {
	if (!seen[variable]++)
		printf "%s\t%s\n", variable, module
}

function read_global_name(line, start,    remaining, variable) {
	remaining = substr(line, start)
	if (!match(remaining, /^g_[A-Za-z0-9_]+/))
		return 0
	variable = substr(remaining, RSTART, RLENGTH)
	emit(variable)
	return RLENGTH
}

function scan_arithmetic_expansion(line, start,    i, char, next_char, consumed) {
	arithmetic_scan_end = length(line)
	for (i = start; i <= length(line); i++) {
		char = substr(line, i, 1)
		next_char = substr(line, i + 1, 1)
		if ((i == 1 || substr(line, i - 1, 1) !~ /[A-Za-z0-9_]/) &&
				substr(line, i, 2) == "g_") {
			consumed = read_global_name(line, i)
			if (consumed > 0) {
				i += consumed - 1
				continue
			}
		}
		if (char == "(") {
			arithmetic_depth++
			continue
		}
		if (char != ")")
			continue
		if (arithmetic_depth > 0) {
			arithmetic_depth--
			continue
		}
		if (next_char == ")") {
			arithmetic_active = 0
			arithmetic_scan_end = i + 1
			return
		}
	}
}

function scan_heredoc_expansions(line,    i, char, next_char, consumed, backslashes) {
	backslashes = 0
	for (i = 1; i <= length(line); i++) {
		char = substr(line, i, 1)
		if (arithmetic_active) {
			scan_arithmetic_expansion(line, i)
			i = arithmetic_scan_end
			backslashes = 0
			continue
		}
		if (char == "\\") {
			backslashes++
			continue
		}
		if (char != "$") {
			backslashes = 0
			continue
		}
		if (backslashes % 2 != 0) {
			backslashes = 0
			continue
		}
		backslashes = 0
		if (substr(line, i + 1, 2) == "((") {
			arithmetic_active = 1
			arithmetic_depth = 0
			scan_arithmetic_expansion(line, i + 3)
			i = arithmetic_scan_end
			continue
		}
		next_char = substr(line, i + 1, 1)
		if (next_char == "{") {
			consumed = read_global_name(line, i + 2)
			if (consumed > 0)
				i += consumed + 1
		} else {
			consumed = read_global_name(line, i + 1)
			if (consumed > 0)
				i += consumed
		}
	}
}

function scan_shell_code(line,    i, char, next_char, consumed, token_start) {
	scanned_shell_code = ""
	token_start = (quote == "")
	for (i = 1; i <= length(line); i++) {
		char = substr(line, i, 1)
		if (arithmetic_active) {
			scan_arithmetic_expansion(line, i)
			i = arithmetic_scan_end
			continue
		}
		if (quote == "\047") {
			if (char == "\047")
				quote = ""
			continue
		}
		if (quote == "\"") {
			if (char == "\\") {
				i++
				continue
			}
			if (char == "\"") {
				quote = ""
				continue
			}
			if (char != "$")
				continue
			if (substr(line, i + 1, 2) == "((") {
				arithmetic_active = 1
				arithmetic_depth = 0
				scan_arithmetic_expansion(line, i + 3)
				i = arithmetic_scan_end
				continue
			}
			next_char = substr(line, i + 1, 1)
			if (next_char == "{") {
				consumed = read_global_name(line, i + 2)
				if (consumed > 0)
					i += consumed + 1
			} else {
				consumed = read_global_name(line, i + 1)
				if (consumed > 0)
					i += consumed
			}
			continue
		}

		if (char == "#" && token_start)
			break
		if (char == "\\") {
			scanned_shell_code = scanned_shell_code char
			if (i < length(line))
				scanned_shell_code = scanned_shell_code substr(line, i + 1, 1)
			i++
			token_start = 0
			continue
		}
		if (char == "\047") {
			quote = "\047"
			token_start = 0
			continue
		}
		if (char == "\"") {
			quote = "\""
			token_start = 0
			continue
		}
		if (char == "$") {
			scanned_shell_code = scanned_shell_code char
			next_char = substr(line, i + 1, 1)
			if (next_char == "{") {
				consumed = read_global_name(line, i + 2)
				if (consumed > 0)
					i += consumed + 1
			} else {
				consumed = read_global_name(line, i + 1)
				if (consumed > 0)
					i += consumed
			}
			token_start = 0
			continue
		}
		if ((i == 1 || substr(line, i - 1, 1) !~ /[A-Za-z0-9_]/) &&
				substr(line, i, 2) == "g_") {
			consumed = read_global_name(line, i)
			if (consumed > 0) {
				i += consumed - 1
				token_start = 0
				continue
			}
		}
		scanned_shell_code = scanned_shell_code char
		token_start = is_shell_token_separator(char)
	}
}

function remember_heredoc(line, code,    fragment) {
	if (index(code, "<<") == 0)
		return
	if (!match(line, /<<-?[ \t]*(\\[A-Za-z_][A-Za-z0-9_]*|[\047"][A-Za-z_][A-Za-z0-9_]*[\047"]|[A-Za-z_][A-Za-z0-9_]*)/))
		return
	fragment = substr(line, RSTART, RLENGTH)
	heredoc_strip_tabs = (fragment ~ /^<<-/)
	heredoc_expands = (fragment !~ /[\047"]/ && fragment !~ /\\/)
	sub(/^<<-?[ \t]*/, "", fragment)
	gsub(/[\047"]/, "", fragment)
	gsub(/\\/, "", fragment)
	heredoc_delimiter = fragment
}

FNR == 1 {
	quote = ""
	heredoc_delimiter = ""
	heredoc_strip_tabs = 0
	heredoc_expands = 0
	arithmetic_active = 0
	arithmetic_depth = 0
}

{
	if (heredoc_delimiter != "") {
		candidate = $0
		if (heredoc_strip_tabs)
			sub(/^\t+/, "", candidate)
		if (candidate == heredoc_delimiter) {
			heredoc_delimiter = ""
			heredoc_strip_tabs = 0
			heredoc_expands = 0
			arithmetic_active = 0
			arithmetic_depth = 0
		} else if (heredoc_expands) {
			scan_heredoc_expansions(candidate)
		}
		next
	}

	scan_shell_code($0)
	if (quote == "")
		remember_heredoc($0, scanned_shell_code)
}
