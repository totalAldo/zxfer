# Extract direct mutable-global writes from POSIX shell source.
# Output: variable<TAB>module<TAB>line<TAB>write-kind.

function is_shell_token_separator(char) {
	return (char == " " || char == "\t" ||
		char == ";" || char == "|" || char == "&" ||
		char == "(" || char == ")" || char == "<" || char == ">")
}

function emit_parameter_default_assignment(text, start,    remaining, variable, operator) {
	if (substr(text, start, 2) != "${")
		return
	remaining = substr(text, start + 2)
	if (!match(remaining, /^g_[A-Za-z0-9_]+/))
		return
	variable = substr(remaining, RSTART, RLENGTH)
	operator = substr(remaining, RLENGTH + 1, 2)
	if (operator == ":=" || substr(operator, 1, 1) == "=")
		emit(variable, "default-assignment")
}

function shell_code(line,    output, i, char, token_start) {
	output = ""
	token_start = (quote == "")
	for (i = 1; i <= length(line); i++) {
		char = substr(line, i, 1)
		if (quote == "") {
			if (char == "#" && token_start)
				break
			if (char == "\\") {
				output = output char
				if (i < length(line)) {
					i++
					output = output substr(line, i, 1)
				}
				token_start = 0
				continue
			}
			if (char == "\"") {
				quote = "\""
				token_start = 0
				continue
			}
			if (char == "\047") {
				quote = "\047"
				token_start = 0
				continue
			}
			output = output char
			token_start = is_shell_token_separator(char)
			continue
		}
		if (quote == "\047") {
			if (char == "\047")
				quote = ""
			continue
		}
		if (char == "\\") {
			i++
			continue
		}
		if (char == "$")
			emit_parameter_default_assignment(line, i)
		if (char == "\"")
			quote = ""
	}
	return output
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

function scan_expanding_heredoc(line,    i, char, backslashes) {
	backslashes = 0
	for (i = 1; i <= length(line); i++) {
		char = substr(line, i, 1)
		if (char == "\\") {
			backslashes++
			continue
		}
		if (char == "$" && backslashes % 2 == 0)
			emit_parameter_default_assignment(line, i)
		backslashes = 0
	}
}

function emit(variable, kind) {
	key = variable SUBSEP FNR SUBSEP kind
	if (!seen[key]++)
		printf "%s\t%s\t%d\t%s\n", variable, module, FNR, kind
}

function emit_assignment_matches(text, pattern, kind,    matched, variable) {
	while (match(text, pattern)) {
		matched = substr(text, RSTART, RLENGTH)
		sub(/^[^A-Za-z0-9_]/, "", matched)
		variable = matched
		sub(/[:+]?=.*/, "", variable)
		emit(variable, kind)
		text = substr(text, RSTART + RLENGTH)
	}
}

function emit_named_tokens(text, kind,    count, token, i) {
	gsub(/[^A-Za-z0-9_]/, " ", text)
	count = split(text, token, / +/)
	for (i = 1; i <= count; i++) {
		if (token[i] ~ /^g_[A-Za-z0-9_]+$/)
			emit(token[i], kind)
	}
}

FNR == 1 {
	quote = ""
	heredoc_delimiter = ""
	heredoc_strip_tabs = 0
	heredoc_expands = 0
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
		} else if (heredoc_expands) {
			scan_expanding_heredoc(candidate)
		}
		next
	}

	line = shell_code($0)
	remember_heredoc($0, line)
	emit_assignment_matches(line, "(^|[^A-Za-z0-9_])g_[A-Za-z0-9_]+=", "assignment")
	emit_assignment_matches(line, "(^|[^A-Za-z0-9_])g_[A-Za-z0-9_]+:=", "default-assignment")

	if (line ~ /(^|[[:space:];])read([[:space:]]|$)/) {
		read_targets = line
		sub(/^.*(^|[[:space:];])read[[:space:]]*/, "", read_targets)
		sub(/[[:space:]]*<.*/, "", read_targets)
		emit_named_tokens(read_targets, "read")
	}
	if (line ~ /(^|[[:space:];])for[[:space:]]+g_[A-Za-z0-9_]+/)
		emit_named_tokens(line, "loop")
	if (line ~ /(^|[[:space:];])unset[[:space:]]+.*g_[A-Za-z0-9_]+/)
		emit_named_tokens(line, "unset")
}
