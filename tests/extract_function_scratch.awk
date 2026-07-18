# Extract function-scoped l_* writes and direct in-process function calls from
# POSIX shell source. The first input is a TAB-separated function-owner table;
# remaining inputs are source modules.
#
# Output fields (TAB-separated): record kind, function, symbol, file, line,
# write/call detail.
# Quoted renderer payloads, heredoc bodies, multiline command substitutions,
# explicit subshell blocks, pipelines, and background calls are excluded. Those
# calls cannot overwrite scratch state in the caller's current shell.

function trim(value) {
	sub(/^[ \t]+/, "", value)
	sub(/[ \t]+$/, "", value)
	return value
}

function is_shell_token_separator(char) {
	return (char == " " || char == "\t" ||
		char == ";" || char == "|" || char == "&" ||
		char == "(" || char == ")" || char == "<" || char == ">")
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
		if (char == "\"")
			quote = ""
	}
	return output
}

function remember_heredoc(line, code,    fragment) {
	if (index(code, "<<") == 0)
		return
	if (!match(line, /<<-?[ \t]*[\047"]?[A-Za-z_][A-Za-z0-9_]*[\047"]?/))
		return
	fragment = substr(line, RSTART, RLENGTH)
	heredoc_strip_tabs = (fragment ~ /^<<-/)
	sub(/^<<-?[ \t]*/, "", fragment)
	gsub(/[\047"]/, "", fragment)
	heredoc_delimiter = fragment
}

function emit_write(variable, kind,    key) {
	key = function_name SUBSEP variable
	if (!seen_write[key]++)
		printf "write\t%s\t%s\t%s\t%d\t%s\n", function_name, variable, FILENAME, FNR, kind
}

function emit_assignment_matches(text, pattern, kind,    matched, variable) {
	while (match(text, pattern)) {
		matched = substr(text, RSTART, RLENGTH)
		sub(/^[^A-Za-z0-9_]/, "", matched)
		variable = matched
		sub(/[:+]?=.*/, "", variable)
		emit_write(variable, kind)
		text = substr(text, RSTART + RLENGTH)
	}
}

function emit_named_writes(text, kind,    count, token, i) {
	gsub(/[^A-Za-z0-9_]/, " ", text)
	count = split(text, token, / +/)
	for (i = 1; i <= count; i++) {
		if (token[i] ~ /^l_[A-Za-z0-9_]+$/)
			emit_write(token[i], kind)
	}
}

function emit_builtin_writes(code,    fragment) {
	if (match(code, /(^|[ \t;])read[ \t]+/)) {
		fragment = substr(code, RSTART + RLENGTH)
		sub(/[;&|<].*$/, "", fragment)
		emit_named_writes(fragment, "read")
	}
	if (match(code, /(^|[ \t;])getopts[ \t]+/)) {
		fragment = substr(code, RSTART + RLENGTH)
		sub(/[;&|<].*$/, "", fragment)
		emit_named_writes(fragment, "getopts")
	}
	if (match(code, /(^|[ \t;])for[ \t]+l_[A-Za-z0-9_]+/)) {
		fragment = substr(code, RSTART, RLENGTH)
		emit_named_writes(fragment, "loop")
	}
}

function emit_call(callee,    key) {
	key = function_name SUBSEP callee
	if (!seen_call[key]++)
		printf "call\t%s\t%s\t%s\t%d\tdirect\n", function_name, callee, FILENAME, FNR
}

function strip_command_prefixes(segment,    previous) {
	do {
		previous = segment
		# Strip one-line POSIX case labels before looking for the command.
		# Refuse unescaped grouping/operator characters so function definitions,
		# command substitutions, and subshell syntax are not mistaken for labels.
		sub(/^([^();&\\]|\\.)*\)[ \t]+/, "", segment)
		sub(/^(if|elif|while|until|then|else|do)[ \t]+/, "", segment)
		sub(/^![ \t]+/, "", segment)
		sub(/^\{[ \t]+/, "", segment)
		sub(/^[A-Za-z_][A-Za-z0-9_]*=[^ \t]*[ \t]+/, "", segment)
	} while (segment != previous)
	return segment
}

function emit_segment_call(segment,    callee) {
	segment = strip_command_prefixes(trim(segment))
	if (segment == "" || segment ~ /^\(/ || segment ~ /\|/ || segment ~ /&[ \t]*$/)
		return
	if (!match(segment, /^[A-Za-z_][A-Za-z0-9_]*/))
		return
	callee = substr(segment, RSTART, RLENGTH)
	if (callee in owner)
		emit_call(callee)
}

function emit_direct_calls(code,    normalized, count, segment, i) {
	normalized = code
	gsub(/&&/, ";", normalized)
	gsub(/\|\|/, ";", normalized)
	count = split(normalized, segment, /;/)
	for (i = 1; i <= count; i++)
		emit_segment_call(segment[i])
}

function starts_multiline_subshell(normalized) {
	if (normalized == "(")
		return 1
	if (normalized ~ /\$\(\([ \t]*$/)
		return 0
	return normalized ~ /\$\([ \t]*$/
}

NR == FNR {
	owner[$1] = $2
	next
}

FNR == 1 {
	quote = ""
	heredoc_delimiter = ""
	heredoc_strip_tabs = 0
	in_function = 0
	function_name = ""
	subshell_depth = 0
}

{
	original = $0
	if (heredoc_delimiter != "") {
		candidate = original
		if (heredoc_strip_tabs)
			sub(/^\t+/, "", candidate)
		if (candidate == heredoc_delimiter) {
			heredoc_delimiter = ""
			heredoc_strip_tabs = 0
		}
		next
	}

	code = shell_code(original)
	remember_heredoc(original, code)
	normalized = trim(code)

	if (!in_function && normalized ~ /^[A-Za-z_][A-Za-z0-9_]*\(\)[ \t]*\{$/) {
		function_name = normalized
		sub(/\(.*/, "", function_name)
		in_function = 1
		subshell_depth = 0
		next
	}

	if (!in_function)
		next
	if (original == "}") {
		in_function = 0
		function_name = ""
		subshell_depth = 0
		next
	}

	if (subshell_depth > 0) {
		if (starts_multiline_subshell(normalized))
			subshell_depth++
		if (normalized ~ /^\)([ \t;]|$)/)
			subshell_depth--
		next
	}

	emit_assignment_matches(code, "(^|[^A-Za-z0-9_])l_[A-Za-z0-9_]+=", "assignment")
	emit_assignment_matches(code, "(^|[^A-Za-z0-9_])l_[A-Za-z0-9_]+:=", "default-assignment")
	emit_builtin_writes(code)
	emit_direct_calls(code)

	if (starts_multiline_subshell(normalized))
		subshell_depth++
}
