# Measure top-level POSIX shell function spans and a stable decision proxy.
#
# Output fields (TAB-separated): file, function, start line, physical lines,
# decisions. The proxy counts executable if/elif/loop/case constructs, case
# arms, and short-circuit operators. Quoted renderer payloads and heredoc data
# are excluded because they are data in the local shell; their exact behavior
# is characterized separately by golden tests.

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
	line_continues = 0
	token_start = shell_token_start
	for (i = 1; i <= length(line); i++) {
		char = substr(line, i, 1)
		if (quote == "") {
			if (char == "#" && token_start)
				break
			if (char == "\\") {
				if (i < length(line)) {
					output = output char
					i++
					output = output substr(line, i, 1)
				} else {
					# POSIX removes an unquoted backslash-newline pair before
					# recognizing tokens. Publish that state so the caller can
					# analyze the complete logical line rather than letting a
					# continued function header evade the scanner.
					line_continues = 1
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
			if (i < length(line))
				i++
			else
				line_continues = 1
			continue
		}
		if (char == "\"")
			quote = ""
	}
	shell_token_start = token_start
	return output
}

function remember_heredoc(line, code,    fragment) {
	if (index(code, "<<") == 0)
		return
	if (!match(line, /<<-?[ \t]*['"]?[A-Za-z_][A-Za-z0-9_]*['"]?/))
		return
	fragment = substr(line, RSTART, RLENGTH)
	heredoc_strip_tabs = (fragment ~ /^<<-/)
	sub(/^<<-?[ \t]*/, "", fragment)
	gsub(/['"]/, "", fragment)
	heredoc_delimiter = fragment
}

function count_operator(code, operator,    count, position, remainder) {
	count = 0
	remainder = code
	while ((position = index(remainder, operator)) > 0) {
		count++
		remainder = substr(remainder, position + length(operator))
	}
	return count
}

function is_shell_separator(char) {
	return char == "" || char ~ /[ \t;|&()]/
}

function contains_function_header(code,    remainder, previous) {
	remainder = code
	while (match(remainder, /[A-Za-z_][A-Za-z0-9_]*[ \t]*\([ \t]*\)/)) {
		previous = (RSTART == 1 ? "" : substr(remainder, RSTART - 1, 1))
		if (is_shell_separator(previous) || previous == "{" || previous == "}")
			return 1
		remainder = substr(remainder, RSTART + RLENGTH)
	}
	return 0
}

function report_definition_violation(line_number, message) {
	printf "Invalid integration fragment [%s]: %s at line %d.\n", \
		FILENAME, message, line_number
	definition_violation = 1
}

# Count only braces that are standalone shell syntax. Parameter expansion and
# brace-expansion characters are not group delimiters.
function count_group_braces(code, brace,    count, i, previous, following) {
	count = 0
	for (i = 1; i <= length(code); i++) {
		if (substr(code, i, 1) != brace)
			continue
		previous = (i == 1 ? "" : substr(code, i - 1, 1))
		following = (i == length(code) ? "" : substr(code, i + 1, 1))
		if (is_shell_separator(previous) && is_shell_separator(following))
			count++
	}
	return count
}

function count_decisions(code,    normalized, count) {
	normalized = trim(code)
	count = 0
	if (normalized ~ /^(if|elif|while|until|for|case)[ \t]/)
		count++
	if (case_depth > 0 && normalized ~ /^[^()]+\)[ \t]*$/)
		count++
	count += count_operator(normalized, "&&")
	count += count_operator(normalized, "||")
	return count
}

FNR == 1 {
	quote = ""
	heredoc_delimiter = ""
	heredoc_strip_tabs = 0
	in_function = 0
	pending_function_name = ""
	pending_function_start = 0
	case_depth = 0
	group_depth = 0
	logical_code = ""
	logical_original = ""
	logical_start = 0
	shell_token_start = 1
	definition_violation = 0
}

{
	physical_original = $0
	if (heredoc_delimiter != "") {
		candidate = physical_original
		if (heredoc_strip_tabs)
			sub(/^\t+/, "", candidate)
		if (candidate == heredoc_delimiter) {
			heredoc_delimiter = ""
			heredoc_strip_tabs = 0
		}
		next
	}

	if (logical_start == 0) {
		logical_start = FNR
		shell_token_start = (quote == "")
	}
	code_piece = shell_code(physical_original)
	logical_code = logical_code code_piece
	if (line_continues) {
		logical_original = logical_original substr(physical_original, 1, length(physical_original) - 1)
		next
	}
	code = logical_code
	original = logical_original physical_original
	code_start = logical_start
	logical_code = ""
	logical_original = ""
	logical_start = 0
	remember_heredoc(original, code)
	normalized = trim(code)

	if (!in_function && pending_function_name != "") {
		if (normalized == "")
			next
		if (normalized == "{") {
			function_name = pending_function_name
			function_start = pending_function_start
			function_decisions = 0
			in_function = 1
			pending_function_name = ""
			pending_function_start = 0
			case_depth = 0
			group_depth = 0
			next
		}
		if (definitions_only)
			report_definition_violation(pending_function_start, \
				"function header does not use a measurable brace body")
		pending_function_name = ""
		pending_function_start = 0
	}

	if (!in_function && normalized ~ /^[A-Za-z_][A-Za-z0-9_]*[ \t]*\([ \t]*\)[ \t]*\{$/) {
		function_name = normalized
		sub(/[ \t]*\(.*/, "", function_name)
		if (headers_only)
			printf "%s\t%s\t%d\n", FILENAME, function_name, code_start
		function_start = code_start
		function_decisions = 0
		in_function = 1
		case_depth = 0
		group_depth = 0
		next
	}
	if (!in_function && normalized ~ /^[A-Za-z_][A-Za-z0-9_]*[ \t]*\([ \t]*\)$/) {
		pending_function_name = normalized
		sub(/[ \t]*\(.*/, "", pending_function_name)
		pending_function_start = code_start
		if (headers_only)
			printf "%s\t%s\t%d\n", FILENAME, pending_function_name, code_start
		next
	}

	if (!in_function) {
		if (definitions_only && normalized != "")
			report_definition_violation(code_start, "executable top-level shell code")
		next
	}

	if (definitions_only && contains_function_header(code))
		report_definition_violation(code_start, "nested function definition")

	function_decisions += count_decisions(code)
	if (normalized ~ /^case[ \t]/)
		case_depth++
	if (normalized ~ /^esac([ \t;]|$)/ && case_depth > 0)
		case_depth--

	if (normalized == "}" && group_depth == 0) {
		if (!headers_only && !definitions_only)
			printf "%s\t%s\t%d\t%d\t%d\n", FILENAME, function_name, function_start, FNR - function_start + 1, function_decisions
		in_function = 0
		case_depth = 0
		group_depth = 0
		next
	}

	group_depth += count_group_braces(code, "{")
	group_depth -= count_group_braces(code, "}")
	if (group_depth < 0)
		group_depth = 0
}

END {
	if (definitions_only && !definition_violation &&
		(pending_function_name != "" || in_function || heredoc_delimiter != "" || logical_start != 0))
		report_definition_violation(FNR, "unterminated shell construct")
	if (definitions_only && definition_violation)
		exit 1
}
