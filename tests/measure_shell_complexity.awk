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
	if (!match(line, /<<-?[ \t]*[\047\"]?[A-Za-z_][A-Za-z0-9_]*[\047\"]?/))
		return
	fragment = substr(line, RSTART, RLENGTH)
	heredoc_strip_tabs = (fragment ~ /^<<-/)
	sub(/^<<-?[ \t]*/, "", fragment)
	gsub(/[\047\"]/, "", fragment)
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
	case_depth = 0
	group_depth = 0
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
		function_start = FNR
		function_decisions = 0
		in_function = 1
		case_depth = 0
		group_depth = 0
		next
	}

	if (!in_function)
		next

	function_decisions += count_decisions(code)
	if (normalized ~ /^case[ \t]/)
		case_depth++
	if (normalized ~ /^esac([ \t;]|$)/ && case_depth > 0)
		case_depth--

	if (normalized == "}" && group_depth == 0) {
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
