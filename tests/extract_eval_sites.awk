# Extract literal production eval command sites from POSIX shell source.
# Output: module<TAB>function<TAB>trimmed source line<TAB>line number.
#
# Quoted renderer payloads and heredoc bodies are excluded so only commands
# parsed by the current shell consume an architecture-policy entry.

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

function strip_command_prefixes(segment,    previous) {
	do {
		previous = segment
		sub(/^([^();&\\]|\\.)*\)[ \t]+/, "", segment)
		sub(/^(if|elif|while|until|then|else|do|command)[ \t]+/, "", segment)
		sub(/^![ \t]+/, "", segment)
		sub(/^\{[ \t]+/, "", segment)
		sub(/^[A-Za-z_][A-Za-z0-9_]*=[^ \t]*[ \t]+/, "", segment)
	} while (segment != previous)
	return segment
}

function emit_eval_commands(code, source_line,    normalized, count, segment, i) {
	normalized = code
	gsub(/&&/, ";", normalized)
	gsub(/\|\|/, ";", normalized)
	gsub(/\|/, ";", normalized)
	gsub(/&/, ";", normalized)
	count = split(normalized, segment, /;/)
	for (i = 1; i <= count; i++) {
		segment[i] = strip_command_prefixes(trim(segment[i]))
		if (segment[i] ~ /^eval([ \t]|$)/)
			printf "%s\t%s\t%s\t%d\n", module, function_name, trim(source_line), FNR
	}
}

FNR == 1 {
	quote = ""
	heredoc_delimiter = ""
	heredoc_strip_tabs = 0
	function_name = "(source)"
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

	if (normalized ~ /^[A-Za-z_][A-Za-z0-9_]*\(\)[ \t]*\{$/) {
		function_name = normalized
		sub(/\(.*/, "", function_name)
		next
	}
	if (original == "}") {
		function_name = "(source)"
		next
	}

	emit_eval_commands(code, original)
}
