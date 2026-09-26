#!/bin/sh
#
# Run zxfer shunit2 suites under a coverage collector.
# Prefers kcov when available; otherwise falls back to a bash xtrace report.
# The bash-xtrace mode runs the suites through tests/run_shunit_tests.sh with
# a ZXFER_TEST_SHELL wrapper that traces each suite into its own file, so it
# shares that runner's worker pool, watchdog and signal teardown.
#

set -eu

ZXFER_ROOT=$(cd "$(dirname "$0")/.." && pwd)
TEST_DIR="$ZXFER_ROOT/tests"
COVERAGE_DIR=${COVERAGE_DIR:-"$ZXFER_ROOT/coverage"}
ZXFER_COVERAGE_MODE=${ZXFER_COVERAGE_MODE:-auto}
ZXFER_COVERAGE_INCLUDE_ENTRYPOINT=${ZXFER_COVERAGE_INCLUDE_ENTRYPOINT:-0}
TARGET_LIST_FILE=
COVERAGE_TRACE_DIR=
COVERAGE_RUNNER_PID=

print_usage() {
	cat <<'USAGE'
Usage: tests/run_coverage.sh [--report-only] [--] [suite ...]

Runs the shunit2 suites under a coverage collector and writes results to
./coverage by default.

The bash-xtrace fallback covers sourced shell modules under src/. It excludes
the top-level ./zxfer entrypoint by default because child-shell execution is
not traced reliably without kcov. Set ZXFER_COVERAGE_INCLUDE_ENTRYPOINT=1 to
include it anyway. It runs the suites through tests/run_shunit_tests.sh, so
that runner's default worker count and ZXFER_TEST_SUITE_TIMEOUT apply.

Modes:
  auto        Prefer kcov when installed, otherwise use bash xtrace.
  kcov        Require kcov.
  bash-xtrace Require the bash xtrace fallback.

Coverage is report-only: no minimum, baseline, or no-regression policy is
applied, and the exit status reflects only the selected suites. The
--report-only flag is accepted for compatibility and has no effect.

The bash-xtrace mode writes repo-relative summary.tsv and missing.txt reports
and appends a TOTAL row.

Examples:
  tests/run_coverage.sh
  ZXFER_COVERAGE_MODE=bash-xtrace tests/run_coverage.sh tests/test_zxfer_reporting.sh
  ZXFER_COVERAGE_MODE=bash-xtrace tests/run_coverage.sh
  COVERAGE_DIR=/tmp/zxfer-coverage tests/run_coverage.sh
USAGE
}

resolve_coverage_collector_mode() {
	case "$ZXFER_COVERAGE_MODE" in
	auto)
		if command -v kcov >/dev/null 2>&1; then
			printf '%s\n' kcov
		else
			printf '%s\n' bash-xtrace
		fi
		;;
	kcov)
		printf '%s\n' kcov
		;;
	bash-xtrace)
		printf '%s\n' bash-xtrace
		;;
	*)
		echo "Unknown coverage mode: $ZXFER_COVERAGE_MODE" >&2
		return 1
		;;
	esac
}

resolve_suite_path() {
	l_suite=$1
	case "$l_suite" in
	/*)
		printf '%s\n' "$l_suite"
		;;
	"$TEST_DIR"/*)
		printf '%s\n' "$l_suite"
		;;
	tests/*)
		printf '%s\n' "$ZXFER_ROOT/$l_suite"
		;;
	*)
		printf '%s\n' "$TEST_DIR/$l_suite"
		;;
	esac
}

resolve_suites() {
	if [ "$#" -eq 0 ]; then
		set -- "$TEST_DIR"/test_*.sh
	fi

	for l_suite in "$@"; do
		l_suite_path=$(resolve_suite_path "$l_suite")
		case "$(basename "$l_suite_path")" in
		test_helper.sh)
			continue
			;;
		esac
		if [ ! -f "$l_suite_path" ]; then
			echo "Missing suite: $l_suite_path" >&2
			return 1
		fi
		printf '%s\n' "$l_suite_path"
	done
}

list_coverage_target_labels() {
	if [ "$ZXFER_COVERAGE_INCLUDE_ENTRYPOINT" = "1" ]; then
		printf '%s\n' zxfer
	fi
	for l_coverage_target_path in "$ZXFER_ROOT"/src/*.sh; do
		[ -f "$l_coverage_target_path" ] || continue
		printf '%s\n' "${l_coverage_target_path#"$ZXFER_ROOT"/}"
	done
}

write_target_file_list() {
	l_target_list_file=$1
	list_coverage_target_labels |
		while IFS= read -r l_coverage_target_label; do
			printf '%s\n' "$ZXFER_ROOT/$l_coverage_target_label"
		done >"$l_target_list_file"
}

run_with_kcov() {
	l_target_list_file=$1
	shift
	mkdir -p "$COVERAGE_DIR/kcov"
	rm -rf "$COVERAGE_DIR/kcov"/*

	l_overall_status=0
	l_kcov_dirs=""
	for l_suite_path in "$@"; do
		l_suite_name=$(basename "$l_suite_path" .sh)
		l_suite_dir="$COVERAGE_DIR/kcov/$l_suite_name"
		echo "==> Running kcov for $l_suite_path"
		if ! kcov --include-pattern="$ZXFER_ROOT/src,$ZXFER_ROOT/zxfer" \
			"$l_suite_dir" "$l_suite_path"; then
			l_overall_status=1
		fi
		l_kcov_dirs="$l_kcov_dirs $l_suite_dir"
	done

	if [ -n "$l_kcov_dirs" ]; then
		# shellcheck disable=SC2086
		set -- $l_kcov_dirs
		kcov --merge "$COVERAGE_DIR/kcov/merged" "$@" >/dev/null
		echo "Coverage report: $COVERAGE_DIR/kcov/merged/index.html"
	fi

	return "$l_overall_status"
}

# Purpose: Write the ZXFER_TEST_SHELL wrapper that runs one suite under bash
# xtrace. Each suite's trace goes to TRACE_DIR/<suite name>.trace on fd 7.
# Usage: write_bash_xtrace_shell BASH_BIN TRACE_DIR WRAPPER_PATH
# The wrapper reads its bash and trace directory from the environment
# (ZXFER_COVERAGE_BASH_BIN, ZXFER_COVERAGE_TRACE_DIR), so no path is quoted
# into its source.
write_bash_xtrace_shell() {
	# Keep the coverage trace off fd 9 because send/receive tests exercise
	# their own queue descriptors on 8/9 and may close them during setUp().
	# Apply fd 7 on the exec itself: some POSIX shells mark descriptors
	# opened by an earlier exec builtin close-on-exec. Set PS4 inside Bash
	# because privileged/root shells may reject an imported PS4 environment
	# value. $0 and the remaining arguments still match a direct
	# `bash suite [args...]` invocation while the wrapper sources the suite.
	cat >"$3" <<'EOF'
#!/bin/sh
l_coverage_trace=$ZXFER_COVERAGE_TRACE_DIR/$(basename "$1" .sh).trace
exec "$ZXFER_COVERAGE_BASH_BIN" --noprofile --norc -c '
l_zxfer_coverage_script=$1
shift
PS4="+\${BASH_SOURCE[0]-\$0}:\${LINENO:-0}: "
BASH_XTRACEFD=7
set -x
. "$l_zxfer_coverage_script"
' "$1" "$@" 7>"$l_coverage_trace"
EOF
	chmod 700 "$3"
	ZXFER_COVERAGE_BASH_BIN=$1
	ZXFER_COVERAGE_TRACE_DIR=$2
	export ZXFER_COVERAGE_BASH_BIN ZXFER_COVERAGE_TRACE_DIR
}

# Purpose: Succeed when BASH_BIN writes "+file:line:" xtrace lines to fd 7.
# Usage: bash_supports_xtrace_line_numbers BASH_BIN SCRATCH_DIR
bash_supports_xtrace_line_numbers() {
	l_probe_dir=$2
	printf '%s\n' 'probe() {' '	printf "%s\n" ok >/dev/null' '}' 'probe' \
		>"$l_probe_dir/probe.sh" || return 1
	write_bash_xtrace_shell "$1" "$l_probe_dir" "$l_probe_dir/xtrace-shell" ||
		return 1
	"$l_probe_dir/xtrace-shell" "$l_probe_dir/probe.sh" >/dev/null 2>&1 || :
	grep -Eq '^\+[^:]+:[0-9]+: ' "$l_probe_dir/probe.trace" 2>/dev/null
}

cleanup_coverage_runner() {
	if [ -n "${TARGET_LIST_FILE:-}" ]; then
		rm -f "$TARGET_LIST_FILE"
		TARGET_LIST_FILE=
	fi
	if [ -n "${COVERAGE_TRACE_DIR:-}" ]; then
		rm -rf "$COVERAGE_TRACE_DIR"
		COVERAGE_TRACE_DIR=
	fi
}

# Purpose: Stop the suite runner on HUP, INT, QUIT or TERM and exit 128+N.
# The runner is a background job, so it ignores INT; it gets TERM and stops
# its own suites before it exits.
# Usage: installed by main's traps only.
handle_coverage_signal() {
	trap '' HUP INT QUIT TERM
	case "$1" in
	HUP) l_coverage_signal_exit=129 ;;
	INT) l_coverage_signal_exit=130 ;;
	QUIT) l_coverage_signal_exit=131 ;;
	*) l_coverage_signal_exit=143 ;;
	esac
	if [ -n "$COVERAGE_RUNNER_PID" ]; then
		kill -s TERM "$COVERAGE_RUNNER_PID" 2>/dev/null || :
		wait "$COVERAGE_RUNNER_PID" 2>/dev/null || :
	fi
	cleanup_coverage_runner
	exit "$l_coverage_signal_exit"
}

render_bash_xtrace_report() {
	l_target_list_file=$1
	l_trace_file=$2
	l_summary_file=$3
	l_missing_file=$4

	: >"$l_summary_file"
	: >"$l_missing_file"

	awk -v target_list_file="$l_target_list_file" \
		-v merged_trace_file="$l_trace_file" \
		-v summary_file="$l_summary_file" \
		-v missing_file="$l_missing_file" \
		-v zxfer_root="$ZXFER_ROOT" '
function trim(s) {
	sub(/^[[:space:]]+/, "", s)
	sub(/[[:space:]]+$/, "", s)
	return s
}
function canonicalize_path(path,    is_abs, part_count, i, part, out_count, result) {
	gsub(/\/+/, "/", path)
	is_abs = (substr(path, 1, 1) == "/")
	part_count = split(path, path_parts, "/")
	for (i in canonical_parts) {
		delete canonical_parts[i]
	}
	out_count = 0
	for (i = 1; i <= part_count; i++) {
		part = path_parts[i]
		if (part == "" || part == ".") {
			continue
		}
		if (part == "..") {
			if (out_count > 0 && canonical_parts[out_count] != "..") {
				delete canonical_parts[out_count]
				out_count--
			} else if (!is_abs) {
				out_count++
				canonical_parts[out_count] = part
			}
			continue
		}
		out_count++
		canonical_parts[out_count] = part
	}
	if (out_count == 0) {
		return is_abs ? "/" : "."
	}
	result = is_abs ? "/" canonical_parts[1] : canonical_parts[1]
	for (i = 2; i <= out_count; i++) {
		result = result "/" canonical_parts[i]
	}
	return result
}
function normalize_path(path, root_prefix) {
	if (root_prefix != "" && index(path, root_prefix) == 1) {
		return substr(path, length(root_prefix) + 1)
	}
	return path
}
function starts_shell_comment_at(line, position,    previous) {
	if (substr(line, position, 1) != "#") {
		return 0
	}
	if (position == 1) {
		return 1
	}
	previous = substr(line, position - 1, 1)
	return (previous == " " || previous == "\t" ||
		previous == ";" || previous == "|" || previous == "&" ||
		previous == "(" || previous == ")" ||
		previous == "<" || previous == ">")
}
function double_quote_state_after_line(line, in_double_quote,    i, ch, escaped, in_single_quote) {
	escaped = 0
	in_single_quote = 0
	for (i = 1; i <= length(line); i++) {
		ch = substr(line, i, 1)
		if (in_single_quote) {
			if (ch == "'\''") {
				in_single_quote = 0
			}
			continue
		}
		if (escaped) {
			escaped = 0
			continue
		}
		if (ch == "\\") {
			escaped = 1
			continue
		}
		if (in_double_quote) {
			if (ch == "\"") {
				in_double_quote = 0
			}
			continue
		}
		if (starts_shell_comment_at(line, i)) {
			break
		}
		if (ch == "'\''") {
			in_single_quote = 1
			continue
		}
		if (ch == "\"") {
			in_double_quote = 1
		}
	}
	return in_double_quote
}
function has_unbalanced_double_quote(line) {
	return double_quote_state_after_line(line, 0)
}
function continues_multiline_double_quote(line) {
	return double_quote_state_after_line(line, 1)
}
function single_quote_state_after_line(line, in_single_quote,    i, ch, escaped, in_double_quote) {
	escaped = 0
	in_double_quote = 0
	for (i = 1; i <= length(line); i++) {
		ch = substr(line, i, 1)
		if (in_single_quote) {
			if (ch == "'\''") {
				in_single_quote = 0
			}
			continue
		}
		if (escaped) {
			escaped = 0
			continue
		}
		if (ch == "\\") {
			escaped = 1
			continue
		}
		if (ch == "\"") {
			in_double_quote = !in_double_quote
			continue
		}
		if (!in_double_quote && starts_shell_comment_at(line, i)) {
			break
		}
		if (!in_double_quote && ch == "'\''") {
			in_single_quote = 1
		}
	}
	return in_single_quote
}
function has_unbalanced_single_quote(line) {
	return single_quote_state_after_line(line, 0)
}
function continues_multiline_single_quote(line) {
	return single_quote_state_after_line(line, 1)
}
function starts_multiline_single_quote(line, t) {
	return has_unbalanced_single_quote(line)
}
function count_trailing_backslashes(line,    i, ch, count) {
	count = 0
	for (i = length(line); i >= 1; i--) {
		ch = substr(line, i, 1)
		if (ch == " " || ch == "\t")
			continue
		if (ch != "\\")
			break
		count++
	}
	return count
}
function ends_with_line_continuation(line, t, trailing_backslashes) {
	t = trim(line)
	if (t == "")
		return 0
	trailing_backslashes = count_trailing_backslashes(line)
	return (trailing_backslashes % 2) == 1
}
function heredoc_delimiter(line,    rest, quote, quote_end, delimiter) {
	if (!match(line, /<<-?[[:space:]]*/)) {
		return ""
	}
	rest = substr(line, RSTART + RLENGTH)
	if (substr(rest, 1, 1) == "\\") {
		rest = substr(rest, 2)
	}
	quote = substr(rest, 1, 1)
	if (quote == "\"" || quote == "'\''") {
		quote_end = index(substr(rest, 2), quote)
		if (quote_end == 0) {
			return ""
		}
		delimiter = substr(rest, 2, quote_end - 1)
	} else {
		if (!match(rest, /^[A-Za-z_][A-Za-z0-9_]*/)) {
			return ""
		}
		delimiter = substr(rest, RSTART, RLENGTH)
	}
	if (delimiter !~ /^[A-Za-z_][A-Za-z0-9_]*$/) {
		return ""
	}
	return delimiter
}
function unclosed_command_substitution_depth(line,    i, ch, next_ch, after_next, depth, escaped, in_single_quote, in_double_quote, scope_index) {
	for (scope_index in command_substitution_outer_double_quote)
		delete command_substitution_outer_double_quote[scope_index]
	for (scope_index in command_substitution_parenthesis_depth)
		delete command_substitution_parenthesis_depth[scope_index]
	depth = 0
	escaped = 0
	in_single_quote = 0
	in_double_quote = 0
	for (i = 1; i <= length(line); i++) {
		ch = substr(line, i, 1)
		next_ch = substr(line, i + 1, 1)
		after_next = substr(line, i + 2, 1)
		if (in_single_quote) {
			if (ch == "'\''") {
				in_single_quote = 0
			}
			continue
		}
		if (escaped) {
			escaped = 0
			continue
		}
		if (ch == "\\") {
			escaped = 1
			continue
		}
		if (ch == "'\''" && !in_double_quote) {
			in_single_quote = 1
			continue
		}
		if (ch == "$" && next_ch == "(" && after_next != "(") {
			depth++
			command_substitution_outer_double_quote[depth] = in_double_quote
			command_substitution_parenthesis_depth[depth] = 0
			# The command inside $(...) has its own quote context even when the
			# substitution itself appears inside an outer double-quoted word.
			in_single_quote = 0
			in_double_quote = 0
			escaped = 0
			i++
			continue
		}
		if (ch == "\"") {
			in_double_quote = !in_double_quote
			continue
		}
		if (!in_double_quote && starts_shell_comment_at(line, i)) {
			break
		}
		if (depth > 0 && !in_double_quote && ch == "(") {
			command_substitution_parenthesis_depth[depth]++
			continue
		}
		if (depth > 0 && !in_double_quote && ch == ")") {
			if (command_substitution_parenthesis_depth[depth] > 0) {
				command_substitution_parenthesis_depth[depth]--
			} else {
				in_double_quote = command_substitution_outer_double_quote[depth]
				delete command_substitution_outer_double_quote[depth]
				delete command_substitution_parenthesis_depth[depth]
				depth--
			}
		}
	}
	return depth
}
function is_case_pattern_line(line, t) {
	t = trim(line)
	if (coverage_case_depth == 0) {
		return 0
	}
	if (t ~ /^esac$/) {
		return 0
	}
	return (t ~ /^.+\)[[:space:]]*(;;)?$/)
}
function starts_multiline_command_substitution(line, t) {
	t = trim(line)
	return (t ~ /\$\([[:space:]]*$/)
}
function opens_command_substitution_subshell(line, t) {
	t = trim(line)
	return (t == "(")
}
function closes_command_substitution_scope(line, t) {
	t = trim(line)
	return (t ~ /^\)/)
}
function is_untraceable_control_syntax_line(line, t) {
	t = trim(line)
	if (t ~ /^(if|elif|while|until)[[:space:]]+(![[:space:]]+)?\($/) {
		return 1
	}
	if (t ~ /^\)([[:space:]]+[^;]+)?;[[:space:]]*(then|do)$/) {
		return 1
	}
	return (t ~ /^(done|[{}()])[[:space:]]*[0-9]*[<>]/)
}
function is_coverable_line(line, t, l_heredoc_delimiter) {
	t = trim(line)
	if (coverage_in_heredoc == 1) {
		if (t == coverage_heredoc_delimiter) {
			coverage_in_heredoc = 0
			coverage_heredoc_delimiter = ""
		}
		return 0
	}
	if (coverage_in_command_substitution == 1) {
		if (starts_multiline_command_substitution(line) || opens_command_substitution_subshell(line)) {
			coverage_command_substitution_depth++
		}
		if (closes_command_substitution_scope(line)) {
			coverage_command_substitution_depth--
			if (coverage_command_substitution_depth <= 0) {
				coverage_in_command_substitution = 0
				coverage_command_substitution_depth = 0
			}
		}
		return 0
	}
	if (coverage_in_multiline_double_quote == 1) {
		if (!continues_multiline_double_quote(line)) {
			coverage_in_multiline_double_quote = 0
		}
		return 0
	}
	if (coverage_in_multiline_single_quote == 1) {
		if (!continues_multiline_single_quote(line)) {
			coverage_in_multiline_single_quote = 0
		}
		return 0
	}
	if (coverage_in_backslash_continuation == 1) {
		if (starts_multiline_command_substitution(line)) {
			coverage_in_command_substitution = 1
			coverage_command_substitution_depth = 1
		} else if (has_unbalanced_double_quote(line)) {
			coverage_in_multiline_double_quote = 1
		} else if (starts_multiline_single_quote(line)) {
			coverage_in_multiline_single_quote = 1
		}
		if (!ends_with_line_continuation(line)) {
			coverage_in_backslash_continuation = 0
		}
		return 0
	}
	if (t == "") return 0
	if (t ~ /^#/) return 0
	if (t ~ /^[{}()]$/) return 0
	if (t ~ /^;;$/) return 0
	if (t ~ /^(then|do|else|fi|done|in)$/) return 0
	if (t ~ /^[A-Za-z_][A-Za-z0-9_]*\(\)[[:space:]]*\{$/) return 0
	if (t ~ /^case[[:space:]].*[[:space:]]in$/) {
		coverage_case_depth++
		return 0
	}
	if (t ~ /^esac$/) {
		if (coverage_case_depth > 0) {
			coverage_case_depth--
		}
		return 0
	}
	if (starts_multiline_command_substitution(line)) {
		coverage_in_command_substitution = 1
		coverage_command_substitution_depth = 1
		return 0
	}
	if (is_case_pattern_line(line)) return 0
	l_heredoc_delimiter = heredoc_delimiter(line)
	if (l_heredoc_delimiter != "") {
		coverage_in_heredoc = 1
		coverage_heredoc_delimiter = l_heredoc_delimiter
		if (t ~ /^(done|[{}])[[:space:]]*<<-?/) {
			return 0
		}
	}
	if (is_untraceable_control_syntax_line(line)) return 0
	if (has_unbalanced_double_quote(line)) {
		coverage_in_multiline_double_quote = 1
		return 0
	}
	if (starts_multiline_single_quote(line)) {
		coverage_in_multiline_single_quote = 1
		return 0
	}
	# Bash attributes an assignment containing a command substitution to a
	# later physical line when the substitution continues there. Exclude only
	# the untraceable opener; independently attributable body lines remain
	# eligible unless an existing multiline rule applies.
	if (unclosed_command_substitution_depth(line) > 0) {
		if (ends_with_line_continuation(line)) {
			coverage_in_backslash_continuation = 1
		}
		return 0
	}
	if (ends_with_line_continuation(line)) {
		coverage_in_backslash_continuation = 1
	}
	return 1
}
BEGIN {
	root_prefix = canonicalize_path(zxfer_root) "/"
	while ((getline file < target_list_file) > 0) {
		normalized_file = canonicalize_path(file)
		target[normalized_file] = 1
		files[++file_count] = normalized_file
		target_label[normalized_file] = normalize_path(normalized_file, root_prefix)
		line_no = 0
		while ((getline source_line < file) > 0) {
			line_no++
			source[normalized_file, line_no] = source_line
			if (is_coverable_line(source_line)) {
				coverable[normalized_file, line_no] = 1
				coverable_count[normalized_file]++
			}
		}
		close(file)
	}
	while ((getline trace_line < merged_trace_file) > 0) {
		if (trace_line ~ /^\++[^:]+:[0-9]+: /) {
			sub(/^\++/, "", trace_line)
			trace_file = trace_line
			sub(/:[0-9]+: .*/, "", trace_file)
			trace_file = canonicalize_path(trace_file)
			trace_line_no = trace_line
			sub(/^[^:]+:/, "", trace_line_no)
			sub(/: .*/, "", trace_line_no)
			trace_line_no += 0
			if ((trace_file in target) && ((trace_file, trace_line_no) in coverable)) {
				hit[trace_file, trace_line_no] = 1
			}
		}
	}
	close(merged_trace_file)

	for (i = 1; i <= file_count; i++) {
		file = files[i]
		hit_count[file] = 0
		for (key in hit) {
			split(key, parts, SUBSEP)
			if (parts[1] == file) {
				hit_count[file]++
			}
		}
		miss_count[file] = coverable_count[file] - hit_count[file]
		if (coverable_count[file] > 0) {
			pct = (hit_count[file] * 100.0) / coverable_count[file]
		} else {
			pct = 100.0
		}
		printf "%.2f\t%d\t%d\t%d\t%s\n", pct, coverable_count[file], hit_count[file], miss_count[file], target_label[file] >> summary_file

		if (miss_count[file] > 0) {
			printf "%s\n", target_label[file] >> missing_file
			for (line_no = 1; (file, line_no) in source; line_no++) {
				if ((file, line_no) in coverable && !((file, line_no) in hit)) {
					printf "  %d:%s\n", line_no, source[file, line_no] >> missing_file
				}
			}
			printf "\n" >> missing_file
		}
	}
}
' /dev/null
}

append_total_summary_row() {
	l_summary_file=$1
	l_tmp_file=$l_summary_file.tmp.$$

	awk -F '\t' '
BEGIN {
	OFS = "\t"
	total_coverable = 0
	total_hit = 0
	total_miss = 0
}
NF >= 5 && $5 != "TOTAL" {
	print $0
	total_coverable += $2
	total_hit += $3
	total_miss += $4
}
END {
	if (total_coverable > 0) {
		pct = (total_hit * 100.0) / total_coverable
	} else {
		pct = 100.0
	}
	printf "%.2f\t%d\t%d\t%d\tTOTAL\n", pct, total_coverable, total_hit, total_miss
}
' "$l_summary_file" >"$l_tmp_file"
	mv "$l_tmp_file" "$l_summary_file"
}

run_with_bash_xtrace() {
	l_target_list_file=$1
	shift
	l_bash_bin=${ZXFER_COVERAGE_BASH_BIN:-}
	if [ -z "$l_bash_bin" ]; then
		# Preserve the legacy BASH_BIN override without using a direct
		# $BASH_BIN expansion, which checkbashisms flags in POSIX scripts.
		l_bash_bin=$(env | awk -F= '
			$1 == "BASH_BIN" {
				sub(/^[^=]*=/, "", $0)
				print $0
				exit
			}
		')
	fi
	if [ -z "$l_bash_bin" ]; then
		l_bash_bin=$(command -v bash || true)
	fi
	if [ -z "$l_bash_bin" ]; then
		echo "bash is required for ZXFER_COVERAGE_MODE=bash-xtrace." >&2
		return 1
	fi

	COVERAGE_TRACE_DIR=$(mktemp -d "${TMPDIR:-/tmp}/zxfer.coverage.XXXXXX")
	mkdir "$COVERAGE_TRACE_DIR/probe" "$COVERAGE_TRACE_DIR/traces"
	if ! bash_supports_xtrace_line_numbers "$l_bash_bin" "$COVERAGE_TRACE_DIR/probe"; then
		echo "The selected bash does not support PS4 line-number tracing." >&2
		return 1
	fi
	write_bash_xtrace_shell "$l_bash_bin" "$COVERAGE_TRACE_DIR/traces" \
		"$COVERAGE_TRACE_DIR/xtrace-shell"

	mkdir -p "$COVERAGE_DIR/bash-xtrace"
	l_merged_trace="$COVERAGE_DIR/bash-xtrace/merged.trace"
	l_summary_file="$COVERAGE_DIR/bash-xtrace/summary.tsv"
	l_missing_file="$COVERAGE_DIR/bash-xtrace/missing.txt"
	: >"$l_merged_trace"
	: >"$l_summary_file"
	: >"$l_missing_file"

	echo "==> Running bash-xtrace coverage through tests/run_shunit_tests.sh"
	ZXFER_TEST_SHELL="$COVERAGE_TRACE_DIR/xtrace-shell" \
		"$TEST_DIR/run_shunit_tests.sh" -- "$@" &
	COVERAGE_RUNNER_PID=$!
	l_overall_status=0
	wait "$COVERAGE_RUNNER_PID" || l_overall_status=1
	COVERAGE_RUNNER_PID=
	for l_trace_file in "$COVERAGE_TRACE_DIR"/traces/*.trace; do
		[ -f "$l_trace_file" ] || continue
		cat "$l_trace_file" >>"$l_merged_trace"
	done

	render_bash_xtrace_report "$l_target_list_file" "$l_merged_trace" "$l_summary_file" "$l_missing_file"
	append_total_summary_row "$l_summary_file"

	echo "Coverage summary: $l_summary_file"
	echo "Missing lines: $l_missing_file"
	echo
	echo "Approximate line coverage (bash xtrace fallback):"
	sort -rn "$l_summary_file" | awk -F '\t' '
BEGIN {
	printf "%-8s %-10s %-10s %-10s %s\n", "pct", "coverable", "hit", "miss", "file"
}
{
	printf "%-8s %-10s %-10s %-10s %s\n", $1 "%", $2, $3, $4, $5
}'

	return "$l_overall_status"
}

main() {
	while [ "$#" -gt 0 ]; do
		case "$1" in
		-h | --help)
			print_usage
			exit 0
			;;
		--report-only)
			# Accepted for compatibility; every coverage run is report-only.
			;;
		--)
			shift
			break
			;;
		-*)
			echo "Unknown argument: $1" >&2
			exit 1
			;;
		*)
			break
			;;
		esac
		shift
	done

	COVERAGE_COLLECTOR_MODE=$(resolve_coverage_collector_mode)

	SUITES=$(resolve_suites "$@")
	if [ -z "$SUITES" ]; then
		echo "No shunit2 suites found." >&2
		exit 1
	fi

	mkdir -p "$COVERAGE_DIR"
	TARGET_LIST_FILE=$(mktemp "${TMPDIR:-/tmp}/zxfer.coverage.targets.XXXXXX")
	trap 'cleanup_coverage_runner' EXIT
	trap 'handle_coverage_signal HUP' HUP
	trap 'handle_coverage_signal INT' INT
	trap 'handle_coverage_signal QUIT' QUIT
	trap 'handle_coverage_signal TERM' TERM
	write_target_file_list "$TARGET_LIST_FILE"

	case "$COVERAGE_COLLECTOR_MODE" in
	kcov)
		if ! command -v kcov >/dev/null 2>&1; then
			echo "kcov is not installed." >&2
			exit 1
		fi
		# shellcheck disable=SC2086
		run_with_kcov "$TARGET_LIST_FILE" $SUITES
		;;
	bash-xtrace)
		# shellcheck disable=SC2086
		run_with_bash_xtrace "$TARGET_LIST_FILE" $SUITES
		;;
	esac
}

if [ "${ZXFER_RUN_COVERAGE_SOURCE_ONLY:-0}" != "1" ]; then
	main "$@"
fi
