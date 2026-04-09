#!/bin/sh
#
# Run zxfer shunit2 suites under a coverage collector.
# Prefers kcov when available; otherwise falls back to a bash xtrace report.
#

set -eu

ZXFER_ROOT=$(cd "$(dirname "$0")/.." && pwd)
TEST_DIR="$ZXFER_ROOT/tests"
COVERAGE_DIR=${COVERAGE_DIR:-"$ZXFER_ROOT/coverage"}
ZXFER_COVERAGE_MODE=${ZXFER_COVERAGE_MODE:-auto}
ZXFER_COVERAGE_INCLUDE_ENTRYPOINT=${ZXFER_COVERAGE_INCLUDE_ENTRYPOINT:-0}
ZXFER_COVERAGE_ENFORCE_POLICY=${ZXFER_COVERAGE_ENFORCE_POLICY:-1}
ZXFER_COVERAGE_REGRESSION_HIT_TOLERANCE=${ZXFER_COVERAGE_REGRESSION_HIT_TOLERANCE:-2}
ZXFER_COVERAGE_TOTAL_REGRESSION_HIT_TOLERANCE=${ZXFER_COVERAGE_TOTAL_REGRESSION_HIT_TOLERANCE:-4}
COVERAGE_POLICY_FILE=${ZXFER_COVERAGE_POLICY_FILE:-"$TEST_DIR/coverage_policy.tsv"}
COVERAGE_BASELINE_DIR=${ZXFER_COVERAGE_BASELINE_DIR:-"$TEST_DIR/coverage_baseline/bash-xtrace"}
COVERAGE_BASELINE_SUMMARY_FILE=${ZXFER_COVERAGE_BASELINE_SUMMARY_FILE:-"$COVERAGE_BASELINE_DIR/summary.tsv"}
COVERAGE_BASELINE_MISSING_FILE=${ZXFER_COVERAGE_BASELINE_MISSING_FILE:-"$COVERAGE_BASELINE_DIR/missing.txt"}

print_usage() {
	cat <<'EOF'
Usage: tests/run_coverage.sh [suite ...]

Runs the shunit2 suites under a coverage collector and writes results to
./coverage by default.

The bash-xtrace fallback covers sourced shell modules under src/. It excludes
the top-level ./zxfer entrypoint by default because child-shell execution is
not traced reliably without kcov. Set ZXFER_COVERAGE_INCLUDE_ENTRYPOINT=1 to
include it anyway.

Modes:
  auto        Prefer kcov when installed, otherwise use bash xtrace.
  kcov        Require kcov.
  bash-xtrace Require the bash xtrace fallback.

Environment:
  ZXFER_COVERAGE_ENFORCE_POLICY=0  disable the bash-xtrace coverage gate
  ZXFER_COVERAGE_POLICY_FILE       override the minimum-coverage policy file
  ZXFER_COVERAGE_BASELINE_DIR      override the committed bash-xtrace baseline dir

The bash-xtrace mode writes repo-relative summary.tsv and missing.txt reports,
appends a TOTAL row, compares them to the committed baseline, and writes a
unified missing.txt diff for CI and pull request visibility.

The committed bash-xtrace baseline uses a small hit-count tolerance during the
no-regression comparison to absorb known shell / platform tracing jitter in the
approximation path.

Committed policy files:
  tests/coverage_policy.tsv
  tests/coverage_baseline/bash-xtrace/summary.tsv
  tests/coverage_baseline/bash-xtrace/missing.txt

Examples:
  tests/run_coverage.sh
  ZXFER_COVERAGE_MODE=bash-xtrace tests/run_coverage.sh tests/test_zxfer_reporting.sh
  COVERAGE_DIR=/tmp/zxfer-coverage tests/run_coverage.sh
EOF
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

write_target_file_list() {
	l_target_list_file=$1
	{
		if [ "$ZXFER_COVERAGE_INCLUDE_ENTRYPOINT" = "1" ]; then
			printf '%s\n' "$ZXFER_ROOT/zxfer"
		fi
		printf '%s\n' "$ZXFER_ROOT"/src/*.sh
	} >"$l_target_list_file"
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

bash_supports_xtrace_line_numbers() {
	l_bash_bin=$1
	l_probe_file=${TMPDIR:-/tmp}/zxfer.coverage.probe.$$
	(
		BASH_XTRACEFD=9
		PS4='+${BASH_SOURCE}:${LINENO}: '
		export BASH_XTRACEFD PS4
		"$l_bash_bin" --noprofile --norc -x <<'EOF' 9>"$l_probe_file" >/dev/null 2>&1
probe() {
	printf '%s\n' ok >/dev/null
}
probe
EOF
	) || true

	if grep -Eq '^\+[^:]+:[0-9]+: ' "$l_probe_file" 2>/dev/null; then
		rm -f "$l_probe_file"
		return 0
	fi

	rm -f "$l_probe_file"
	return 1
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
function has_unbalanced_double_quote(line,    i, ch, escaped, quote_count) {
	escaped = 0
	quote_count = 0
	for (i = 1; i <= length(line); i++) {
		ch = substr(line, i, 1)
		if (escaped) {
			escaped = 0
			continue
		}
		if (ch == "\\") {
			escaped = 1
			continue
		}
		if (ch == "\"") {
			quote_count++
		}
	}
	return (quote_count % 2) == 1
}
function has_unbalanced_single_quote(line,    i, ch, quote_count) {
	quote_count = 0
	for (i = 1; i <= length(line); i++) {
		ch = substr(line, i, 1)
		if (ch == "'\''") {
			quote_count++
		}
	}
	return (quote_count % 2) == 1
}
function starts_multiline_single_quote(line, t) {
	t = trim(line)
	return (has_unbalanced_single_quote(line) && t ~ /'\''$/)
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
function heredoc_delimiter(line,    match_count, start, length_part, delimiter) {
	match_count = match(line, /<<-?[[:space:]]*[A-Za-z_][A-Za-z0-9_]*/)
	if (match_count == 0) {
		return ""
	}
	start = RSTART
	length_part = RLENGTH
	delimiter = substr(line, start, length_part)
	sub(/^<<-?[[:space:]]*/, "", delimiter)
	return delimiter
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
		if (has_unbalanced_double_quote(line)) {
			coverage_in_multiline_double_quote = 0
		}
		return 0
	}
	if (coverage_in_multiline_single_quote == 1) {
		if (has_unbalanced_single_quote(line)) {
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
	if (l_heredoc_delimiter != "" && t ~ /^(done|[{}])[[:space:]].*<<-?[[:space:]]*[A-Za-z_][A-Za-z0-9_]*$/) {
		coverage_in_heredoc = 1
		coverage_heredoc_delimiter = l_heredoc_delimiter
		return 0
	}
	if (has_unbalanced_double_quote(line)) {
		coverage_in_multiline_double_quote = 1
		return 0
	}
	if (starts_multiline_single_quote(line)) {
		coverage_in_multiline_single_quote = 1
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

write_missing_diff_file() {
	l_missing_file=$1
	l_missing_diff_file=$2
	l_status=0

	if [ ! -f "$COVERAGE_BASELINE_MISSING_FILE" ]; then
		printf '%s\n' "Committed missing.txt baseline not found: $COVERAGE_BASELINE_MISSING_FILE" >"$l_missing_diff_file"
		return 0
	fi

	set +e
	diff -u "$COVERAGE_BASELINE_MISSING_FILE" "$l_missing_file" >"$l_missing_diff_file"
	l_status=$?
	set -e
	case "$l_status" in
	0)
		printf '%s\n' "No missing-line changes relative to $COVERAGE_BASELINE_MISSING_FILE." >"$l_missing_diff_file"
		;;
	1)
		:
		;;
	*)
		return "$l_status"
		;;
	esac
}

write_policy_disabled_report() {
	l_policy_report_file=$1
	l_policy_failures_file=$2

	{
		printf '%s\n' "Coverage policy enforcement disabled (ZXFER_COVERAGE_ENFORCE_POLICY=0)."
		printf '%s\n' "No minimum or no-regression checks were applied."
	} >"$l_policy_report_file"
	printf '%s\n' "type	target	current_pct	required_pct	note" >"$l_policy_failures_file"
}

enforce_bash_xtrace_policy() {
	l_summary_file=$1
	l_policy_report_file=$2
	l_policy_failures_file=$3

	awk -F '\t' \
		-v summary_file="$l_summary_file" \
		-v policy_file="$COVERAGE_POLICY_FILE" \
		-v baseline_file="$COVERAGE_BASELINE_SUMMARY_FILE" \
		-v regression_hit_tolerance="${ZXFER_COVERAGE_REGRESSION_HIT_TOLERANCE:-2}" \
		-v total_regression_hit_tolerance="${ZXFER_COVERAGE_TOTAL_REGRESSION_HIT_TOLERANCE:-4}" \
		-v report_file="$l_policy_report_file" \
		-v failures_file="$l_policy_failures_file" '
function trim(s) {
	sub(/^[[:space:]]+/, "", s)
	sub(/[[:space:]]+$/, "", s)
	return s
}
function format_pct(value) {
	if (value == "") {
		return "-"
	}
	return sprintf("%.2f", value + 0)
}
function record_failure(type, target, current, expected, note) {
	failures++
	failure_type[failures] = type
	failure_target[failures] = target
	failure_current[failures] = current
	failure_expected[failures] = expected
	failure_note[failures] = note
}
function read_policy_file(   line, fields, target, min_pct) {
	while ((getline line < policy_file) > 0) {
		line = trim(line)
		if (line == "" || line ~ /^#/) {
			continue
		}
		split(line, fields, "\t")
		target = trim(fields[1])
		min_pct = trim(fields[2])
		if (target == "" || min_pct == "") {
			record_failure("invalid-policy", policy_file, "", "", "Malformed policy line: " line)
			continue
		}
		policy_min[target] = min_pct + 0
		policy_seen[target] = 1
	}
	close(policy_file)
}
function read_summary_file(path, pct_store, hit_store, seen_store,   line, fields, target, pct, hit) {
	while ((getline line < path) > 0) {
		if (line == "") {
			continue
		}
		split(line, fields, "\t")
		target = trim(fields[5])
		pct = trim(fields[1])
		hit = trim(fields[3])
		if (target == "" || pct == "" || hit == "") {
			record_failure("invalid-summary", path, "", "", "Malformed summary line: " line)
			continue
		}
		pct_store[target] = pct + 0
		hit_store[target] = hit + 0
		seen_store[target] = 1
	}
	close(path)
}
BEGIN {
	read_policy_file()
	read_summary_file(baseline_file, baseline_pct, baseline_hit, baseline_seen)
	read_summary_file(summary_file, current_pct, current_hit, current_seen)

	if (!("TOTAL" in current_seen)) {
		record_failure("missing-total", "TOTAL", "", "", "Current summary.tsv is missing the TOTAL row.")
	}

	for (target in current_seen) {
		if (!(target in policy_seen)) {
			record_failure("missing-policy", target, current_pct[target], "", "Target missing from coverage policy.")
		}
		if (!(target in baseline_seen)) {
			record_failure("missing-baseline", target, current_pct[target], "", "Target missing from committed coverage baseline.")
		}
	}

	for (target in policy_seen) {
		if (!(target in current_seen) && !(target in reported_missing_current)) {
			record_failure("missing-current", target, "", policy_min[target], "Policy target missing from current summary.")
			reported_missing_current[target] = 1
		}
	}

	for (target in baseline_seen) {
		if (!(target in current_seen) && !(target in reported_missing_current)) {
			record_failure("missing-current", target, "", baseline_pct[target], "Baseline target missing from current summary.")
			reported_missing_current[target] = 1
		}
	}

	for (target in current_seen) {
		if ((target in policy_seen) && (current_pct[target] + 0.000001 < policy_min[target])) {
			record_failure("minimum", target, current_pct[target], policy_min[target], "Coverage fell below the configured minimum.")
		}
		if ((target in baseline_seen) && (current_pct[target] + 0.000001 < baseline_pct[target])) {
			regression_tolerance = (target == "TOTAL" ? total_regression_hit_tolerance + 0 : regression_hit_tolerance + 0)
			if (!(target in current_hit) || !(target in baseline_hit) ||
				(current_hit[target] + regression_tolerance) < baseline_hit[target]) {
				record_failure("regression", target, current_pct[target], baseline_pct[target], "Coverage regressed relative to the committed baseline.")
			}
		}
	}

	print "type\ttarget\tcurrent_pct\trequired_pct\tnote" > failures_file
	for (i = 1; i <= failures; i++) {
		printf "%s\t%s\t%s\t%s\t%s\n", \
			failure_type[i], \
			failure_target[i], \
			format_pct(failure_current[i]), \
			format_pct(failure_expected[i]), \
			failure_note[i] >> failures_file
	}

	if (failures > 0) {
		print "Coverage policy failed." > report_file
		print "Minimums: " policy_file >> report_file
		print "Baseline: " baseline_file >> report_file
		print "" >> report_file
		for (i = 1; i <= failures; i++) {
			printf "- %s: %s (current=%s required=%s) %s\n", \
				failure_type[i], \
				failure_target[i], \
				format_pct(failure_current[i]), \
				format_pct(failure_expected[i]), \
				failure_note[i] >> report_file
		}
		exit 1
	}

	print "Coverage policy passed." > report_file
	print "Minimums: " policy_file >> report_file
	print "Baseline: " baseline_file >> report_file
	exit 0
}
' /dev/null
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
	if ! bash_supports_xtrace_line_numbers "$l_bash_bin"; then
		echo "The selected bash does not support PS4 line-number tracing." >&2
		return 1
	fi

	mkdir -p "$COVERAGE_DIR/bash-xtrace"
	l_trace_dir=$(mktemp -d "${TMPDIR:-/tmp}/zxfer.coverage.XXXXXX")
	l_merged_trace="$COVERAGE_DIR/bash-xtrace/merged.trace"
	l_summary_file="$COVERAGE_DIR/bash-xtrace/summary.tsv"
	l_missing_file="$COVERAGE_DIR/bash-xtrace/missing.txt"
	l_missing_diff_file="$COVERAGE_DIR/bash-xtrace/missing.diff"
	l_policy_report_file="$COVERAGE_DIR/bash-xtrace/policy_report.txt"
	l_policy_failures_file="$COVERAGE_DIR/bash-xtrace/policy_failures.tsv"
	: >"$l_merged_trace"
	: >"$l_summary_file"
	: >"$l_missing_file"

	l_overall_status=0
	for l_suite_path in "$@"; do
		l_suite_name=$(basename "$l_suite_path" .sh)
		l_trace_file="$l_trace_dir/$l_suite_name.trace"
		echo "==> Running bash-xtrace coverage for $l_suite_path"
		if ! BASH_XTRACEFD=9 PS4='+${BASH_SOURCE}:${LINENO}: ' \
			"$l_bash_bin" --noprofile --norc -x "$l_suite_path" \
			9>"$l_trace_file"; then
			l_overall_status=1
		fi
		cat "$l_trace_file" >>"$l_merged_trace"
	done

	render_bash_xtrace_report "$l_target_list_file" "$l_merged_trace" "$l_summary_file" "$l_missing_file"
	append_total_summary_row "$l_summary_file"
	write_missing_diff_file "$l_missing_file" "$l_missing_diff_file"

	l_policy_status=0
	if [ "$ZXFER_COVERAGE_ENFORCE_POLICY" = "0" ]; then
		write_policy_disabled_report "$l_policy_report_file" "$l_policy_failures_file"
	else
		if ! enforce_bash_xtrace_policy "$l_summary_file" "$l_policy_report_file" "$l_policy_failures_file"; then
			l_policy_status=1
		fi
	fi

	echo "Coverage summary: $l_summary_file"
	echo "Missing lines: $l_missing_file"
	echo "Missing diff: $l_missing_diff_file"
	echo "Coverage policy report: $l_policy_report_file"
	echo
	echo "Approximate line coverage (bash xtrace fallback):"
	sort -rn "$l_summary_file" | awk -F '\t' '
BEGIN {
	printf "%-8s %-10s %-10s %-10s %s\n", "pct", "coverable", "hit", "miss", "file"
}
{
	printf "%-8s %-10s %-10s %-10s %s\n", $1 "%", $2, $3, $4, $5
}'

	rm -rf "$l_trace_dir"
	if [ "$l_overall_status" -ne 0 ]; then
		return "$l_overall_status"
	fi
	return "$l_policy_status"
}

main() {
	if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
		print_usage
		exit 0
	fi

	SUITES=$(resolve_suites "$@")
	if [ -z "$SUITES" ]; then
		echo "No shunit2 suites found." >&2
		exit 1
	fi

	mkdir -p "$COVERAGE_DIR"
	TARGET_LIST_FILE=$(mktemp "${TMPDIR:-/tmp}/zxfer.coverage.targets.XXXXXX")
	trap 'rm -f "$TARGET_LIST_FILE"' EXIT INT TERM HUP QUIT
	write_target_file_list "$TARGET_LIST_FILE"

	case "$ZXFER_COVERAGE_MODE" in
	auto)
		if command -v kcov >/dev/null 2>&1; then
			# shellcheck disable=SC2086
			run_with_kcov "$TARGET_LIST_FILE" $SUITES
		else
			# shellcheck disable=SC2086
			run_with_bash_xtrace "$TARGET_LIST_FILE" $SUITES
		fi
		;;
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
	*)
		echo "Unknown coverage mode: $ZXFER_COVERAGE_MODE" >&2
		exit 1
		;;
	esac
}

if [ "${ZXFER_RUN_COVERAGE_SOURCE_ONLY:-0}" != "1" ]; then
	main "$@"
fi
