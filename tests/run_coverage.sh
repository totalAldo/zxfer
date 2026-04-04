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

Examples:
  tests/run_coverage.sh
  ZXFER_COVERAGE_MODE=bash-xtrace tests/run_coverage.sh tests/test_zxfer_common.sh
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

	awk -v target_list_file="$l_target_list_file" \
		-v merged_trace_file="$l_trace_file" \
		-v summary_file="$l_summary_file" \
		-v missing_file="$l_missing_file" '
function trim(s) {
	sub(/^[[:space:]]+/, "", s)
	sub(/[[:space:]]+$/, "", s)
	return s
}
function is_coverable_line(line, t) {
	t = trim(line)
	if (t == "") return 0
	if (t ~ /^#/) return 0
	if (t ~ /^[{}]$/) return 0
	if (t ~ /^;;$/) return 0
	if (t ~ /^(then|do|else|fi|done|esac|in)$/) return 0
	if (t ~ /^[A-Za-z_][A-Za-z0-9_]*\(\)[[:space:]]*\{$/) return 0
	if (t ~ /^case[[:space:]].*[[:space:]]in$/) return 0
	return 1
}
BEGIN {
	while ((getline file < target_list_file) > 0) {
		target[file] = 1
		files[++file_count] = file
		line_no = 0
		while ((getline source_line < file) > 0) {
			line_no++
			source[file, line_no] = source_line
			if (is_coverable_line(source_line)) {
				coverable[file, line_no] = 1
				coverable_count[file]++
			}
		}
		close(file)
	}
	while ((getline trace_line < merged_trace_file) > 0) {
		if (trace_line ~ /^\++[^:]+:[0-9]+: /) {
			sub(/^\++/, "", trace_line)
			trace_file = trace_line
			sub(/:[0-9]+: .*/, "", trace_file)
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
		printf "%.2f\t%d\t%d\t%d\t%s\n", pct, coverable_count[file], hit_count[file], miss_count[file], file >> summary_file

		if (miss_count[file] > 0) {
			printf "%s\n", file >> missing_file
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

run_with_bash_xtrace() {
	l_target_list_file=$1
	shift
	l_bash_bin=${BASH_BIN:-$(command -v bash || true)}
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

	rm -rf "$l_trace_dir"
	return "$l_overall_status"
}

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
