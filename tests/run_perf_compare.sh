#!/bin/sh
#
# Informative performance comparison wrapper for two zxfer binaries.
#

set -eu

TESTS_DIR=$(dirname "$0")
ZXFER_PERF_COMPARE_ROOT=$(cd "$TESTS_DIR/.." && pwd -P)
ZXFER_PERF_COMPARE_RUNNER="$ZXFER_PERF_COMPARE_ROOT/tests/run_perf_tests.sh"

ZXFER_PERF_COMPARE_BASELINE_BIN=""
ZXFER_PERF_COMPARE_CANDIDATE_BIN=""
ZXFER_PERF_COMPARE_BASELINE_LABEL="baseline"
ZXFER_PERF_COMPARE_CANDIDATE_LABEL="candidate"
ZXFER_PERF_COMPARE_PROFILE="smoke"
ZXFER_PERF_COMPARE_CASES=""
ZXFER_PERF_COMPARE_SAMPLES=""
ZXFER_PERF_COMPARE_WARMUPS=""
ZXFER_PERF_COMPARE_OUTPUT_DIR=""
ZXFER_PERF_COMPARE_YES=0
ZXFER_PERF_COMPARE_THRESHOLD_PCT=${ZXFER_PERF_REGRESSION_THRESHOLD_PCT:-10}

zxfer_perf_compare_usage() {
	cat <<'EOF'
usage: ./tests/run_perf_compare.sh --baseline-bin PATH --candidate-bin PATH --baseline-label LABEL --candidate-label LABEL --profile smoke|standard --case LIST --samples N --warmups N --output-dir DIR [--yes]

Runs tests/run_perf_tests.sh once per binary into baseline/ and candidate/,
then writes top-level compare.tsv and compare.md. Regressions are advisory only;
the wrapper fails only when argument validation or a sample harness run fails.
EOF
}

zxfer_perf_compare_die() {
	printf '%s\n' "ERROR: $*" >&2
	exit 1
}

zxfer_perf_compare_positive_integer_p() {
	case ${1:-} in
	'' | *[!0-9]*) return 1 ;;
	esac
	[ "$1" -gt 0 ]
}

zxfer_perf_compare_nonnegative_integer_p() {
	case ${1:-} in
	'' | *[!0-9]*) return 1 ;;
	esac
	[ "$1" -ge 0 ]
}

zxfer_perf_compare_tsv_value() {
	printf '%s\n' "$1" | awk '
		{
			if (NR > 1) printf " "
			gsub(/\t|\r/, " ")
			printf "%s", $0
		}
	'
}

zxfer_perf_compare_parse_args() {
	while [ $# -gt 0 ]; do
		case "$1" in
		--baseline-bin)
			shift
			[ $# -gt 0 ] && [ -n "$1" ] || zxfer_perf_compare_die "--baseline-bin requires a path"
			ZXFER_PERF_COMPARE_BASELINE_BIN=$1
			;;
		--candidate-bin)
			shift
			[ $# -gt 0 ] && [ -n "$1" ] || zxfer_perf_compare_die "--candidate-bin requires a path"
			ZXFER_PERF_COMPARE_CANDIDATE_BIN=$1
			;;
		--baseline-label)
			shift
			[ $# -gt 0 ] && [ -n "$1" ] || zxfer_perf_compare_die "--baseline-label requires a value"
			ZXFER_PERF_COMPARE_BASELINE_LABEL=$1
			;;
		--candidate-label)
			shift
			[ $# -gt 0 ] && [ -n "$1" ] || zxfer_perf_compare_die "--candidate-label requires a value"
			ZXFER_PERF_COMPARE_CANDIDATE_LABEL=$1
			;;
		--profile)
			shift
			[ $# -gt 0 ] && [ -n "$1" ] || zxfer_perf_compare_die "--profile requires a value"
			ZXFER_PERF_COMPARE_PROFILE=$1
			;;
		--case)
			shift
			[ $# -gt 0 ] && [ -n "$1" ] || zxfer_perf_compare_die "--case requires a value"
			ZXFER_PERF_COMPARE_CASES=$1
			;;
		--samples)
			shift
			[ $# -gt 0 ] && [ -n "$1" ] || zxfer_perf_compare_die "--samples requires a value"
			ZXFER_PERF_COMPARE_SAMPLES=$1
			;;
		--warmups)
			shift
			[ $# -gt 0 ] && [ -n "$1" ] || zxfer_perf_compare_die "--warmups requires a value"
			ZXFER_PERF_COMPARE_WARMUPS=$1
			;;
		--output-dir)
			shift
			[ $# -gt 0 ] && [ -n "$1" ] || zxfer_perf_compare_die "--output-dir requires a path"
			ZXFER_PERF_COMPARE_OUTPUT_DIR=$1
			;;
		--yes)
			ZXFER_PERF_COMPARE_YES=1
			;;
		-h | --help)
			zxfer_perf_compare_usage
			exit 0
			;;
		*)
			zxfer_perf_compare_die "Unknown argument: $1"
			;;
		esac
		shift
	done
}

zxfer_perf_compare_validate_args() {
	[ -x "$ZXFER_PERF_COMPARE_RUNNER" ] ||
		zxfer_perf_compare_die "Performance runner is not executable: $ZXFER_PERF_COMPARE_RUNNER"
	[ -n "$ZXFER_PERF_COMPARE_BASELINE_BIN" ] ||
		zxfer_perf_compare_die "--baseline-bin is required"
	[ -n "$ZXFER_PERF_COMPARE_CANDIDATE_BIN" ] ||
		zxfer_perf_compare_die "--candidate-bin is required"
	[ -x "$ZXFER_PERF_COMPARE_BASELINE_BIN" ] ||
		zxfer_perf_compare_die "Baseline binary is not executable: $ZXFER_PERF_COMPARE_BASELINE_BIN"
	[ -x "$ZXFER_PERF_COMPARE_CANDIDATE_BIN" ] ||
		zxfer_perf_compare_die "Candidate binary is not executable: $ZXFER_PERF_COMPARE_CANDIDATE_BIN"
	[ -n "$ZXFER_PERF_COMPARE_OUTPUT_DIR" ] ||
		zxfer_perf_compare_die "--output-dir is required"

	case "$ZXFER_PERF_COMPARE_PROFILE" in
	smoke | standard) ;;
	*)
		zxfer_perf_compare_die "Unsupported performance profile: $ZXFER_PERF_COMPARE_PROFILE"
		;;
	esac

	if [ -n "$ZXFER_PERF_COMPARE_SAMPLES" ]; then
		zxfer_perf_compare_positive_integer_p "$ZXFER_PERF_COMPARE_SAMPLES" ||
			zxfer_perf_compare_die "--samples must be a positive integer"
	fi
	if [ -n "$ZXFER_PERF_COMPARE_WARMUPS" ]; then
		zxfer_perf_compare_nonnegative_integer_p "$ZXFER_PERF_COMPARE_WARMUPS" ||
			zxfer_perf_compare_die "--warmups must be a non-negative integer"
	fi
}

zxfer_perf_compare_run_one() {
	l_label=$1
	l_bin=$2
	l_output_dir=$3

	set -- --label "$l_label" --profile "$ZXFER_PERF_COMPARE_PROFILE" --output-dir "$l_output_dir"
	if [ "$ZXFER_PERF_COMPARE_YES" -eq 1 ]; then
		set -- "$@" --yes
	fi
	if [ -n "$ZXFER_PERF_COMPARE_CASES" ]; then
		set -- "$@" --case "$ZXFER_PERF_COMPARE_CASES"
	fi
	if [ -n "$ZXFER_PERF_COMPARE_SAMPLES" ]; then
		set -- "$@" --samples "$ZXFER_PERF_COMPARE_SAMPLES"
	fi
	if [ -n "$ZXFER_PERF_COMPARE_WARMUPS" ]; then
		set -- "$@" --warmups "$ZXFER_PERF_COMPARE_WARMUPS"
	fi

	ZXFER_BIN=$l_bin "$ZXFER_PERF_COMPARE_RUNNER" "$@" ||
		zxfer_perf_compare_die "Performance sample run failed for $l_label"
}

zxfer_perf_compare_write_tsv() {
	l_baseline_summary=$1
	l_candidate_summary=$2
	l_compare_file=$3
	l_baseline_label=$(zxfer_perf_compare_tsv_value "$ZXFER_PERF_COMPARE_BASELINE_LABEL")
	l_candidate_label=$(zxfer_perf_compare_tsv_value "$ZXFER_PERF_COMPARE_CANDIDATE_LABEL")

	awk -F '\t' -v threshold="$ZXFER_PERF_COMPARE_THRESHOLD_PCT" \
		-v baseline_label="$l_baseline_label" \
		-v candidate_label="$l_candidate_label" '
		function excluded(name) {
			return name == "run_label" ||
				name == "case" ||
				name == "samples" ||
				name == "failed_samples"
		}
		function numeric(value) {
			return value ~ /^-?[0-9]+([.][0-9]+)?$/
		}
		function compare(c, metric, baseline, candidate, pct, pct_text, warning) {
			if (!numeric(baseline) || !numeric(candidate)) return
			pct_text = ""
			warning = ""
			if (baseline != 0) {
				pct = ((candidate - baseline) / baseline) * 100
				pct_text = sprintf("%.2f", pct)
				if (metric ~ /throughput/ && pct < -threshold) warning = "regression"
				if (metric !~ /throughput/ && pct > threshold) warning = "regression"
			}
			printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", c, metric, baseline_label, baseline, candidate_label, candidate, pct_text, warning
		}
		FNR == NR {
			if (FNR == 1) {
				for (i = 1; i <= NF; i++) baseline_col[$i] = i
				next
			}
			c = $(baseline_col["case"])
			baseline_seen[c] = 1
			for (name in baseline_col) {
				if (!excluded(name)) baseline[c SUBSEP name] = $(baseline_col[name])
			}
			next
		}
		FNR == 1 {
			print "case\tmetric\tbaseline_label\tbaseline\tcandidate_label\tcandidate\tpct_delta\twarning"
			for (i = 1; i <= NF; i++) {
				candidate_name[i] = $i
				candidate_col[$i] = i
			}
			next
		}
		FNR > 1 {
			c = $(candidate_col["case"])
			if (!baseline_seen[c]) next
			for (i = 1; i <= NF; i++) {
				metric = candidate_name[i]
				if (excluded(metric)) continue
				if (!(metric in baseline_col)) continue
				compare(c, metric, baseline[c SUBSEP metric], $i)
			}
		}
	' "$l_baseline_summary" "$l_candidate_summary" >"$l_compare_file"
}

zxfer_perf_compare_write_markdown() {
	l_compare_file=$1
	l_markdown_file=$2

	awk -F '\t' '
		NR == 1 {
			print "# zxfer performance comparison"
			print ""
			print "| case | metric | baseline | candidate | delta % | warning |"
			print "| --- | --- | ---: | ---: | ---: | --- |"
			next
		}
		{
			printf "| `%s` | `%s` | %s | %s | %s | %s |\n", $1, $2, $4, $6, $7, $8
		}
	' "$l_compare_file" >"$l_markdown_file"
}

zxfer_perf_compare_main() {
	zxfer_perf_compare_parse_args "$@"
	zxfer_perf_compare_validate_args

	mkdir -p "$ZXFER_PERF_COMPARE_OUTPUT_DIR"
	ZXFER_PERF_COMPARE_OUTPUT_DIR=$(cd -P "$ZXFER_PERF_COMPARE_OUTPUT_DIR" && pwd)

	zxfer_perf_compare_run_one "$ZXFER_PERF_COMPARE_BASELINE_LABEL" "$ZXFER_PERF_COMPARE_BASELINE_BIN" "$ZXFER_PERF_COMPARE_OUTPUT_DIR/baseline"
	zxfer_perf_compare_run_one "$ZXFER_PERF_COMPARE_CANDIDATE_LABEL" "$ZXFER_PERF_COMPARE_CANDIDATE_BIN" "$ZXFER_PERF_COMPARE_OUTPUT_DIR/candidate"

	zxfer_perf_compare_write_tsv \
		"$ZXFER_PERF_COMPARE_OUTPUT_DIR/baseline/summary.tsv" \
		"$ZXFER_PERF_COMPARE_OUTPUT_DIR/candidate/summary.tsv" \
		"$ZXFER_PERF_COMPARE_OUTPUT_DIR/compare.tsv"
	zxfer_perf_compare_write_markdown "$ZXFER_PERF_COMPARE_OUTPUT_DIR/compare.tsv" "$ZXFER_PERF_COMPARE_OUTPUT_DIR/compare.md"

	printf '%s\n' "Performance comparison artifacts written to $ZXFER_PERF_COMPARE_OUTPUT_DIR"
}

if [ "${ZXFER_RUN_PERF_COMPARE_SOURCE_ONLY:-0}" != "1" ]; then
	zxfer_perf_compare_main "$@"
fi
