#!/bin/sh
# Offline timing/RSS gate for recursive property-prefetch grouping. The
# deterministic fixtures are plain text; this script never invokes ZFS or SSH.

ZXFER_PROPERTY_PREFETCH_BENCHMARK_ROOT=${ZXFER_PROPERTY_PREFETCH_BENCHMARK_ROOT:-$(cd "$(dirname "$0")/.." && pwd -P)}
ZXFER_PROPERTY_PREFETCH_BENCHMARK_SCRIPT="$ZXFER_PROPERTY_PREFETCH_BENCHMARK_ROOT/tests/run_property_prefetch_benchmark.sh"
ZXFER_PROPERTY_PREFETCH_BENCHMARK_MODULE="$ZXFER_PROPERTY_PREFETCH_BENCHMARK_ROOT/src/zxfer_property_state.sh"
ZXFER_PROPERTY_PREFETCH_BENCHMARK_AWK=${ZXFER_PROPERTY_PREFETCH_BENCHMARK_AWK:-awk}
ZXFER_PROPERTY_PREFETCH_BENCHMARK_TIME=${ZXFER_PROPERTY_PREFETCH_BENCHMARK_TIME:-/usr/bin/time}
ZXFER_PROPERTY_PREFETCH_BENCHMARK_SAMPLES=${ZXFER_PROPERTY_PREFETCH_BENCHMARK_SAMPLES:-7}
ZXFER_PROPERTY_PREFETCH_BENCHMARK_WARMUPS=${ZXFER_PROPERTY_PREFETCH_BENCHMARK_WARMUPS:-1}
ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR=""
ZXFER_PROPERTY_PREFETCH_BENCHMARK_MEMORY_MODE=""
ZXFER_PROPERTY_PREFETCH_BENCHMARK_MEMORY_UNIT=unavailable
ZXFER_PROPERTY_PREFETCH_BENCHMARK_PROGRAMS_LOADED=0

# Purpose: Stabilize numeric parsing and machine-readable benchmark evidence.
# Usage: Called only by executable benchmark entry paths, never while the file
# is merely sourced by contract tests.
zxfer_property_prefetch_benchmark_set_stable_locale() {
	LC_ALL=C
	export LC_ALL
}

zxfer_property_prefetch_benchmark_usage() {
	cat <<'EOF'
usage: ./tests/run_property_prefetch_benchmark.sh --output-dir DIR [options]

  --awk PATH       POSIX awk implementation to measure (default: awk)
  --samples N      alternating samples per implementation (default: 7)
  --warmups N      untimed runs per implementation (default: 1)
  --output-dir DIR new artifact directory (required)
  -h, --help       show this help

The fixed workloads batch 20 iterations at 100 datasets and 5 iterations at
1,000 datasets. Acceptance requires byte-identical output, no 100-dataset
timing regression, at least 10% improvement at 1,000 datasets, and no median
peak-RSS regression when the host time utility exposes that measurement.
EOF
}

zxfer_property_prefetch_benchmark_error() {
	printf '%s\n' "ERROR: $*" >&2
	return 1
}

zxfer_property_prefetch_benchmark_uint_p() {
	case ${1:-} in
	'' | *[!0-9]*) return 1 ;;
	esac
	[ "$1" -ge "${2:-0}" ]
}

zxfer_property_prefetch_benchmark_parse_args() {
	while [ $# -gt 0 ]; do
		case "$1" in
		--awk | --samples | --warmups | --output-dir)
			l_prefetch_benchmark_option=$1
			shift
			[ $# -gt 0 ] || zxfer_property_prefetch_benchmark_error "$l_prefetch_benchmark_option requires a value" || return 1
			case "$l_prefetch_benchmark_option" in
			--awk) ZXFER_PROPERTY_PREFETCH_BENCHMARK_AWK=$1 ;;
			--samples) ZXFER_PROPERTY_PREFETCH_BENCHMARK_SAMPLES=$1 ;;
			--warmups) ZXFER_PROPERTY_PREFETCH_BENCHMARK_WARMUPS=$1 ;;
			--output-dir) ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR=$1 ;;
			esac
			;;
		-h | --help)
			zxfer_property_prefetch_benchmark_usage
			return 2
			;;
		*) zxfer_property_prefetch_benchmark_error "Unknown argument: $1" || return 1 ;;
		esac
		shift
	done
}

zxfer_property_prefetch_benchmark_validate_args() {
	[ -f "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_MODULE" ] ||
		zxfer_property_prefetch_benchmark_error "Property-state module not found" || return 1
	[ -x "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_AWK" ] ||
		command -v "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_AWK" >/dev/null 2>&1 ||
		zxfer_property_prefetch_benchmark_error "AWK is not executable: $ZXFER_PROPERTY_PREFETCH_BENCHMARK_AWK" || return 1
	[ -x "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_TIME" ] ||
		zxfer_property_prefetch_benchmark_error "Time utility is not executable: $ZXFER_PROPERTY_PREFETCH_BENCHMARK_TIME" || return 1
	command -v cmp >/dev/null 2>&1 ||
		zxfer_property_prefetch_benchmark_error "Required POSIX utility is unavailable: cmp" || return 1
	[ -n "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR" ] ||
		zxfer_property_prefetch_benchmark_error "--output-dir is required" || return 1
	zxfer_property_prefetch_benchmark_uint_p "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_SAMPLES" 1 ||
		zxfer_property_prefetch_benchmark_error "--samples must be positive" || return 1
	zxfer_property_prefetch_benchmark_uint_p "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_WARMUPS" 0 ||
		zxfer_property_prefetch_benchmark_error "--warmups must be non-negative" || return 1
	case "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR" in
	-*) zxfer_property_prefetch_benchmark_error "Relative output paths may not begin with '-'" || return 1 ;;
	*'
'* | *"	"*) zxfer_property_prefetch_benchmark_error "Output paths may not contain tabs or newlines" || return 1 ;;
	esac
	l_prefetch_benchmark_output_parent=$(dirname "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR") ||
		zxfer_property_prefetch_benchmark_error "Unable to resolve output directory parent" || return 1
	[ -d "$l_prefetch_benchmark_output_parent" ] ||
		zxfer_property_prefetch_benchmark_error "Output directory parent does not exist: $l_prefetch_benchmark_output_parent" || return 1
	[ ! -e "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR" ] ||
		zxfer_property_prefetch_benchmark_error "Output directory already exists" || return 1
}

# Purpose: Claim the validated evidence directory without following a race
# winner created between argument validation and benchmark startup.
# Usage: The parent must already exist; this deliberately uses plain mkdir so
# an existing directory or symlink fails before any fixed artifact is written.
zxfer_property_prefetch_benchmark_create_output_dir() {
	l_prefetch_output_create_status=0
	(
		umask 077
		mkdir "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR"
	) || l_prefetch_output_create_status=$?
	if [ "$l_prefetch_output_create_status" -ne 0 ]; then
		zxfer_property_prefetch_benchmark_error \
			"Unable to create new output directory: $ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR" || :
		return "$l_prefetch_output_create_status"
	fi
	l_prefetch_output_physical_dir=$(cd "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR" && pwd -P) || {
		zxfer_property_prefetch_benchmark_error \
			"Unable to resolve newly created output directory: $ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR" || :
		return 1
	}
	ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR=$l_prefetch_output_physical_dir
}

zxfer_property_prefetch_benchmark_load_programs() {
	[ "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_PROGRAMS_LOADED" -eq 0 ] || return 0
	# shellcheck source=src/zxfer_property_state.sh
	. "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_MODULE"
	ZXFER_PROPERTY_PREFETCH_BENCHMARK_PROGRAMS_LOADED=1
}

zxfer_property_prefetch_benchmark_generate_fixture() {
	l_prefetch_fixture_count=$1
	l_prefetch_fixture_dir=$2
	mkdir -p "$l_prefetch_fixture_dir" || return "$?"
	ZXFER_PROPERTY_PREFETCH_FIXTURE_FILTER="$l_prefetch_fixture_dir/filter.tsv" \
		ZXFER_PROPERTY_PREFETCH_FIXTURE_MACHINE="$l_prefetch_fixture_dir/machine.tsv" \
		ZXFER_PROPERTY_PREFETCH_FIXTURE_HUMAN="$l_prefetch_fixture_dir/human.tsv" \
		"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_AWK" \
		-v count="$l_prefetch_fixture_count" \
		'
		BEGIN {
			filter = ENVIRON["ZXFER_PROPERTY_PREFETCH_FIXTURE_FILTER"]
			machine = ENVIRON["ZXFER_PROPERTY_PREFETCH_FIXTURE_MACHINE"]
			human = ENVIRON["ZXFER_PROPERTY_PREFETCH_FIXTURE_HUMAN"]
			for (i = 1; i <= count; i++) {
				d = sprintf("tank/bench/d%04d", i)
				print d > filter
				print d "\ttype\tfilesystem\t-" > machine
				print d "\tcreation\t1700000000\t-" > machine
				print d "\tused\t1048576\t-" > machine
				print d "\tavailable\t10737418240\t-" > machine
				print d "\treferenced\t1048576\t-" > machine
				print d "\tcompressratio\t1.00x\t-" > machine
				print d "\tmounted\tyes\t-" > machine
				print d "\torigin\t-\t-" > machine
				print d "\tcompression\tlz4\tlocal" > machine
				print d "\trecordsize\t131072\tdefault" > machine
				print d "\tuser:label\tgroup%," i "=x;y\tlocal" > machine
				print d "\ttype\tfilesystem\t-" > human
				print d "\tcreation\tTue Nov 14 22:13 2023\t-" > human
				print d "\tused\t1.00M\t-" > human
				print d "\tavailable\t10.0G\t-" > human
				print d "\treferenced\t1.00M\t-" > human
				print d "\tcompressratio\t1.00x\t-" > human
				print d "\tmounted\tyes\t-" > human
				print d "\torigin\t-\t-" > human
				print d "\tcompression\tlz4\tlocal" > human
				print d "\trecordsize\t128K\tdefault" > human
				print d "\tuser:label\tgroup%," i "=x;y\tlocal" > human
			}
			close(filter); close(machine); close(human)
		}'
}

zxfer_property_prefetch_benchmark_run_once() {
	l_prefetch_once_kind=$1
	l_prefetch_once_filter=$2
	l_prefetch_once_machine=$3
	l_prefetch_once_human=$4
	l_prefetch_once_scratch=$5
	case "$l_prefetch_once_kind" in
	baseline)
		zxfer_group_recursive_property_tree_by_dataset "$l_prefetch_once_filter" \
			"$l_prefetch_once_machine" >"$l_prefetch_once_scratch/machine.tsv" || return "$?"
		zxfer_group_recursive_property_tree_by_dataset "$l_prefetch_once_filter" \
			"$l_prefetch_once_human" >"$l_prefetch_once_scratch/human.tsv" || return "$?"
		"$g_cmd_awk" -F "$(printf '\t')" "$ZXFER_MERGE_RECURSIVE_PROPERTY_TREES_AWK" \
			"$l_prefetch_once_scratch/machine.tsv" "$l_prefetch_once_scratch/human.tsv" \
			>"$l_prefetch_once_scratch/output.tsv"
		;;
	candidate)
		zxfer_group_and_merge_recursive_property_trees_by_dataset \
			"$l_prefetch_once_filter" "$l_prefetch_once_machine" "$l_prefetch_once_human" \
			>"$l_prefetch_once_scratch/output.tsv"
		;;
	*) return 64 ;;
	esac
}

zxfer_property_prefetch_benchmark_worker() {
	zxfer_property_prefetch_benchmark_set_stable_locale
	l_prefetch_worker_kind=$1
	l_prefetch_worker_awk=$2
	l_prefetch_worker_fixture=$3
	l_prefetch_worker_scratch=$4
	l_prefetch_worker_iterations=$5
	mkdir -p "$l_prefetch_worker_scratch" || return "$?"
	zxfer_property_prefetch_benchmark_load_programs || return "$?"
	g_cmd_awk=$l_prefetch_worker_awk
	l_prefetch_worker_i=1
	while [ "$l_prefetch_worker_i" -le "$l_prefetch_worker_iterations" ]; do
		zxfer_property_prefetch_benchmark_run_once "$l_prefetch_worker_kind" \
			"$l_prefetch_worker_fixture/filter.tsv" "$l_prefetch_worker_fixture/machine.tsv" \
			"$l_prefetch_worker_fixture/human.tsv" "$l_prefetch_worker_scratch" || return "$?"
		l_prefetch_worker_i=$((l_prefetch_worker_i + 1))
	done
}

zxfer_property_prefetch_benchmark_detect_memory() {
	l_prefetch_memory_probe=$1
	if "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_TIME" -f 'ZXFER_RSS=%M' /bin/sh -c ':' \
		>/dev/null 2>"$l_prefetch_memory_probe" &&
		awk -F= '$1 == "ZXFER_RSS" && $2 ~ /^[0-9]+$/ { ok = 1 } END { exit !ok }' "$l_prefetch_memory_probe"; then
		ZXFER_PROPERTY_PREFETCH_BENCHMARK_MEMORY_MODE=gnu
		ZXFER_PROPERTY_PREFETCH_BENCHMARK_MEMORY_UNIT=KiB
	elif "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_TIME" -l /bin/sh -c ':' \
		>/dev/null 2>"$l_prefetch_memory_probe" &&
		awk '/maximum resident set size/ && $1 ~ /^[0-9]+$/ { ok = 1 } END { exit !ok }' "$l_prefetch_memory_probe"; then
		ZXFER_PROPERTY_PREFETCH_BENCHMARK_MEMORY_MODE=bsd
		ZXFER_PROPERTY_PREFETCH_BENCHMARK_MEMORY_UNIT=native_maxrss
	fi
}

zxfer_property_prefetch_benchmark_measure() {
	l_prefetch_measure_kind=$1
	l_prefetch_measure_impl=$2
	l_prefetch_measure_size=$3
	l_prefetch_measure_sample=$4
	l_prefetch_measure_iterations=$5
	l_prefetch_measure_fixture=$6
	l_prefetch_measure_scratch=$7
	l_prefetch_measure_raw=$8
	set -- "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_SCRIPT" --worker \
		"$l_prefetch_measure_impl" "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_AWK" \
		"$l_prefetch_measure_fixture" "$l_prefetch_measure_scratch" "$l_prefetch_measure_iterations"
	case "$l_prefetch_measure_kind:$ZXFER_PROPERTY_PREFETCH_BENCHMARK_MEMORY_MODE" in
	timing:*)
		"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_TIME" -p "$@" >/dev/null 2>"$l_prefetch_measure_raw" || return "$?"
		l_prefetch_measure_value=$(awk '$1 == "real" { print $2; exit }' "$l_prefetch_measure_raw")
		[ -n "$l_prefetch_measure_value" ] || return 1
		l_prefetch_measure_per_iteration=$(awk -v s="$l_prefetch_measure_value" -v n="$l_prefetch_measure_iterations" \
			'BEGIN { printf "%.6f", s * 1000 / n }')
		printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$l_prefetch_measure_size" "$l_prefetch_measure_impl" \
			"$l_prefetch_measure_sample" "$l_prefetch_measure_iterations" \
			"$l_prefetch_measure_value" "$l_prefetch_measure_per_iteration"
		;;
	memory:gnu)
		"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_TIME" -f 'ZXFER_RSS=%M' "$@" >/dev/null 2>"$l_prefetch_measure_raw" || return "$?"
		l_prefetch_measure_value=$(awk -F= '$1 == "ZXFER_RSS" { print $2; exit }' "$l_prefetch_measure_raw")
		;;
	memory:bsd)
		"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_TIME" -l "$@" >/dev/null 2>"$l_prefetch_measure_raw" || return "$?"
		l_prefetch_measure_value=$(awk '/maximum resident set size/ { print $1; exit }' "$l_prefetch_measure_raw")
		;;
	*) return 64 ;;
	esac
	[ "$l_prefetch_measure_kind" = timing ] && return 0
	case "$l_prefetch_measure_value" in '' | *[!0-9]*) return 1 ;; esac
	printf '%s\t%s\t%s\t%s\t%s\n' "$l_prefetch_measure_size" "$l_prefetch_measure_impl" \
		"$l_prefetch_measure_sample" "$l_prefetch_measure_value" "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_MEMORY_UNIT"
}

zxfer_property_prefetch_benchmark_run_size() {
	l_prefetch_size_count=$1
	l_prefetch_size_iterations=$2
	l_prefetch_size_fixture="$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR/fixture-$l_prefetch_size_count"
	l_prefetch_size_scratch="$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR/scratch-$l_prefetch_size_count"
	zxfer_property_prefetch_benchmark_generate_fixture "$l_prefetch_size_count" "$l_prefetch_size_fixture" || return "$?"
	mkdir -p "$l_prefetch_size_scratch/baseline" "$l_prefetch_size_scratch/candidate" || return "$?"
	zxfer_property_prefetch_benchmark_load_programs || return "$?"
	g_cmd_awk=$ZXFER_PROPERTY_PREFETCH_BENCHMARK_AWK
	for l_prefetch_size_impl in baseline candidate; do
		zxfer_property_prefetch_benchmark_run_once "$l_prefetch_size_impl" \
			"$l_prefetch_size_fixture/filter.tsv" "$l_prefetch_size_fixture/machine.tsv" \
			"$l_prefetch_size_fixture/human.tsv" "$l_prefetch_size_scratch/$l_prefetch_size_impl" || return "$?"
	done
	l_prefetch_size_identical=no
	cmp -s "$l_prefetch_size_scratch/baseline/output.tsv" \
		"$l_prefetch_size_scratch/candidate/output.tsv" && l_prefetch_size_identical=yes
	printf '%s\t%s\n' "$l_prefetch_size_count" "$l_prefetch_size_identical" >>"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR/identity.tsv"
	[ "$l_prefetch_size_identical" = yes ] || return 1

	l_prefetch_size_warmup=0
	while [ "$l_prefetch_size_warmup" -lt "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_WARMUPS" ]; do
		for l_prefetch_size_impl in baseline candidate; do
			zxfer_property_prefetch_benchmark_worker "$l_prefetch_size_impl" "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_AWK" \
				"$l_prefetch_size_fixture" "$l_prefetch_size_scratch/$l_prefetch_size_impl" 1 || return "$?"
		done
		l_prefetch_size_warmup=$((l_prefetch_size_warmup + 1))
	done
	l_prefetch_size_sample=1
	while [ "$l_prefetch_size_sample" -le "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_SAMPLES" ]; do
		case $((l_prefetch_size_sample % 2)) in 1) set -- baseline candidate ;; *) set -- candidate baseline ;; esac
		for l_prefetch_size_impl in "$@"; do
			zxfer_property_prefetch_benchmark_measure timing "$l_prefetch_size_impl" \
				"$l_prefetch_size_count" "$l_prefetch_size_sample" "$l_prefetch_size_iterations" \
				"$l_prefetch_size_fixture" "$l_prefetch_size_scratch/$l_prefetch_size_impl" \
				"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR/time-$l_prefetch_size_count-$l_prefetch_size_impl-$l_prefetch_size_sample.txt" \
				>>"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR/samples.tsv" || return "$?"
			[ -z "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_MEMORY_MODE" ] ||
				zxfer_property_prefetch_benchmark_measure memory "$l_prefetch_size_impl" \
					"$l_prefetch_size_count" "$l_prefetch_size_sample" 1 "$l_prefetch_size_fixture" \
					"$l_prefetch_size_scratch/$l_prefetch_size_impl" \
					"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR/memory-$l_prefetch_size_count-$l_prefetch_size_impl-$l_prefetch_size_sample.txt" \
					>>"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR/memory.tsv" || return "$?"
		done
		l_prefetch_size_sample=$((l_prefetch_size_sample + 1))
	done
}

zxfer_property_prefetch_benchmark_write_summary() {
	l_prefetch_summary_identity=$1
	l_prefetch_summary_samples=$2
	l_prefetch_summary_memory=$3
	l_prefetch_summary_output=$4
	awk -F '	' -v memory_mode="$ZXFER_PROPERTY_PREFETCH_BENCHMARK_MEMORY_MODE" \
		-v memory_unit="$ZXFER_PROPERTY_PREFETCH_BENCHMARK_MEMORY_UNIT" '
		BEGIN {
			identity_file = ARGV[1]
			samples_file = ARGV[2]
			memory_file = ARGV[3]
		}
		function median(kind, d, impl, values, n, i, j, value) {
			n = kind == "time" ? time_count[d, impl] : memory_count[d, impl]
			for (i = 1; i <= n; i++)
				values[i] = kind == "time" ? times[d, impl, i] : memories[d, impl, i]
			for (i = 2; i <= n; i++) {
				value = values[i]; j = i - 1
				while (j >= 1 && values[j] > value) { values[j + 1] = values[j]; j-- }
				values[j + 1] = value
			}
			if (!n) return -1
			return n % 2 ? values[(n + 1) / 2] : (values[n / 2] + values[n / 2 + 1]) / 2
		}
		FILENAME == identity_file && FNR > 1 { identical[$1] = $2; next }
		FILENAME == samples_file && FNR > 1 { times[$1, $2, ++time_count[$1, $2]] = $6; next }
		FILENAME == memory_file && FNR > 1 { memories[$1, $2, ++memory_count[$1, $2]] = $4; next }
		END {
			print "dataset_count\tbyte_identical\tbaseline_median_ms\tcandidate_median_ms\timprovement_pct\ttiming_gate\tbaseline_peak_rss_median\tcandidate_peak_rss_median\tmemory_unit\tmemory_gate\toverall_gate"
			sizes[1] = 100; sizes[2] = 1000
			for (s = 1; s <= 2; s++) {
				d = sizes[s]; base = median("time", d, "baseline"); candidate = median("time", d, "candidate")
				improvement = base > 0 ? ((base - candidate) / base) * 100 : -999
				timing_gate = (d == 100 ? improvement >= 0 : improvement >= 10) ? "pass" : "fail"
				base_memory = candidate_memory = "unavailable"; memory_gate = "unavailable"
				if (memory_mode != "") {
					base_memory = median("memory", d, "baseline")
					candidate_memory = median("memory", d, "candidate")
					memory_gate = candidate_memory <= base_memory ? "pass" : "fail"
				}
				overall = identical[d] == "yes" && timing_gate == "pass" && memory_gate != "fail" ? "pass" : "fail"
				printf "%d\t%s\t%.6f\t%.6f\t%.2f\t%s\t%s\t%s\t%s\t%s\t%s\n", d, identical[d], base, candidate, improvement, timing_gate, base_memory, candidate_memory, memory_unit, memory_gate, overall
				if (overall != "pass") failed = 1
			}
			exit failed
		}' "$l_prefetch_summary_identity" "$l_prefetch_summary_samples" \
		"$l_prefetch_summary_memory" >"$l_prefetch_summary_output"
}

zxfer_property_prefetch_benchmark_main() {
	zxfer_property_prefetch_benchmark_set_stable_locale
	zxfer_property_prefetch_benchmark_parse_args "$@"
	l_prefetch_main_status=$?
	[ "$l_prefetch_main_status" -eq 0 ] || {
		[ "$l_prefetch_main_status" -eq 2 ] && return 0
		return "$l_prefetch_main_status"
	}
	zxfer_property_prefetch_benchmark_validate_args || return "$?"
	zxfer_property_prefetch_benchmark_create_output_dir || return "$?"
	printf 'dataset_count\timplementation\tsample\titerations\telapsed_seconds\telapsed_ms_per_iteration\n' >"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR/samples.tsv"
	printf 'dataset_count\timplementation\tsample\tpeak_rss\tunit\n' >"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR/memory.tsv"
	printf 'dataset_count\tbyte_identical\n' >"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR/identity.tsv"
	zxfer_property_prefetch_benchmark_detect_memory "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR/memory-probe.txt"
	zxfer_property_prefetch_benchmark_run_size 100 20 || return "$?"
	zxfer_property_prefetch_benchmark_run_size 1000 5 || return "$?"
	zxfer_property_prefetch_benchmark_write_summary \
		"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR/identity.tsv" \
		"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR/samples.tsv" \
		"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR/memory.tsv" \
		"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR/summary.tsv"
	l_prefetch_main_status=$?
	cat "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR/summary.tsv"
	printf '%s\n' "Artifacts: $ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR"
	return "$l_prefetch_main_status"
}

if [ "${1:-}" = --worker ]; then
	shift
	[ $# -eq 5 ] || exit 64
	zxfer_property_prefetch_benchmark_worker "$@"
	exit "$?"
fi
if [ "${ZXFER_RUN_PROPERTY_PREFETCH_BENCHMARK_SOURCE_ONLY:-0}" != 1 ]; then
	zxfer_property_prefetch_benchmark_main "$@"
fi
