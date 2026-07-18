#!/bin/sh
#
# Conservative dispatcher for zxfer's existing validation entrypoints.
#

set -eu

ZXFER_ROOT=$(cd "$(dirname "$0")/.." && pwd)
VALIDATION_MAP=${ZXFER_VALIDATION_MAP:-"$ZXFER_ROOT/tests/validation_map.tsv"}
VALIDATION_PROFILES=${ZXFER_VALIDATION_PROFILES:-"$ZXFER_ROOT/tests/validation_profiles.tsv"}
ZXFER_VALIDATE_JOBS=${ZXFER_VALIDATE_JOBS:-4}
TAB=$(printf '\t')

QUICK_UNIT_SUITES=
QUICK_INTEGRATION_GROUPS=
QUICK_PERF_CASES=
QUICK_DOC_SURFACES=

print_usage() {
	cat <<'EOF'
Usage: tests/validate.sh [--list] <profile> [profile arguments]

Run one validation profile from tests/validation_profiles.tsv.

Profiles:
  doctor     report shells, QEMU/ZFS commands, and cached lint availability
  quick      map explicit or changed paths to offline focused unit checks
  full       complete lint, shunit2, and enforced bash-xtrace coverage
  portable   static checkbashisms, shfmt, and ShellCheck portability checks
  docs       actionlint, codespell, budget, and manpage checks (may populate tool cache)
  vm         smoke/local disposable-guest VM profile only
  bootstrap  download or refresh the pinned lint tools

Examples:
  ./tests/validate.sh quick src/zxfer_cli.sh
  ./tests/validate.sh quick
  ./tests/validate.sh doctor
  ./tests/validate.sh full
  ./tests/validate.sh vm smoke --guest ubuntu

With no path arguments, quick inspects staged, unstaged, and untracked Git
paths. It prints why each path maps to its selected unit suites and wider
integration/performance/documentation recommendations, but executes only the
offline budget and unit checks. It never downloads tools, starts VMs, invokes
ZFS, or runs the direct host integration harness.

Set ZXFER_VALIDATE_JOBS to a positive integer to change the default four-way
unit-suite concurrency used by quick and full.
EOF
}

die() {
	printf '%s\n' "$*" >&2
	exit 1
}

require_validate_jobs() {
	case "$ZXFER_VALIDATE_JOBS" in
	'' | *[!0-9]* | 0)
		die "ZXFER_VALIDATE_JOBS must be a positive integer: $ZXFER_VALIDATE_JOBS"
		;;
	esac
}

print_profiles() {
	awk -F '\t' '
		BEGIN {
			printf "%-10s %-14s %s\n", "PROFILE", "HOST RISK", "DESCRIPTION"
		}
		$1 !~ /^#/ && $1 != "" && !seen[$1]++ {
			printf "%-10s %-14s %s\n", $1, $3, $4
		}
	' "$VALIDATION_PROFILES"
}

command_path() {
	command -v "$1" 2>/dev/null || true
}

report_command_availability() {
	l_label=$1
	l_command=$2
	l_path=$(command_path "$l_command")

	if [ -n "$l_path" ]; then
		printf '  %-20s available (%s)\n' "$l_label" "$l_path"
	else
		printf '  %-20s not found\n' "$l_label"
	fi
}

set_lint_cache_root() {
	if [ -n "${ZXFER_LINT_TOOL_DIR:-}" ]; then
		LINT_CACHE_ROOT=$ZXFER_LINT_TOOL_DIR
	elif [ -n "${XDG_CACHE_HOME:-}" ]; then
		LINT_CACHE_ROOT=$XDG_CACHE_HOME/zxfer/lint-tools
	elif [ -n "${HOME:-}" ]; then
		LINT_CACHE_ROOT=$HOME/.cache/zxfer/lint-tools
	else
		LINT_CACHE_ROOT=${TMPDIR:-/tmp}/zxfer-lint-tools
	fi
}

find_cached_lint_tool() {
	l_tool=$1
	l_binary=$2

	for l_candidate in \
		"$LINT_CACHE_ROOT/$l_tool"/*/*/"$l_binary" \
		"$LINT_CACHE_ROOT/$l_tool"/*/"$l_binary" \
		"$LINT_CACHE_ROOT/$l_tool"/*/venv/bin/"$l_binary"; do
		if [ -x "$l_candidate" ]; then
			printf '%s\n' "$l_candidate"
			return 0
		fi
	done
	return 1
}

report_cached_lint_tool() {
	l_tool=$1
	l_binary=$2
	l_path=$(find_cached_lint_tool "$l_tool" "$l_binary" 2>/dev/null || true)

	if [ -n "$l_path" ]; then
		printf '  %-20s cached (%s)\n' "$l_tool" "$l_path"
	else
		printf '  %-20s not cached\n' "$l_tool"
	fi
}

run_doctor() {
	l_status=0

	printf '%s\n' "Validation profiles: $VALIDATION_PROFILES"
	printf '%s\n' "Change map: $VALIDATION_MAP"
	printf '%s\n' "Core commands:"
	# This is the common offline baseline used by budget, architecture, quick,
	# and full dispatch before optional lint or guest tooling is considered.
	for l_command in \
		awk cat comm cut git grep mktemp sed sort tr uniq wc xargs; do
		if [ -n "$(command_path "$l_command")" ]; then
			report_command_availability "$l_command" "$l_command"
		else
			report_command_availability "$l_command" "$l_command"
			l_status=1
		fi
	done

	printf '%s\n' "Shells:"
	report_command_availability "POSIX sh" sh
	report_command_availability dash dash
	report_command_availability bash bash
	report_command_availability posh posh
	report_command_availability "busybox ash" busybox

	printf '%s\n' "QEMU commands (optional):"
	report_command_availability qemu-system-aarch64 qemu-system-aarch64
	report_command_availability qemu-system-x86_64 qemu-system-x86_64
	report_command_availability qemu-img qemu-img

	printf '%s\n' "ZFS commands (reported only; never invoked):"
	report_command_availability zfs zfs
	report_command_availability zpool zpool

	set_lint_cache_root
	printf 'Cached lint tools under %s:\n' "$LINT_CACHE_ROOT"
	report_cached_lint_tool actionlint actionlint
	report_cached_lint_tool checkbashisms checkbashisms
	report_cached_lint_tool shfmt shfmt
	report_cached_lint_tool codespell codespell
	report_cached_lint_tool shellcheck shellcheck

	for l_entrypoint in \
		tests/run_lint.sh \
		tests/run_shunit_tests.sh \
		tests/run_coverage.sh \
		tests/run_vm_matrix.sh; do
		if [ ! -x "$ZXFER_ROOT/$l_entrypoint" ]; then
			printf 'Missing validation entrypoint: %s\n' "$l_entrypoint" >&2
			l_status=1
		fi
	done

	return "$l_status"
}

append_unique_word() {
	l_list=$1
	l_value=$2

	case " $l_list " in
	*" $l_value "*)
		printf '%s\n' "$l_list"
		;;
	*)
		if [ -n "$l_list" ]; then
			printf '%s %s\n' "$l_list" "$l_value"
		else
			printf '%s\n' "$l_value"
		fi
		;;
	esac
}

append_mapping_field() {
	l_kind=$1
	l_field=$2
	l_changed_path=$3

	[ "$l_field" != - ] || return 0
	l_saved_ifs=$IFS
	IFS=,
	# Repository-controlled map fields intentionally split on commas.
	# shellcheck disable=SC2086
	set -- $l_field
	IFS=$l_saved_ifs
	for l_value in "$@"; do
		[ -n "$l_value" ] || continue
		if [ "$l_value" = @self ]; then
			[ -f "$ZXFER_ROOT/$l_changed_path" ] || continue
			l_value=$l_changed_path
		fi
		case "$l_kind" in
		unit)
			QUICK_UNIT_SUITES=$(append_unique_word "$QUICK_UNIT_SUITES" "$l_value")
			;;
		integration)
			QUICK_INTEGRATION_GROUPS=$(append_unique_word "$QUICK_INTEGRATION_GROUPS" "$l_value")
			;;
		perf)
			QUICK_PERF_CASES=$(append_unique_word "$QUICK_PERF_CASES" "$l_value")
			;;
		docs)
			QUICK_DOC_SURFACES=$(append_unique_word "$QUICK_DOC_SURFACES" "$l_value")
			;;
		esac
	done
}

print_mapping_field() {
	l_label=$1
	l_field=$2
	if [ "$l_field" = - ]; then
		printf '    %-12s %s\n' "$l_label:" "none"
	else
		printf '    %-12s %s\n' "$l_label:" "$l_field"
	fi
}

map_changed_path() {
	l_changed_path=$1
	l_matched=0

	while IFS=$TAB read -r l_pattern l_units l_integration l_perf l_docs l_reason; do
		case "$l_pattern" in
		'' | '#'*)
			continue
			;;
		esac
		# The repository-controlled path key is intentionally a case glob.
		# shellcheck disable=SC2254
		case "$l_changed_path" in
		$l_pattern)
			l_matched=1
			printf '==> quick map: %s\n' "$l_changed_path"
			printf '    matched:     %s\n' "$l_pattern"
			printf '    reason:      %s\n' "$l_reason"
			print_mapping_field unit "$l_units"
			print_mapping_field integration "$l_integration"
			print_mapping_field perf "$l_perf"
			print_mapping_field docs "$l_docs"
			append_mapping_field unit "$l_units" "$l_changed_path"
			append_mapping_field integration "$l_integration" "$l_changed_path"
			append_mapping_field perf "$l_perf" "$l_changed_path"
			append_mapping_field docs "$l_docs" "$l_changed_path"
			break
			;;
		esac
	done <"$VALIDATION_MAP"

	[ "$l_matched" -eq 1 ] || die "No validation mapping matched: $l_changed_path"
}

normalize_changed_path() {
	l_path=$1
	case "$l_path" in
	"$ZXFER_ROOT")
		printf '%s\n' .
		;;
	"$ZXFER_ROOT"/*)
		printf '%s\n' "${l_path#"$ZXFER_ROOT"/}"
		;;
	./*)
		printf '%s\n' "${l_path#./}"
		;;
	*)
		printf '%s\n' "$l_path"
		;;
	esac
}

collect_explicit_paths() {
	for l_path in "$@"; do
		normalize_changed_path "$l_path"
	done | awk 'NF && !seen[$0]++'
}

collect_git_changed_paths() {
	l_raw_paths=
	command -v git >/dev/null 2>&1 || {
		printf '%s\n' "quick validation requires git when no paths are provided." >&2
		return 1
	}
	l_raw_paths=$(
		cd "$ZXFER_ROOT"
		git diff --name-only -- || exit 1
		git diff --cached --name-only -- || exit 1
		git ls-files --others --exclude-standard || exit 1
	) || return 1
	printf '%s\n' "$l_raw_paths" | awk 'NF && !seen[$0]++'
}

print_quick_recommendations() {
	printf '%s\n' "==> quick recommendations (reported, not executed)"
	printf '    integration: %s\n' "${QUICK_INTEGRATION_GROUPS:-none}"
	printf '    perf:        %s\n' "${QUICK_PERF_CASES:-none}"
	printf '    docs:        %s\n' "${QUICK_DOC_SURFACES:-none}"
}

run_quick() {
	l_status=0
	if [ "${1:-}" = -- ]; then
		shift
	fi

	if [ "$#" -gt 0 ]; then
		l_changed_paths=$(collect_explicit_paths "$@")
		printf '%s\n' "==> quick path source: explicit arguments"
	else
		l_changed_paths=$(collect_git_changed_paths)
		printf '%s\n' "==> quick path source: staged, unstaged, and untracked Git paths"
	fi

	if [ -z "$l_changed_paths" ]; then
		printf '%s\n' "No changed paths were found; running the offline budget check only."
	else
		while IFS= read -r l_changed_path; do
			[ -n "$l_changed_path" ] || continue
			map_changed_path "$l_changed_path"
		done <<EOF
$l_changed_paths
EOF
	fi

	print_quick_recommendations
	printf '%s\n' "==> quick offline check: budget"
	if ! "$ZXFER_ROOT/tests/run_lint.sh" budget; then
		l_status=1
	fi

	if [ -n "$QUICK_UNIT_SUITES" ]; then
		require_validate_jobs
		printf '==> quick offline unit suites: %s\n' "$QUICK_UNIT_SUITES"
		# Map values are repository-controlled paths without shell whitespace.
		# shellcheck disable=SC2086
		if ! "$ZXFER_ROOT/tests/run_shunit_tests.sh" \
			--jobs "$ZXFER_VALIDATE_JOBS" $QUICK_UNIT_SUITES; then
			l_status=1
		fi
	else
		printf '%s\n' "==> quick offline unit suites: none selected"
	fi

	return "$l_status"
}

run_bounded_vm() {
	l_vm_profile=local

	case "${1:-}" in
	'') ;;
	smoke | local)
		l_vm_profile=$1
		shift
		;;
	--profile)
		shift
		[ "$#" -gt 0 ] || die "vm --profile requires smoke or local."
		l_vm_profile=$1
		shift
		;;
	--profile=*)
		l_vm_profile=${1#--profile=}
		shift
		;;
	-*) ;;
	*)
		die "vm profile must be smoke or local: $1"
		;;
	esac

	case "$l_vm_profile" in
	smoke | local) ;;
	*)
		die "vm profile must be smoke or local: $l_vm_profile"
		;;
	esac
	for l_arg in "$@"; do
		case "$l_arg" in
		--profile | --profile=*)
			die "Specify the smoke/local VM profile only once, immediately after vm."
			;;
		esac
	done

	"$ZXFER_ROOT/tests/run_vm_matrix.sh" --profile "$l_vm_profile" "$@"
}

run_step() {
	l_step=$1
	shift

	case "$l_step" in
	doctor)
		run_doctor
		;;
	quick-offline)
		run_quick "$@"
		;;
	lint)
		"$ZXFER_ROOT/tests/run_lint.sh"
		;;
	shunit-full)
		require_validate_jobs
		"$ZXFER_ROOT/tests/run_shunit_tests.sh" --jobs "$ZXFER_VALIDATE_JOBS"
		;;
	coverage-full)
		ZXFER_COVERAGE_MODE=bash-xtrace \
			"$ZXFER_ROOT/tests/run_coverage.sh" --enforce
		;;
	portability-lint)
		"$ZXFER_ROOT/tests/run_lint.sh" checkbashisms shfmt shellcheck
		;;
	docs-lint)
		"$ZXFER_ROOT/tests/run_lint.sh" actionlint codespell budget manpages
		;;
	vm-bounded)
		run_bounded_vm "$@"
		;;
	lint-bootstrap)
		"$ZXFER_ROOT/tests/run_lint.sh" --bootstrap-only all
		;;
	*)
		die "Unknown validation step in $VALIDATION_PROFILES: $l_step"
		;;
	esac
}

run_profile() {
	l_selected_profile=$1
	shift
	l_found=0

	while IFS=$TAB read -r l_profile l_step _l_host_risk _l_description; do
		case "$l_profile" in
		'' | '#'*)
			continue
			;;
		esac
		[ "$l_profile" = "$l_selected_profile" ] || continue
		l_found=1
		printf '==> validation [%s]: %s\n' "$l_selected_profile" "$l_step"
		run_step "$l_step" "$@"
	done <"$VALIDATION_PROFILES"

	[ "$l_found" -eq 1 ] || die "Unknown validation profile: $l_selected_profile"
}

[ -r "$VALIDATION_MAP" ] || die "Missing validation map: $VALIDATION_MAP"
[ -r "$VALIDATION_PROFILES" ] || die "Missing validation profiles: $VALIDATION_PROFILES"

case "${1:-}" in
-h | --help)
	print_usage
	exit 0
	;;
--list)
	print_profiles
	exit 0
	;;
'')
	print_usage >&2
	exit 1
	;;
esac

PROFILE=$1
shift
case "$PROFILE" in
quick | vm) ;;
*)
	[ "$#" -eq 0 ] || die "The $PROFILE validation profile does not accept additional arguments."
	;;
esac

run_profile "$PROFILE" "$@"
