#!/bin/sh
#
# Integration tests for zxfer using temporary ZFS pools backed by sparse files.
#

if [ "${ZXFER_RUN_INTEGRATION_SOURCE_ONLY:-0}" != "1" ]; then
	set -eu
fi

if [ -n "${ZXFER_INTEGRATION_TESTS_DIR:-}" ]; then
	INTEGRATION_TESTS_DIR=$ZXFER_INTEGRATION_TESTS_DIR
else
	INTEGRATION_TESTS_DIR=$(dirname "$0")
fi

ZXFER_BIN=${ZXFER_BIN:-"./zxfer"}
SPARSE_SIZE_MB=${SPARSE_SIZE_MB:-256}
OS_NAME=$(uname -s)
MACOS_OPENZFS_ZFS_BIN="/usr/local/zfs/bin/zfs"
ZXFER_CONFIRM_EACH_COMMAND=1
ZXFER_CONFIRM_COMMANDS=${ZXFER_CONFIRM_COMMANDS:-"chmod chown kill ln mkdir mktemp mkfile perl python3 rm truncate zfs zpool"}
ZXFER_CONFIRM_WRAPPER_DIR=""
ZXFER_REAL_BIN=""
ZXFER_SKIP_TESTS=${ZXFER_SKIP_TESTS:-""}
ZXFER_ONLY_TESTS=${ZXFER_ONLY_TESTS:-""}
ZXFER_KEEP_GOING=${ZXFER_KEEP_GOING:-0}
ZXFER_ABORT_REQUESTED=${ZXFER_ABORT_REQUESTED:-0}
ZXFER_LIST_FAILED_TESTS_ONLY=${ZXFER_LIST_FAILED_TESTS_ONLY:-0}
ZXFER_PRESERVE_WORKDIR_ON_FAILURE=${ZXFER_PRESERVE_WORKDIR_ON_FAILURE:-0}
ZXFER_FAILED_TESTS=""
ZXFER_LAST_TEST_STDOUT_CAPTURE=""
ZXFER_LAST_TEST_STDERR_CAPTURE=""
SRC_POOL_CREATED=0
DEST_POOL_CREATED=0
TEST_POOL_MARKER_PROP="org.zxfer:test"
TEST_POOL_WORKDIR_PROP="org.zxfer:workdir"
TEST_POOL_RUN_PROP="org.zxfer:run"
TEST_POOL_VDEV_PROP="org.zxfer:vdev"
TEST_RUN_ID=""

# shellcheck source=tests/helpers/zfs_test_reporting.sh
. "$INTEGRATION_TESTS_DIR/helpers/zfs_test_reporting.sh"
# shellcheck source=tests/helpers/zfs_test_host.sh
. "$INTEGRATION_TESTS_DIR/helpers/zfs_test_host.sh"
# shellcheck source=tests/helpers/zfs_pool_fixtures.sh
. "$INTEGRATION_TESTS_DIR/helpers/zfs_pool_fixtures.sh"
# shellcheck source=tests/helpers/zxfer_remote_fixtures.sh
. "$INTEGRATION_TESTS_DIR/helpers/zxfer_remote_fixtures.sh"
# shellcheck source=tests/helpers/integration_test_registry.sh
. "$INTEGRATION_TESTS_DIR/helpers/integration_test_registry.sh"

has_parallel() {
	if ! command -v parallel >/dev/null 2>&1; then
		return 1
	fi
	return 0
}

print_usage() {
	cat <<'EOF'
usage: ./tests/run_integration_zxfer.sh [--yes] [--skip-test name[,name...]] [--only-test name[,name...]] [--keep-going] [--failed-tests-only] [--help]

By default the integration harness prompts for approval before data-modifying
wrapped external commands (for example zpool, zfs, rm, mkdir, mktemp, chmod,
chown, truncate, mkfile, perl, python3, ln, and kill). Pass --yes to disable
confirmations and run unattended.

Use --skip-test <name> to skip one integration test function. Repeat the flag
to skip more than one test, or set ZXFER_SKIP_TESTS to a whitespace-separated
list of test function names.

Use --only-test <name> to run only one or more named integration test
functions. The flag accepts a single function name or a comma-delimited list,
can be repeated, and can also be set through ZXFER_ONLY_TESTS.

Use --keep-going to continue after a failing integration test and print a
summary of failed test functions at the end.

Use --failed-tests-only to suppress passing test chatter and replay only the
captured output from failing integration tests plus the final summary. This
mode still prints a concise `[N/TOTAL] PASS test_name` or `[N/TOTAL] SKIP
test_name` line for each non-failing test so unattended runs show forward
progress without full chatter.
EOF
}

append_test_names() {
	l_current=$1
	l_spec=$2
	l_result=$l_current
	l_tests=

	[ -n "$l_spec" ] || {
		printf '%s\n' "$l_result"
		return
	}

	l_tests=$(printf '%s\n' "$l_spec" | tr ',' ' ')
	for l_test in $l_tests; do
		[ -n "$l_test" ] || continue
		case " $l_result " in
		*" $l_test "*) ;;
		*)
			if [ -n "$l_result" ]; then
				l_result="$l_result $l_test"
			else
				l_result=$l_test
			fi
			;;
		esac
	done

	printf '%s\n' "$l_result"
}

append_skip_test() {
	l_test=$1

	ZXFER_SKIP_TESTS=$(append_test_names "${ZXFER_SKIP_TESTS:-}" "$l_test")
}

append_only_test() {
	l_test=$1

	ZXFER_ONLY_TESTS=$(append_test_names "${ZXFER_ONLY_TESTS:-}" "$l_test")
}

normalize_requested_test_filters() {
	ZXFER_SKIP_TESTS=$(append_test_names "" "${ZXFER_SKIP_TESTS:-}")
	ZXFER_ONLY_TESTS=$(append_test_names "" "${ZXFER_ONLY_TESTS:-}")
}

parse_args() {
	while [ $# -gt 0 ]; do
		case "$1" in
		--yes)
			ZXFER_CONFIRM_EACH_COMMAND=0
			;;
		--skip-test)
			shift
			if [ $# -eq 0 ] || [ -z "$1" ]; then
				printf '%s\n' "--skip-test requires a test function name." >&2
				print_usage >&2
				exit 2
			fi
			append_skip_test "$1"
			;;
		--only-test)
			shift
			if [ $# -eq 0 ] || [ -z "$1" ]; then
				printf '%s\n' "--only-test requires at least one test function name." >&2
				print_usage >&2
				exit 2
			fi
			append_only_test "$1"
			;;
		--keep-going)
			ZXFER_KEEP_GOING=1
			;;
		--failed-tests-only)
			ZXFER_LIST_FAILED_TESTS_ONLY=1
			;;
		-h | --help)
			print_usage
			exit 0
			;;
		*)
			printf 'Unknown argument: %s\n' "$1" >&2
			print_usage >&2
			exit 2
			;;
		esac
		shift
	done
}

reset_test_output_captures() {
	ZXFER_LAST_TEST_STDOUT_CAPTURE=""
	ZXFER_LAST_TEST_STDERR_CAPTURE=""
}

replay_test_output_captures() {
	l_test_name=${1:-integration_test}

	if [ -n "${ZXFER_LAST_TEST_STDOUT_CAPTURE:-}" ] &&
		[ -s "$ZXFER_LAST_TEST_STDOUT_CAPTURE" ]; then
		printf '%s\n' "--- $l_test_name stdout ---"
		cat "$ZXFER_LAST_TEST_STDOUT_CAPTURE"
	fi
	if [ -n "${ZXFER_LAST_TEST_STDERR_CAPTURE:-}" ] &&
		[ -s "$ZXFER_LAST_TEST_STDERR_CAPTURE" ]; then
		printf '%s\n' "--- $l_test_name stderr ---" >&2
		cat "$ZXFER_LAST_TEST_STDERR_CAPTURE" >&2
	fi
}

cleanup_test_output_captures() {
	if [ -n "${ZXFER_LAST_TEST_STDOUT_CAPTURE:-}" ]; then
		rm -f "$ZXFER_LAST_TEST_STDOUT_CAPTURE" >/dev/null 2>&1 || true
	fi
	if [ -n "${ZXFER_LAST_TEST_STDERR_CAPTURE:-}" ]; then
		rm -f "$ZXFER_LAST_TEST_STDERR_CAPTURE" >/dev/null 2>&1 || true
	fi
	reset_test_output_captures
}

run_test_body() {
	l_func=$1

	reset_test_output_captures
	if ! list_failed_tests_only_enabled; then
		set +e
		(
			"$l_func"
		)
		l_status=$?
		set -e
		return "$l_status"
	fi

	ZXFER_LAST_TEST_STDOUT_CAPTURE=$(mktemp "$WORKDIR/${l_func}.stdout.XXXXXX") ||
		fail "Unable to create stdout capture for integration test $l_func."
	ZXFER_LAST_TEST_STDERR_CAPTURE=$(mktemp "$WORKDIR/${l_func}.stderr.XXXXXX") || {
		rm -f "$ZXFER_LAST_TEST_STDOUT_CAPTURE" >/dev/null 2>&1 || true
		reset_test_output_captures
		fail "Unable to create stderr capture for integration test $l_func."
	}

	set +e
	(
		"$l_func"
	) >"$ZXFER_LAST_TEST_STDOUT_CAPTURE" 2>"$ZXFER_LAST_TEST_STDERR_CAPTURE"
	l_status=$?
	set -e
	return "$l_status"
}

test_is_selected() {
	l_test=$1

	if [ -z "${ZXFER_ONLY_TESTS:-}" ]; then
		return 0
	fi

	case " ${ZXFER_ONLY_TESTS:-} " in
	*" $l_test "*) return 0 ;;
	*) return 1 ;;
	esac
}

validate_requested_only_tests() {
	[ -n "${ZXFER_ONLY_TESTS:-}" ] || return 0

	for l_test in $ZXFER_ONLY_TESTS; do
		l_found=0
		for l_available_test in $TEST_SEQUENCE; do
			if [ "$l_available_test" = "$l_test" ]; then
				l_found=1
				break
			fi
		done
		if [ "$l_found" -ne 1 ]; then
			fail "Unknown integration test requested via --only-test: $l_test"
		fi
	done
}

build_requested_test_sequence() {
	l_sequence=

	normalize_requested_test_filters
	if [ -z "${ZXFER_ONLY_TESTS:-}" ]; then
		printf '%s\n' "$TEST_SEQUENCE"
		return
	fi

	validate_requested_only_tests
	for l_test in $TEST_SEQUENCE; do
		if test_is_selected "$l_test"; then
			l_sequence=$(append_test_names "$l_sequence" "$l_test")
		else
			:
		fi
	done

	printf '%s\n' "$l_sequence"
}

test_is_skipped() {
	l_test=$1

	case " ${ZXFER_SKIP_TESTS:-} " in
	*" $l_test "*) return 0 ;;
	*) return 1 ;;
	esac
}

append_failed_test() {
	l_test=$1

	[ -n "$l_test" ] || return
	case " ${ZXFER_FAILED_TESTS:-} " in
	*" $l_test "*) ;;
	*)
		if [ -n "${ZXFER_FAILED_TESTS:-}" ]; then
			ZXFER_FAILED_TESTS="$ZXFER_FAILED_TESTS $l_test"
		else
			ZXFER_FAILED_TESTS=$l_test
		fi
		;;
	esac
}

write_command_confirmation_wrapper() {
	l_cmd=$1
	l_wrapper_path=$2

	cat >"$l_wrapper_path" <<EOF
#!/bin/sh
l_cmd_name='$l_cmd'
l_wrapper_dir='${ZXFER_CONFIRM_WRAPPER_DIR}'

if [ "\${ZXFER_CONFIRM_EACH_COMMAND:-0}" = "1" ]; then
	if [ ! -r /dev/tty ]; then
		printf '%s\n' "Command confirmation requested but /dev/tty is unavailable." >&2
		exit 1
	fi
	printf '%s\n' "About to run command:" >/dev/tty
	printf '  %s\n' "\$l_cmd_name" >/dev/tty
	for l_arg in "\$@"; do
		printf '  %s\n' "\$l_arg" >/dev/tty
	done
	printf '%s' "Approve? [y/N] " >/dev/tty
	IFS= read -r l_reply </dev/tty || exit 1
	case "\$l_reply" in
	y | Y | yes | YES) ;;
	*)
		printf '%s\n' "Declined command: \$l_cmd_name" >&2
		exit 1
		;;
	esac
fi

l_search_path=
l_oldifs=\$IFS
IFS=:
for l_entry in \$PATH; do
	[ "\$l_entry" = "\$l_wrapper_dir" ] && continue
	if [ "\$l_search_path" = "" ]; then
		l_search_path=\$l_entry
	else
		l_search_path="\$l_search_path:\$l_entry"
	fi
done
IFS=\$l_oldifs
PATH=\$l_search_path
export PATH

l_real_cmd=\$(command -v "\$l_cmd_name" 2>/dev/null || :)
if [ "\$l_real_cmd" = "" ]; then
	printf '%s\n' "Unable to resolve wrapped command \$l_cmd_name." >&2
	exit 127
fi

exec "\$l_real_cmd" "\$@"
EOF
	chmod +x "$l_wrapper_path"
}

write_zxfer_confirmation_wrapper() {
	l_wrapper_path=$1

	cat >"$l_wrapper_path" <<EOF
#!/bin/sh
l_real_zxfer='${ZXFER_REAL_BIN}'
l_wrapper_dir='${ZXFER_CONFIRM_WRAPPER_DIR}'

if [ "\${ZXFER_CONFIRM_EACH_COMMAND:-0}" = "1" ]; then
	if [ ! -r /dev/tty ]; then
		printf '%s\n' "Command confirmation requested but /dev/tty is unavailable." >&2
		exit 1
	fi
	printf '%s\n' "About to run command:" >/dev/tty
	printf '  %s\n' "\$l_real_zxfer" >/dev/tty
	for l_arg in "\$@"; do
		printf '  %s\n' "\$l_arg" >/dev/tty
	done
	printf '%s' "Approve? [y/N] " >/dev/tty
	IFS= read -r l_reply </dev/tty || exit 1
	case "\$l_reply" in
	y | Y | yes | YES) ;;
	*)
		printf '%s\n' "Declined command: \$l_real_zxfer" >&2
		exit 1
		;;
	esac
fi

case ":\${ZXFER_SECURE_PATH:-}:" in
*:"\$l_wrapper_dir":*) ;;
*)
	if [ -n "\${ZXFER_SECURE_PATH:-}" ]; then
		ZXFER_SECURE_PATH="\$l_wrapper_dir:\$ZXFER_SECURE_PATH"
		export ZXFER_SECURE_PATH
	else
		case ":\${ZXFER_SECURE_PATH_APPEND:-}:" in
		*:"\$l_wrapper_dir":*) ;;
		*)
			if [ -n "\${ZXFER_SECURE_PATH_APPEND:-}" ]; then
				ZXFER_SECURE_PATH_APPEND="\$l_wrapper_dir:\$ZXFER_SECURE_PATH_APPEND"
			else
				ZXFER_SECURE_PATH_APPEND="\$l_wrapper_dir"
			fi
			export ZXFER_SECURE_PATH_APPEND
			;;
		esac
	fi
	;;
esac

exec "\$l_real_zxfer" "\$@"
EOF
	chmod +x "$l_wrapper_path"
}

setup_command_confirmation_wrappers() {
	if [ "$ZXFER_CONFIRM_EACH_COMMAND" != "1" ]; then
		return
	fi

	ZXFER_REAL_BIN=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	if [ ! -x "$ZXFER_REAL_BIN" ]; then
		fail "zxfer binary not executable at $ZXFER_REAL_BIN"
	fi

	ZXFER_CONFIRM_WRAPPER_DIR="$WORKDIR/command_confirm_wrappers"
	safe_rm_rf "$ZXFER_CONFIRM_WRAPPER_DIR"
	mkdir -p "$ZXFER_CONFIRM_WRAPPER_DIR"
	export ZXFER_CONFIRM_WRAPPER_DIR ZXFER_CONFIRM_EACH_COMMAND

	for l_cmd in $ZXFER_CONFIRM_COMMANDS; do
		write_command_confirmation_wrapper "$l_cmd" "$ZXFER_CONFIRM_WRAPPER_DIR/$l_cmd"
	done
	write_zxfer_confirmation_wrapper "$ZXFER_CONFIRM_WRAPPER_DIR/zxfer"

	PATH="$ZXFER_CONFIRM_WRAPPER_DIR:$PATH"
	export PATH
	ZXFER_BIN="$ZXFER_CONFIRM_WRAPPER_DIR/zxfer"
	log "Confirmation enabled for data-modifying wrapped commands"
}

get_file_mode_octal() {
	l_path=$1

	if l_mode=$(stat -c '%a' "$l_path" 2>/dev/null); then
		case "$l_mode" in
		'' | *[!0-9]*) ;;
		*)
			printf '%s\n' "$l_mode"
			return 0
			;;
		esac
	fi
	if l_mode=$(stat -f '%OLp' "$l_path" 2>/dev/null); then
		case "$l_mode" in
		'' | *[!0-9]*) ;;
		*)
			printf '%s\n' "$l_mode"
			return 0
			;;
		esac
	fi

	return 1
}

find_backup_metadata_file_for_exact_pair() {
	l_backup_root=$1
	l_source_dataset=$2
	l_destination_dataset=$3

	find "$l_backup_root" -type f -name '.zxfer_backup_info.*' 2>/dev/null |
		while IFS= read -r l_backup_file || [ -n "$l_backup_file" ]; do
			if awk -v source_root="$l_source_dataset" \
				-v destination_root="$l_destination_dataset" '
				BEGIN {
					format_seen = 0
					source_seen = 0
					destination_seen = 0
					root_row_seen = 0
				}
				$0 == "#format_version:2" {
					format_seen = 1
					next
				}
				$0 == "#source_root:" source_root {
					source_seen = 1
					next
				}
				$0 == "#destination_root:" destination_root {
					destination_seen = 1
					next
				}
				index($0, ".\t") == 1 {
					root_row_seen = 1
					next
				}
				END {
					exit(format_seen && source_seen && destination_seen && root_row_seen ? 0 : 1)
				}
			' "$l_backup_file" >/dev/null 2>&1; then
				printf '%s\n' "$l_backup_file"
				break
			fi
		done
}

set_test_dataset_mountpoint() {
	l_dataset=$1
	l_mountpoint=$2

	if ! is_safe_workdir_path "$l_mountpoint"; then
		fail "Refusing to set test dataset mountpoint outside WORKDIR: $l_mountpoint"
	fi
	mkdir -p "$l_mountpoint"
	zfs set mountpoint="$l_mountpoint" "$l_dataset"
	zfs mount "$l_dataset" >/dev/null 2>&1 || true
}

write_progress_logger_script() {
	l_path=$1
	safe_rm_f "$l_path"
	cat >"$l_path" <<'EOF'
#!/bin/sh
log_file=$1
size_arg=$2
title_arg=$3

printf 'size=%s\n' "$size_arg" >>"$log_file"
printf 'title=%s\n' "$title_arg" >>"$log_file"
bytes=$(wc -c | tr -d '[:space:]')
printf 'bytes=%s\n' "$bytes" >>"$log_file"
EOF
	chmod +x "$l_path"
}

find_csh_shell() {
	command -v csh 2>/dev/null || command -v tcsh 2>/dev/null || true
}

write_exec_wrapper_script() {
	l_path=$1
	safe_rm_f "$l_path"
	cat >"$l_path" <<'EOF'
#!/bin/sh
log=${MOCK_WRAPPER_LOG:-}
wrapper_name=$(basename "$0")
[ -n "$log" ] && printf '%s:%s\n' "$wrapper_name" "$*" >>"$log"
exec "$@"
EOF
	chmod +x "$l_path"
}

run_zxfer() {
	log "Running: $ZXFER_BIN $*"
	# Preserve inline env overrides when run_zxfer is invoked as VAR=... run_zxfer.
	ZXFER_BACKUP_DIR=${ZXFER_BACKUP_DIR-} \
		ZXFER_SECURE_PATH=${ZXFER_SECURE_PATH-} \
		ZXFER_SECURE_PATH_APPEND=${ZXFER_SECURE_PATH_APPEND-} \
		ZXFER_SSH_USER_KNOWN_HOSTS_FILE=${ZXFER_SSH_USER_KNOWN_HOSTS_FILE-} \
		ZXFER_SSH_USE_AMBIENT_CONFIG=${ZXFER_SSH_USE_AMBIENT_CONFIG-} \
		MOCK_SSH_LOG=${MOCK_SSH_LOG-} \
		MOCK_SSH_ARGV_LOG=${MOCK_SSH_ARGV_LOG-} \
		MOCK_SSH_REMOTE_SHELL=${MOCK_SSH_REMOTE_SHELL-} \
		MOCK_SSH_CAPABILITY_RESPONSE_FILE=${MOCK_SSH_CAPABILITY_RESPONSE_FILE-} \
		MOCK_SSH_COMMAND_V_TOOL=${MOCK_SSH_COMMAND_V_TOOL-} \
		MOCK_SSH_COMMAND_V_RESULT=${MOCK_SSH_COMMAND_V_RESULT-} \
		MOCK_SSH_FORCE_UNAME=${MOCK_SSH_FORCE_UNAME-} \
		MOCK_SSH_FILTER_PROPERTY=${MOCK_SSH_FILTER_PROPERTY-} \
		MOCK_SSH_MISSING_TOOL=${MOCK_SSH_MISSING_TOOL-} \
		MOCK_WRAPPER_LOG=${MOCK_WRAPPER_LOG-} \
		MOCK_SVCADM_LOG=${MOCK_SVCADM_LOG-} \
		"$ZXFER_BIN" "$@"
}

run_test() {
	l_index=$1
	l_total=$2
	l_func=$3

	if ! list_failed_tests_only_enabled; then
		log "$(printf '[%d/%d] Starting %s%s%s' "$l_index" "$l_total" "$YELLOW" "$l_func" "$RESET")"
	fi
	if test_is_skipped "$l_func"; then
		emit_failed_tests_only_status_line "$l_index" "$l_total" "SKIP" "$l_func"
		if ! list_failed_tests_only_enabled; then
			log "$(printf '%s[%d/%d] SKIP%s %s' "$YELLOW" "$l_index" "$l_total" "$RESET" "$l_func")"
		fi
		return
	fi
	if run_test_body "$l_func"; then
		l_status=0
	else
		l_status=$?
	fi
	if [ "$l_status" -ne 0 ]; then
		if list_failed_tests_only_enabled; then
			log_summary "$(printf '%s[%d/%d] FAIL%s %s (exit %s)' "$RED" "$l_index" "$l_total" "$RESET" "$l_func" "$l_status")"
			replay_test_output_captures "$l_func"
		fi
		cleanup_test_output_captures
		if [ "${ZXFER_ABORT_REQUESTED:-0}" -eq 1 ] || [ "$l_status" -eq 130 ] || [ "$l_status" -eq 143 ]; then
			exit "$l_status"
		fi
		if ! list_failed_tests_only_enabled; then
			log "$(printf '%s[%d/%d] FAIL%s %s (exit %s)' "$RED" "$l_index" "$l_total" "$RESET" "$l_func" "$l_status")"
		fi
		append_failed_test "$l_func"
		if [ "$ZXFER_KEEP_GOING" -eq 1 ]; then
			return
		fi
		exit "$l_status"
	fi
	cleanup_test_output_captures
	if list_failed_tests_only_enabled; then
		emit_failed_tests_only_status_line "$l_index" "$l_total" "PASS" "$l_func"
	else
		log "$(printf '%s[%d/%d] PASS%s %s' "$GREEN" "$l_index" "$l_total" "$RESET" "$l_func")"
	fi
}

assert_usage_error_case() {
	l_desc=$1
	l_expected_msg=$2
	shift 2

	set +e
	l_output=$("$ZXFER_BIN" "$@" 2>&1)
	l_status=$?
	set -e

	if [ "$l_status" -eq 0 ]; then
		fail "$l_desc: expected zxfer to exit with a usage error."
	fi

	if [ "$l_status" -ne 2 ]; then
		fail "$l_desc: expected exit status 2, got $l_status. Output: $l_output"
	fi

	if ! printf '%s\n' "$l_output" | grep -F "Error: $l_expected_msg" >/dev/null 2>&1; then
		fail "$l_desc: usage output missing \"Error: $l_expected_msg\". Output: $l_output"
	fi

	if ! printf '%s\n' "$l_output" | grep -F "usage:" >/dev/null 2>&1; then
		fail "$l_desc: usage output missing usage synopsis. Output: $l_output"
	fi
}

assert_error_case() {
	l_desc=$1
	l_expected_msg=$2
	l_expected_status=${3:-1}
	shift 3

	set +e
	l_output=$("$ZXFER_BIN" "$@" 2>&1)
	l_status=$?
	set -e

	if [ "$l_status" -eq 0 ]; then
		fail "$l_desc: expected zxfer to exit with error status $l_expected_status."
	fi

	if [ "$l_status" -ne "$l_expected_status" ]; then
		fail "$l_desc: expected exit status $l_expected_status, got $l_status. Output: $l_output"
	fi

	if ! printf '%s\n' "$l_output" | grep -F "$l_expected_msg" >/dev/null 2>&1; then
		fail "$l_desc: output missing \"$l_expected_msg\". Output: $l_output"
	fi
}

abort_integration_run() {
	l_signal=$1
	l_status=$2

	ZXFER_ABORT_REQUESTED=1
	trap - INT TERM
	log_summary "Received $l_signal, aborting integration test run."
	exit "$l_status"
}

cleanup() {
	set +e
	l_exit_status=$?
	l_cleanup_ok=1
	l_preserve_reason=""
	l_job_pids=$(ps -o pid= -o ppid= 2>/dev/null | awk -v ppid="$$" '
		$2 == ppid {print $1}
	' || true)
	if [ -n "$l_job_pids" ]; then
		# shellcheck disable=SC2086  # split into individual PIDs on purpose
		kill $l_job_pids 2>/dev/null || true
		# shellcheck disable=SC2086  # wait accepts individual PIDs
		wait $l_job_pids 2>/dev/null || true
	fi
	destroy_test_pool_if_owned "source" "${SRC_POOL:-}" "${SRC_POOL_CREATED:-0}" "${SRC_IMG:-}" || l_cleanup_ok=0
	destroy_test_pool_if_owned "destination" "${DEST_POOL:-}" "${DEST_POOL_CREATED:-0}" "${DEST_IMG:-}" || l_cleanup_ok=0
	if [ "$l_cleanup_ok" -ne 1 ]; then
		l_preserve_reason="test pools were not fully cleaned up"
	elif [ "$l_exit_status" -ne 0 ] && [ "${ZXFER_PRESERVE_WORKDIR_ON_FAILURE:-0}" = "1" ]; then
		l_preserve_reason="the integration run failed and ZXFER_PRESERVE_WORKDIR_ON_FAILURE=1"
	fi

	if [ -z "$l_preserve_reason" ]; then
		[ -n "${WORKDIR:-}" ] && safe_rm_rf "$WORKDIR"
	else
		printf 'WARNING: preserving integration workdir %s because %s.\n' \
			"${WORKDIR:-<unset>}" "$l_preserve_reason" >&2
	fi

	return "$l_exit_status"
}

main() {
	parse_args "$@"
	zxfer_load_integration_test_fragments ||
		fail "Unable to load integration test fragments."
	zxfer_validate_integration_registry ||
		fail "Unable to load the integration test registry."
	TEST_SEQUENCE=$(zxfer_integration_registry_names) ||
		fail "Unable to read the integration test registry sequence."
	PRE_POOL_TEST_SEQUENCE=$(zxfer_integration_registry_pre_pool_names) ||
		fail "Unable to read the integration pre-pool test sequence."
	configure_platform_tool_paths
	require_platform_permissions
	require_cmd zpool
	require_cmd zfs
	require_cmd mktemp

	assert_exists "$ZXFER_BIN" "zxfer binary not found at $ZXFER_BIN"

	WORKDIR=$(mktemp -d -t zxfer_integration.XXXXXX)
	WORKDIR=$(cd -P "$WORKDIR" && pwd)
	TEST_RUN_ID="$(date +%s).$$"
	trap cleanup EXIT
	trap 'abort_integration_run INT 130' INT
	trap 'abort_integration_run TERM 143' TERM
	setup_command_confirmation_wrappers

	for l_pre_pool_test in $PRE_POOL_TEST_SEQUENCE; do
		if run_test_body "$l_pre_pool_test"; then
			cleanup_test_output_captures
		else
			l_status=$?
			log_summary "$(printf '%sPRECHECK FAIL%s %s (exit %s)' "$RED" "$RESET" "$l_pre_pool_test" "$l_status")"
			replay_test_output_captures "$l_pre_pool_test"
			cleanup_test_output_captures
			exit "$l_status"
		fi
	done
	SRC_POOL=$(generate_test_pool_name "src")
	DEST_POOL=$(generate_test_pool_name "dest")
	SRC_MOUNT_ROOT="$WORKDIR/mnt/src"
	DEST_MOUNT_ROOT="$WORKDIR/mnt/dest"
	mkdir -p "$SRC_MOUNT_ROOT" "$DEST_MOUNT_ROOT"

	SRC_IMG="$WORKDIR/${SRC_POOL}.img"
	DEST_IMG="$WORKDIR/${DEST_POOL}.img"
	create_sparse_file "$SRC_IMG" "$SPARSE_SIZE_MB"
	create_sparse_file "$DEST_IMG" "$SPARSE_SIZE_MB"

	log "Creating source pool $SRC_POOL"
	create_test_pool "source" "$SRC_POOL" "$SRC_IMG" "$SRC_MOUNT_ROOT"
	SRC_POOL_CREATED=1
	log "Creating destination pool $DEST_POOL"
	create_test_pool "destination" "$DEST_POOL" "$DEST_IMG" "$DEST_MOUNT_ROOT"
	DEST_POOL_CREATED=1

	TEST_SEQUENCE=$(build_requested_test_sequence)
	# shellcheck disable=SC2086
	set -- $TEST_SEQUENCE
	TOTAL_TESTS=$#

	l_index=1
	for test_func in $TEST_SEQUENCE; do
		run_test "$l_index" "$TOTAL_TESTS" "$test_func"
		l_index=$((l_index + 1))
	done

	if [ -n "${ZXFER_FAILED_TESTS:-}" ]; then
		log_summary "Integration failures: $ZXFER_FAILED_TESTS"
		exit 1
	fi

	log_summary "All integration tests passed."
}

if [ "${ZXFER_RUN_INTEGRATION_SOURCE_ONLY:-0}" = "1" ]; then
	zxfer_load_integration_test_fragments || return 1
else
	main "$@"
fi
