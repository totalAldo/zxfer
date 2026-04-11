#!/bin/sh
#
# Library for the VM-backed guest runner.
#

if [ -z "${ZXFER_ROOT:-}" ] || [ ! -d "$ZXFER_ROOT/tests" ]; then
	case "$0" in
	/*)
		ZXFER_ROOT=$(cd "$(dirname "$0")/.." 2>/dev/null && pwd -P) || {
			printf '%s\n' "ERROR: Unable to resolve ZXFER_ROOT for tests/vm/lib.sh" >&2
			exit 1
		}
		;;
	*)
		ZXFER_ROOT=$(cd "${PWD:-.}/$(dirname "$0")/.." 2>/dev/null && pwd -P) || {
			printf '%s\n' "ERROR: Unable to resolve ZXFER_ROOT for tests/vm/lib.sh" >&2
			exit 1
		}
		;;
	esac
fi

# shellcheck source=tests/vm/common.sh
. "$ZXFER_ROOT/tests/vm/common.sh"
# shellcheck source=tests/vm/guest_catalog.sh
. "$ZXFER_ROOT/tests/vm/guest_catalog.sh"
# shellcheck source=tests/vm/backend_qemu.sh
. "$ZXFER_ROOT/tests/vm/backend_qemu.sh"
# shellcheck source=tests/vm/backend_ci_managed.sh
. "$ZXFER_ROOT/tests/vm/backend_ci_managed.sh"

zxfer_vm_reset_state() {
	zxfer_vm_reset_shared_state
	ZXFER_VM_PROFILE=local
	ZXFER_VM_BACKEND=auto
	ZXFER_VM_TEST_LAYER=${ZXFER_VM_TEST_LAYER:-integration}
	ZXFER_VM_GUEST_FILTERS=
	ZXFER_VM_KEEP_GOING=1
	ZXFER_VM_LIST_ONLY=0
	ZXFER_VM_LIST_PROFILES_ONLY=0
	ZXFER_VM_SELECTED_GUESTS=
	ZXFER_VM_PRESERVE_FAILED_GUESTS=0
	ZXFER_VM_JOBS=${ZXFER_VM_JOBS:-1}
	ZXFER_VM_STREAM_GUEST_OUTPUT=${ZXFER_VM_STREAM_GUEST_OUTPUT:-0}
	ZXFER_VM_FAILED_TESTS_ONLY=${ZXFER_VM_FAILED_TESTS_ONLY:-0}
	ZXFER_VM_ONLY_TESTS=${ZXFER_VM_ONLY_TESTS:-}
}

zxfer_vm_print_usage() {
	cat <<'EOF'
Usage: ./tests/run_vm_matrix.sh [option ...]

Run zxfer guest-backed test workflows inside disposable VM guests.

Options:
  --profile name         guest profile to run: smoke, local, full, ci
  --backend name         backend to use: auto, qemu, ci-managed
  --test-layer name      guest test layer to run: integration, shunit2
  --guest name           select a specific guest; repeat to choose multiple guests
  --jobs count           run up to count guests at once (default: 1)
  --artifacts-dir path   override the host artifact root
  --cache-dir path       override the guest-image cache directory
  --stream-guest-output  mirror guest stdout/stderr to the console live
  --only-test name[,name...]
                         run only the named in-guest integration tests; repeat
                         the flag or pass a comma-delimited list
  --failed-tests-only    suppress passing integration-test chatter inside guests
                         and imply --stream-guest-output
  --preserve-failed-guests
                         keep qemu overlays and state directories for failed guests
  --list-guests          print supported guest names and exit
  --list-profiles        print supported profile names and exit
  -h, --help             show this help

Environment:
  ZXFER_VM_ARTIFACT_ROOT  override the host artifact root
  ZXFER_VM_CACHE_DIR      override the guest-image cache directory
  ZXFER_VM_JOBS           override the guest concurrency (default: 1)
  ZXFER_VM_TEST_LAYER     guest test layer to run (default: integration)
  ZXFER_VM_STREAM_GUEST_OUTPUT=1
                          mirror guest stdout/stderr to the console live
  ZXFER_VM_ONLY_TESTS     whitespace- or comma-delimited in-guest integration
                          test names to run through --only-test passthrough
  ZXFER_VM_FAILED_TESTS_ONLY=1
                          suppress passing integration-test chatter inside guests
                          and imply live guest output streaming
  ZXFER_VM_QEMU_AARCH64_EFI
                          override the detected aarch64 UEFI firmware path
  ZXFER_VM_CI_MANAGED_GUEST
                          make backend auto resolve to ci-managed for one guest
EOF
}

zxfer_vm_parse_args() {
	while [ $# -gt 0 ]; do
		case "$1" in
		--profile)
			shift
			[ $# -gt 0 ] || zxfer_vm_die "--profile requires a value"
			ZXFER_VM_PROFILE=$1
			;;
		--backend)
			shift
			[ $# -gt 0 ] || zxfer_vm_die "--backend requires a value"
			ZXFER_VM_BACKEND=$1
			;;
		--test-layer)
			shift
			[ $# -gt 0 ] || zxfer_vm_die "--test-layer requires a value"
			ZXFER_VM_TEST_LAYER=$1
			;;
		--guest)
			shift
			[ $# -gt 0 ] || zxfer_vm_die "--guest requires a value"
			ZXFER_VM_GUEST_FILTERS=$(zxfer_vm_append_word "$ZXFER_VM_GUEST_FILTERS" "$1")
			;;
		--jobs)
			shift
			[ $# -gt 0 ] || zxfer_vm_die "--jobs requires a value"
			ZXFER_VM_JOBS=$1
			;;
		--artifacts-dir)
			shift
			[ $# -gt 0 ] || zxfer_vm_die "--artifacts-dir requires a value"
			ZXFER_VM_ARTIFACT_ROOT=$1
			;;
		--cache-dir)
			shift
			[ $# -gt 0 ] || zxfer_vm_die "--cache-dir requires a value"
			ZXFER_VM_CACHE_DIR=$1
			;;
		--preserve-failed-guests)
			ZXFER_VM_PRESERVE_FAILED_GUESTS=1
			;;
		--stream-guest-output)
			ZXFER_VM_STREAM_GUEST_OUTPUT=1
			;;
		--only-test)
			shift
			[ $# -gt 0 ] || zxfer_vm_die "--only-test requires at least one test name"
			ZXFER_VM_ONLY_TESTS=$(zxfer_vm_append_test_names "$ZXFER_VM_ONLY_TESTS" "$1")
			;;
		--failed-tests-only)
			ZXFER_VM_FAILED_TESTS_ONLY=1
			;;
		--list-guests)
			ZXFER_VM_LIST_ONLY=1
			;;
		--list-profiles)
			ZXFER_VM_LIST_PROFILES_ONLY=1
			;;
		-h | --help)
			zxfer_vm_print_usage
			exit 0
			;;
		*)
			zxfer_vm_die "Unknown argument: $1"
			;;
		esac
		shift
	done
}

zxfer_vm_validate_options() {
	case "$ZXFER_VM_TEST_LAYER" in
	integration | shunit2) ;;
	*)
		zxfer_vm_die "Unsupported test layer: $ZXFER_VM_TEST_LAYER"
		;;
	esac

	zxfer_vm_positive_integer_p "$ZXFER_VM_JOBS" ||
		zxfer_vm_die "--jobs must be a positive integer"

	case "$ZXFER_VM_TEST_LAYER" in
	integration)
		:
		;;
	*)
		[ "${ZXFER_VM_FAILED_TESTS_ONLY:-0}" = "0" ] ||
			zxfer_vm_die "--failed-tests-only is only supported with --test-layer integration"
		[ -z "${ZXFER_VM_ONLY_TESTS:-}" ] ||
			zxfer_vm_die "--only-test is only supported with --test-layer integration"
		;;
	esac
}

zxfer_vm_append_test_names() {
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
		if ! zxfer_vm_list_contains "$l_result" "$l_test"; then
			l_result=$(zxfer_vm_append_word "$l_result" "$l_test")
		fi
	done

	printf '%s\n' "$l_result"
}

zxfer_vm_normalize_requested_tests() {
	ZXFER_VM_ONLY_TESTS=$(zxfer_vm_append_test_names "" "${ZXFER_VM_ONLY_TESTS:-}")
}

zxfer_vm_normalize_output_modes() {
	if [ "$ZXFER_VM_FAILED_TESTS_ONLY" = "1" ]; then
		ZXFER_VM_STREAM_GUEST_OUTPUT=1
	fi
}

zxfer_vm_test_layer_log_label() {
	case "${1:-$ZXFER_VM_TEST_LAYER}" in
	integration)
		printf '%s\n' "integration"
		;;
	shunit2)
		printf '%s\n' "shunit2"
		;;
	*)
		return 1
		;;
	esac
}

zxfer_vm_test_layer_run_label() {
	case "${1:-$ZXFER_VM_TEST_LAYER}" in
	integration)
		printf '%s\n' "integration harness"
		;;
	shunit2)
		printf '%s\n' "shunit2 runner"
		;;
	*)
		return 1
		;;
	esac
}

zxfer_vm_print_guest_list() {
	printf '%s\n' "ubuntu"
	printf '%s\n' "freebsd"
	printf '%s\n' "omnios"
}

zxfer_vm_print_profile_list() {
	printf '%s\n' "smoke"
	printf '%s\n' "local"
	printf '%s\n' "full"
	printf '%s\n' "ci"
}

zxfer_vm_resolve_backend() {
	case "$ZXFER_VM_BACKEND" in
	auto)
		if [ -n "${ZXFER_VM_CI_MANAGED_GUEST:-}" ]; then
			ZXFER_VM_BACKEND=ci-managed
			ZXFER_VM_GUEST_FILTERS=$ZXFER_VM_CI_MANAGED_GUEST
		else
			ZXFER_VM_BACKEND=qemu
		fi
		;;
	qemu | ci-managed) ;;
	*)
		zxfer_vm_die "Unsupported backend: $ZXFER_VM_BACKEND"
		;;
	esac
}

zxfer_vm_select_guests() {
	l_profile_guests=$(zxfer_vm_profile_guests "$ZXFER_VM_PROFILE") ||
		zxfer_vm_die "Unsupported profile: $ZXFER_VM_PROFILE"
	l_selected=

	if [ -n "$ZXFER_VM_GUEST_FILTERS" ]; then
		for l_guest in $ZXFER_VM_GUEST_FILTERS; do
			zxfer_vm_guest_exists "$l_guest" ||
				zxfer_vm_die "Unknown guest: $l_guest"
			zxfer_vm_list_contains "$l_profile_guests" "$l_guest" ||
				zxfer_vm_die "Guest [$l_guest] is not part of profile [$ZXFER_VM_PROFILE]"
			if ! zxfer_vm_list_contains "$l_selected" "$l_guest"; then
				l_selected=$(zxfer_vm_append_word "$l_selected" "$l_guest")
			fi
		done
	else
		l_selected=$l_profile_guests
	fi

	[ -n "$l_selected" ] || zxfer_vm_die "No guests selected for profile [$ZXFER_VM_PROFILE]"
	ZXFER_VM_SELECTED_GUESTS=$l_selected
}

zxfer_vm_backend_validate_selection() {
	case "$ZXFER_VM_BACKEND" in
	ci-managed)
		[ "$(zxfer_vm_count_words "$ZXFER_VM_SELECTED_GUESTS")" -eq 1 ] ||
			zxfer_vm_die "The ci-managed backend can only run one guest at a time"
		;;
	esac
}

zxfer_vm_backend_check_host() {
	case "$ZXFER_VM_BACKEND" in
	qemu)
		zxfer_vm_backend_qemu_check_host
		;;
	ci-managed)
		zxfer_vm_backend_ci_managed_check_host
		;;
	*)
		zxfer_vm_die "Unknown backend check handler: $ZXFER_VM_BACKEND"
		;;
	esac
}

zxfer_vm_backend_run_guest() {
	l_guest=$1
	l_artifact_dir=$2

	case "$ZXFER_VM_BACKEND" in
	qemu)
		zxfer_vm_backend_qemu_run_guest "$l_guest" "$l_artifact_dir"
		;;
	ci-managed)
		zxfer_vm_backend_ci_managed_run_guest "$l_guest" "$l_artifact_dir"
		;;
	*)
		zxfer_vm_die "Unknown backend guest handler: $ZXFER_VM_BACKEND"
		;;
	esac
}

zxfer_vm_integration_harness_extra_args() {
	zxfer_vm_normalize_requested_tests
	l_args=

	if [ "${ZXFER_VM_FAILED_TESTS_ONLY:-0}" = "1" ]; then
		l_args=$(zxfer_vm_append_word "$l_args" "--failed-tests-only")
	fi
	for l_test in $ZXFER_VM_ONLY_TESTS; do
		l_args=$(zxfer_vm_append_word "$l_args" "--only-test")
		l_args=$(zxfer_vm_append_word "$l_args" "$l_test")
	done

	printf '%s\n' "$l_args"
}

zxfer_vm_render_guest_test_script() {
	l_guest=$1
	l_repo_dir=$2
	l_tmpdir=$3
	l_test_layer=${4:-$ZXFER_VM_TEST_LAYER}
	l_guest_shell=
	l_integration_args=
	l_shunit_jobs=

	case "$l_test_layer" in
	integration)
		l_guest_shell=$(zxfer_vm_guest_qemu_shell "$l_guest") ||
			zxfer_vm_die "No guest shell is defined for guest [$l_guest]"
		l_integration_args=$(zxfer_vm_integration_harness_extra_args)
		cat <<EOF
mkdir -p "$l_tmpdir"
cd "$l_repo_dir"
env TMPDIR="$l_tmpdir" \\
	ZXFER_PRESERVE_WORKDIR_ON_FAILURE=1 \\
	$l_guest_shell ./tests/run_integration_zxfer.sh --yes --keep-going${l_integration_args:+ $l_integration_args}
EOF
		;;
	shunit2)
		l_shunit_jobs=$(zxfer_vm_guest_shunit_jobs "$l_guest") ||
			zxfer_vm_die "No shunit2 guest job count is defined for guest [$l_guest]"
		case "$l_guest" in
		omnios)
			cat <<EOF
mkdir -p "$l_tmpdir"
cd "$l_repo_dir"
bash_bin=\$(command -v bash)
wrapper=\$(mktemp "$l_tmpdir/zxfer-bash-posix.XXXXXX")
printf '%s\n' '#!/bin/sh' "exec \"\$bash_bin\" --posix \"\\\$@\"" >"\$wrapper"
chmod 755 "\$wrapper"
export ZXFER_TEST_SHELL="\$wrapper"
env TMPDIR="$l_tmpdir" "\$bash_bin" ./tests/run_shunit_tests.sh --jobs $l_shunit_jobs
EOF
			;;
		*)
			cat <<EOF
mkdir -p "$l_tmpdir"
cd "$l_repo_dir"
env TMPDIR="$l_tmpdir" ./tests/run_shunit_tests.sh --jobs $l_shunit_jobs
EOF
			;;
		esac
		;;
	*)
		zxfer_vm_die "Unsupported test layer: $l_test_layer"
		;;
	esac
}

zxfer_vm_register_background_pid() {
	l_pid=$1

	[ -n "$l_pid" ] || return 0
	if ! zxfer_vm_list_contains "$ZXFER_VM_ACTIVE_BACKGROUND_PIDS" "$l_pid"; then
		ZXFER_VM_ACTIVE_BACKGROUND_PIDS=$(zxfer_vm_append_word "$ZXFER_VM_ACTIVE_BACKGROUND_PIDS" "$l_pid")
	fi
}

zxfer_vm_unregister_background_pid() {
	l_pid=$1

	[ -n "$l_pid" ] || return 0
	ZXFER_VM_ACTIVE_BACKGROUND_PIDS=$(zxfer_vm_remove_word "$ZXFER_VM_ACTIVE_BACKGROUND_PIDS" "$l_pid")
}

zxfer_vm_cleanup_runner_state() {
	if [ -n "${ZXFER_VM_PARALLEL_STATE_DIR:-}" ]; then
		rm -rf "$ZXFER_VM_PARALLEL_STATE_DIR"
		ZXFER_VM_PARALLEL_STATE_DIR=
	fi
}

zxfer_vm_stop_active_background_guests() {
	l_signal=${1:-TERM}

	for l_pid in $ZXFER_VM_ACTIVE_BACKGROUND_PIDS; do
		kill "-$l_signal" "$l_pid" >/dev/null 2>&1 || true
	done
}

zxfer_vm_wait_for_active_background_guests() {
	l_pids=$ZXFER_VM_ACTIVE_BACKGROUND_PIDS

	for l_pid in $l_pids; do
		wait "$l_pid" >/dev/null 2>&1 || true
		zxfer_vm_unregister_background_pid "$l_pid"
	done
}

zxfer_vm_handle_signal() {
	l_signal=$1
	l_status=$(zxfer_vm_signal_exit_status "$l_signal")

	if [ "${ZXFER_VM_SHUTTING_DOWN:-0}" = "1" ]; then
		exit "$l_status"
	fi

	ZXFER_VM_SHUTTING_DOWN=1
	trap - HUP INT TERM
	zxfer_vm_warn "Received $l_signal; stopping active VM guests."
	zxfer_vm_stop_active_background_guests TERM
	zxfer_vm_wait_for_active_background_guests
	zxfer_vm_cleanup_runner_state
	exit "$l_status"
}

zxfer_vm_prepare_guest_run() {
	l_guest=$1

	ZXFER_VM_CURRENT_GUEST=$l_guest
	if zxfer_vm_guest_requires_strict_isolation "$l_guest"; then
		ZXFER_VM_STRICT_ISOLATION_REQUIRED=1
	else
		ZXFER_VM_STRICT_ISOLATION_REQUIRED=0
	fi
}

zxfer_vm_prepare_guest_artifact_dir() {
	l_guest=$1
	l_artifact_dir=$(zxfer_vm_join_path "$ZXFER_VM_ARTIFACT_ROOT" "$l_guest")

	rm -rf "$l_artifact_dir"
	zxfer_vm_mkdir_p "$l_artifact_dir"
	printf '%s\n' "$l_artifact_dir"
}

zxfer_vm_run_selected_guests_serial() {
	l_failed=
	l_status=0

	for l_guest in $ZXFER_VM_SELECTED_GUESTS; do
		zxfer_vm_prepare_guest_run "$l_guest"
		l_artifact_dir=$(zxfer_vm_prepare_guest_artifact_dir "$l_guest")

		if zxfer_vm_backend_run_guest "$l_guest" "$l_artifact_dir"; then
			:
		else
			l_status=$?
			l_failed=$(zxfer_vm_append_word "$l_failed" "$l_guest")
			if [ "$ZXFER_VM_KEEP_GOING" != "1" ]; then
				break
			fi
		fi
	done

	ZXFER_VM_FAILED_GUESTS=$l_failed
	return "$l_status"
}

zxfer_vm_spawn_guest_background() {
	l_guest=$1
	l_status_file=$2
	l_artifact_dir=$3

	ZXFER_VM_LAST_BACKGROUND_PID=
	(
		l_status=0
		zxfer_vm_prepare_guest_run "$l_guest"
		if zxfer_vm_backend_run_guest "$l_guest" "$l_artifact_dir"; then
			l_status=0
		else
			l_status=$?
		fi
		printf '%s\n' "$l_status" >"$l_status_file"
		exit "$l_status"
	) &
	ZXFER_VM_LAST_BACKGROUND_PID=$!
	return 0
}

zxfer_vm_wait_for_guest_background() {
	l_entry=$1
	l_guest=
	l_rest=
	l_pid=
	l_status_file=
	l_status=0

	l_guest=${l_entry%%:*}
	l_rest=${l_entry#*:}
	l_pid=${l_rest%%:*}
	l_status_file=${l_rest#*:}

	if wait "$l_pid"; then
		l_status=0
	else
		l_status=$?
	fi
	if [ -r "$l_status_file" ]; then
		l_status=$(cat "$l_status_file" 2>/dev/null || printf '%s\n' "$l_status")
	fi

	ZXFER_VM_LAST_WAIT_GUEST=$l_guest
	ZXFER_VM_LAST_WAIT_PID=$l_pid
	ZXFER_VM_LAST_WAIT_STATUS=$l_status
	return 0
}

zxfer_vm_run_selected_guests_parallel() {
	l_failed=
	l_status=0
	l_running_entries=
	l_running_count=0
	l_stop_queue=0

	ZXFER_VM_PARALLEL_STATE_DIR=$(mktemp -d -t "zxfer_vm_parallel.XXXXXX") ||
		zxfer_vm_die "Unable to create parallel VM state directory"

	for l_guest in $ZXFER_VM_SELECTED_GUESTS; do
		if [ "$l_stop_queue" = "1" ]; then
			break
		fi

		l_artifact_dir=$(zxfer_vm_prepare_guest_artifact_dir "$l_guest")
		l_status_file=$(zxfer_vm_join_path "$ZXFER_VM_PARALLEL_STATE_DIR" "$l_guest.status")
		zxfer_vm_spawn_guest_background "$l_guest" "$l_status_file" "$l_artifact_dir"
		l_pid=$ZXFER_VM_LAST_BACKGROUND_PID
		zxfer_vm_register_background_pid "$l_pid"
		zxfer_vm_log "==> [$l_guest] started in background (pid $l_pid)"
		l_running_entries=$(zxfer_vm_append_word "$l_running_entries" "$l_guest:$l_pid:$l_status_file")
		l_running_count=$((l_running_count + 1))

		if [ "$l_running_count" -ge "$ZXFER_VM_JOBS" ]; then
			# shellcheck disable=SC2086  # Intentional split of the running entry queue.
			set -- $l_running_entries
			zxfer_vm_wait_for_guest_background "$1"
			shift
			l_running_entries=$*
			l_running_count=$((l_running_count - 1))
			l_wait_guest=$ZXFER_VM_LAST_WAIT_GUEST
			l_wait_pid=$ZXFER_VM_LAST_WAIT_PID
			l_wait_status=$ZXFER_VM_LAST_WAIT_STATUS
			zxfer_vm_unregister_background_pid "$l_wait_pid"
			if [ "$l_wait_status" -ne 0 ]; then
				l_status=$l_wait_status
				l_failed=$(zxfer_vm_append_word "$l_failed" "$l_wait_guest")
				if [ "$ZXFER_VM_KEEP_GOING" != "1" ]; then
					l_stop_queue=1
				fi
			fi
		fi
	done

	for l_entry in $l_running_entries; do
		zxfer_vm_wait_for_guest_background "$l_entry"
		l_wait_guest=$ZXFER_VM_LAST_WAIT_GUEST
		l_wait_pid=$ZXFER_VM_LAST_WAIT_PID
		l_wait_status=$ZXFER_VM_LAST_WAIT_STATUS
		zxfer_vm_unregister_background_pid "$l_wait_pid"
		if [ "$l_wait_status" -ne 0 ]; then
			l_status=$l_wait_status
			l_failed=$(zxfer_vm_append_word "$l_failed" "$l_wait_guest")
		fi
	done

	zxfer_vm_cleanup_runner_state
	ZXFER_VM_FAILED_GUESTS=$l_failed
	return "$l_status"
}

zxfer_vm_guest_requires_strict_isolation() {
	case "$ZXFER_VM_PROFILE/$ZXFER_VM_BACKEND/$1" in
	ci/qemu/ubuntu)
		return 0
		;;
	*)
		return 1
		;;
	esac
}

zxfer_vm_run_selected_guests() {
	if [ "$ZXFER_VM_JOBS" -gt 1 ] &&
		[ "$(zxfer_vm_count_words "$ZXFER_VM_SELECTED_GUESTS")" -gt 1 ]; then
		zxfer_vm_run_selected_guests_parallel
		return $?
	fi

	zxfer_vm_run_selected_guests_serial
	return $?
}

zxfer_vm_print_summary() {
	if [ -n "$ZXFER_VM_FAILED_GUESTS" ]; then
		printf '%s\n' "==> VM matrix summary: failed guests:$ZXFER_VM_FAILED_GUESTS"
	else
		printf '%s\n' "==> VM matrix summary: all selected guests passed"
	fi
}

zxfer_vm_main() {
	l_saved_traps=

	zxfer_vm_reset_state
	zxfer_vm_parse_args "$@"
	zxfer_vm_normalize_requested_tests
	zxfer_vm_validate_options
	zxfer_vm_normalize_output_modes

	if [ "$ZXFER_VM_LIST_ONLY" = "1" ]; then
		zxfer_vm_print_guest_list
		return 0
	fi
	if [ "$ZXFER_VM_LIST_PROFILES_ONLY" = "1" ]; then
		zxfer_vm_print_profile_list
		return 0
	fi

	zxfer_vm_detect_host_platform
	zxfer_vm_set_default_paths
	zxfer_vm_detect_acceleration_capabilities
	zxfer_vm_resolve_backend
	zxfer_vm_select_guests
	zxfer_vm_backend_validate_selection
	zxfer_vm_backend_check_host

	zxfer_vm_mkdir_p "$ZXFER_VM_CACHE_DIR"
	zxfer_vm_mkdir_p "$ZXFER_VM_ARTIFACT_ROOT"

	zxfer_vm_log "==> VM matrix host: $ZXFER_VM_HOST_OS/$ZXFER_VM_HOST_ARCH"
	zxfer_vm_log "==> VM matrix backend: $ZXFER_VM_BACKEND"
	zxfer_vm_log "==> VM matrix profile: $ZXFER_VM_PROFILE"
	zxfer_vm_log "==> VM matrix test layer: $(zxfer_vm_test_layer_log_label)"
	zxfer_vm_log "==> VM matrix guests: $ZXFER_VM_SELECTED_GUESTS"
	zxfer_vm_log "==> VM matrix jobs: $ZXFER_VM_JOBS"
	if [ "$ZXFER_VM_STREAM_GUEST_OUTPUT" = "1" ]; then
		zxfer_vm_log "==> VM matrix guest output: live"
	else
		zxfer_vm_log "==> VM matrix guest output: artifact files only"
	fi
	if [ "$ZXFER_VM_FAILED_TESTS_ONLY" = "1" ]; then
		zxfer_vm_log "==> VM matrix harness mode: failed tests only"
	fi

	l_saved_traps=$(trap)
	trap 'zxfer_vm_handle_signal HUP' HUP
	trap 'zxfer_vm_handle_signal INT' INT
	trap 'zxfer_vm_handle_signal TERM' TERM

	if zxfer_vm_run_selected_guests; then
		eval "$l_saved_traps"
		zxfer_vm_print_summary
		return 0
	else
		l_status=$?
		eval "$l_saved_traps"
		zxfer_vm_print_summary
		return "$l_status"
	fi
}
