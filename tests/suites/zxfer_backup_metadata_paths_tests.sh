#!/bin/sh
# Backup-directory preflight tests for src/zxfer_backup_metadata.sh: the
# root-level symlinks macOS keeps (/tmp, /var) trusted locally and remotely,
# the csh/tcsh login-shell handoff, and the remote dry run's render-failure
# propagation. Run by tests/test_zxfer_backup_metadata.sh under the
# remote-host fixture.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

# Run the -T backup-root check for DIR on backup@example.com.
zxfer_backup_test_check_remote_dir() {
	g_option_k_backup_property_mode=1
	g_option_n_dryrun=0
	g_option_T_target_host="backup@example.com"
	g_backup_storage_root=$1
	zxfer_check_backup_storage_dir_if_needed
}

test_ensure_local_backup_dir_allows_trusted_absolute_root_symlink_components() {
	target_dir=$(mktemp -d /tmp/zxfer-local-trusted.XXXXXX)/subdir
	rm -rf "${target_dir%/subdir}"

	zxfer_ensure_local_backup_dir "$target_dir"
	status=$?

	assertEquals "Trusted top-level system symlink components should not block local backup directory creation, which keeps default /var- or /tmp-backed paths working on macOS." \
		0 "$status"
	assertTrue "Trusted absolute symlink components should still allow the secure backup directory to be created under the symlink target." \
		"[ -d \"$target_dir\" ]"

	rm -rf "${target_dir%/subdir}"
}

test_check_backup_storage_dir_if_needed_remote_handles_csh_remote_login_shell() {
	l_csh_shell=$(find_csh_shell_for_tests)
	if [ "$l_csh_shell" = "" ]; then
		return 0
	fi

	realistic_ssh_bin="$TEST_TMPDIR/fake_ssh_backup_csh_exec"
	realistic_ssh_log="$TEST_TMPDIR/fake_ssh_backup_csh_exec.log"
	target_dir="$TEST_TMPDIR_PHYSICAL/ensure_remote_backup_csh/child"
	create_fake_ssh_join_csh_exec_bin "$realistic_ssh_bin" "$l_csh_shell"

	g_cmd_ssh="$realistic_ssh_bin"
	FAKE_SSH_LOG="$realistic_ssh_log"
	export FAKE_SSH_LOG

	zxfer_backup_test_check_remote_dir "$target_dir"
	status=$?
	unset FAKE_SSH_LOG

	assertEquals "Remote backup directory preparation should succeed through csh/tcsh login shells." \
		0 "$status"
	assertTrue "The csh/tcsh remote handoff should create the requested secure backup directory." \
		"[ -d \"$target_dir\" ]"
	assertEquals "The csh/tcsh backup handoff should receive one physical command line after the host line." \
		2 "$(sed -n '$=' "$realistic_ssh_log")"
}

test_check_backup_storage_dir_if_needed_remote_allows_trusted_absolute_root_symlink_components() {
	target_dir=$(mktemp -d /tmp/zxfer-remote-trusted.XXXXXX)/subdir
	rm -rf "${target_dir%/subdir}"

	(
		zxfer_build_remote_sh_c_command() {
			g_zxfer_remote_sh_c_command_result=$1
			printf '%s\n' "$1"
		}
		zxfer_invoke_ssh_shell_command_for_host() {
			sh -c "$2"
		}
		zxfer_throw_error() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_backup_test_check_remote_dir "$target_dir"
	)
	status=$?

	assertEquals "Trusted top-level system symlink components should not block remote backup directory preparation, which keeps default /var- or /tmp-backed remote roots working on macOS." \
		0 "$status"
	assertTrue "Trusted absolute symlink components should still allow the remote backup directory helper to create the requested directory." \
		"[ -d \"$target_dir\" ]"

	rm -rf "${target_dir%/subdir}"
}

test_check_backup_storage_dir_if_needed_preserves_remote_dry_run_render_failures() {
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=1
		g_option_v_verbose=1
		g_option_T_target_host="target.example doas"
		g_backup_storage_root="/var/db/zxfer"
		zxfer_render_remote_backup_dry_run_shell_command() {
			return 46
		}
		zxfer_check_backup_storage_dir_if_needed
	'

	assertEquals "Remote dry-run backup preflight should preserve prepared renderer failures." \
		46 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_check_backup_storage_dir_if_needed_preserves_remote_dry_run_builder_failures() {
	l_builder_log="$TEST_TMPDIR/remote_backup_dry_run_builder_failure.log"
	: >"$l_builder_log"

	# shellcheck disable=SC2016  # Evaluated by zxfer_test_capture_subshell.
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=1
		g_option_v_verbose=1
		g_option_T_target_host="target.example doas"
		g_backup_storage_root="/var/db/zxfer"
		zxfer_build_remote_backup_dir_prepare_cmd() {
			return 45
		}
		zxfer_render_remote_backup_dry_run_shell_command() {
			printf "%s\n" unexpected-renderer-call >>"$l_builder_log"
		}
		zxfer_check_backup_storage_dir_if_needed
	'

	assertEquals "Remote dry-run backup preflight should preserve directory-script builder failures." \
		45 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "A failed directory-script build must stop before remote rendering." \
		"" "$(cat "$l_builder_log")"
}
