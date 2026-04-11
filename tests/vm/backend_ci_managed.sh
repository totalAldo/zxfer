#!/bin/sh
#
# CI-managed backend for guest-local VM test execution.
#

zxfer_vm_backend_ci_managed_check_host() {
	return 0
}

zxfer_vm_backend_ci_managed_run_guest() {
	l_guest=$1
	l_artifact_dir=$2
	l_guest_label=$(zxfer_vm_guest_label "$l_guest") || return 1
	l_prepare_script=
	l_guest_shell=
	l_test_script=
	l_guest_tmpdir=
	l_status=0

	l_prepare_script=$(zxfer_vm_guest_prepare_script "$l_guest" "ci-managed" "$ZXFER_VM_TEST_LAYER") ||
		zxfer_vm_die "No ci-managed preparation script is defined for guest [$l_guest]"
	l_guest_shell=$(zxfer_vm_guest_qemu_shell "$l_guest") ||
		zxfer_vm_die "No ci-managed shell is defined for guest [$l_guest]"
	l_guest_tmpdir=$(zxfer_vm_join_path "$l_artifact_dir" "workdir")
	l_test_script=$(zxfer_vm_render_guest_test_script "$l_guest" "$ZXFER_ROOT" "$l_guest_tmpdir")

	zxfer_vm_log "==> [$l_guest_label] running guest-local $(zxfer_vm_test_layer_run_label "$ZXFER_VM_TEST_LAYER") via ci-managed backend"
	zxfer_vm_mkdir_p "$l_artifact_dir"
	rm -rf "$l_guest_tmpdir"
	zxfer_vm_mkdir_p "$l_guest_tmpdir"

	cat <<EOF >"$l_artifact_dir/prepare.sh"
#!/bin/sh
set -eu
$l_prepare_script
EOF
	chmod 700 "$l_artifact_dir/prepare.sh"

	cat <<EOF >"$l_artifact_dir/run-harness.sh"
#!/bin/sh
set -eu
$l_test_script
EOF
	chmod 700 "$l_artifact_dir/run-harness.sh"

	if [ "$ZXFER_VM_STREAM_GUEST_OUTPUT" != "1" ]; then
		zxfer_vm_log "==> [$l_guest_label] guest logs: $l_artifact_dir/prepare.stdout, $l_artifact_dir/prepare.stderr, $l_artifact_dir/harness.stdout, $l_artifact_dir/harness.stderr"
	fi

	zxfer_vm_log "==> [$l_guest_label] running guest preparation"
	if zxfer_vm_run_command_with_captured_output \
		"$l_artifact_dir/prepare.stdout" \
		"$l_artifact_dir/prepare.stderr" \
		"[$l_guest prepare stdout]" \
		"[$l_guest prepare stderr]" \
		"$l_guest_shell" "$l_artifact_dir/prepare.sh"; then
		:
	else
		zxfer_vm_die "Guest preparation failed for [$l_guest]; see $l_artifact_dir/prepare.stderr"
	fi

	zxfer_vm_log "==> [$l_guest_label] running $(zxfer_vm_test_layer_run_label "$ZXFER_VM_TEST_LAYER")"
	if zxfer_vm_run_command_with_captured_output \
		"$l_artifact_dir/harness.stdout" \
		"$l_artifact_dir/harness.stderr" \
		"[$l_guest harness stdout]" \
		"[$l_guest harness stderr]" \
		"$l_guest_shell" "$l_artifact_dir/run-harness.sh"; then
		l_status=0
	else
		l_status=$?
		zxfer_vm_report_guest_command_failure \
			"$l_guest_label" \
			"$(zxfer_vm_test_layer_run_label "$ZXFER_VM_TEST_LAYER")" \
			"$l_status" \
			"$l_artifact_dir/harness.stdout" \
			"$l_artifact_dir/harness.stderr"
	fi

	return "$l_status"
}
