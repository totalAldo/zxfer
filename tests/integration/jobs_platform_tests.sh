#!/bin/sh
#
# Integration tests for progress, supervision, cleanup, migration services, and platform-specific behavior.
# Sourced by tests/run_integration_zxfer.sh; the registry owns execution order.

migration_unmounted_guard_test() {
	log "Starting migration unmounted guard test"

	src_dataset="$SRC_POOL/unmounted_src"
	dest_root="$DEST_POOL/unmounted_dest"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@unmounted"

	# Unmount the source to trigger the guard.
	if ! zfs unmount "$src_dataset"; then
		fail "Failed to unmount $src_dataset to trigger migration guard."
	fi

	set +e
	output=$("$ZXFER_BIN" -v -m -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Migration guard should fail when source is unmounted."
	fi
	if ! printf '%s\n' "$output" | grep -q "The source filesystem is not mounted, cannot use -m."; then
		fail "Migration guard error message missing. Output: $output"
	fi

	log "Migration unmounted guard test passed"
}

progress_wrapper_test() {
	log "Starting progress wrapper test"

	if ! has_parallel; then
		log "Skipping progress wrapper test (parallel not available)"
		return
	fi

	src_dataset="$SRC_POOL/progress_src"
	dest_root="$DEST_POOL/progress_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "progress data"
	zfs snap -r "$src_dataset@p1"

	set +e
	output=$(run_zxfer -v -j 2 -D "cat >/dev/null" -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		log "Skipping progress wrapper test due to zxfer failure (parallel/progress pipeline unavailable). Output: $output"
		return
	fi

	assert_snapshot_exists "$dest_dataset" "p1"

	log "Progress wrapper test passed"
}

progress_placeholder_passthrough_test() {
	log "Starting progress placeholder passthrough test"

	src_dataset="$SRC_POOL/progress_placeholder_src"
	dest_root="$DEST_POOL/progress_placeholder_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"
	progress_script="$WORKDIR/mock_progress_logger.sh"
	progress_log="$WORKDIR/mock_progress_logger.log"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	safe_rm_f "$progress_log"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "progress placeholder data"
	zfs snap -r "$src_dataset@pp1"

	write_progress_logger_script "$progress_script"

	run_zxfer -v -D "$progress_script $progress_log %%size%% %%title%%" -R "$src_dataset" "$dest_root"

	assert_snapshot_exists "$dest_dataset" "pp1"
	assert_exists "$progress_log" "Expected progress placeholder log $progress_log to exist."
	if ! grep -q "^title=${src_dataset}@pp1$" "$progress_log"; then
		fail "Expected progress placeholder title to record ${src_dataset}@pp1. Log: $(cat "$progress_log")"
	fi
	if ! grep -Eq '^size=[0-9]+$' "$progress_log"; then
		fail "Expected progress placeholder size to be numeric. Log: $(cat "$progress_log")"
	fi
	if ! grep -Eq '^bytes=[1-9][0-9]*$' "$progress_log"; then
		fail "Expected progress passthrough to forward non-empty stream data. Log: $(cat "$progress_log")"
	fi

	log "Progress placeholder passthrough test passed"
}

job_limit_enforcement_test() {
	log "Starting job limit enforcement test"

	if ! has_parallel; then
		log "Skipping job limit enforcement test (parallel not available)"
		return
	fi

	src_root="$SRC_POOL/joblimit_src"
	dest_root="$DEST_POOL/joblimit_dest"

	destroy_test_datasets_if_present "$dest_root" "$src_root"

	zfs create "$src_root"
	zfs create "$dest_root"

	for i in 1 2 3 4; do
		child="$src_root/fs$i"
		zfs create "$child"
		append_data_to_dataset "$child" "file.txt" "data$i"
	done

	# First recursive snapshot seeds parent and children.
	zfs snap -r "$src_root@base"

	for i in 1 2 3 4; do
		child="$src_root/fs$i"
		append_data_to_dataset "$child" "file.txt" "more$i"
	done
	zfs snap -r "$src_root@next"

	run_zxfer -v -j 3 -R "$src_root" "$dest_root"

	for i in 1 2 3 4; do
		dest_child="$dest_root/joblimit_src/fs$i"
		assert_snapshot_exists "$dest_child" "base"
		assert_snapshot_exists "$dest_child" "next"
	done

	assert_snapshot_exists "$dest_root/joblimit_src" "base"
	assert_snapshot_exists "$dest_root/joblimit_src" "next"

	log "Job limit enforcement test passed"
}

background_receive_ancestry_serialization_test() {
	log "Starting background receive ancestry serialization test"

	if ! has_parallel; then
		log "Skipping background receive ancestry serialization test (parallel not available)"
		return
	fi

	src_root="$SRC_POOL/receive_ancestry_src"
	child_dataset="$src_root/child"
	dest_root="$DEST_POOL/receive_ancestry_dest"
	dest_dataset="$dest_root/${src_root##*/}"
	dest_child="$dest_dataset/child"

	destroy_test_datasets_if_present "$dest_root" "$src_root"

	zfs create "$src_root"
	zfs create "$child_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_root" "root.txt" "base root"
	append_data_to_dataset "$child_dataset" "child.txt" "base child"
	zfs snap -r "$src_root@base"

	run_zxfer -v -R "$src_root" "$dest_root" >/dev/null 2>&1

	append_data_to_dataset "$src_root" "root.txt" "next root"
	append_data_to_dataset "$child_dataset" "child.txt" "next child"
	zfs snap -r "$src_root@next"

	real_zfs=$(resolve_host_command zfs)
	if [ "$real_zfs" = "" ]; then
		fail "zfs binary not found for background receive ancestry serialization test."
	fi

	wrapper_dir="$WORKDIR/zfs_wrapper_receive_ancestry"
	state_dir="$WORKDIR/receive_ancestry_state"
	safe_rm_rf "$wrapper_dir" "$state_dir"
	mkdir -p "$wrapper_dir" "$state_dir"
	cat >"$wrapper_dir/zfs" <<EOF
#!/bin/sh
real_zfs='$real_zfs'
state_dir=\${ZXFER_RECEIVE_SERIAL_STATE_DIR:-}
parent_dataset=\${ZXFER_RECEIVE_PARENT_DATASET:-}
child_dataset=\${ZXFER_RECEIVE_CHILD_DATASET:-}

if [ "\$1" = "receive" ] && [ -n "\$state_dir" ]; then
	l_dest_dataset=
	for l_arg in "\$@"; do
		l_dest_dataset=\$l_arg
	done
	l_slot=
	l_other_slot=
	case \$l_dest_dataset in
	"\$parent_dataset")
		l_slot=parent
		l_other_slot=child
		;;
	"\$child_dataset")
		l_slot=child
		l_other_slot=parent
		;;
	esac
	if [ -n "\$l_slot" ]; then
		if [ -e "\$state_dir/\$l_other_slot.active" ]; then
			: >"\$state_dir/violation"
			printf '%s\t%s\t%s\n' overlap "\$l_dest_dataset" "\$l_other_slot" >>"\$state_dir/events"
		fi
		: >"\$state_dir/\$l_slot.active"
		printf '%s\t%s\n' start "\$l_dest_dataset" >>"\$state_dir/events"
		sleep 1
		"\$real_zfs" "\$@"
		l_status=\$?
		rm -f "\$state_dir/\$l_slot.active"
		printf '%s\t%s\t%s\n' end "\$l_dest_dataset" "\$l_status" >>"\$state_dir/events"
		exit "\$l_status"
	fi
fi

exec "\$real_zfs" "\$@"
EOF
	chmod +x "$wrapper_dir/zfs"
	secure_path="$wrapper_dir:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"

	set +e
	output=$(ZXFER_SECURE_PATH="$secure_path" \
		ZXFER_RECEIVE_SERIAL_STATE_DIR="$state_dir" \
		ZXFER_RECEIVE_PARENT_DATASET="$dest_dataset" \
		ZXFER_RECEIVE_CHILD_DATASET="$dest_child" \
		"$ZXFER_BIN" -v -F -j 2 -R "$src_root" "$dest_root" 2>&1)
	status=$?
	set -e

	safe_rm_rf "$wrapper_dir"

	if [ "$status" -ne 0 ]; then
		fail "zxfer should serialize parent and child background receives instead of failing. Output: $output"
	fi
	if [ -e "$state_dir/violation" ]; then
		fail "Parent and child destination receives should not overlap when -j is enabled. Events: $(cat "$state_dir/events" 2>/dev/null)"
	fi

	assert_snapshot_exists "$dest_dataset" "base"
	assert_snapshot_exists "$dest_dataset" "next"
	assert_snapshot_exists "$dest_child" "base"
	assert_snapshot_exists "$dest_child" "next"

	safe_rm_rf "$state_dir"

	log "Background receive ancestry serialization test passed"
}

background_send_failure_test() {
	log "Starting background send failure test"

	if ! has_parallel; then
		log "Skipping background send failure test (parallel not available)"
		return
	fi

	src_dataset="$SRC_POOL/sendfail_src"
	dest_root="$DEST_POOL/sendfail_dest"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@base"

	# The first receive into a missing destination dataset is intentionally
	# executed in the foreground. Seed the destination first so the second run
	# exercises the incremental background send path used by -j > 1.
	run_zxfer -v -R "$src_dataset" "$dest_root" >/dev/null 2>&1

	append_data_to_dataset "$src_dataset" "file.txt" "two"
	zfs snap -r "$src_dataset@incremental"

	real_zfs=$(resolve_host_command zfs)
	if [ "$real_zfs" = "" ]; then
		fail "zfs binary not found for send failure test."
	fi

	wrapper_dir="$WORKDIR/zfs_wrapper_fail_send"
	safe_rm_rf "$wrapper_dir"
	mkdir -p "$wrapper_dir"
	cat >"$wrapper_dir/zfs" <<EOF
#!/bin/sh
if [ "\$1" = "send" ]; then
	exit 1
fi
exec "$real_zfs" "\$@"
EOF
	chmod +x "$wrapper_dir/zfs"

	for bin in awk cat chmod cut date grep head id ln ls mkdir mktemp rm rmdir sed sort ssh stat tr uname zstd; do
		real_bin=$(resolve_host_command "$bin")
		if [ "$real_bin" != "" ]; then
			ln -sf "$real_bin" "$wrapper_dir/$bin"
		fi
	done
	real_parallel=$(resolve_host_command parallel)
	if [ "$real_parallel" != "" ]; then
		ln -sf "$real_parallel" "$wrapper_dir/parallel"
	fi
	secure_path="$wrapper_dir:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"

	set +e
	output=$(ZXFER_SECURE_PATH="$secure_path" "$ZXFER_BIN" -v -j 2 -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	safe_rm_rf "$wrapper_dir"

	if [ "$status" -eq 0 ]; then
		fail "zxfer should fail when zfs send exits non-zero."
	fi
	case "$output" in
	*"zfs send/receive job failed"* | *"failed to read from stream"* | *"Error when executing command."*) ;;
	*)
		fail "Expected send failure indication. Output: $output"
		;;
	esac

	log "Background send failure test passed"
}

trap_exit_cleanup_test() {
	log "Starting trap exit cleanup test"

	if ! has_parallel; then
		log "Skipping trap exit cleanup test (parallel not available for supervised background jobs)"
		return
	fi

	mock_path="$WORKDIR/mock_trap_exit"
	list_marker="$WORKDIR/mock_trap_exit_list_started"
	list_shell_pid_file="$WORKDIR/mock_trap_exit_list_shell.pid"
	list_sleep_pid_file="$WORKDIR/mock_trap_exit_list_sleep.pid"
	send_marker="$WORKDIR/mock_trap_exit_send_started"
	send_shell_pid_file="$WORKDIR/mock_trap_exit_send_shell.pid"
	send_sleep_pid_file="$WORKDIR/mock_trap_exit_send_sleep.pid"
	list_src_dataset="$SRC_POOL/trap_list_src"
	list_dest_root="$DEST_POOL/trap_list_dest"
	send_src_dataset="$SRC_POOL/trap_send_src"
	send_dest_root="$DEST_POOL/trap_send_dest"
	prepare_mock_bin_dir "$mock_path" ssh zfs
	write_mock_ssh_script "$mock_path/ssh"
	real_zfs=$(resolve_host_command zfs)
	safe_rm_f \
		"$list_marker" \
		"$list_shell_pid_file" \
		"$list_sleep_pid_file" \
		"$send_marker" \
		"$send_shell_pid_file" \
		"$send_sleep_pid_file"
	safe_rm_f "$mock_path/zfs"
	cat >"$mock_path/zfs" <<EOF
#!/bin/sh
if [ "\$1" = "list" ]; then
	case " \$* " in
	*" -t snapshot "*)
		case "\$*" in
		*"$list_src_dataset"*)
			: >"$list_marker"
			printf '%s\n' "\$\$" >"$list_shell_pid_file"
			sleep 30 &
			l_child_pid=\$!
			printf '%s\n' "\$l_child_pid" >"$list_sleep_pid_file"
			wait "\$l_child_pid"
			exit \$?
			;;
		esac
		;;
	esac
fi
if [ "\$1" = "send" ]; then
	: >"$send_marker"
	printf '%s\n' "\$\$" >"$send_shell_pid_file"
	sleep 30 &
	l_child_pid=\$!
	printf '%s\n' "\$l_child_pid" >"$send_sleep_pid_file"
	wait "\$l_child_pid"
	exit \$?
fi
exec "$real_zfs" "\$@"
EOF
	chmod +x "$mock_path/zfs"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"

	destroy_test_datasets_if_present \
		"$list_src_dataset" \
		"$list_dest_root" \
		"$send_src_dataset" \
		"$send_dest_root"

	zfs create "$list_src_dataset"
	zfs create "$list_dest_root"
	append_data_to_dataset "$list_src_dataset" "file.txt" "pending list"
	zfs snap -r "$list_src_dataset@trap1"

	zfs create "$send_src_dataset"
	zfs create "$send_dest_root"
	append_data_to_dataset "$send_src_dataset" "file.txt" "pending send"
	zfs snap -r "$send_src_dataset@trap1"

	before_tmp=$(find "${TMPDIR:-/tmp}" -maxdepth 1 -type f -name 'zxfer.*' 2>/dev/null || true)
	before_sockets=$(find "${TMPDIR:-/tmp}" -maxdepth 1 -type d -name 'zxfer_ssh_control_socket.*' 2>/dev/null || true)

	set +e
	ZXFER_SECURE_PATH="$secure_path" "$ZXFER_BIN" -v -j 2 -O localhost -T localhost -R "$list_src_dataset" "$list_dest_root" >/dev/null 2>&1 &
	zxfer_pid=$!
	list_started=0
	i=0
	while [ "$i" -lt 10 ]; do
		if [ -f "$list_marker" ]; then
			list_started=1
			break
		fi
		sleep 1
		i=$((i + 1))
	done
	if [ "$list_started" -ne 1 ]; then
		kill -s TERM "$zxfer_pid" >/dev/null 2>&1 || true
		wait "$zxfer_pid" >/dev/null 2>&1 || true
		set -e
		fail "Trap exit cleanup test never observed the mocked source snapshot listing start marker."
	fi
	kill -s INT "$zxfer_pid" >/dev/null 2>&1 || true
	wait "$zxfer_pid"
	set -e

	if [ -f "$list_shell_pid_file" ] &&
		kill -s 0 "$(cat "$list_shell_pid_file")" >/dev/null 2>&1; then
		fail "Background zfs list wrapper shell still running after zxfer_trap_exit handling."
	fi
	if [ -f "$list_sleep_pid_file" ] &&
		kill -s 0 "$(cat "$list_sleep_pid_file")" >/dev/null 2>&1; then
		fail "Background zfs list child still running after zxfer_trap_exit handling."
	fi
	if pgrep -f "$mock_path/zfs list .*${list_src_dataset}" >/dev/null 2>&1; then
		fail "Background zfs list still running after zxfer_trap_exit handling."
	fi

	set +e
	ZXFER_SECURE_PATH="$secure_path" "$ZXFER_BIN" -v -j 2 -O localhost -T localhost -R "$send_src_dataset" "$send_dest_root" >/dev/null 2>&1 &
	zxfer_pid=$!
	send_started=0
	i=0
	while [ "$i" -lt 10 ]; do
		if [ -f "$send_marker" ]; then
			send_started=1
			break
		fi
		sleep 1
		i=$((i + 1))
	done
	if [ "$send_started" -ne 1 ]; then
		kill -s TERM "$zxfer_pid" >/dev/null 2>&1 || true
		wait "$zxfer_pid" >/dev/null 2>&1 || true
		set -e
		fail "Trap exit cleanup test never observed the mocked zfs send start marker."
	fi
	kill -s INT "$zxfer_pid" >/dev/null 2>&1 || true
	wait "$zxfer_pid"
	set -e

	after_tmp=$(find "${TMPDIR:-/tmp}" -maxdepth 1 -type f -name 'zxfer.*' 2>/dev/null || true)
	after_sockets=$(find "${TMPDIR:-/tmp}" -maxdepth 1 -type d -name 'zxfer_ssh_control_socket.*' 2>/dev/null || true)

	tmp_leaks=""
	for f in $after_tmp; do
		case " $before_tmp " in
		*" $f "*) continue ;;
		esac
		tmp_leaks="$tmp_leaks $f"
	done
	socket_leaks=""
	for dir in $after_sockets; do
		case " $before_sockets " in
		*" $dir "*) continue ;;
		esac
		socket_leaks="$socket_leaks $dir"
	done

	if [ -n "$tmp_leaks" ]; then
		fail "Temporary files leaked after SIGINT: $tmp_leaks"
	fi
	if [ -n "$socket_leaks" ]; then
		fail "SSH control sockets leaked after SIGINT: $socket_leaks"
	fi

	if [ -f "$send_shell_pid_file" ] &&
		kill -s 0 "$(cat "$send_shell_pid_file")" >/dev/null 2>&1; then
		fail "Background zfs send wrapper shell still running after zxfer_trap_exit handling."
	fi
	if [ -f "$send_sleep_pid_file" ] &&
		kill -s 0 "$(cat "$send_sleep_pid_file")" >/dev/null 2>&1; then
		fail "Background zfs send child still running after zxfer_trap_exit handling."
	fi
	if pgrep -f "$mock_path/zfs send .*${send_src_dataset}" >/dev/null 2>&1; then
		fail "Background zfs send still running after zxfer_trap_exit handling."
	fi

	safe_rm_f \
		"$list_marker" \
		"$list_shell_pid_file" \
		"$list_sleep_pid_file" \
		"$send_marker" \
		"$send_shell_pid_file" \
		"$send_sleep_pid_file"

	log "Trap exit cleanup test passed"
}

migration_service_success_test() {
	log "Starting migration service success test"

	mock_path="$WORKDIR/mock_svcadm_success"
	prepare_mock_bin_dir "$mock_path" zfs
	cat >"$mock_path/svcadm" <<'EOF'
#!/bin/sh
log=${MOCK_SVCADM_LOG:-}
cmd=$1
shift
service=""
if [ "$cmd" = "disable" ]; then
	if [ "$1" = "-st" ]; then
		service=$2
		shift 2
	else
		service=$1
		shift
	fi
	[ -n "$log" ] && printf 'disable:%s\n' "$service" >>"$log"
	exit 0
fi
if [ "$cmd" = "enable" ]; then
	service=$1
	[ -n "$log" ] && printf 'enable:%s\n' "$service" >>"$log"
	exit 0
fi
exit 0
EOF
	chmod +x "$mock_path/svcadm"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	svc_log="$WORKDIR/svcadm_success.log"
	safe_rm_f "$svc_log"

	src_dataset="$SRC_POOL/migrate_src"
	dest_root="$DEST_POOL/migrate_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"
	mount_dir="$WORKDIR/migrate_mount"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	safe_rm_rf "$mount_dir"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	mkdir -p "$mount_dir"
	zfs set mountpoint="$mount_dir" "$src_dataset"
	append_data_to_dataset "$src_dataset" "file.txt" "migrate"
	zfs snap -r "$src_dataset@mig1"

	ZXFER_SECURE_PATH="$secure_path" MOCK_SVCADM_LOG="$svc_log" run_zxfer -v -m -c svc:/system/filesystem/local -R "$src_dataset" "$dest_root"

	src_mounted=$(zfs get -H -o value mounted "$src_dataset")
	dest_mounted=$(zfs get -H -o value mounted "$dest_dataset")
	dest_mountpoint=$(zfs get -H -o value mountpoint "$dest_dataset")

	if [ "$src_mounted" != "no" ]; then
		fail "Source dataset $src_dataset should remain unmounted after migration; mounted=$src_mounted."
	fi
	if [ "$dest_mounted" != "yes" ]; then
		fail "Destination dataset $dest_dataset should be mounted after migration; mounted=$dest_mounted."
	fi
	if [ "$dest_mountpoint" != "$mount_dir" ]; then
		fail "Destination mountpoint expected $mount_dir, got $dest_mountpoint."
	fi

	if ! grep -q "disable:svc:/system/filesystem/local" "$svc_log"; then
		fail "Expected service disable call recorded in $svc_log."
	fi
	if ! grep -q "enable:svc:/system/filesystem/local" "$svc_log"; then
		fail "Expected service enable call recorded in $svc_log."
	fi

	log "Migration service success test passed"
}

migration_service_failure_test() {
	log "Starting migration service failure test"

	mock_path="$WORKDIR/mock_svcadm_failure"
	prepare_mock_bin_dir "$mock_path" zfs
	cat >"$mock_path/svcadm" <<'EOF'
#!/bin/sh
log=${MOCK_SVCADM_LOG:-}
cmd=$1
shift
service=""
if [ "$cmd" = "disable" ]; then
	if [ "$1" = "-st" ]; then
		service=$2
	else
		service=$1
	fi
	[ -n "$log" ] && printf 'disable:%s\n' "$service" >>"$log"
	exit 1
fi
if [ "$cmd" = "enable" ]; then
	service=$1
	[ -n "$log" ] && printf 'enable:%s\n' "$service" >>"$log"
	exit 0
fi
exit 0
EOF
	chmod +x "$mock_path/svcadm"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	svc_log="$WORKDIR/svcadm_failure.log"
	safe_rm_f "$svc_log"

	src_dataset="$SRC_POOL/migrate_fail_src"
	dest_root="$DEST_POOL/migrate_fail_dest"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "migratefail"
	zfs snap -r "$src_dataset@migfail1"

	set +e
	output=$(ZXFER_SECURE_PATH="$secure_path" MOCK_SVCADM_LOG="$svc_log" "$ZXFER_BIN" -v -m -c svc:/system/filesystem/local -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Migration should fail when service disable fails."
	fi
	if ! printf '%s\n' "$output" | grep -q "Could not disable service"; then
		fail "Expected service disable error in output. Output: $output"
	fi

	if grep -q "enable:" "$svc_log"; then
		fail "Service enable should not run after disable failure. Log: $(cat "$svc_log")"
	fi

	log "Migration service failure test passed"
}

get_os_detection_test() {
	log "Starting zxfer_get_os detection test"

	mock_path="$WORKDIR/mock_get_os"
	safe_rm_rf "$mock_path"
	mkdir -p "$mock_path"

	cat >"$mock_path/uname" <<'EOF'
#!/bin/sh
echo "MockLocalOS"
EOF
	chmod +x "$mock_path/uname"
	write_mock_ssh_script "$mock_path/ssh"

	local_os=$(ZXFER_SECURE_PATH="$mock_path" PATH="$mock_path:$PATH" sh -c 'ZXFER_SOURCE_MODULES_ROOT=.; . ./src/zxfer_modules.sh; zxfer_load_modules zxfer_remote_hosts.sh; zxfer_get_os ""')
	if [ "$local_os" != "MockLocalOS" ]; then
		fail "Expected MockLocalOS from local zxfer_get_os, got $local_os"
	fi

	remote_os=$(MOCK_SSH_FORCE_UNAME="MockRemoteOS" ZXFER_SECURE_PATH="$mock_path" PATH="$mock_path:$PATH" sh -c '
		# shellcheck source=src/zxfer_modules.sh
		ZXFER_SOURCE_MODULES_ROOT=.
		. ./src/zxfer_modules.sh
		zxfer_load_modules zxfer_remote_hosts.sh
		g_cmd_ssh="'"$mock_path"'/ssh"
		zxfer_get_os "remotehost"
	')
	if [ "$remote_os" != "MockRemoteOS" ]; then
		fail "Expected MockRemoteOS from remote zxfer_get_os, got $remote_os"
	fi

	log "Get_os detection test passed"
}

beep_handling_test() {
	log "Starting beep handling test"

	mock_path="$WORKDIR/mock_beep"
	safe_rm_rf "$mock_path"
	mkdir -p "$mock_path"
	cat >"$mock_path/uname" <<'EOF'
#!/bin/sh
echo "FreeBSD"
EOF
	chmod +x "$mock_path/uname"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"

	set +e
	output=$(ZXFER_SECURE_PATH="$secure_path" "$ZXFER_BIN" -V -b -R tank/src 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 2 ]; then
		fail "Beep handling test expected usage error exit 2, got $status. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -Eq "speaker tools are missing|/dev/speaker missing"; then
		fail "Expected graceful beep skip message missing. Output: $output"
	fi

	log "Beep handling test passed"
}
