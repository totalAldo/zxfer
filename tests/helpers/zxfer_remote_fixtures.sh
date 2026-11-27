#!/bin/sh
# Shared local mock-remote fixtures for zxfer integration and performance harnesses.

prepare_mock_bin_dir() {
	l_dir=$1
	shift

	safe_rm_rf "$l_dir"
	mkdir -p "$l_dir"

	for l_bin in "$@"; do
		l_actual=$(resolve_host_command "$l_bin")
		if [ "$l_actual" = "" ]; then
			fail "Required binary $l_bin not found on host; cannot prepare mock PATH."
		fi
		ln -s "$l_actual" "$l_dir/$l_bin"
	done
}

write_passthrough_zstd() {
	l_path=$1
	safe_rm_f "$l_path"
	cat >"$l_path" <<'EOF'
#!/bin/sh
# Minimal zstd stand-in that simply passes stdin to stdout for integration tests.
while [ $# -gt 0 ]; do
	case "$1" in
	-d) shift ;;
	-T*) shift ;;
	-*) shift ;;
	*) break ;;
	esac
done
cat
EOF
	chmod +x "$l_path"
}

write_mock_ssh_script() {
	l_path=$1
	safe_rm_f "$l_path"
	cat >"$l_path" <<'EOF'
#!/bin/sh
# Lightweight ssh stand-in that honors control sockets and runs commands locally.

# Decode only zxfer's fixed, single-quoted long-script transport for matching
# fixture controls. The original rendered command is still executed unchanged.
# Parsing is deliberately non-evaluating so logged or generated shell text can
# never become a second command path inside the test harness.
mock_ssh_decode_chunked_remote_script() {
	printf '%s\n' "$1" | LC_ALL=C awk '
		function reject() { exit 1 }
		NR != 1 { reject() }
		{
			input = $0
			quote = sprintf("%c", 39)
			argument = ""
			argument_count = 0
			have_argument = 0
			in_quote = 0
			for (position = 1; position <= length(input); position++) {
				character = substr(input, position, 1)
				if (in_quote) {
					if (character == quote) in_quote = 0
					else argument = argument character
					continue
				}
				if (character == quote) {
					in_quote = 1
					have_argument = 1
					continue
				}
				if (character == "\\") {
					position++
					if (position > length(input)) reject()
					character = substr(input, position, 1)
					if (character != quote) reject()
					argument = argument character
					have_argument = 1
					continue
				}
				if (character == " " || character == "\t") {
					if (have_argument) {
						arguments[++argument_count] = argument
						argument = ""
						have_argument = 0
					}
					continue
				}
				reject()
			}
			if (in_quote) reject()
			if (have_argument) arguments[++argument_count] = argument
			transport_position = 0
			for (candidate = 1; candidate <= argument_count - 4; candidate++) {
				if (arguments[candidate] == "sh" &&
					arguments[candidate + 1] == "-c" &&
					arguments[candidate + 3] == "sh" &&
					index(arguments[candidate + 2],
						"for l_part do case $l_part in") != 0) {
					if (transport_position != 0) reject()
					transport_position = candidate
				}
			}
			if (transport_position == 0) reject()
			for (position = 1; position < transport_position; position++) {
				if (arguments[position] == "") reject()
			}

			script = ""
			for (position = transport_position + 4;
				position <= argument_count; position++) {
				if (arguments[position] == "n")
					script = script "\n"
				else if (substr(arguments[position], 1, 1) == "d")
					script = script substr(arguments[position], 2)
				else
					reject()
			}
			printf "%s", script
			decoded = 1
		}
		END { if (!decoded) exit 1 }
	'
}

mock_ssh_matches_missing_tool_probe() {
	l_cmd=$1

	[ -n "${MOCK_SSH_MISSING_TOOL:-}" ] || return 1

	case "$l_cmd" in
	*"command -v"*"$MOCK_SSH_MISSING_TOOL"*) printf '%s\n' "10"; return 0 ;;
	*"l_path=\$(command -v"*"$MOCK_SSH_MISSING_TOOL"*) printf '%s\n' "10"; return 0 ;;
	*) return 1 ;;
	esac
}

mock_ssh_emit_capability_response() {
	l_cmd=$1
	l_tools=""

	case "$l_cmd" in
	*"ZXFER_REMOTE_CAPS_V2"*)
		;;
	*)
		return 1
		;;
	esac

	if [ -n "${MOCK_SSH_CAPABILITY_RESPONSE_FILE:-}" ]; then
		cat "$MOCK_SSH_CAPABILITY_RESPONSE_FILE"
		return $?
	fi

	l_tools=$(printf '%s\n' "$l_cmd" | awk '
		found == 0 {
			if ($0 ~ /<<'\''ZXFER_REMOTE_CAPABILITY_TOOLS'\''$/) {
				found = 1
			}
			next
		}
		$0 == "ZXFER_REMOTE_CAPABILITY_TOOLS" {
			exit
		}
		$0 != "" {
			print
		}
	')
	if [ -z "$l_tools" ]; then
		l_tools=$(printf '%s\n' "zfs" "parallel" "cat")
	fi

	printf '%s\n' "ZXFER_REMOTE_CAPS_V2"
	printf 'os\t%s\n' "${MOCK_SSH_FORCE_UNAME:-$(uname 2>/dev/null)}"
	printf '%s\n' "$l_tools" |
	while IFS= read -r l_tool || [ -n "$l_tool" ]; do
		[ -n "$l_tool" ] || continue
		if [ -n "${MOCK_SSH_MISSING_TOOL:-}" ] && [ "$l_tool" = "$MOCK_SSH_MISSING_TOOL" ]; then
			printf 'tool\t%s\t1\t-\n' "$l_tool"
			continue
		fi
		l_path=$(command -v "$l_tool" 2>/dev/null)
		l_status=$?
		if [ "$l_status" -eq 0 ]; then
			printf 'tool\t%s\t0\t%s\n' "$l_tool" "$l_path"
		else
			printf 'tool\t%s\t%s\t-\n' "$l_tool" "$l_status"
		fi
	done
	printf '%s\n' 'end'
	return 0
}

mock_ssh_matches_command_v_override() {
	l_cmd=$1

	[ -n "${MOCK_SSH_COMMAND_V_TOOL:-}" ] || return 1
	[ -n "${MOCK_SSH_COMMAND_V_RESULT:-}" ] || return 1

	case "$l_cmd" in
	*"command -v"*"$MOCK_SSH_COMMAND_V_TOOL"*)
		printf '%s\n' "$MOCK_SSH_COMMAND_V_RESULT"
		return 0
		;;
	*) return 1 ;;
	esac
}

mock_ssh_is_uname_command() {
	l_cmd=$1

	case "$l_cmd" in
	uname | "'uname'" | '"uname"')
		return 0
		;;
	*)
		return 1
		;;
	esac
}

l_socket=""
l_op=""
l_host=""

if [ -n "${MOCK_SSH_ARGV_LOG:-}" ]; then
	printf '%s\n' "---" >>"$MOCK_SSH_ARGV_LOG"
	for l_arg in "$@"; do
		printf 'argv:%s\n' "$l_arg" >>"$MOCK_SSH_ARGV_LOG"
	done
fi

while [ $# -gt 0 ]; do
	case "$1" in
	-M)
		shift
		;;
	-S)
		l_socket=$2
		shift 2
		;;
	-O)
		l_op=$2
		shift 2
		;;
	-o)
		shift 2
		;;
	-p)
		shift 2
		;;
	-f | -n | -N)
		shift
		;;
	--)
		shift
		break
		;;
	-*)
		shift
		;;
	*)
		l_host=$1
		shift
		break
		;;
	esac
done

if [ -n "$l_socket" ]; then
	mkdir -p "$(dirname "$l_socket")" || exit 1
	: >"$l_socket"
fi

if [ "$l_op" = "exit" ]; then
	[ -n "${MOCK_SSH_LOG:-}" ] && printf 'close %s\n' "$l_host" >>"$MOCK_SSH_LOG"
	exit 0
fi

[ $# -gt 0 ] || exit 0

if [ -n "${MOCK_SSH_LOG:-}" ]; then
	if [ $# -eq 1 ]; then
		printf '%s\n' "$1" >>"$MOCK_SSH_LOG"
	else
		printf '%s\n' "$*" >>"$MOCK_SSH_LOG"
	fi
fi

	if [ $# -eq 1 ]; then
		l_original_cmd=$1
		l_match_cmd=$l_original_cmd
		case $l_original_cmd in
		*'for l_part do case $l_part in'*)
			if l_decoded_cmd=$(mock_ssh_decode_chunked_remote_script "$l_original_cmd"); then
				l_match_cmd=$l_decoded_cmd
			fi
			;;
		esac
		l_shell=${MOCK_SSH_REMOTE_SHELL:-sh}

		if [ -n "${MOCK_SSH_FORCE_UNAME:-}" ] && mock_ssh_is_uname_command "$l_match_cmd"; then
			printf '%s\n' "$MOCK_SSH_FORCE_UNAME"
			exit 0
		fi

		if mock_ssh_emit_capability_response "$l_match_cmd"; then
			exit 0
		fi

		if mock_ssh_matches_command_v_override "$l_match_cmd"; then
			exit 0
		fi

		if l_missing_probe_status=$(mock_ssh_matches_missing_tool_probe "$l_match_cmd"); then
			exit "$l_missing_probe_status"
		fi

		exec "$l_shell" -c "$l_original_cmd"
	fi

if [ -n "${MOCK_SSH_FORCE_UNAME:-}" ] && mock_ssh_is_uname_command "$1"; then
	printf '%s\n' "$MOCK_SSH_FORCE_UNAME"
	exit 0
fi

	if [ "${1##*/}" = "sh" ] && [ "${2:-}" = "-c" ]; then
		l_cmd=$3
		shift 3
		if mock_ssh_emit_capability_response "$l_cmd"; then
			exit 0
		fi
		if l_missing_probe_status=$(mock_ssh_matches_missing_tool_probe "$l_cmd"); then
			exit "$l_missing_probe_status"
		fi
		exec sh -c "$l_cmd" "$@"
	fi

exec "$@"
EOF
	chmod +x "$l_path"
}

setup_integration_mock_ssh_env() {
	l_mock_dir=$1
	shift

	prepare_mock_bin_dir "$l_mock_dir" ssh "$@"
	write_mock_ssh_script "$l_mock_dir/ssh"
	printf '%s\n' "$l_mock_dir:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
}

capture_zxfer_temp_snapshot() (
	set +f
	l_primary_tmp=${TMPDIR:-/tmp}
	l_primary_tmp=${l_primary_tmp%/}
	[ -n "$l_primary_tmp" ] || l_primary_tmp=/tmp
	set -- "$l_primary_tmp"
	if [ "$l_primary_tmp" != "/tmp" ] && [ -d /tmp ]; then
		set -- "$l_primary_tmp" /tmp
	fi
	for l_tmp_root in "$@"; do
		[ -d "$l_tmp_root" ] || continue
		for l_candidate in "$l_tmp_root"/zxfer.*; do
			[ -e "$l_candidate" ] || [ -L "$l_candidate" ] || continue
			printf '%s\n' "$l_candidate"
		done
	done
)

assert_no_zxfer_temp_leaks() {
	l_before_snapshot=$1
	l_leak_label=$2
	l_after_snapshot=$(capture_zxfer_temp_snapshot)
	l_leaks=""

	while IFS= read -r l_entry || [ -n "$l_entry" ]; do
		[ -n "$l_entry" ] || continue
		if printf '%s\n' "$l_before_snapshot" | grep -F -x -- "$l_entry" >/dev/null 2>&1; then
			continue
		fi
		l_leaks="${l_leaks:+$l_leaks }$l_entry"
	done <<EOF
$l_after_snapshot
EOF

	if [ -n "$l_leaks" ]; then
		fail "$l_leak_label: $l_leaks"
	fi
}

write_mock_svcadm_script() {
	l_path=$1
	l_disable_status=${2:-0}
	safe_rm_f "$l_path"
	cat >"$l_path" <<EOF
#!/bin/sh
log=\${MOCK_SVCADM_LOG:-}
cmd=\$1
shift
service=""
if [ "\$cmd" = "disable" ]; then
	if [ "\$1" = "-st" ]; then
		service=\$2
	else
		service=\$1
	fi
	[ -n "\$log" ] && printf 'disable:%s\n' "\$service" >>"\$log"
	exit $l_disable_status
fi
if [ "\$cmd" = "enable" ]; then
	service=\$1
	[ -n "\$log" ] && printf 'enable:%s\n' "\$service" >>"\$log"
	exit 0
fi
exit 0
EOF
	chmod +x "$l_path"
}

run_capability_fallback_case() {
	l_case_id=$1
	l_role_flag=$2
	l_host_spec=$3
	l_payload_kind=$4
	l_expect_outcome=$5
	l_snap=$6

	l_marker_rel="${l_case_id}_marker"
	l_marker="$WORKDIR/$l_marker_rel"
	l_mock_path="$WORKDIR/mock_${l_case_id}"
	l_capability_file="$WORKDIR/mock_${l_case_id}.txt"
	l_secure_path=$(setup_integration_mock_ssh_env "$l_mock_path")
	l_zxfer_bin_abs=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	safe_rm_f "$l_marker" "$l_capability_file"

	case "$l_payload_kind" in
	control_whitespace)
		{
			printf '%s\n' "ZXFER_REMOTE_CAPS_V2"
			printf 'os\t%s\n' "MockRemoteOS"
			printf "tool\tzfs\t0\t/tmp/mock_remote_helper.\$(touch %s)/zfs\r\n" "$l_marker_rel"
			printf 'tool\tparallel\t1\t-\n'
			printf 'tool\tcat\t1\t-\n'
		} >"$l_capability_file"
		;;
	malformed)
		{
			printf '%s\n' "ZXFER_REMOTE_CAPS_V2"
			printf 'os\t%s\n' "MockRemoteOS"
			printf 'tool\tzfs\t0\t%s\n' "/remote/bin/zfs"
			printf 'tool\tparallel\t1\t-\n'
			printf '%s\n' "\$(touch $l_marker_rel)"
			printf 'tool\tcat\t1\t-\n'
		} >"$l_capability_file"
		;;
	*)
		fail "Unknown capability fallback payload kind: $l_payload_kind"
		;;
	esac

	l_src_dataset="$SRC_POOL/${l_case_id}_src"
	l_dest_root="$DEST_POOL/${l_case_id}_dest"
	l_dest_dataset="$l_dest_root/${l_src_dataset##*/}"

	destroy_test_datasets_if_present "$l_src_dataset" "$l_dest_root"
	zfs create "$l_src_dataset"
	zfs create "$l_dest_root"
	append_data_to_dataset "$l_src_dataset" "file.txt" "$l_case_id"
	zfs snap -r "$l_src_dataset@$l_snap"

	l_missing_tool=""
	if [ "$l_expect_outcome" = "fail_missing_zfs" ]; then
		l_missing_tool="zfs"
	fi

	set +e
	l_output=$(
		cd "$WORKDIR" &&
			MOCK_SSH_CAPABILITY_RESPONSE_FILE="$l_capability_file" \
				MOCK_SSH_MISSING_TOOL="$l_missing_tool" \
				ZXFER_SECURE_PATH="$l_secure_path" \
				"$l_zxfer_bin_abs" -v "$l_role_flag" "$l_host_spec" -R "$l_src_dataset" "$l_dest_root" 2>&1
	)
	l_status=$?
	set -e

	if [ "$l_expect_outcome" = "fail_missing_zfs" ]; then
		if [ "$l_status" -eq 0 ]; then
			fail "Malformed remote capability responses should fail closed. Output: $l_output"
		fi
		if ! printf '%s\n' "$l_output" | grep -F "Required dependency \"zfs\" not found on host $l_host_spec" >/dev/null 2>&1; then
			fail "Malformed remote capability responses should fall back to a secure remote dependency probe and fail closed. Output: $l_output"
		fi
		assert_dataset_absent "$l_dest_dataset"
	else
		if [ "$l_status" -ne 0 ]; then
			fail "Capability fallback case $l_case_id expected success, got $l_status. Output: $l_output"
		fi
		assert_snapshot_exists "$l_dest_dataset" "$l_snap"
	fi
	if [ -e "$l_marker" ]; then
		fail "Capability response payloads should not execute embedded shell fragments; marker file was created at $l_marker."
	fi

	safe_rm_rf "$l_mock_path"
	safe_rm_f "$l_capability_file"
}

run_remote_parallel_failure_case() {
	l_mode=$1
	l_case_id="remote_${l_mode}_parallel"
	l_mock_path="$WORKDIR/mock_${l_case_id}"
	l_tmpdir="$WORKDIR/${l_case_id}_tmp"
	l_missing_tool=""

	case "$l_mode" in
	missing)
		if ! has_parallel; then
			log "Skipping remote missing parallel origin test (local parallel not available)"
			return 0
		fi
		l_secure_path=$(setup_integration_mock_ssh_env "$l_mock_path" parallel)
		l_missing_tool="parallel"
		l_expected_msg="parallel not found on origin host localhost but -j 2 was requested"
		;;
	incompatible)
		l_secure_path=$(setup_integration_mock_ssh_env "$l_mock_path")
		l_version_banner="parallel from elsewhere"
		l_expected_msg="incompatible parallel mock cannot execute zxfer workloads"
		l_exit_code=64
		;;
	rendered_failure)
		l_secure_path=$(setup_integration_mock_ssh_env "$l_mock_path")
		l_version_banner="GNU parallel 20260122"
		l_expected_msg="remote parallel command failed during source listing"
		l_exit_code=124
		;;
	*)
		fail "Unknown remote parallel failure mode: $l_mode"
		;;
	esac

	if [ "$l_mode" != "missing" ]; then
		cat >"$l_mock_path/parallel" <<EOF
#!/bin/sh
if [ "\$1" = "--will-cite" ]; then
	shift
fi
if [ "\$1" = "--version" ]; then
	printf '%s\n' "$l_version_banner"
	exit 0
fi
printf '%s\n' "$l_expected_msg" >&2
exit $l_exit_code
EOF
		chmod +x "$l_mock_path/parallel"
	fi

	l_src_dataset="$SRC_POOL/${l_case_id}_src"
	l_dest_root="$DEST_POOL/${l_case_id}_dest"

	destroy_test_datasets_if_present "$l_dest_root" "$l_src_dataset"
	safe_rm_rf "$l_tmpdir"
	mkdir -p "$l_tmpdir"

	zfs create "$l_src_dataset"
	zfs create "$l_dest_root"
	l_idx=1
	while [ "$l_idx" -le 16 ]; do
		zfs create "$l_src_dataset/child$l_idx"
		l_idx=$((l_idx + 1))
	done
	zfs snap -r "$l_src_dataset@pfail1"

	set +e
	l_output=$(
		TMPDIR="$l_tmpdir" \
			MOCK_SSH_MISSING_TOOL="$l_missing_tool" \
			ZXFER_SECURE_PATH="$l_secure_path" \
			"$ZXFER_BIN" -v -j 2 -O localhost -R "$l_src_dataset" "$l_dest_root" 2>&1
	)
	l_status=$?
	set -e

	if [ "$l_status" -eq 0 ]; then
		fail "Remote parallel failure case $l_mode should fail closed. Output: $l_output"
	fi
	assert_dataset_absent "$l_dest_root/${l_src_dataset##*/}"
	if ! printf '%s\n' "$l_output" | grep -F "$l_expected_msg" >/dev/null 2>&1; then
		fail "Expected remote parallel diagnostic [$l_expected_msg]. Output: $l_output"
	fi
	if [ "$l_mode" != "missing" ]; then
		if ! printf '%s\n' "$l_output" | grep -F "Failed to retrieve snapshots from the source:" >/dev/null 2>&1; then
			fail "Remote parallel failure ($l_mode) should surface through the source snapshot pipeline. Output: $l_output"
		fi
		for l_forbidden in \
			"requires GNU parallel" \
			"is not GNU parallel" \
			"Falling back to serial source snapshot listing."; do
			if printf '%s\n' "$l_output" | grep -F "$l_forbidden" >/dev/null 2>&1; then
				fail "Remote parallel failure ($l_mode) emitted unexpected diagnostic [$l_forbidden]. Output: $l_output"
			fi
		done
	fi

	safe_rm_rf "$l_mock_path" "$l_tmpdir"
}

run_legacy_backup_rejection_case() {
	l_mode=$1
	l_prefix="legacy_backup"
	l_mock_path=""
	l_secure_path=""
	if [ "$l_mode" = "remote" ]; then
		l_prefix="remote_legacy_backup"
		l_mock_path="$WORKDIR/mock_remote_legacy_backup"
		l_secure_path=$(setup_integration_mock_ssh_env "$l_mock_path")
	fi

	l_backup_dir="$WORKDIR/${l_prefix}_dir"
	l_src_dataset="$SRC_POOL/${l_prefix}_src"
	l_dest_root="$DEST_POOL/${l_prefix}_dest"
	l_restore_root="$DEST_POOL/${l_prefix}_restore"
	l_restore_dataset="$l_restore_root/${l_src_dataset##*/}"

	destroy_test_datasets_if_present "$l_dest_root" "$l_restore_root" "$l_src_dataset"
	safe_rm_rf "$l_backup_dir"

	zfs create "$l_src_dataset"
	zfs create "$l_dest_root"
	zfs set test:prop=legacy "$l_src_dataset"
	append_data_to_dataset "$l_src_dataset" "file.txt" "$l_prefix content"
	zfs snap -r "$l_src_dataset@legacy1"

	if [ "$l_mode" = "remote" ]; then
		ZXFER_BACKUP_DIR="$l_backup_dir" ZXFER_SECURE_PATH="$l_secure_path" run_zxfer -v -k -T localhost -R "$l_src_dataset" "$l_dest_root"
		ZXFER_BACKUP_DIR="$l_backup_dir" ZXFER_SECURE_PATH="$l_secure_path" run_zxfer -v -k -T localhost -R "$l_src_dataset" "$l_restore_root"
	else
		ZXFER_BACKUP_DIR="$l_backup_dir" run_zxfer -v -k -R "$l_src_dataset" "$l_dest_root"
		ZXFER_BACKUP_DIR="$l_backup_dir" run_zxfer -v -k -R "$l_src_dataset" "$l_restore_root"
	fi

	l_restore_backup_file=$(find_backup_metadata_file_for_exact_pair "$l_backup_dir" "$l_src_dataset" "$l_restore_dataset")
	if [ "$l_restore_backup_file" = "" ]; then
		fail "Exact-pair restore backup metadata file not found for $l_mode legacy rejection test."
	fi

	l_src_mount=$(get_mountpoint "$l_src_dataset")
	l_legacy_backup="$l_src_mount/.zxfer_backup_info.${l_src_dataset##*/}"
	mv "$l_restore_backup_file" "$l_legacy_backup"
	chmod 600 "$l_legacy_backup"

	destroy_test_datasets_if_present "$l_dest_root" "$l_restore_root"
	zfs create "$l_dest_root"
	zfs set test:prop=mutated "$l_src_dataset"
	zfs snap -r "$l_src_dataset@legacy2"
	safe_rm_rf "$l_backup_dir"

	set +e
	if [ "$l_mode" = "remote" ]; then
		l_output=$(ZXFER_BACKUP_DIR="$l_backup_dir" ZXFER_SECURE_PATH="$l_secure_path" "$ZXFER_BIN" -v -e -O localhost -R "$l_src_dataset" "$l_restore_root" 2>&1)
	else
		l_output=$(ZXFER_BACKUP_DIR="$l_backup_dir" "$ZXFER_BIN" -v -e -R "$l_src_dataset" "$l_restore_root" 2>&1)
	fi
	l_status=$?
	set -e

	[ -z "$l_mock_path" ] || safe_rm_rf "$l_mock_path"

	if [ "$l_status" -eq 0 ]; then
		fail "Restore ($l_mode) with legacy backup metadata should fail closed. Output: $l_output"
	fi
	if ! printf '%s\n' "$l_output" | grep -q "Cannot find backup property file" >/dev/null 2>&1; then
		fail "Expected missing-backup failure for $l_mode legacy backup metadata. Output: $l_output"
	fi
	if zfs list "$l_restore_dataset" >/dev/null 2>&1; then
		fail "Restore dataset ($l_mode) should not be created when only legacy backup metadata is available."
	fi
}
