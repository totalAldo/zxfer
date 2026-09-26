#!/bin/sh
# Focused fake executable writers for suites that explicitly opt in.
# shellcheck disable=SC2317,SC2329

# Purpose: Write an ssh stand-in that logs argv to FAKE_SSH_LOG and exits with
# FAKE_SSH_EXIT_STATUS.
# Usage: zxfer_test_write_env_fake_ssh PATH [echo]. By default it prints
# FAKE_SSH_STDOUT and FAKE_SSH_STDERR as given. In echo mode it prints
# FAKE_SSH_STDOUT_OVERRIDE, or nothing when FAKE_SSH_SUPPRESS_STDOUT=1, or else
# its own path and argv, one per line.
zxfer_test_write_env_fake_ssh() {
	l_zxfer_test_fake_ssh_path=$1

	if [ "${2:-}" = echo ]; then
		cat >"$l_zxfer_test_fake_ssh_path" <<'EOF' || return "$?"
#!/bin/sh
if [ -n "${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "$@" >>"$FAKE_SSH_LOG"
fi
if [ -n "${FAKE_SSH_STDOUT_OVERRIDE:-}" ]; then
	printf '%s\n' "$FAKE_SSH_STDOUT_OVERRIDE"
	exit "${FAKE_SSH_EXIT_STATUS:-0}"
fi
if [ "${FAKE_SSH_SUPPRESS_STDOUT:-0}" = "1" ]; then
	exit "${FAKE_SSH_EXIT_STATUS:-0}"
fi
printf '%s\n' "$0"
printf '%s\n' "$@"
exit "${FAKE_SSH_EXIT_STATUS:-0}"
EOF
	else
		cat >"$l_zxfer_test_fake_ssh_path" <<'EOF' || return "$?"
#!/bin/sh
if [ -n "${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "$@" >>"$FAKE_SSH_LOG"
fi
if [ -n "${FAKE_SSH_STDOUT:-}" ] && [ -z "${FAKE_SSH_SUPPRESS_STDOUT:-}" ]; then
	printf '%s' "$FAKE_SSH_STDOUT"
fi
if [ -n "${FAKE_SSH_STDERR:-}" ]; then
	printf '%s' "$FAKE_SSH_STDERR" >&2
fi
exit "${FAKE_SSH_EXIT_STATUS:-0}"
EOF
	fi
	chmod +x "$l_zxfer_test_fake_ssh_path"
}

# Purpose: Print the path of a csh-family shell, or nothing when none exists.
# Usage: l_csh_shell=$(find_csh_shell_for_tests)
find_csh_shell_for_tests() {
	command -v csh 2>/dev/null || command -v tcsh 2>/dev/null || true
}

# Purpose: Write an ssh stand-in that joins the remote argv with spaces, as
# real ssh does, and runs the result locally.
# Usage: create_fake_ssh_join_exec_bin PATH [CSH_SHELL]. The joined command
# runs under /bin/sh -c, or under CSH_SHELL -fc when one is given. Each call
# appends the host and the joined command to FAKE_SSH_LOG when it is set.
create_fake_ssh_join_exec_bin() {
	l_path=$1
	if [ -n "${2:-}" ]; then
		l_remote_shell_cmd="\"$2\" -fc"
	else
		l_remote_shell_cmd="/bin/sh -c"
	fi
	cat >"$l_path" <<EOF || return "$?"
#!/bin/sh
while [ \$# -gt 0 ]; do
	case "\$1" in
	-o | -S | -O)
		shift 2
		;;
	-M | -N | -fN)
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
		break
		;;
	esac
done
host=\$1
shift
remote_cmd=""
for arg in "\$@"; do
	if [ "\$remote_cmd" = "" ]; then
		remote_cmd=\$arg
	else
		remote_cmd="\$remote_cmd \$arg"
	fi
done
if [ -n "\${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "\$host" >>"\$FAKE_SSH_LOG"
	printf '%s\n' "\$remote_cmd" >>"\$FAKE_SSH_LOG"
fi
$l_remote_shell_cmd "\$remote_cmd"
EOF
	chmod +x "$l_path"
}

# Purpose: Write the joining ssh stand-in that runs the remote command under csh.
# Usage: create_fake_ssh_join_csh_exec_bin PATH CSH_SHELL
create_fake_ssh_join_csh_exec_bin() {
	create_fake_ssh_join_exec_bin "$1" "$2"
}

# Purpose: Run a source listing builder and print the command or message it
# published, keeping its status.
# Usage: output=$(zxfer_test_print_source_listing BUILDER)
zxfer_test_print_source_listing() {
	"$1"
	l_test_listing_status=$?
	[ -z "${g_zxfer_source_snapshot_list_cmd_result:-}" ] ||
		printf '%s\n' "$g_zxfer_source_snapshot_list_cmd_result"
	return "$l_test_listing_status"
}
