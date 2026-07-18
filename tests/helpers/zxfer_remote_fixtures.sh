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
		l_cmd=$1
		l_shell=${MOCK_SSH_REMOTE_SHELL:-sh}

		if [ -n "${MOCK_SSH_FORCE_UNAME:-}" ] && mock_ssh_is_uname_command "$l_cmd"; then
			printf '%s\n' "$MOCK_SSH_FORCE_UNAME"
			exit 0
		fi

		if mock_ssh_emit_capability_response "$l_cmd"; then
			exit 0
		fi

		if mock_ssh_matches_command_v_override "$l_cmd"; then
			exit 0
		fi

		if [ -n "${MOCK_SSH_FILTER_PROPERTY:-}" ] && printf '%s\n' "$l_cmd" |
			grep -q "^zfs get -Ho property all "; then
			l_pool=${l_cmd#*all }
			if [ -n "$l_pool" ]; then
				zfs get -Ho property all "$l_pool" | grep -v "^${MOCK_SSH_FILTER_PROPERTY}$"
				exit 0
			fi
		fi
		if l_missing_probe_status=$(mock_ssh_matches_missing_tool_probe "$l_cmd"); then
			exit "$l_missing_probe_status"
		fi

		exec "$l_shell" -c "$l_cmd"
	fi

if [ -n "${MOCK_SSH_FORCE_UNAME:-}" ] && mock_ssh_is_uname_command "$1"; then
	printf '%s\n' "$MOCK_SSH_FORCE_UNAME"
	exit 0
fi

if [ -n "${MOCK_SSH_FILTER_PROPERTY:-}" ] &&
	[ $# -ge 6 ] &&
	[ "${1##*/}" = "zfs" ] &&
	[ "$2" = "get" ] &&
	[ "$3" = "-Ho" ] &&
	[ "$4" = "property" ] &&
	[ "$5" = "all" ]; then
	l_pool=$6
	"$1" "$2" "$3" "$4" "$5" "$6" | grep -v "^${MOCK_SSH_FILTER_PROPERTY}$"
	exit $?
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
