#!/bin/sh
#
# Shared mock-toolchain helpers for black-box driving ./zxfer with a canned
# zfs, a mock ssh and counting wrappers. Sourced by shunit2 suites and bench
# runners:
#   tests/test_zxfer_mock_toolchain.sh (self-test)
#   tests/helpers/blackbox.sh (black-box suites)
#   tests/run_microbench.sh / tests/test_zxfer_microbench_budgets.sh
#   tests/run_perf_ab.sh
#
# Standalone by design: no dependency on the integration harness. The only
# host requirements are POSIX sh plus the standard userland already required
# by zxfer itself.
#
# Canned zfs runtime environment (read by the generated mock zfs script):
#   MOCK_ZFS_LOG            append-only argv log; one invocation per line,
#                           argv joined with single spaces, mutating
#                           subcommands prefixed with "MUTATE ", and each
#                           receive followed by an "END receive <dataset>"
#                           line when its stream ends. Unset or empty
#                           disables logging.
#   MOCK_ZFS_FIXTURE_DIR    directory containing "manifest" plus fixture
#                           files for read-only discovery answers.
#   MOCK_ZFS_DEFAULT_STATUS exit status for unmatched read-only commands
#                           (default 1).
#   MOCK_ZFS_STRICT_RECEIVE 1 makes a receive whose stream is empty fail the
#                           way zfs receive does ("cannot receive: failed to
#                           read from stream", status 1, no END line)
#                           instead of accepting it; any other value accepts.
#   MOCK_ZFS_ARGV_LOG       optional path where invocations append bracketed
#                           argument records before executing.
#   MOCK_ZFS_ARGV_VERBS     colon-delimited subcommand filter for
#                           MOCK_ZFS_ARGV_LOG (e.g. "create:set:inherit");
#                           unset or empty logs every subcommand.
#   MOCK_ZFS_ARGV_FORMAT    "inline" (default: "[a] [b] "), "lines"
#                           ("[a]\n[b]\n"), or "counted" ("ARGV N\n[a]\n").
#   MOCK_ZFS_DELAY_RECEIVE_GLOB
#                           optional sh glob matched against receive's last
#                           operand; matching calls sleep
#                           $MOCK_ZFS_DELAY_RECEIVE_SECONDS before running.
#   MOCK_ZFS_RECEIVE_BYTES_LOG
#                           optional path appending "<dataset> <bytes>" per
#                           receive and failing on 0 bytes.
#   MOCK_ZFS_RECEIVE_STREAM_DIR
#                           optional directory where each receive appends a
#                           copy of its stdin under <dataset> ("/" -> "_").
#   MOCK_ZFS_FAIL_AFTER_OUTPUT_DIR
#                           optional directory holding argv, status, and
#                           stderr files: the first matching call emits its
#                           normal output, then prints stderr and exits status.
#
# Manifest format ($MOCK_ZFS_FIXTURE_DIR/manifest), one rule per line:
#   <glob-pattern><TAB><fixture-file><TAB><exit-status>[<TAB>once]
# Static fixture trees may also carry $MOCK_ZFS_FIXTURE_DIR/.exact/ (built by
# zxfer_mockbin_index_manifest) for O(1) exact-key lookup with .exact/.globs
# holding any glob rules.
# The pattern is matched (sh case glob, first match wins) against the argv
# key: all zfs arguments joined with single spaces, e.g.
#   "list -Hr -o name,guid -s creation -t snapshot srcpool/data".
# <fixture-file> is relative to $MOCK_ZFS_FIXTURE_DIR; "-" emits no output.
# <exit-status> defaults to 0 when omitted. Lines starting with "#" and
# blank lines are skipped. "|" alternation is not supported; use one
# pattern per line.
# The optional 4th field "once" marks a consumable rule: after its first
# match the mock rewrites the manifest without that line, so the next lookup
# for the same argv falls through to a later rule. This is the only stateful
# manifest feature (the manifest lives in the per-case scratch state dir, so
# the rewrite is safe); use it to answer the same query differently before
# and after a receive, e.g. a diverged-guid listing healed by convergence.
# Consumable rules are not safe under concurrent mock invocations.
#
# Counting wrapper runtime environment:
#   MOCK_SPAWN_LOG          append-only spawn log; one tool name per line.
#                           Unset logs to /dev/null (no counting overhead).
#
# Socket-aware mock ssh runtime environment (zxfer_mockbin_write_socket_ssh):
#   MOCK_SSH_LOG            append-only "<kind><TAB><argv>" log, one line per
#                           invocation; summarize it with
#                           zxfer_mockbin_ssh_log_rows.
#   MOCK_SSH_HANDSHAKE_S    seconds each new connection sleeps (unset: none).
#   MOCK_SSH_MUX_S          seconds each multiplexed or control call sleeps.
#   MOCK_SSH_REJECT_CONTROL_MASTER
#                           1 rejects -M with status 255 ("ssh: illegal
#                           option -- M") and logs kind "reject".
#   MOCK_SSH_READ_STDIN     1 drains local stdin into `sh -c` like OpenSSH.
#   MOCK_SSH_CAPABILITY_RESPONSE_FILE
#                           optional file printed instead of running a remote
#                           ZXFER_REMOTE_CAPS_V2 capability probe.
#
# Fault injection, shared by the canned zfs, the socket-aware mock ssh and
# every counting wrapper (all unset: no counting and no overhead):
#   MOCK_FAIL_TOOL          which mock counts its calls: zfs (default), ssh,
#                           or a counting wrapper's tool name.
#   MOCK_FAIL_CALL          N >= 1: the Nth counted call prints the failure
#                           stderr and exits with the failure status instead
#                           of answering (a receive reads no stream, the ssh
#                           runs no command). 0 counts calls and never fails,
#                           so a clean run numbers its calls the same way.
#   MOCK_FAIL_MATCH         optional sh glob matched against the space-joined
#                           argv; when set, only matching calls are counted,
#                           so "the Kth call of this argv" stays the same call
#                           when concurrent calls (background discovery, -j)
#                           start in a different order.
#   MOCK_FAIL_DIR           existing directory for the counter, empty before
#                           each run: counted call n creates the file "n"
#                           holding its argv (noclobber creation is atomic, so
#                           concurrent calls never share a number). Required
#                           whenever MOCK_FAIL_CALL is set.
#   MOCK_FAIL_STDERR        stderr text of the failing call; set but empty
#                           prints nothing. Defaults: zfs "cannot open
#                           '<last argument>': I/O error" (an operational
#                           error, never "dataset does not exist", which
#                           zxfer reads as absence), ssh "Connection to
#                           <host> closed by remote host.", a wrapper
#                           "<tool>: mock failure".
#   MOCK_FAIL_STATUS        exit status of the failing call (default 1; 255
#                           for ssh).
# The failing call appends "FAIL <n> <tool> <argv>" (newlines in the argv
# become spaces) to MOCK_ZFS_LOG when that is set, so its position orders it
# against the zfs calls; the mock ssh also logs it to MOCK_SSH_LOG with the
# kind "fail". A misconfigured counter (no MOCK_FAIL_DIR, a non-numeric
# MOCK_FAIL_CALL) makes every counted call exit 125 with a stderr note.
#
# shellcheck shell=sh

# Fixed fixture topology used by zxfer_mockbin_build_fixture_tree. Constants
# so suites and bench runners reference one source of truth for the dataset
# names the manifests are keyed on.
ZXFER_MOCKBIN_SOURCE_ROOT="srcpool/data"
ZXFER_MOCKBIN_DEST_ROOT="dstpool/back"
ZXFER_MOCKBIN_DEST_MAPPED_ROOT="dstpool/back/data"
ZXFER_MOCKBIN_SYSTEM_SECURE_PATH="/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"

# Purpose: Resolve one host tool to an absolute path from the caller's PATH.
# Usage: Internal lookup for zxfer_mockbin_prepare_dir and counting-wrapper
# callers that need the real binary location before shadowing it.
# Returns: Absolute path on stdout, or status 1 with a stderr note.
zxfer_mockbin_resolve_host_tool() {
	l_mockbin_tool=$1

	l_mockbin_resolved=$(command -v "$l_mockbin_tool" 2>/dev/null || :)
	case "$l_mockbin_resolved" in
	/*)
		printf '%s\n' "$l_mockbin_resolved"
		return 0
		;;
	esac

	printf 'mock_toolchain_helper: required host tool not found: %s\n' \
		"$l_mockbin_tool" >&2
	return 1
}

# Purpose: Symlink real host tools into a mock bin directory so zxfer can
# resolve them through a secure PATH that leads with that directory.
# Usage: zxfer_mockbin_prepare_dir <dir> <real-tool>... — creates <dir> when
# missing and replaces existing entries, so it is safe to call repeatedly or
# after dropping a canned zfs into the same directory.
zxfer_mockbin_prepare_dir() {
	l_mockbin_dir=$1
	shift

	mkdir -p "$l_mockbin_dir" || return 1

	for l_mockbin_entry in "$@"; do
		l_mockbin_real=$(zxfer_mockbin_resolve_host_tool "$l_mockbin_entry") ||
			return 1
		ln -sf "$l_mockbin_real" "$l_mockbin_dir/$l_mockbin_entry" || return 1
	done
}

# Purpose: Print the fault-injection functions every generated mock embeds
# (the MOCK_FAIL_* contract in the header).
# Usage: zxfer_mockbin_emit_fail_injection, inside a generated script. The
# script then runs `if mock_fail_claim TOOL KEY; then ...; mock_fail_now TOOL
# STATUS STDERR KEY; fi` before it acts: mock_fail_claim returns 0 only for
# the call that must fail, and mock_fail_now logs it and exits.
zxfer_mockbin_emit_fail_injection() {
	cat <<'EOF'
# Fault injection; see MOCK_FAIL_* in tests/mock_toolchain_helper.sh.
mock_fail_claim() {
	[ -n "${MOCK_FAIL_CALL:-}" ] || return 1
	[ "${MOCK_FAIL_TOOL:-zfs}" = "$1" ] || return 1
	if [ -n "${MOCK_FAIL_MATCH:-}" ]; then
		# shellcheck disable=SC2254  # the match is a glob on purpose
		case "$2" in
		$MOCK_FAIL_MATCH) ;;
		*) return 1 ;;
		esac
	fi
	case $MOCK_FAIL_CALL in
	*[!0123456789]*)
		printf 'mock %s: MOCK_FAIL_CALL must be a number: %s\n' "$1" \
			"$MOCK_FAIL_CALL" >&2
		exit 125
		;;
	esac
	if [ -z "${MOCK_FAIL_DIR:-}" ] || [ ! -d "$MOCK_FAIL_DIR" ]; then
		printf 'mock %s: MOCK_FAIL_CALL needs an existing MOCK_FAIL_DIR\n' "$1" >&2
		exit 125
	fi
	# Claim the lowest free number: a noclobber redirection creates the file
	# with O_EXCL, so concurrent calls never share one. ".last" only says
	# where to start looking; a stale or torn read just starts lower. printf,
	# not the special builtin :, because a failed redirection on : exits dash.
	mock_fail_n=""
	[ ! -f "$MOCK_FAIL_DIR/.last" ] ||
		IFS= read -r mock_fail_n <"$MOCK_FAIL_DIR/.last" || :
	case $mock_fail_n in
	'' | *[!0123456789]*) mock_fail_n=0 ;;
	esac
	set -C
	while :; do
		mock_fail_n=$((mock_fail_n + 1))
		if { printf '%s\n' "$2" >"$MOCK_FAIL_DIR/$mock_fail_n"; } 2>/dev/null; then
			break
		fi
		if [ ! -e "$MOCK_FAIL_DIR/$mock_fail_n" ]; then
			set +C
			printf 'mock %s: cannot claim a call number in %s\n' "$1" \
				"$MOCK_FAIL_DIR" >&2
			exit 125
		fi
	done
	set +C
	{ printf '%s\n' "$mock_fail_n" >|"$MOCK_FAIL_DIR/.last"; } 2>/dev/null || :
	[ "$MOCK_FAIL_CALL" -ne 0 ] && [ "$mock_fail_n" -eq "$MOCK_FAIL_CALL" ]
}

mock_fail_now() {
	mock_fail_nl='
'
	mock_fail_line="FAIL $mock_fail_n $1 $4"
	while :; do
		case $mock_fail_line in
		*"$mock_fail_nl"*)
			mock_fail_line="${mock_fail_line%%"$mock_fail_nl"*} ${mock_fail_line#*"$mock_fail_nl"}"
			;;
		*) break ;;
		esac
	done
	[ -z "${MOCK_ZFS_LOG:-}" ] || printf '%s\n' "$mock_fail_line" >>"$MOCK_ZFS_LOG"
	if [ -n "${MOCK_FAIL_STDERR+set}" ]; then
		mock_fail_text=$MOCK_FAIL_STDERR
	else
		mock_fail_text=$3
	fi
	[ -z "$mock_fail_text" ] || printf '%s\n' "$mock_fail_text" >&2
	exit "${MOCK_FAIL_STATUS:-$2}"
}
EOF
}

# Purpose: Write the canned zfs mock that logs every invocation and answers
# read-only discovery from a manifest-driven fixture directory.
# Usage: zxfer_mockbin_write_canned_zfs <path> — typically
# <mockdir>/zfs so the secure PATH resolves it ahead of any real zfs.
# Side effects: Overwrites <path> and marks it executable. Behavior of the
# generated script:
#   - every argv is appended to $MOCK_ZFS_LOG (space-joined, one line);
#   - destroy/rollback/create/set/inherit/snapshot/rename/clone/promote/
#     hold/release/bookmark are logged with a "MUTATE " prefix and exit per
#     manifest (default 0);
#   - receive/recv is logged without the MUTATE prefix, consumes stdin to
#     /dev/null, logs "END receive <dataset>" (its last argument) once the
#     stream ends, and exits per manifest (default 0); with
#     MOCK_ZFS_STRICT_RECEIVE=1 an empty stream fails instead;
#   - send emits matched fixture bytes, or a one-line dummy stream when
#     unmatched, and exits per manifest (default 0);
#   - all other subcommands are read-only: matched rules emit the fixture
#     and exit with the rule status; unmatched commands print a stderr note
#     and exit $MOCK_ZFS_DEFAULT_STATUS (default 1);
#   - with MOCK_FAIL_TOOL unset or zfs, the MOCK_FAIL_CALL-th call fails
#     before any of the above (logged as "FAIL <n> zfs <argv>").
zxfer_mockbin_write_canned_zfs() {
	l_mockbin_zfs_path=$1

	{
		cat <<'EOF'
#!/bin/sh
# Canned zfs mock generated by tests/mock_toolchain_helper.sh.
# See that helper's header comment for the manifest and env contract.

mock_key=$*
mock_fixture=""
mock_status=0

EOF
		zxfer_mockbin_emit_fail_injection
		cat <<'EOF'

mock_log_line() {
	if [ -n "${MOCK_ZFS_LOG:-}" ]; then
		printf '%s\n' "$1" >>"$MOCK_ZFS_LOG"
	fi
}

mock_encode_key() {
	mock_enc_rest=$1
	mock_enc_out=""
	while :; do
		case $mock_enc_rest in
		*%*)
			mock_enc_out="$mock_enc_out${mock_enc_rest%%\%*}%25"
			mock_enc_rest=${mock_enc_rest#*%}
			;;
		*)
			mock_enc_out="$mock_enc_out$mock_enc_rest"
			break
			;;
		esac
	done
	mock_enc_rest=$mock_enc_out
	mock_enc_out=""
	while :; do
		case $mock_enc_rest in
		*/*)
			mock_enc_out="$mock_enc_out${mock_enc_rest%%/*}%2F"
			mock_enc_rest=${mock_enc_rest#*/}
			;;
		*)
			mock_enc_out="$mock_enc_out$mock_enc_rest"
			break
			;;
		esac
	done
	mock_encoded_key=$mock_enc_out
}

# First manifest rule whose glob pattern matches the space-joined argv wins.
# Static fixture dirs built with zxfer_mockbin_index_manifest answer exact keys
# in O(1) from .exact/<encoded_key> and scan only .exact/.globs on a miss.
# A matching rule whose optional 4th field is "once" is consumed: the
# manifest is rewritten without that line before returning, so the next
# lookup for the same argv falls through to a later rule.
mock_manifest_lookup() {
	[ -n "${MOCK_ZFS_FIXTURE_DIR:-}" ] || return 1
	mock_tab=$(printf '\t')
	if [ -d "$MOCK_ZFS_FIXTURE_DIR/.exact" ]; then
		mock_encode_key "$mock_key"
		if [ "${#mock_encoded_key}" -le 200 ]; then
			mock_exact_file="$MOCK_ZFS_FIXTURE_DIR/.exact/$mock_encoded_key"
			if [ -r "$mock_exact_file" ]; then
				IFS= read -r mock_exact_line <"$mock_exact_file" ||
					return 1
				mock_fixture=${mock_exact_line%%"$mock_tab"*}
				if [ "$mock_exact_line" = "$mock_fixture" ]; then
					mock_status=0
				else
					mock_status=${mock_exact_line#*"$mock_tab"}
					mock_status=${mock_status%%"$mock_tab"*}
				fi
				mock_status=${mock_status:-0}
				return 0
			fi
		fi
		mock_manifest="$MOCK_ZFS_FIXTURE_DIR/.exact/.globs"
		[ -s "$mock_manifest" ] || return 1
	else
		mock_manifest="$MOCK_ZFS_FIXTURE_DIR/manifest"
		[ -r "$mock_manifest" ] || return 1
	fi
	mock_line_no=0
	while IFS= read -r mock_raw_line || [ -n "$mock_raw_line" ]; do
		mock_line_no=$((mock_line_no + 1))
		mock_pattern=${mock_raw_line%%"$mock_tab"*}
		case "$mock_pattern" in
		'' | '#'*)
			continue
			;;
		esac
		mock_rule_fixture=""
		mock_rule_status=""
		mock_rule_flag=""
		if [ "$mock_raw_line" != "$mock_pattern" ]; then
			mock_rest=${mock_raw_line#*"$mock_tab"}
			mock_rule_fixture=${mock_rest%%"$mock_tab"*}
			if [ "$mock_rest" != "$mock_rule_fixture" ]; then
				mock_rest=${mock_rest#*"$mock_tab"}
				mock_rule_status=${mock_rest%%"$mock_tab"*}
				if [ "$mock_rest" != "$mock_rule_status" ]; then
					mock_rule_flag=${mock_rest#*"$mock_tab"}
					mock_rule_flag=${mock_rule_flag%%"$mock_tab"*}
				fi
			fi
		fi
		# shellcheck disable=SC2254  # manifest patterns glob-match on purpose
		case "$mock_key" in
		$mock_pattern)
			mock_fixture=$mock_rule_fixture
			mock_status=${mock_rule_status:-0}
			if [ "$mock_rule_flag" = "once" ]; then
				awk -v consumed_line="$mock_line_no" 'NR != consumed_line' \
					"$mock_manifest" >"$mock_manifest.consumed" &&
					mv "$mock_manifest.consumed" "$mock_manifest"
			fi
			return 0
			;;
		esac
	done <"$mock_manifest"
	return 1
}

mock_emit_fixture() {
	if [ -n "$mock_fixture" ] && [ "$mock_fixture" != "-" ]; then
		cat "$MOCK_ZFS_FIXTURE_DIR/$mock_fixture"
	fi
}

mock_finish_with_status() {
	if [ "$mock_after_output_triggered" -eq 1 ]; then
		cat "$MOCK_ZFS_FAIL_AFTER_OUTPUT_DIR/stderr" >&2
		IFS= read -r mock_after_status <"$MOCK_ZFS_FAIL_AFTER_OUTPUT_DIR/status" ||
			mock_after_status=1
		exit "$mock_after_status"
	fi
	exit "$1"
}

if [ -n "${MOCK_ZFS_ARGV_LOG:-}" ]; then
	mock_log_argv=0
	if [ -z "${MOCK_ZFS_ARGV_VERBS:-}" ]; then
		mock_log_argv=1
	else
		case ":${MOCK_ZFS_ARGV_VERBS}:" in
		*":${1:-}:"*) mock_log_argv=1 ;;
		esac
	fi
	if [ "$mock_log_argv" -eq 1 ]; then
		mock_argv_nl='
'
		case "${MOCK_ZFS_ARGV_FORMAT:-inline}" in
		counted)
			mock_argv_line="ARGV $#"
			for mock_arg in "$@"; do
				mock_argv_line="$mock_argv_line$mock_argv_nl[$mock_arg]"
			done
			printf '%s\n' "$mock_argv_line" >>"$MOCK_ZFS_ARGV_LOG"
			;;
		lines)
			if [ "$#" -gt 0 ]; then
				mock_argv_line=""
				for mock_arg in "$@"; do
					mock_argv_line="${mock_argv_line:+$mock_argv_line$mock_argv_nl}[$mock_arg]"
				done
				printf '%s\n' "$mock_argv_line" >>"$MOCK_ZFS_ARGV_LOG"
			fi
			;;
		*)
			mock_argv_line=""
			for mock_arg in "$@"; do
				mock_argv_line="$mock_argv_line[$mock_arg] "
			done
			printf '%s\n' "$mock_argv_line" >>"$MOCK_ZFS_ARGV_LOG"
			;;
		esac
	fi
fi

mock_after_output_triggered=0
if [ -n "${MOCK_ZFS_FAIL_AFTER_OUTPUT_DIR:-}" ] &&
	[ -r "$MOCK_ZFS_FAIL_AFTER_OUTPUT_DIR/argv" ]; then
	IFS= read -r mock_after_argv <"$MOCK_ZFS_FAIL_AFTER_OUTPUT_DIR/argv" ||
		mock_after_argv=""
	if [ "$mock_key" = "$mock_after_argv" ] &&
		(set -C && printf '' >"$MOCK_ZFS_FAIL_AFTER_OUTPUT_DIR/fired") 2>/dev/null; then
		mock_after_output_triggered=1
	fi
fi

if mock_fail_claim zfs "$mock_key"; then
	mock_fail_operand=""
	for mock_arg in "$@"; do
		mock_fail_operand=$mock_arg
	done
	mock_fail_now zfs 1 "cannot open '$mock_fail_operand': I/O error" "$mock_key"
fi

case "${1:-}" in
destroy | rollback | create | set | inherit | snapshot | rename | clone | promote | hold | release | bookmark)
	mock_log_line "MUTATE $mock_key"
	if mock_manifest_lookup; then
		mock_emit_fixture
		mock_finish_with_status "$mock_status"
	fi
	mock_finish_with_status 0
	;;
receive | recv)
	for mock_arg in "$@"; do
		mock_receive_dataset=$mock_arg
	done
	if [ -n "${MOCK_ZFS_DELAY_RECEIVE_GLOB:-}" ]; then
		# shellcheck disable=SC2254  # delay target is a glob on purpose
		case "${mock_receive_dataset:-}" in
		$MOCK_ZFS_DELAY_RECEIVE_GLOB)
			sleep "${MOCK_ZFS_DELAY_RECEIVE_SECONDS:-1}" || exit "$?"
			;;
		esac
	fi
	mock_log_line "$mock_key"
	if [ -n "${MOCK_ZFS_RECEIVE_STREAM_DIR:-}" ] ||
		[ -n "${MOCK_ZFS_RECEIVE_BYTES_LOG:-}" ]; then
		if [ -n "${MOCK_ZFS_RECEIVE_STREAM_DIR:-}" ]; then
			mkdir -p "$MOCK_ZFS_RECEIVE_STREAM_DIR" || exit 1
			mock_stream_copy="$MOCK_ZFS_RECEIVE_STREAM_DIR/$(printf '%s' "$mock_receive_dataset" | tr / _)"
			mock_recv_bytes=$(tee -a "$mock_stream_copy" | wc -c | tr -d ' ')
		else
			mock_recv_bytes=$(wc -c | tr -d ' ')
		fi
		if [ -n "${MOCK_ZFS_RECEIVE_BYTES_LOG:-}" ]; then
			printf '%s %s\n' "$mock_receive_dataset" "$mock_recv_bytes" \
				>>"$MOCK_ZFS_RECEIVE_BYTES_LOG"
		fi
		if [ "$mock_recv_bytes" -eq 0 ] &&
			{ [ -n "${MOCK_ZFS_RECEIVE_BYTES_LOG:-}" ] ||
				[ "${MOCK_ZFS_STRICT_RECEIVE:-}" = 1 ]; }; then
			printf '%s\n' 'cannot receive: failed to read from stream' >&2
			exit 1
		fi
	elif [ "${MOCK_ZFS_STRICT_RECEIVE:-}" = 1 ]; then
		# Like zfs receive, refuse a stream without a single byte; read, a
		# builtin, takes the first line without spawning a byte counter.
		mock_stream_head=""
		if ! IFS= read -r mock_stream_head && [ -z "$mock_stream_head" ]; then
			printf '%s\n' 'cannot receive: failed to read from stream' >&2
			exit 1
		fi
		cat >/dev/null
	else
		cat >/dev/null
	fi
	mock_log_line "END receive $mock_receive_dataset"
	if mock_manifest_lookup; then
		mock_emit_fixture
		mock_finish_with_status "$mock_status"
	fi
	mock_finish_with_status 0
	;;
send)
	mock_log_line "$mock_key"
	if mock_manifest_lookup; then
		mock_emit_fixture
		mock_finish_with_status "$mock_status"
	fi
	printf 'ZXFERMOCKSTREAM %s\n' "$mock_key"
	mock_finish_with_status 0
	;;
*)
	mock_log_line "$mock_key"
	if mock_manifest_lookup; then
		mock_emit_fixture
		mock_finish_with_status "$mock_status"
	fi
	printf 'mock zfs: no manifest match for: %s\n' "$mock_key" >&2
	mock_finish_with_status "${MOCK_ZFS_DEFAULT_STATUS:-1}"
	;;
esac
EOF
	} >"$l_mockbin_zfs_path" || return 1
	chmod +x "$l_mockbin_zfs_path"
}

# Purpose: Write a minimal counting wrapper that records one spawn per
# invocation and then execs the real tool unchanged.
# Usage: zxfer_mockbin_write_counting_wrapper <path> <real-tool-abs-path> —
# the logged name is the basename of <path>; place the wrapper in the mock
# bin directory so it shadows the system tool via the secure PATH. With
# MOCK_FAIL_TOOL set to that name, the MOCK_FAIL_CALL-th call fails instead
# of running the tool (it is still logged as a spawn).
zxfer_mockbin_write_counting_wrapper() {
	l_mockbin_wrapper_path=$1
	l_mockbin_wrapper_real=$2

	case "$l_mockbin_wrapper_real" in
	/*) ;;
	*)
		printf 'zxfer_mockbin_write_counting_wrapper: real tool path must be absolute: %s\n' \
			"$l_mockbin_wrapper_real" >&2
		return 1
		;;
	esac
	if [ ! -x "$l_mockbin_wrapper_real" ]; then
		printf 'zxfer_mockbin_write_counting_wrapper: real tool not executable: %s\n' \
			"$l_mockbin_wrapper_real" >&2
		return 1
	fi

	l_mockbin_wrapper_name=${l_mockbin_wrapper_path##*/}
	{
		printf '#!/bin/sh\n'
		# shellcheck disable=SC2016  # MOCK_SPAWN_LOG must expand at wrapper runtime
		printf 'printf '\''%%s\\n'\'' "%s" >>"${MOCK_SPAWN_LOG:-/dev/null}"\n' \
			"$l_mockbin_wrapper_name"
		zxfer_mockbin_emit_fail_injection
		# shellcheck disable=SC2016  # $* must expand at wrapper runtime
		printf 'if mock_fail_claim "%s" "$*"; then\n\tmock_fail_now "%s" 1 "%s: mock failure" "$*"\nfi\n' \
			"$l_mockbin_wrapper_name" "$l_mockbin_wrapper_name" \
			"$l_mockbin_wrapper_name"
		printf 'exec "%s" "$@"\n' "$l_mockbin_wrapper_real"
	} >"$l_mockbin_wrapper_path" || return 1
	chmod +x "$l_mockbin_wrapper_path"
}

# Purpose: Write a mock ssh with control-socket semantics that logs whether
# each invocation opened a new connection, so benches can budget connections.
# Usage: zxfer_mockbin_write_socket_ssh <path>. Each call appends
# "<kind><TAB><argv>" to $MOCK_SSH_LOG, where kind is:
#   version  the `ssh -M -V` support probe (no host);
#   master   `-M -S SOCKET`: creates SOCKET as a plain file and exits, as a
#            real `-fN` master does once it is up;
#   control  `-O check|exit` over SOCKET; exit removes it;
#   mux      a command over a live SOCKET;
#   direct   a command without -S, or whose SOCKET is not live (real ssh then
#            connects on its own);
#   reject   a `-M` call rejected when MOCK_SSH_REJECT_CONTROL_MASTER=1;
#   fail     the MOCK_FAIL_CALL-th call with MOCK_FAIL_TOOL=ssh: it exits
#            255 without creating a socket or running a command.
# master and direct are new connections and sleep $MOCK_SSH_HANDSHAKE_S when
# set; mux and control calls sleep $MOCK_SSH_MUX_S when set. Options that
# take a separate value in OpenSSH (-o, -p, -i, -l, -F, -J ...) skip it, and
# the first operand is the host. Commands drop the host token and run
# locally through `sh -c`, so "remote" helpers resolve from the inherited
# mock PATH.
zxfer_mockbin_write_socket_ssh() {
	l_mockbin_ssh_path=$1
	# Resolved now so the mock never runs (and counts) a wrapped rm.
	l_mockbin_ssh_rm=$(zxfer_mockbin_resolve_host_tool rm) || return 1

	{
		printf '#!/bin/sh\n'
		printf "mock_rm='%s'\n" "$l_mockbin_ssh_rm"
		zxfer_mockbin_emit_fail_injection
		cat <<'EOF'
mock_argv=$*
mock_socket=""
mock_op=""
mock_master=0
mock_version=0
while [ $# -gt 0 ]; do
	case "$1" in
	-M) mock_master=1 ;;
	-V) mock_version=1 ;;
	-S)
		mock_socket=$2
		shift
		;;
	-O)
		mock_op=$2
		shift
		;;
	-B | -b | -c | -D | -E | -e | -F | -I | -i | -J | -L | -l | -m | -o | -P | -p | -Q | -R | -W | -w)
		shift
		;;
	-*) ;;
	*) break ;;
	esac
	shift
done

if [ "${MOCK_SSH_REJECT_CONTROL_MASTER:-}" = 1 ] && [ "$mock_master" -eq 1 ]; then
	[ -z "${MOCK_SSH_LOG:-}" ] ||
		printf 'reject\t%s\n' "$mock_argv" >>"$MOCK_SSH_LOG"
	printf '%s\n' 'ssh: illegal option -- M' >&2
	exit 255
fi

if [ "$mock_version" -eq 1 ] && [ $# -eq 0 ]; then
	mock_kind=version
elif [ "$mock_master" -eq 1 ] && [ -n "$mock_socket" ]; then
	mock_kind=master
elif [ -n "$mock_op" ]; then
	mock_kind=control
elif [ -n "$mock_socket" ] && [ -e "$mock_socket" ]; then
	mock_kind=mux
else
	mock_kind=direct
fi
if mock_fail_claim ssh "$mock_argv"; then
	[ -z "${MOCK_SSH_LOG:-}" ] ||
		printf 'fail\t%s\n' "$mock_argv" >>"$MOCK_SSH_LOG"
	mock_fail_now ssh 255 "Connection to ${1:-localhost} closed by remote host." \
		"$mock_argv"
fi
[ -z "${MOCK_SSH_LOG:-}" ] ||
	printf '%s\t%s\n' "$mock_kind" "$mock_argv" >>"$MOCK_SSH_LOG"

case $mock_kind in
master | direct) mock_delay=${MOCK_SSH_HANDSHAKE_S:-} ;;
mux | control) mock_delay=${MOCK_SSH_MUX_S:-} ;;
*) mock_delay="" ;;
esac
[ -z "$mock_delay" ] || sleep "$mock_delay"

case $mock_kind in
version)
	printf '%s\n' 'OpenSSH_mock' >&2
	exit 0
	;;
master)
	: >"$mock_socket" || exit 255
	exit 0
	;;
control)
	if [ ! -e "$mock_socket" ]; then
		printf 'Control socket connect(%s): No such file or directory\n' \
			"$mock_socket" >&2
		exit 255
	fi
	[ "$mock_op" != exit ] || "$mock_rm" -f "$mock_socket"
	exit 0
	;;
esac
[ $# -gt 1 ] || exit 0
shift
if [ -n "${MOCK_SSH_CAPABILITY_RESPONSE_FILE:-}" ]; then
	mock_cap_rest=$*
	mock_cap_joined=""
	mock_cap_boundary="' 'd"
	while :; do
		case $mock_cap_rest in
		*"$mock_cap_boundary"*)
			mock_cap_joined="$mock_cap_joined${mock_cap_rest%%"$mock_cap_boundary"*}"
			mock_cap_rest=${mock_cap_rest#*"$mock_cap_boundary"}
			;;
		*)
			mock_cap_joined="$mock_cap_joined$mock_cap_rest"
			break
			;;
		esac
	done
	case $mock_cap_joined in
	*ZXFER_REMOTE_CAPS_V2*)
		cat "$MOCK_SSH_CAPABILITY_RESPONSE_FILE"
		exit 0
		;;
	esac
fi
if [ "${MOCK_SSH_READ_STDIN:-}" = 1 ]; then
	cat | sh -c "$*"
	exit "$?"
fi
exec sh -c "$*"
EOF
	} >"$l_mockbin_ssh_path" || return 1
	chmod +x "$l_mockbin_ssh_path"
}

# Purpose: Summarize a zxfer_mockbin_write_socket_ssh log as three TSV rows:
# ssh_connections (master opens plus direct commands), ssh_invocations (every
# call, probes and control calls included) and ssh_master_opens.
# Usage: zxfer_mockbin_ssh_log_rows <scenario> <ssh-log>. Lines that do not
# start with a known kind (continuations of an argv holding a newline) are
# skipped.
zxfer_mockbin_ssh_log_rows() {
	awk -F '\t' -v scenario="$1" '
		$1 ~ /^(version|master|control|mux|direct)$/ {
			calls++
		}
		$1 == "master" {
			masters++
		}
		$1 == "master" || $1 == "direct" {
			connections++
		}
		END {
			printf "%s\tssh_connections\t%d\n", scenario, connections
			printf "%s\tssh_invocations\t%d\n", scenario, calls
			printf "%s\tssh_master_opens\t%d\n", scenario, masters
		}
	' "$2"
}

# Purpose: Emit name<TAB>guid snapshot records for the fixture tree with
# deterministic 19-digit guids derived from dataset and snapshot indexes.
# Usage: Internal generator shared by the fixture-state writer. Arguments:
# <root-dataset> <dataset-count> <snap-count> <order> where order
# "creation" interleaves snapshot-major (mimics `zfs list -s creation -r`)
# and "dataset" groups dataset-major (mimics plain `zfs list -r`).
zxfer_mockbin_emit_snapshot_records() {
	l_mockbin_emit_root=$1
	l_mockbin_emit_datasets=$2
	l_mockbin_emit_snaps=$3
	l_mockbin_emit_order=$4

	awk -v root="$l_mockbin_emit_root" -v n="$l_mockbin_emit_datasets" \
		-v s="$l_mockbin_emit_snaps" -v order="$l_mockbin_emit_order" '
		function dataset_name(d) {
			return (d == 0) ? root : root "/child" d
		}
		# guid layout: "1" + 4-digit dataset idx + 5-digit snap idx +
		# 9-digit constant = 19 deterministic digits.
		function guid(d, i) {
			return sprintf("1%04d%05d%09d", d, i, 7)
		}
		BEGIN {
			if (order == "creation") {
				for (i = 1; i <= s; i++)
					for (d = 0; d <= n; d++)
						printf "%s@snap%d\t%s\n", dataset_name(d), i, guid(d, i)
			} else {
				for (d = 0; d <= n; d++)
					for (i = 1; i <= s; i++)
						printf "%s@snap%d\t%s\n", dataset_name(d), i, guid(d, i)
			}
		}
	'
}

# Purpose: Write one complete canned-zfs fixture state directory (fixture
# files plus manifest) for a given destination snapshot depth.
# Usage: Internal writer for zxfer_mockbin_build_fixture_tree. Arguments:
# <state-dir> <dataset-count> <src-snaps> <dst-snaps>.
zxfer_mockbin_write_fixture_state_dir() {
	l_mockbin_state_dir=$1
	l_mockbin_state_datasets=$2
	l_mockbin_state_src_snaps=$3
	l_mockbin_state_dst_snaps=$4

	mkdir -p "$l_mockbin_state_dir" || return 1

	zxfer_mockbin_emit_snapshot_records "$ZXFER_MOCKBIN_SOURCE_ROOT" \
		"$l_mockbin_state_datasets" "$l_mockbin_state_src_snaps" creation \
		>"$l_mockbin_state_dir/src_snapshots.list" || return 1
	# Plain `zfs list -r -t snapshot` (no -s creation) groups dataset-major;
	# this answers the fast recursive no-op proof's source identity listing.
	zxfer_mockbin_emit_snapshot_records "$ZXFER_MOCKBIN_SOURCE_ROOT" \
		"$l_mockbin_state_datasets" "$l_mockbin_state_src_snaps" dataset \
		>"$l_mockbin_state_dir/src_snapshots_dataset.list" || return 1
	zxfer_mockbin_emit_snapshot_records "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
		"$l_mockbin_state_datasets" "$l_mockbin_state_dst_snaps" dataset \
		>"$l_mockbin_state_dir/dst_snapshots.list" || return 1
	{
		printf '%s\n' "$ZXFER_MOCKBIN_SOURCE_ROOT"
		l_mockbin_state_index=1
		while [ "$l_mockbin_state_index" -le "$l_mockbin_state_datasets" ]; do
			printf '%s/child%d\n' "$ZXFER_MOCKBIN_SOURCE_ROOT" "$l_mockbin_state_index"
			l_mockbin_state_index=$((l_mockbin_state_index + 1))
		done
	} >"$l_mockbin_state_dir/src_datasets.list"

	# Mimic `zfs list -H <dataset>`: name, used, avail, refer, mountpoint.
	printf '%s\t96K\t1.0G\t24K\t/%s\n' "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" >"$l_mockbin_state_dir/dst_exists.list"

	{
		printf '%s\n' "$ZXFER_MOCKBIN_DEST_ROOT"
		printf '%s\n' "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
		l_mockbin_state_index=1
		while [ "$l_mockbin_state_index" -le "$l_mockbin_state_datasets" ]; do
			printf '%s/child%d\n' "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
				"$l_mockbin_state_index"
			l_mockbin_state_index=$((l_mockbin_state_index + 1))
		done
	} >"$l_mockbin_state_dir/dst_datasets.list"

	# Per-dataset depth-1 snapshot listings answer non-recursive (-N) batched
	# view listings and fallback rechecks for datasets outside the batched
	# view root; -R runs serve rechecks from the recursive listing above.
	l_mockbin_state_index=0
	while [ "$l_mockbin_state_index" -le "$l_mockbin_state_datasets" ]; do
		if [ "$l_mockbin_state_index" -eq 0 ]; then
			l_mockbin_state_dataset=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT
		else
			l_mockbin_state_dataset="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child$l_mockbin_state_index"
		fi
		awk -v dataset="$l_mockbin_state_dataset" -v d="$l_mockbin_state_index" \
			-v s="$l_mockbin_state_dst_snaps" 'BEGIN {
				for (i = 1; i <= s; i++)
					printf "%s@snap%d\t1%04d%05d%09d\n", dataset, i, d, i, 7
			}' >"$l_mockbin_state_dir/dst_d1_$l_mockbin_state_index.list" || return 1
		l_mockbin_state_index=$((l_mockbin_state_index + 1))
	done

	{
		printf '%s\t%s\t%s\n' \
			"list -Hr -o name,guid -s creation -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT" \
			src_snapshots.list 0
		printf '%s\t%s\t%s\n' \
			"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT" \
			src_snapshots_dataset.list 0
		printf '%s\t%s\t%s\n' \
			"list -Hr -t filesystem,volume -o name $ZXFER_MOCKBIN_SOURCE_ROOT" \
			src_datasets.list 0
		printf '%s\t%s\t%s\n' \
			"list -H $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" dst_exists.list 0
		printf '%s\t%s\t%s\n' \
			"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
			dst_snapshots.list 0
		printf '%s\t%s\t%s\n' \
			"list -t filesystem,volume -Hr -o name $ZXFER_MOCKBIN_DEST_ROOT" \
			dst_datasets.list 0
		l_mockbin_state_index=0
		while [ "$l_mockbin_state_index" -le "$l_mockbin_state_datasets" ]; do
			if [ "$l_mockbin_state_index" -eq 0 ]; then
				l_mockbin_state_dataset=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT
			else
				l_mockbin_state_dataset="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child$l_mockbin_state_index"
			fi
			printf '%s\t%s\t%s\n' \
				"list -H -d 1 -o name,guid -t snapshot $l_mockbin_state_dataset" \
				"dst_d1_$l_mockbin_state_index.list" 0
			l_mockbin_state_index=$((l_mockbin_state_index + 1))
		done
	} >"$l_mockbin_state_dir/manifest"
	zxfer_mockbin_index_manifest "$l_mockbin_state_dir"
}

# Purpose: Build an O(1) .exact/<encoded-key> lookup directory for a static
# manifest directory that contains no consumable "once" rules. To preserve
# exact first-match-wins precedence, only non-pattern rules preceding the first
# glob/escaped/long rule are indexed into .exact/; that rule and any later
# rules land in .exact/.globs in manifest order.
# Usage: zxfer_mockbin_index_manifest <state-dir>
zxfer_mockbin_index_manifest() {
	l_mockbin_idx_dir=$1

	rm -rf "$l_mockbin_idx_dir/.exact" || return 1
	[ -r "$l_mockbin_idx_dir/manifest" ] || return 0
	mkdir -p "$l_mockbin_idx_dir/.exact" || return 1

	l_mockbin_idx_status=0
	LC_ALL=C awk -F '\t' -v dir="$l_mockbin_idx_dir/.exact" '
		BEGIN {
			globs_file = dir "/.globs"
			printf "" > globs_file
		}
		$1 == "" || substr($1, 1, 1) == "#" { next }
		$4 == "once" {
			exit 2
		}
		{
			key = $1
			fixture = $2
			status = ($3 == "") ? "0" : $3
			encoded = key
			gsub(/%/, "%25", encoded)
			gsub(/\//, "%2F", encoded)
			if (seen_glob || index(key, "*") || index(key, "?") || index(key, "[") || index(key, "\\") || length(encoded) > 200) {
				seen_glob = 1
				if (!(encoded in seen)) {
					printf "%s\t%s\t%s\n", key, fixture, status > globs_file
				}
				next
			}
			if (!(encoded in seen)) {
				seen[encoded] = 1
				target = dir "/" encoded
				printf "%s\t%s\n", fixture, status > target
				close(target)
			}
		}
		END {
			close(globs_file)
		}
	' "$l_mockbin_idx_dir/manifest" || l_mockbin_idx_status=$?
	if [ "$l_mockbin_idx_status" -ne 0 ]; then
		rm -rf "$l_mockbin_idx_dir/.exact"
		[ "$l_mockbin_idx_status" -eq 2 ] || return 1
	fi
}

# Purpose: Generate the deterministic dataset/snapshot fixture tree plus the
# manifests covering every discovery command shape the current ./zxfer
# issues for local recursive replication.
# Usage: zxfer_mockbin_build_fixture_tree <fixture-dir> <num-datasets>
# <snaps-per-dataset>. Source tree is srcpool/data with child1..childN and
# snap1..snapS each (root included). Builds two complete state dirs to use
# as MOCK_ZFS_FIXTURE_DIR:
#   <fixture-dir>/noop         destination identical to the source;
#   <fixture-dir>/incremental  every destination dataset missing the last
#                              snapshot (requires snaps-per-dataset >= 2).
zxfer_mockbin_build_fixture_tree() {
	l_mockbin_tree_root=$1
	l_mockbin_tree_datasets=$2
	l_mockbin_tree_snaps=$3

	case "$l_mockbin_tree_datasets" in
	'' | *[!0123456789]*)
		printf 'zxfer_mockbin_build_fixture_tree: num-datasets must be a non-negative integer: %s\n' \
			"$l_mockbin_tree_datasets" >&2
		return 1
		;;
	esac
	case "$l_mockbin_tree_snaps" in
	'' | *[!0123456789]* | 0 | 1)
		printf 'zxfer_mockbin_build_fixture_tree: snaps-per-dataset must be >= 2: %s\n' \
			"$l_mockbin_tree_snaps" >&2
		return 1
		;;
	esac

	zxfer_mockbin_write_fixture_state_dir "$l_mockbin_tree_root/noop" \
		"$l_mockbin_tree_datasets" "$l_mockbin_tree_snaps" \
		"$l_mockbin_tree_snaps" || return 1
	zxfer_mockbin_write_fixture_state_dir "$l_mockbin_tree_root/incremental" \
		"$l_mockbin_tree_datasets" "$l_mockbin_tree_snaps" \
		"$((l_mockbin_tree_snaps - 1))" || return 1
}

# Purpose: Print the 68 properties a current OpenZFS filesystem reports, one
# "name|value|source" row each, as the template of the property fixtures.
# Usage: zxfer_mockbin_emit_property_template. LOCAL marks the properties the
# fixture root sets locally (compression, atime, xattr, mountpoint) and its
# children inherit; the mountpoint value is replaced per dataset.
zxfer_mockbin_emit_property_template() {
	cat <<'EOF'
type|filesystem|-
creation|1700000000|-
used|98304|-
available|1073741824|-
referenced|24576|-
compressratio|1.00x|-
mounted|yes|-
quota|0|default
reservation|0|default
recordsize|131072|default
mountpoint|-|LOCAL
sharenfs|off|default
checksum|on|default
compression|lz4|LOCAL
atime|off|LOCAL
devices|on|default
exec|on|default
setuid|on|default
readonly|off|default
zoned|off|default
snapdir|hidden|default
aclmode|discard|default
aclinherit|restricted|default
createtxg|100|-
canmount|on|default
xattr|sa|LOCAL
copies|1|default
version|5|-
utf8only|off|-
normalization|none|-
casesensitivity|sensitive|-
vscan|off|default
nbmand|off|default
sharesmb|off|default
refquota|0|default
refreservation|0|default
guid|1000000000000000007|-
primarycache|all|default
secondarycache|all|default
usedbysnapshots|0|-
usedbydataset|24576|-
usedbychildren|73728|-
usedbyrefreservation|0|-
logbias|latency|default
objsetid|54|-
dedup|off|default
mlslabel|none|default
sync|standard|default
dnodesize|legacy|default
refcompressratio|1.00x|-
written|0|-
logicalused|45056|-
logicalreferenced|12288|-
volmode|default|default
filesystem_limit|18446744073709551615|default
snapshot_limit|18446744073709551615|default
filesystem_count|18446744073709551615|default
snapshot_count|18446744073709551615|default
snapdev|hidden|default
acltype|off|default
context|none|default
fscontext|none|default
defcontext|none|default
rootcontext|none|default
relatime|on|default
redundant_metadata|all|default
overlay|on|default
encryption|off|default
EOF
}

# Purpose: Append the shared recursive-tree and root property `zfs get` rules
# used by both zxfer_mockbin_add_property_fixtures and blackbox.sh.
# Usage: zxfer_mockbin_append_property_tree_rules <manifest-path>
zxfer_mockbin_append_property_tree_rules() {
	{
		for l_mockbin_props_view in -Hpo -Ho; do
			printf '%s\t%s\t0\n' \
				"get -r -t filesystem,volume $l_mockbin_props_view name,property,value,source all $ZXFER_MOCKBIN_SOURCE_ROOT" \
				src_props_tree.list \
				"get -r -t filesystem,volume $l_mockbin_props_view name,property,value,source all $ZXFER_MOCKBIN_DEST_ROOT" \
				dst_props_tree.list \
				"get $l_mockbin_props_view property,value,source all $ZXFER_MOCKBIN_SOURCE_ROOT" \
				src_props_root.list \
				"get $l_mockbin_props_view property,value,source all $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
				dst_props_root.list
		done
		printf '%s\t%s\t0\n' \
			"get -r -t filesystem,volume -Ho name,property all $ZXFER_MOCKBIN_SOURCE_ROOT" \
			src_props_tree.names \
			"get -r -t filesystem,volume -Ho name,property all $ZXFER_MOCKBIN_DEST_ROOT" \
			dst_props_tree.names
	} >>"$1"
}

# Purpose: Add matching property fixtures to one canned-zfs state directory
# of the fixture tree, so a -P run reads 68 properties per dataset that
# already agree on both sides and plans no set or inherit.
# Usage: zxfer_mockbin_add_property_fixtures <state-dir> <num-datasets>.
# Answers every `zfs get` shape a -P pass issues: the current launcher's
# recursive machine and human reads and name lists (rooted at the source
# root and at the destination root), their per-dataset fallbacks, and the
# type and volsize probes; plus the per-dataset reads of older launchers
# such as upstream-compat-final.
zxfer_mockbin_add_property_fixtures() {
	l_mockbin_props_dir=$1
	l_mockbin_props_datasets=$2

	for l_mockbin_props_side in src dst; do
		if [ "$l_mockbin_props_side" = src ]; then
			l_mockbin_props_root=$ZXFER_MOCKBIN_SOURCE_ROOT
		else
			l_mockbin_props_root=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT
		fi
		# One pass per side writes each dataset's rows (<side>_props_root.list,
		# <side>_props_child<N>.list), the recursive tree rows and name list
		# that lead with the dataset, and the per-dataset name list.
		zxfer_mockbin_emit_property_template | awk \
			-v prefix="$l_mockbin_props_dir/${l_mockbin_props_side}_props" \
			-v root="$l_mockbin_props_root" -v n="$l_mockbin_props_datasets" '
			BEGIN {
				FS = "|"
				OFS = "\t"
			}
			{
				name[NR] = $1
				value[NR] = $2
				source[NR] = $3
			}
			END {
				for (d = 0; d <= n; d++) {
					dataset = (d == 0) ? root : root "/child" d
					file = prefix ((d == 0) ? "_root" : "_child" d) ".list"
					for (i = 1; i <= NR; i++) {
						v = (name[i] == "mountpoint") ? "/" dataset : value[i]
						s = source[i]
						if (s == "LOCAL")
							s = (d == 0) ? "local" : "inherited from " root
						print name[i], v, s > file
						print dataset, name[i], v, s > (prefix "_tree.list")
						print dataset, name[i] > (prefix "_tree.names")
					}
					close(file)
				}
				for (i = 1; i <= NR; i++)
					print name[i] > (prefix ".names")
			}
		' || return 1
	done
	printf 'filesystem\n' >"$l_mockbin_props_dir/props_type.list" || return 1
	printf -- '-\n' >"$l_mockbin_props_dir/props_volsize.list" || return 1

	zxfer_mockbin_append_property_tree_rules "$l_mockbin_props_dir/manifest" || return 1
	{
		l_mockbin_props_index=1
		while [ "$l_mockbin_props_index" -le "$l_mockbin_props_datasets" ]; do
			for l_mockbin_props_view in -Hpo -Ho; do
				printf '%s\t%s\t0\n' \
					"get $l_mockbin_props_view property,value,source all $ZXFER_MOCKBIN_SOURCE_ROOT/child$l_mockbin_props_index" \
					"src_props_child$l_mockbin_props_index.list" \
					"get $l_mockbin_props_view property,value,source all $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child$l_mockbin_props_index" \
					"dst_props_child$l_mockbin_props_index.list"
			done
			l_mockbin_props_index=$((l_mockbin_props_index + 1))
		done
		printf '%s\t%s\t0\n' \
			"get -Ho property all $ZXFER_MOCKBIN_SOURCE_ROOT*" src_props.names \
			"get -Ho property all $ZXFER_MOCKBIN_DEST_MAPPED_ROOT*" dst_props.names \
			"get -Hpo value type *" props_type.list \
			"get -Hpo value volsize *" props_volsize.list
	} >>"$l_mockbin_props_dir/manifest" || return 1
	zxfer_mockbin_index_manifest "$l_mockbin_props_dir"
}

# Purpose: Print the ZXFER_SECURE_PATH value that resolves mocks first and
# everything else from the standard system directories.
# Usage: zxfer_mockbin_secure_path_env <mockdir> — pass the result as
# ZXFER_SECURE_PATH when invoking ./zxfer so the canned zfs and any counting
# wrappers shadow the system tools.
zxfer_mockbin_secure_path_env() {
	l_mockbin_secure_dir=$1

	printf '%s:%s\n' "$l_mockbin_secure_dir" "$ZXFER_MOCKBIN_SYSTEM_SECURE_PATH"
}

# Purpose: Create a private bench work directory directly under /tmp, with a
# tmp/ subdirectory to pass to zxfer as TMPDIR, whatever the caller's
# TMPDIR. Path lengths change zxfer's work: remote commands embed the mock
# bin path, and a script past 768 bytes is rendered a second time in
# chunks; and a TMPDIR past about 50 bytes pushes control sockets over the
# ~104-byte sun_path limit, so zxfer takes a socket-directory fallback that
# adds helper spawns and time. A short fixed parent keeps counts and
# timings the same on every host.
# Usage: zxfer_mockbin_make_bench_workdir <name-prefix>; prints the
# directory, or returns 1. The caller removes it.
zxfer_mockbin_make_bench_workdir() {
	l_mockbin_workdir=$(mktemp -d "/tmp/$1.XXXXXX") || return 1
	[ -d "$l_mockbin_workdir" ] || return 1
	mkdir "$l_mockbin_workdir/tmp" || {
		rm -rf "$l_mockbin_workdir"
		return 1
	}
	printf '%s\n' "$l_mockbin_workdir"
}

# Purpose: Run the real ./zxfer launcher black-box against a mock bin dir and
# one canned-zfs fixture state directory.
# Usage: zxfer_mockbin_run_zxfer <mockdir> <fixture-state-dir> <zfs-log>
# [zxfer-arg...]. Resolves the launcher from $ZXFER_MOCKBIN_ZXFER_BIN, then
# $ZXFER_TEST_ZXFER_BIN (set by tests/test_helper.sh), then
# $ZXFER_ROOT/zxfer. Refuses to run when the canned zfs is missing from
# <mockdir> so a misbuilt mock dir can never let zxfer resolve a real zfs.
# Stdout/stderr pass through; redirect at the call site. Returns zxfer's exit
# status.
# Side effects: Runs the launcher with a fixture-scoped secure PATH and canned
# ZFS state while leaving the caller's environment unchanged.
zxfer_mockbin_run_zxfer() {
	l_mockbin_run_mockdir=$1
	l_mockbin_run_state=$2
	l_mockbin_run_log=$3
	shift 3

	l_mockbin_run_bin=${ZXFER_MOCKBIN_ZXFER_BIN:-${ZXFER_TEST_ZXFER_BIN:-${ZXFER_ROOT:-.}/zxfer}}
	if [ ! -x "$l_mockbin_run_bin" ]; then
		printf 'zxfer_mockbin_run_zxfer: zxfer launcher not executable: %s\n' \
			"$l_mockbin_run_bin" >&2
		return 1
	fi
	if [ ! -x "$l_mockbin_run_mockdir/zfs" ]; then
		printf 'zxfer_mockbin_run_zxfer: canned zfs missing in %s; refusing to run zxfer\n' \
			"$l_mockbin_run_mockdir" >&2
		return 1
	fi

	MOCK_ZFS_LOG="$l_mockbin_run_log" \
		MOCK_ZFS_FIXTURE_DIR="$l_mockbin_run_state" \
		ZXFER_SECURE_PATH=$(zxfer_mockbin_secure_path_env "$l_mockbin_run_mockdir") \
		ZXFER_SECURE_PATH_APPEND="" \
		"$l_mockbin_run_bin" "$@"
}
