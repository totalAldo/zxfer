#!/bin/sh

# Template: replace the email address, log path, dataset names, and optional
# origin/target host settings before use. This example enables ZXFER_ERROR_LOG,
# captures the current run's structured failure report from stderr, and emails
# it through mailx, BSD mail, or sendmail when zxfer exits non-zero.

set -eu

SCRIPT_DIR=$(
	CDPATH=
	cd -- "$(dirname "$0")" && pwd
)
SCRIPT_PATH="$SCRIPT_DIR/$(basename "$0")"
REPO_ROOT=$(
	CDPATH=
	cd -- "$SCRIPT_DIR/.." && pwd
)

ALERT_TO=${ALERT_TO:-"root@example.com"}
ERROR_LOG=${ERROR_LOG:-"/var/log/zxfer/error.log"}
SRC_DATASET=${SRC_DATASET:-"tank/src"}
DEST_DATASET=${DEST_DATASET:-"backup/dst"}
ORIGIN_HOST=${ORIGIN_HOST:-""}
TARGET_HOST=${TARGET_HOST:-""}
RAW_SEND=${RAW_SEND:-0}
MAILER=${MAILER:-"auto"}
MAIL_FROM=${MAIL_FROM:-""}
MAIL_FROM_FLAG=${MAIL_FROM_FLAG:-"-r"}
SENDMAIL_FROM_FLAG=${SENDMAIL_FROM_FLAG:-"-f"}
ALERT_PATH=${ALERT_PATH:-"/usr/local/sbin:/usr/local/bin:/opt/homebrew/sbin:/opt/homebrew/bin:/opt/local/sbin:/opt/local/bin:/usr/sbin:/usr/bin:/usr/lib:/sbin:/bin"}
ZXFER_BIN=${ZXFER_BIN:-"$REPO_ROOT/zxfer"}

g_mailer_kind=""
g_mailer_command=""

extract_latest_failure_report() {
	l_log_file=$1

	awk '
/^zxfer: failure report begin$/ {
	block = $0 ORS
	capture = 1
	next
}
capture {
	block = block $0 ORS
	if ($0 ~ /^zxfer: failure report end$/) {
		last = block
		block = ""
		capture = 0
	}
}
END {
	printf "%s", last
}
' "$l_log_file"
}

report_field() {
	l_key=$1

	awk -F': ' -v key="$l_key" '
$0 ~ ("^" key ": ") {
	sub("^[^:]+: ", "")
	print
	exit
}
'
}

find_in_alert_path() {
	l_name=$1

	PATH=$ALERT_PATH command -v "$l_name" 2>/dev/null || true
}

resolve_mailer_command() {
	case "$MAILER" in
	auto)
		for l_candidate in mailx mail sendmail; do
			l_path=$(find_in_alert_path "$l_candidate")
			if [ -n "$l_path" ]; then
				g_mailer_kind=$l_candidate
				g_mailer_command=$l_path
				return 0
			fi
		done
		printf '%s\n' "mailx, mail, or sendmail is required to deliver zxfer alerts. Set ALERT_PATH if your mailer lives outside the default search path." >&2
		return 1
		;;
	mailx | mail | sendmail)
		l_path=$(find_in_alert_path "$MAILER")
		if [ -z "$l_path" ]; then
			printf '%s\n' "Requested mailer \"$MAILER\" was not found in ALERT_PATH: $ALERT_PATH" >&2
			return 1
		fi
		g_mailer_kind=$MAILER
		g_mailer_command=$l_path
		return 0
		;;
	*)
		printf '%s\n' "Unsupported MAILER value \"$MAILER\". Use auto, mailx, mail, or sendmail." >&2
		return 1
		;;
	esac
}

send_alert_with_mail_client() {
	l_command=$1
	l_subject=$2
	l_body=$3

	set -- "$l_command" -s "$l_subject"
	if [ -n "$MAIL_FROM" ]; then
		if [ -z "$MAIL_FROM_FLAG" ]; then
			printf '%s\n' "MAIL_FROM is set but MAIL_FROM_FLAG is empty. Set MAIL_FROM_FLAG or clear MAIL_FROM." >&2
			return 1
		fi
		set -- "$@" "$MAIL_FROM_FLAG" "$MAIL_FROM"
	fi
	set -- "$@" "$ALERT_TO"
	printf '%s\n' "$l_body" | "$@"
}

send_alert_with_sendmail() {
	l_command=$1
	l_subject=$2
	l_body=$3

	set -- "$l_command" -t
	if [ -n "$MAIL_FROM" ]; then
		if [ -z "$SENDMAIL_FROM_FLAG" ]; then
			printf '%s\n' "MAIL_FROM is set but SENDMAIL_FROM_FLAG is empty. Set SENDMAIL_FROM_FLAG or clear MAIL_FROM." >&2
			return 1
		fi
		set -- "$@" "$SENDMAIL_FROM_FLAG" "$MAIL_FROM"
	fi

	{
		printf 'To: %s\n' "$ALERT_TO"
		printf 'Subject: %s\n' "$l_subject"
		if [ -n "$MAIL_FROM" ]; then
			printf 'From: %s\n' "$MAIL_FROM"
		fi
		printf 'Content-Type: text/plain; charset=UTF-8\n'
		printf '\n'
		printf '%s\n' "$l_body"
	} | "$@"
}

send_alert_mail() {
	l_subject=$1
	l_body=$2

	resolve_mailer_command || return 1

	case "$g_mailer_kind" in
	mailx | mail)
		send_alert_with_mail_client "$g_mailer_command" "$l_subject" "$l_body"
		return
		;;
	sendmail)
		send_alert_with_sendmail "$g_mailer_command" "$l_subject" "$l_body"
		return
		;;
	esac
}

run_zxfer() {
	set -- "$ZXFER_BIN" -v
	if [ -n "$ORIGIN_HOST" ]; then
		set -- "$@" -O "$ORIGIN_HOST"
	fi
	if [ -n "$TARGET_HOST" ]; then
		set -- "$@" -T "$TARGET_HOST"
	fi
	if [ "$RAW_SEND" = "1" ]; then
		set -- "$@" -w
	fi
	set -- "$@" -R "$SRC_DATASET" "$DEST_DATASET"
	ZXFER_ERROR_LOG="$ERROR_LOG" "$@"
}

validate_error_log_path() {
	case "$ERROR_LOG" in
	/*) return 0 ;;
	*)
		printf '%s\n' "ERROR_LOG must be an absolute path because zxfer requires ZXFER_ERROR_LOG to be absolute." >&2
		return 1
		;;
	esac
}

error_log_parent_dir() {
	case "$ERROR_LOG" in
	/*/*) printf '%s\n' "${ERROR_LOG%/*}" ;;
	/*) printf '%s\n' / ;;
	*) printf '%s\n' . ;;
	esac
}

ensure_error_log_parent() {
	log_dir=$(error_log_parent_dir)
	umask 077
	mkdir -p "$log_dir"
}

current_host_name() {
	uname -n 2>/dev/null || hostname 2>/dev/null || printf '%s\n' unknown-host
}

make_temp_dir() {
	if l_tmp=$(mktemp -d "${TMPDIR:-/tmp}/zxfer_mail_example.XXXXXX" 2>/dev/null); then
		printf '%s\n' "$l_tmp"
		return 0
	fi

	mktemp -d -t zxfer_mail_example.XXXXXX
}

self_test_fail() {
	printf '%s\n' "self-test: $*" >&2
	exit 1
}

self_test_assert_contains() {
	l_path=$1
	l_text=$2

	if ! grep -F -- "$l_text" "$l_path" >/dev/null 2>&1; then
		if [ -f "$l_path" ]; then
			printf '%s\n' "self-test: expected \"$l_text\" in $l_path, got:" >&2
			cat "$l_path" >&2
		else
			printf '%s\n' "self-test: expected \"$l_text\" in missing file $l_path" >&2
		fi
		exit 1
	fi
}

write_self_test_mail_capture() {
	l_path=$1

	cat >"$l_path" <<'EOF'
#!/bin/sh
log_dir=${MOCK_MAIL_LOG_DIR:-}
name=$(basename "$0")

[ -n "$log_dir" ] || exit 1
printf '%s\n' "$@" >"$log_dir/$name.argv"
cat >"$log_dir/$name.body"
EOF
	chmod +x "$l_path"
}

write_self_test_zxfer() {
	l_path=$1

	cat >"$l_path" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" >"${MOCK_ZXFER_ARGS_LOG:?}"

emit_report() {
	printf 'zxfer: failure report begin\n'
	printf 'timestamp: 2026-04-04T08:10:07+0200\n'
	printf 'hostname: example-host\n'
	printf 'exit_status: 3\n'
	printf 'failure_class: runtime\n'
	printf 'failure_stage: snapshot discovery\n'
	printf 'message: Simulated zxfer failure for example self-test.\n'
	printf 'source_root: %s\n' "${MOCK_SOURCE_ROOT:-tank/source}"
	printf 'destination_root: %s\n' "${MOCK_DEST_ROOT:-backup/dest}"
	printf 'zxfer: failure report end\n'
}

emit_report >&2

if [ "${MOCK_ZXFER_APPEND_ERROR_LOG:-1}" = "1" ]; then
	emit_report >>"${ZXFER_ERROR_LOG:?}"
fi

exit "${MOCK_ZXFER_STATUS:-3}"
EOF
	chmod +x "$l_path"
}

run_script_for_self_test() {
	l_case_dir=$1
	l_alert_path=$2
	shift 2

	set +e
	env \
		PATH="$l_case_dir/bin:$PATH" \
		ALERT_PATH="$l_alert_path" \
		ZXFER_BIN="$l_case_dir/bin/zxfer" \
		MAILER="auto" \
		MAIL_FROM="" \
		MAIL_FROM_FLAG="-r" \
		SENDMAIL_FROM_FLAG="-f" \
		ORIGIN_HOST="" \
		TARGET_HOST="" \
		RAW_SEND=0 \
		MOCK_MAIL_LOG_DIR="$l_case_dir" \
		MOCK_ZXFER_ARGS_LOG="$l_case_dir/zxfer.argv" \
		ALERT_TO="alerts@example.com" \
		ERROR_LOG="$l_case_dir/error.log" \
		SRC_DATASET="tank/source" \
		DEST_DATASET="backup/dest" \
		"$@" \
		sh "$SCRIPT_PATH" >/dev/null 2>"$l_case_dir/stderr"
	l_status=$?
	set -e

	printf '%s\n' "$l_status"
}

self_test_mailx_case() {
	l_tmpdir=$1
	l_case_dir="$l_tmpdir/mailx"

	mkdir -p "$l_case_dir/bin"
	write_self_test_zxfer "$l_case_dir/bin/zxfer"
	write_self_test_mail_capture "$l_case_dir/bin/mailx"

	l_status=$(run_script_for_self_test "$l_case_dir" "$l_case_dir/bin" \
		TARGET_HOST="root@target.example" \
		RAW_SEND=1 \
		MAIL_FROM="zxfer@example.com")
	if [ "$l_status" -ne 3 ]; then
		self_test_fail "mailx case expected exit status 3, got $l_status"
	fi

	self_test_assert_contains "$l_case_dir/mailx.argv" "-s"
	self_test_assert_contains "$l_case_dir/mailx.argv" "-r"
	self_test_assert_contains "$l_case_dir/mailx.argv" "zxfer@example.com"
	self_test_assert_contains "$l_case_dir/mailx.argv" "alerts@example.com"
	self_test_assert_contains "$l_case_dir/mailx.body" "zxfer: failure report begin"
	self_test_assert_contains "$l_case_dir/mailx.body" "Source root: tank/source"
	self_test_assert_contains "$l_case_dir/zxfer.argv" "-T"
	self_test_assert_contains "$l_case_dir/zxfer.argv" "root@target.example"
	self_test_assert_contains "$l_case_dir/zxfer.argv" "-w"
}

self_test_mail_fallback_case() {
	l_tmpdir=$1
	l_case_dir="$l_tmpdir/mail"

	mkdir -p "$l_case_dir/bin"
	write_self_test_zxfer "$l_case_dir/bin/zxfer"
	write_self_test_mail_capture "$l_case_dir/bin/mail"

	l_status=$(run_script_for_self_test "$l_case_dir" "$l_case_dir/bin" MAIL_FROM="operator@example.com")
	if [ "$l_status" -ne 3 ]; then
		self_test_fail "mail fallback case expected exit status 3, got $l_status"
	fi

	self_test_assert_contains "$l_case_dir/mail.argv" "-s"
	self_test_assert_contains "$l_case_dir/mail.argv" "-r"
	self_test_assert_contains "$l_case_dir/mail.argv" "operator@example.com"
	self_test_assert_contains "$l_case_dir/mail.body" "Destination root: backup/dest"
}

self_test_sendmail_case() {
	l_tmpdir=$1
	l_case_dir="$l_tmpdir/sendmail"

	mkdir -p "$l_case_dir/bin"
	write_self_test_zxfer "$l_case_dir/bin/zxfer"
	write_self_test_mail_capture "$l_case_dir/bin/sendmail"

	l_status=$(run_script_for_self_test "$l_case_dir" "$l_case_dir/bin" MAIL_FROM="daemon@example.com")
	if [ "$l_status" -ne 3 ]; then
		self_test_fail "sendmail fallback case expected exit status 3, got $l_status"
	fi

	self_test_assert_contains "$l_case_dir/sendmail.argv" "-t"
	self_test_assert_contains "$l_case_dir/sendmail.argv" "-f"
	self_test_assert_contains "$l_case_dir/sendmail.argv" "daemon@example.com"
	self_test_assert_contains "$l_case_dir/sendmail.body" "To: alerts@example.com"
	self_test_assert_contains "$l_case_dir/sendmail.body" "From: daemon@example.com"
	self_test_assert_contains "$l_case_dir/sendmail.body" "Subject: zxfer failure [runtime/snapshot discovery]"
}

self_test_usr_lib_sendmail_case() {
	l_tmpdir=$1
	l_case_dir="$l_tmpdir/usr-lib-sendmail"

	mkdir -p "$l_case_dir/bin" "$l_case_dir/usr/lib"
	write_self_test_zxfer "$l_case_dir/bin/zxfer"
	write_self_test_mail_capture "$l_case_dir/usr/lib/sendmail"

	l_status=$(run_script_for_self_test "$l_case_dir" "$l_case_dir/usr/lib" MAIL_FROM="solaris@example.com")
	if [ "$l_status" -ne 3 ]; then
		self_test_fail "/usr/lib sendmail case expected exit status 3, got $l_status"
	fi

	self_test_assert_contains "$l_case_dir/sendmail.argv" "-t"
	self_test_assert_contains "$l_case_dir/sendmail.argv" "-f"
	self_test_assert_contains "$l_case_dir/sendmail.argv" "solaris@example.com"
	self_test_assert_contains "$l_case_dir/sendmail.body" "From: solaris@example.com"
}

self_test_missing_mailer_case() {
	l_tmpdir=$1
	l_case_dir="$l_tmpdir/no-mailer"

	mkdir -p "$l_case_dir/bin"
	write_self_test_zxfer "$l_case_dir/bin/zxfer"

	l_status=$(run_script_for_self_test "$l_case_dir" "$l_case_dir/bin")
	if [ "$l_status" -ne 3 ]; then
		self_test_fail "missing mailer case should preserve zxfer exit status 3, got $l_status"
	fi

	self_test_assert_contains "$l_case_dir/stderr" "mailx, mail, or sendmail is required to deliver zxfer alerts"
	self_test_assert_contains "$l_case_dir/stderr" "warning: failed to deliver zxfer alert mail."
}

self_test_relative_error_log_case() {
	l_tmpdir=$1
	l_case_dir="$l_tmpdir/relative-error-log"

	mkdir -p "$l_case_dir/bin"
	write_self_test_zxfer "$l_case_dir/bin/zxfer"
	write_self_test_mail_capture "$l_case_dir/bin/mailx"

	set +e
	output=$(
		ALERT_PATH="$l_case_dir/bin" \
			ZXFER_BIN="$l_case_dir/bin/zxfer" \
			ERROR_LOG="relative.log" \
			ALERT_TO="alerts@example.com" \
			sh "$SCRIPT_PATH" 2>&1
	)
	l_status=$?
	set -e

	if [ "$l_status" -ne 2 ]; then
		self_test_fail "relative ERROR_LOG case expected exit status 2, got $l_status. Output: $output"
	fi
	case "$output" in
	*"ERROR_LOG must be an absolute path"*) ;;
	*)
		self_test_fail "relative ERROR_LOG case did not print the absolute-path validation error. Output: $output"
		;;
	esac
	if [ -e "$l_case_dir/mailx.argv" ] || [ -e "$l_case_dir/zxfer.argv" ]; then
		self_test_fail "relative ERROR_LOG case should fail before running zxfer or the mailer."
	fi
}

self_test_root_error_log_parent_case() {
	l_old_error_log=${ERROR_LOG-}
	ERROR_LOG=/error.log
	l_parent=$(error_log_parent_dir)
	ERROR_LOG=$l_old_error_log

	if [ "$l_parent" != "/" ]; then
		self_test_fail "root-level ERROR_LOG case expected parent /, got $l_parent"
	fi
}

self_test_ignores_stale_error_log_case() {
	l_tmpdir=$1
	l_case_dir="$l_tmpdir/stale-error-log"

	mkdir -p "$l_case_dir/bin"
	write_self_test_zxfer "$l_case_dir/bin/zxfer"
	write_self_test_mail_capture "$l_case_dir/bin/mailx"
	cat >"$l_case_dir/error.log" <<'EOF'
zxfer: failure report begin
failure_class: runtime
failure_stage: old-stage
message: old failure
source_root: old/src
destination_root: old/dst
zxfer: failure report end
EOF

	l_status=$(run_script_for_self_test "$l_case_dir" "$l_case_dir/bin" \
		MOCK_ZXFER_APPEND_ERROR_LOG=0 \
		MOCK_SOURCE_ROOT="current/src" \
		MOCK_DEST_ROOT="current/dst")
	if [ "$l_status" -ne 3 ]; then
		self_test_fail "stale ERROR_LOG case expected exit status 3, got $l_status"
	fi

	self_test_assert_contains "$l_case_dir/mailx.body" "Source root: current/src"
	self_test_assert_contains "$l_case_dir/mailx.body" "Destination root: current/dst"
	if grep -F -- "old failure" "$l_case_dir/mailx.body" >/dev/null 2>&1; then
		self_test_fail "stale ERROR_LOG case reused an older failure block from ERROR_LOG."
	fi
}

self_test_ignores_caller_mail_env_case() {
	set +e
	output=$(
		MAILER="bogus" \
			MAIL_FROM="polluted@example.com" \
			MAIL_FROM_FLAG="--broken-from-flag" \
			SENDMAIL_FROM_FLAG="--broken-envelope-flag" \
			ORIGIN_HOST="origin@example.com" \
			TARGET_HOST="target@example.com" \
			RAW_SEND=1 \
			ZXFER_MAIL_SELF_TEST_NO_RECURSE=1 \
			sh "$SCRIPT_PATH" --self-test 2>&1
	)
	l_status=$?
	set -e

	if [ "$l_status" -ne 0 ]; then
		self_test_fail "caller-env isolation case expected exit status 0, got $l_status. Output: $output"
	fi
	case "$output" in
	*"self-test passed."*) ;;
	*)
		self_test_fail "caller-env isolation case did not report success. Output: $output"
		;;
	esac
}

self_test_rejects_extra_args_case() {
	set +e
	output=$(sh "$SCRIPT_PATH" --self-test unexpected 2>&1)
	l_status=$?
	set -e

	if [ "$l_status" -ne 2 ]; then
		self_test_fail "extra-argument case expected exit status 2, got $l_status"
	fi
	case "$output" in
	*"usage:"*) ;;
	*)
		self_test_fail "extra-argument case did not print usage. Output: $output"
		;;
	esac
}

run_self_test() {
	l_tmpdir=$(make_temp_dir)
	trap 'rm -rf "$l_tmpdir"' EXIT HUP INT TERM

	self_test_mailx_case "$l_tmpdir"
	self_test_mail_fallback_case "$l_tmpdir"
	self_test_sendmail_case "$l_tmpdir"
	self_test_usr_lib_sendmail_case "$l_tmpdir"
	self_test_missing_mailer_case "$l_tmpdir"
	self_test_relative_error_log_case "$l_tmpdir"
	self_test_root_error_log_parent_case
	self_test_ignores_stale_error_log_case "$l_tmpdir"
	if [ "${ZXFER_MAIL_SELF_TEST_NO_RECURSE:-0}" != 1 ]; then
		self_test_ignores_caller_mail_env_case
	fi
	self_test_rejects_extra_args_case

	trap - EXIT HUP INT TERM
	rm -rf "$l_tmpdir"
	printf '%s\n' "error-log-email-notify.sh self-test passed."
}

main() {
	if [ "$#" -eq 1 ] && [ "${1:-}" = "--self-test" ]; then
		run_self_test
		exit 0
	fi

	if [ $# -gt 0 ]; then
		printf 'usage: %s [--self-test]\n' "$(basename "$0")" >&2
		exit 2
	fi

	if ! validate_error_log_path; then
		exit 2
	fi

	ensure_error_log_parent
	run_tmpdir=$(make_temp_dir)
	stderr_capture="$run_tmpdir/zxfer.stderr"
	trap 'rm -rf "$run_tmpdir"' EXIT HUP INT TERM

	set +e
	run_zxfer 2>"$stderr_capture"
	status=$?
	set -e
	if [ -s "$stderr_capture" ]; then
		cat "$stderr_capture" >&2 || :
	fi

	if [ "$status" -eq 0 ]; then
		exit 0
	fi

	report=$(extract_latest_failure_report "$stderr_capture")

	if [ -z "$report" ]; then
		report="No structured failure report was found in the current zxfer stderr output."
	fi

	failure_class=$(printf '%s\n' "$report" | report_field failure_class || :)
	failure_stage=$(printf '%s\n' "$report" | report_field failure_stage || :)
	source_root=$(printf '%s\n' "$report" | report_field source_root || :)
	destination_root=$(printf '%s\n' "$report" | report_field destination_root || :)

	if [ -z "$failure_class" ]; then
		failure_class="runtime"
	fi
	if [ -z "$failure_stage" ]; then
		failure_stage="unknown-stage"
	fi

	l_host=$(current_host_name)
	subject="zxfer failure [$failure_class/$failure_stage] on $l_host"
	body=$(
		cat <<EOF
zxfer exited with status $status on $l_host.

Source root: ${source_root:-$SRC_DATASET}
Destination root: ${destination_root:-$DEST_DATASET}
Error log: $ERROR_LOG

$report
EOF
	)

	set +e
	send_alert_mail "$subject" "$body"
	mail_status=$?
	set -e

	if [ "$mail_status" -ne 0 ]; then
		printf '%s\n' "warning: failed to deliver zxfer alert mail." >&2
	fi

	exit "$status"
}

main "$@"
