#!/bin/sh
#
# Black-box send/receive pipeline suite for ./zxfer.
#
# Drives the real launcher against the canned zfs behind a strict receive
# wrapper that records each stream's size and, like a real `zfs receive`,
# fails on an empty stream. Pins that every -D progress stream reaches both
# its receive and its dialog under -j 1 (eval in zxfer's shell) and -j 3
# (a job shell with no zxfer functions), and that -n never sends.
#
# shellcheck disable=SC1090,SC2016,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/blackbox.sh
. "$TESTS_DIR/helpers/blackbox.sh"

# Purpose: Put a strict receive in front of the canned zfs: each receive
# appends "DATASET BYTES" to $CASE_DIR/receive.bytes and fails on 0 bytes.
# Usage: sendrecv_write_strict_receive_zfs
sendrecv_write_strict_receive_zfs() {
	mv "$MOCKBIN_DIR/zfs" "$MOCKBIN_DIR/zfs.canned" ||
		fail "Unable to stage the canned zfs behind the strict receive."
	cat >"$MOCKBIN_DIR/zfs" <<EOF
#!/bin/sh
case "\${1:-}" in
receive | recv)
	for strict_arg in "\$@"; do
		strict_dataset=\$strict_arg
	done
	strict_bytes=\$(wc -c | tr -d ' ')
	printf '%s %s\n' "\$strict_dataset" "\$strict_bytes" >>"$CASE_DIR/receive.bytes"
	if [ "\$strict_bytes" -eq 0 ]; then
		echo "cannot receive: failed to read from stream" >&2
		exit 1
	fi
	exec "$MOCKBIN_DIR/zfs.canned" "\$@" </dev/null
	;;
esac
exec "$MOCKBIN_DIR/zfs.canned" "\$@"
EOF
	chmod +x "$MOCKBIN_DIR/zfs"
}

# Purpose: Build the -j incremental environment with the strict receive, both
# %%size%% probes answering 4096, and a dialog that keeps one stream copy per
# run in $CASE_DIR/dialog.<pid>.stream.
# Usage: sendrecv_setup_progress_env <state-name>
sendrecv_setup_progress_env() {
	planning_setup_parallel_jobs_env "$1"
	sendrecv_write_strict_receive_zfs
	printf '4096\n' >"$STATE_DIR/written.value"
	printf 'size\t4096\n' >"$STATE_DIR/send_estimate.list"
	printf 'get -Hpo value written@* *\twritten.value\t0\nsend -nPv *\tsend_estimate.list\t0\n' \
		>>"$STATE_DIR/manifest" ||
		fail "Unable to append the size estimate manifest rules."
	cat >"$MOCKBIN_DIR/progress_dialog" <<EOF
#!/bin/sh
printf '%s\n' "\$*" >>"$CASE_DIR/dialog.argv"
cat >"$CASE_DIR/dialog.\$\$.stream"
EOF
	chmod +x "$MOCKBIN_DIR/progress_dialog"
}

# Purpose: Assert a -D run exited 0 and every dataset's stream reached its
# receive and its dialog exactly once.
# Usage: sendrecv_assert_every_stream_delivered <status> <label>
sendrecv_assert_every_stream_delivered() {
	assertEquals "$2 should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$1"
	planning_assert_no_mutations
	for l_sendrecv_suffix in "" /child1 /child2; do
		l_sendrecv_source="$ZXFER_MOCKBIN_SOURCE_ROOT$l_sendrecv_suffix"
		l_sendrecv_dest="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_sendrecv_suffix"
		assertEquals "$2: $l_sendrecv_dest must be received exactly once" \
			1 "$(grep -cx "receive $l_sendrecv_dest" "$ZFS_LOG")"
		assertTrue "$2: the dialog must start with the title and size for $l_sendrecv_source" \
			"grep -Fxq -- '$l_sendrecv_source@snap3 4096' '$CASE_DIR/dialog.argv'"
		assertTrue "$2: a dialog must read the $l_sendrecv_source stream" \
			"grep -Fxq 'ZXFERMOCKSTREAM send -I $l_sendrecv_source@snap2 $l_sendrecv_source@snap3' $CASE_DIR/dialog.*.stream"
	done
	assertEquals "$2: every receive must have read a non-empty stream: $(cat "$CASE_DIR/receive.bytes")" \
		"3 0" "$(awk '{ n++ } $2 <= 0 { empty++ } END { print n + 0, empty + 0 }' "$CASE_DIR/receive.bytes")"
	planning_assert_no_parallel_job_leftovers
}

# Invariant: with -j 3 each pipeline runs in a job shell that has no zxfer
# functions; its plain-shell -D stage must still feed both the receive and
# the dialog. Regression: the stage called a zxfer function, so every
# receive got an empty stream.
test_progress_dialog_with_parallel_jobs_delivers_every_stream() {
	sendrecv_setup_progress_env progress_j3

	TMPDIR="$JOB_TMP_DIR" planning_run_zxfer "$STATE_DIR" -j 3 \
		-D "$MOCKBIN_DIR/progress_dialog %%title%% %%size%%" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	sendrecv_assert_every_stream_delivered $? "-j 3 -D"
	planning_assert_log_has_line \
		"get -Hpo value written@snap2 $ZXFER_MOCKBIN_SOURCE_ROOT"
}

# Invariant: the same stage run by eval in zxfer's own shell (-j 1) feeds
# both the receive and the dialog, sized by the exact send estimate.
test_progress_dialog_in_the_foreground_delivers_every_stream() {
	sendrecv_setup_progress_env progress_j1

	TMPDIR="$JOB_TMP_DIR" planning_run_zxfer "$STATE_DIR" -j 1 \
		-D "$MOCKBIN_DIR/progress_dialog %%title%% %%size%%" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	sendrecv_assert_every_stream_delivered $? "-j 1 -D"
	planning_assert_log_has_line \
		"send -nPv -I $ZXFER_MOCKBIN_SOURCE_ROOT@snap2 $ZXFER_MOCKBIN_SOURCE_ROOT@snap3"
}

# Invariant: -n with -j and -D never sends, receives, probes a size, or
# starts a dialog; the job scheduler is never reached in a dry run.
test_dry_run_with_parallel_jobs_and_progress_never_sends() {
	sendrecv_setup_progress_env progress_dryrun

	TMPDIR="$JOB_TMP_DIR" planning_run_zxfer "$STATE_DIR" -n -j 3 \
		-D "$MOCKBIN_DIR/progress_dialog %%title%% %%size%%" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	dry_run_status=$?
	assertEquals "-n -j 3 -D should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$dry_run_status"
	planning_assert_no_mutations
	planning_assert_no_send_receive
	assertFalse "a dry run must not start the progress dialog" \
		"[ -e '$CASE_DIR/dialog.argv' ]"
	assertFalse "a dry run must not probe a size" \
		"grep -q 'written@' '$ZFS_LOG'"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
