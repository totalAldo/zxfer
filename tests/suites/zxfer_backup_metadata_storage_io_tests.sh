#!/bin/sh
# Backup storage paths, local directory protections, and the remote
# directory/write/read protocol tests (rendered programs run locally through
# `sh -c` behind a stubbed ssh transport).
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

# Stub the ssh transport so a rendered remote program runs locally.
zxfer_backup_test_stub_local_transport() {
	zxfer_build_remote_sh_c_command() {
		g_zxfer_remote_sh_c_command_result=$1
		printf '%s\n' "$1"
	}
	zxfer_invoke_ssh_shell_command_for_host() { sh -c "$2"; }
}

# Run the real pair writer with a failed filesystem operation, locally or
# through the rendered SSH program. The wrappers affect only this subshell.
zxfer_backup_test_fail_pair_write() (
	BACKUP_TEST_PAIR_FAILURE=$1
	BACKUP_TEST_PAIR_BIN="$TEST_TMPDIR/pair-failure-bin"
	BACKUP_TEST_REAL_MV=$(command -v mv)
	BACKUP_TEST_REAL_MKTEMP=$(command -v mktemp)
	BACKUP_TEST_REAL_CAT=$(command -v cat)
	export BACKUP_TEST_PAIR_FAILURE BACKUP_TEST_PRIMARY_FILE BACKUP_TEST_FORWARDED_FILE
	export BACKUP_TEST_REAL_MV BACKUP_TEST_REAL_MKTEMP BACKUP_TEST_REAL_CAT
	mkdir -p "$BACKUP_TEST_PAIR_BIN"
	cat >"$BACKUP_TEST_PAIR_BIN/tool" <<'EOF'
#!/bin/sh
case ${0##*/} in
mktemp)
	if [ "$BACKUP_TEST_PAIR_FAILURE" = stage ] &&
		[ "${1%/*}" = "${BACKUP_TEST_FORWARDED_FILE%/*}" ]; then
		exit 1
	fi
	exec "$BACKUP_TEST_REAL_MKTEMP" "$@"
	;;
cat)
	if [ "$BACKUP_TEST_PAIR_FAILURE" = recovery_read ] &&
		[ "${1:-}" = "$BACKUP_TEST_PRIMARY_FILE" ]; then
		exit 1
	fi
	exec "$BACKUP_TEST_REAL_CAT" "$@"
	;;
mv)
	case $BACKUP_TEST_PAIR_FAILURE in
	primary)
		[ "$3" != "$BACKUP_TEST_PRIMARY_FILE" ] || exit 1
		;;
	forwarded | rollback)
		[ "$3" != "$BACKUP_TEST_FORWARDED_FILE" ] || exit 1
		if [ "$BACKUP_TEST_PAIR_FAILURE" = rollback ]; then
			case $2 in *.zxfer-backup-recovery.*) exit 1 ;; esac
		fi
		;;
	signal_primary | signal_forwarded)
		"$BACKUP_TEST_REAL_MV" "$@" || exit "$?"
		if { [ "$BACKUP_TEST_PAIR_FAILURE" = signal_primary ] && [ "$3" = "$BACKUP_TEST_PRIMARY_FILE" ]; } ||
			{ [ "$BACKUP_TEST_PAIR_FAILURE" = signal_forwarded ] && [ "$3" = "$BACKUP_TEST_FORWARDED_FILE" ]; }; then
			kill -s TERM "$PPID"
		fi
		exit 0
		;;
	esac
	exec "$BACKUP_TEST_REAL_MV" "$@"
	;;
esac
EOF
	chmod 700 "$BACKUP_TEST_PAIR_BIN/tool"
	for pair_tool in mv mktemp cat; do
		ln -sf tool "$BACKUP_TEST_PAIR_BIN/$pair_tool"
	done
	PATH="$BACKUP_TEST_PAIR_BIN:$PATH"
	export PATH
	if [ "$2" = remote ]; then
		g_option_T_target_host="target.example"
		g_zxfer_secure_path=$PATH
		zxfer_backup_test_stub_local_transport
		zxfer_resolve_cli_command_safe() { g_zxfer_resolved_cli_command_result="cat"; }
	fi
	zxfer_write_backup_properties
)

test_backup_pair_failures_preserve_both_previous_files_locally_and_remotely() {
	for pair_mode in local remote; do
		for pair_failure in stage recovery_read primary forwarded; do
			zxfer_backup_test_use_private_root "pair_${pair_mode}_${pair_failure}"
			g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
			(umask 077 && mkdir -p "${BACKUP_TEST_PRIMARY_FILE%/*}" "${BACKUP_TEST_FORWARDED_FILE%/*}")
			printf 'old primary\n' >"$BACKUP_TEST_PRIMARY_FILE"
			printf 'old forwarded\n' >"$BACKUP_TEST_FORWARDED_FILE"
			output=$(zxfer_backup_test_fail_pair_write "$pair_failure" "$pair_mode" 2>&1)
			assertEquals "$pair_mode $pair_failure must report failure: $output" 1 "$?"
			assertEquals "$pair_mode $pair_failure preserves the primary." "old primary" "$(cat "$BACKUP_TEST_PRIMARY_FILE")"
			assertEquals "$pair_mode $pair_failure preserves the alias." "old forwarded" "$(cat "$BACKUP_TEST_FORWARDED_FILE")"
			assertEquals "Handled failure cleans its private staging and recovery files." \
				"" "$(find "$g_backup_storage_root" -name '.zxfer-backup-*')"
		done
	done
}

test_backup_pair_second_publish_failure_removes_a_new_primary() {
	for pair_mode in local remote; do
		zxfer_backup_test_use_private_root "pair_new_$pair_mode"
		g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
		output=$(zxfer_backup_test_fail_pair_write forwarded "$pair_mode" 2>&1)
		assertEquals "$pair_mode must report failure: $output" 1 "$?"
		assertFalse "A failed first publication must leave neither file live." "[ -e '$BACKUP_TEST_PRIMARY_FILE' ]"
		assertFalse "[ -e '$BACKUP_TEST_FORWARDED_FILE' ]"
	done
}

test_backup_pair_rollback_failure_keeps_private_recovery_and_reports_its_path() {
	for pair_mode in local remote; do
		zxfer_backup_test_use_private_root "pair_recovery_$pair_mode"
		g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
		(umask 077 && mkdir -p "${BACKUP_TEST_PRIMARY_FILE%/*}" "${BACKUP_TEST_FORWARDED_FILE%/*}")
		printf 'old primary\n' >"$BACKUP_TEST_PRIMARY_FILE"
		printf 'old forwarded\n' >"$BACKUP_TEST_FORWARDED_FILE"
		output=$(zxfer_backup_test_fail_pair_write rollback "$pair_mode" 2>&1)
		assertEquals "$pair_mode must report failure: $output" 1 "$?"
		recovery_file=$(find "$g_backup_storage_root" -name '.zxfer-backup-recovery.*')
		assertNotNull "A failed rollback preserves the old contents for recovery." "$recovery_file"
		assertEquals "old primary" "$(cat "$recovery_file")"
		assertContains "$output" "$recovery_file"
		assertContains "$output" "restoring backup metadata rollback state"
		assertFalse "A failed rollback must not leave the new primary looking authoritative." "[ -e '$BACKUP_TEST_PRIMARY_FILE' ]"
		assertEquals "old forwarded" "$(cat "$BACKUP_TEST_FORWARDED_FILE")"
		case "$(ls -ln "$recovery_file")" in
		-rw-------*) ;;
		*) fail "Recovery metadata must remain private." ;;
		esac
	done
}

test_backup_pair_defers_signals_until_publication_state_is_consistent() {
	for pair_mode in local remote; do
		for pair_signal in signal_primary signal_forwarded; do
			zxfer_backup_test_use_private_root "pair_${pair_mode}_${pair_signal}"
			g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
			(umask 077 && mkdir -p "${BACKUP_TEST_PRIMARY_FILE%/*}" "${BACKUP_TEST_FORWARDED_FILE%/*}")
			printf 'old primary\n' >"$BACKUP_TEST_PRIMARY_FILE"
			printf 'old forwarded\n' >"$BACKUP_TEST_FORWARDED_FILE"
			output=$(zxfer_backup_test_fail_pair_write "$pair_signal" "$pair_mode" 2>&1)
			assertEquals "$pair_mode $pair_signal reports interruption: $output" 1 "$?"
			if [ "$pair_signal" = signal_primary ]; then
				assertEquals "A signal after the first rename restores the old primary." "old primary" "$(cat "$BACKUP_TEST_PRIMARY_FILE")"
				assertEquals "old forwarded" "$(cat "$BACKUP_TEST_FORWARDED_FILE")"
			else
				assertContains "A signal after the second rename keeps the completed primary." \
					"$(cat "$BACKUP_TEST_PRIMARY_FILE")" "$g_backup_file_contents"
				assertContains "A signal after the second rename keeps the completed alias." \
					"$(cat "$BACKUP_TEST_FORWARDED_FILE")" "$g_backup_file_contents"
			fi
			assertEquals "Interrupted writes leave no disposable staging files." \
				"" "$(find "$g_backup_storage_root" -name '.zxfer-backup-*')"
		done
	done
}

test_get_backup_metadata_filename_renders_the_documented_identity_paths() {
	g_backup_file_extension=".zxfer_backup_info"
	long_source="tank/$(printf 'a%.0s' 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30)"
	# The man page example is the hex of "tank/src\nbackup/dst" in 48-character
	# chunks under the fixed v2 leaf name; a longer identity takes more chunks.
	# Retired writers keyed the name by cksum of "tank/src<LF>backup/dst" with
	# no final newline. source|destination|kind|name
	for case_spec in \
		"tank/src|backup/dst||.zxfer_backup_info.v2/h/74616e6b2f7372630a6261636b75702f647374/.zxfer_backup_info.v2" \
		"tank/my :src|backup/dst||.zxfer_backup_info.v2/h/74616e6b2f6d79203a7372630a6261636b75702f647374/.zxfer_backup_info.v2" \
		"$long_source|backup/dst||.zxfer_backup_info.v2/h/74616e6b2f61616161616161616161616161616161616161/61616161616161616161610a6261636b75702f647374/.zxfer_backup_info.v2" \
		"tank/src|backup/dst|legacy|.zxfer_backup_info.src.k1537737481.19"; do
		name_source=${case_spec%%|*}
		rest=${case_spec#*|}
		name_destination=${rest%%|*}
		rest=${rest#*|}
		name_kind=${rest%%|*}
		assertEquals "[$name_source] -> [$name_destination] ${name_kind:-current} name." \
			"${rest#*|}" \
			"$(zxfer_get_backup_metadata_filename "$name_source" "$name_destination" ${name_kind:+"$name_kind"})"
	done

	for name_kind in current legacy; do
		output=$(
			g_cmd_awk=false
			zxfer_get_backup_metadata_filename tank/src backup/dst "$name_kind"
		)
		assertEquals "A failed $name_kind identity pass fails closed without a partial name." \
			"1 " "$? $output"
	done
}

test_ensure_local_backup_dir_creates_private_directories_and_refuses_unsafe_paths() {
	base="$TEST_TMPDIR_PHYSICAL/ensure_local"
	mkdir -p "$base"
	zxfer_ensure_local_backup_dir "$base/new/nested"
	case "$(ls -ldn "$base/new/nested")" in
	drwx------*) ;;
	*) fail "Created directories must be 0700: $(ls -ldn "$base/new/nested")" ;;
	esac
	case "$(ls -ldn "$base/new")" in
	drwx------*) ;;
	*) fail "Intermediate directories are created under umask 077: $(ls -ldn "$base/new")" ;;
	esac

	ln -s "$base/new" "$base/link"
	: >"$base/file"
	failing_chmod_dir="$TEST_TMPDIR/ensure_local_failing_chmod"
	mkdir -p "$failing_chmod_dir"
	printf '#!/bin/sh\nexit 1\n' >"$failing_chmod_dir/chmod"
	chmod +x "$failing_chmod_dir/chmod"
	# directory|simulated failure|expected error
	for case_spec in \
		"$base/link||Refusing to use backup directory $base/link because it is a symlink." \
		"$base/link/child||Refusing to use backup directory $base/link/child because path component $base/link is a symlink." \
		"$base/file||Refusing to use backup directory $base/file because it is not a directory." \
		"$base/new|owner 4321|Refusing to use backup directory $base/new because it is owned by UID 4321 instead of" \
		"$base/new|owner unknown|Cannot determine the owner of backup directory $base/new." \
		"$base/missing|mkdir|Error creating secure backup directory $base/missing." \
		"$base/new|chmod|Error securing backup directory $base/new."; do
		dir=${case_spec%%|*}
		rest=${case_spec#*|}
		failure=${rest%%|*}
		output=$(
			(
				# No case statement here: bash 3.2 (macOS /bin/sh) mis-parses
				# case patterns inside command substitution.
				if [ "$failure" = "owner unknown" ]; then
					zxfer_get_path_owner_uid() { return 1; }
				elif [ "${failure%% *}" = owner ]; then
					zxfer_get_path_owner_uid() { printf '%s\n' "${failure#owner }"; }
				elif [ "$failure" = mkdir ]; then
					mkdir() { return 1; }
				elif [ "$failure" = chmod ]; then
					PATH="$failing_chmod_dir:$PATH"
				fi
				zxfer_ensure_local_backup_dir "$dir"
			) 2>&1
		)
		assertEquals "Directory [$dir] ${failure:+with $failure }must be refused." 1 "$?"
		assertContains "$output" "${rest#*|}"
	done
}

# The local pair writer runs /bin/sh, like job shells, so a secure PATH
# without sh (as the integration harness narrows ZXFER_SECURE_PATH) still
# publishes both files.
test_write_backup_properties_runs_bin_sh_without_sh_on_path() {
	zxfer_backup_test_use_private_root write_without_sh
	g_option_T_target_host=""
	g_option_n_dryrun=0
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	no_sh_path="$TEST_TMPDIR/backup-path-without-sh"
	mkdir -p "$no_sh_path"
	for no_sh_tool in awk cat chmod date id ls mkdir mktemp mv rm stat; do
		case $(command -v "$no_sh_tool" 2>/dev/null) in
		/*) ln -sf "$(command -v "$no_sh_tool")" "$no_sh_path/$no_sh_tool" ;;
		esac
	done

	output=$(
		(
			PATH=$no_sh_path
			zxfer_write_backup_properties
		) 2>&1
	)
	assertEquals "A local -k write needs no sh on PATH; output: $output" 0 "$?"
	for metadata_file in "$BACKUP_TEST_PRIMARY_FILE" "$BACKUP_TEST_FORWARDED_FILE"; do
		assertContains "The write publishes $metadata_file." \
			"$(cat "$metadata_file" 2>/dev/null)" "$g_backup_file_contents"
	done
}

test_write_backup_properties_reports_remote_dependency_write_and_transport_failures() {
	zxfer_backup_test_use_private_root write_remote_failures
	g_option_T_target_host="target.example"
	g_option_n_dryrun=0
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	empty_dir="$TEST_TMPDIR/write_remote_empty_bin"
	mkdir -p "$empty_dir"

	output=$(
		(
			g_zxfer_secure_path="$empty_dir"
			zxfer_backup_test_stub_local_transport
			zxfer_resolve_cli_command_safe() { g_zxfer_resolved_cli_command_result="cat"; }
			zxfer_throw_error() {
				printf 'class=%s %s\n' "${g_zxfer_failure_class:-}" "$1"
				exit 1
			}
			zxfer_write_backup_properties
		) 2>&1
	)
	assertEquals 1 "$?"
	assertContains "$output" "Required dependency \"mkdir\" not found on host target.example in secure PATH ($empty_dir)."
	assertContains "$output" "class=dependency Required remote backup-write helper dependency not found on host target.example in secure PATH ($empty_dir)."

	for case_spec in \
		"92||Error writing backup file. Is filesystem mounted?" \
		"255|ssh: connect refused|Failed to contact host target.example while writing backup metadata $BACKUP_TEST_PRIMARY_FILE." \
		"1||Error writing backup file. Is filesystem mounted?"; do
		remote_status=${case_spec%%|*}
		rest=${case_spec#*|}
		remote_stderr=${rest%%|*}
		expected=${rest#*|}
		output=$(
			(
				REMOTE_STATUS=$remote_status
				REMOTE_STDERR=$remote_stderr
				zxfer_resolve_cli_command_safe() { g_zxfer_resolved_cli_command_result="cat"; }
				zxfer_invoke_ssh_shell_command_for_host() {
					[ -z "$REMOTE_STDERR" ] || printf '%s\n' "$REMOTE_STDERR" >&2
					return "$REMOTE_STATUS"
				}
				zxfer_write_backup_properties
			) 2>&1
		)
		assertEquals "Remote status $remote_status must fail the write." 1 "$?"
		assertContains "$output" "$expected"
		[ -z "$remote_stderr" ] || assertContains "Transport stderr is forwarded." "$output" "$remote_stderr"
	done

	output=$(
		(
			zxfer_resolve_cli_command_safe() {
				g_zxfer_resolved_cli_command_result='Required dependency "cat" not found on host target.example.'
				return 1
			}
			zxfer_throw_error() {
				printf 'class=%s %s\n' "${g_zxfer_failure_class:-}" "$1"
				exit 1
			}
			zxfer_write_backup_properties
		) 2>&1
	)
	assertEquals 1 "$?"
	assertContains "$output" "class=dependency Required dependency \"cat\" not found on host target.example."
}

test_check_backup_storage_dir_if_needed_reports_remote_dependency_prepare_and_transport_failures() {
	empty_dir="$TEST_TMPDIR/ensure_remote_empty_bin"
	mkdir -p "$empty_dir"
	target_dir="$TEST_TMPDIR_PHYSICAL/ensure_remote_dir/child"
	g_option_k_backup_property_mode=1
	g_option_T_target_host="target.example"
	g_backup_storage_root=$target_dir

	(
		zxfer_backup_test_stub_local_transport
		zxfer_check_backup_storage_dir_if_needed
	)
	assertEquals 0 "$?"
	case "$(ls -ldn "$target_dir")" in
	drwx------*) ;;
	*) fail "The remote program creates the directory 0700: $(ls -ldn "$target_dir")" ;;
	esac

	output=$(
		(
			g_zxfer_secure_path="$empty_dir"
			zxfer_backup_test_stub_local_transport
			zxfer_throw_error() {
				printf 'class=%s %s\n' "${g_zxfer_failure_class:-}" "$1"
				exit 1
			}
			zxfer_check_backup_storage_dir_if_needed
		) 2>&1
	)
	assertEquals 1 "$?"
	assertContains "$output" "Required dependency \"mkdir\" not found on host target.example in secure PATH ($empty_dir)."
	assertContains "$output" "class=dependency Required remote backup-directory helper dependency not found on host target.example in secure PATH ($empty_dir)."

	for case_spec in \
		"92|Backup path exists but is not a directory.|Error preparing backup directory on target.example." \
		"255|ssh: connect refused|Failed to contact host target.example while preparing backup directory $target_dir." \
		"1||Error preparing backup directory on target.example."; do
		remote_status=${case_spec%%|*}
		rest=${case_spec#*|}
		remote_stderr=${rest%%|*}
		expected=${rest#*|}
		output=$(
			(
				REMOTE_STATUS=$remote_status
				REMOTE_STDERR=$remote_stderr
				zxfer_invoke_ssh_shell_command_for_host() {
					[ -z "$REMOTE_STDERR" ] || printf '%s\n' "$REMOTE_STDERR" >&2
					return "$REMOTE_STATUS"
				}
				zxfer_check_backup_storage_dir_if_needed
			) 2>&1
		)
		assertEquals "Remote status $remote_status must fail directory preparation." 1 "$?"
		assertContains "$output" "$expected"
		[ -z "$remote_stderr" ] || assertContains "Remote stderr is forwarded." "$output" "$remote_stderr"
	done
}

test_run_remote_backup_script_feeds_stdin_and_reports_capture_failures() {
	marker="$TEST_TMPDIR/run_remote_script_payload"
	(
		zxfer_backup_test_stub_local_transport
		zxfer_run_remote_backup_script "target.example" "cat >'$marker';" destination "testing" backup-test 92 <<EOF
payload line
EOF
	)
	assertEquals 0 "$?"
	assertEquals "Standard input reaches the remote program." "payload line" "$(cat "$marker")"

	output=$(
		(
			zxfer_backup_test_stub_local_transport
			zxfer_read_runtime_artifact_file() {
				return 1
			}
			zxfer_run_remote_backup_script "target.example" "echo warning >&2; true;" destination "testing" backup-test 92
		) 2>&1
	)
	assertEquals "A truncated capture fails closed." 1 "$?"
	assertContains "$output" "Failed to reload local remote helper capture while testing on host target.example."
	assertContains "$output" "Failed to read remote probe stderr capture from local staging."
}

test_build_remote_backup_dir_prepare_cmd_runs_locally_and_rejects_symlinked_components() {
	base="$TEST_TMPDIR_PHYSICAL/remote_prepare"
	mkdir -p "$base/real"
	ln -s "$base/real" "$base/link"
	: >"$base/file"

	sh -c "$(zxfer_build_remote_backup_dir_prepare_cmd "$base/new/nested" "target.example")"
	assertEquals 0 "$?"
	case "$(ls -ldn "$base/new/nested")" in
	drwx------*) ;;
	*) fail "The rendered program creates 0700 directories: $(ls -ldn "$base/new/nested")" ;;
	esac

	for case_spec in \
		"$base/link|Refusing to use symlinked zxfer backup directory." \
		"$base/link/child|Refusing to use backup directory $base/link/child because path component $base/link is a symlink." \
		"$base/file|Backup path exists but is not a directory."; do
		output=$(sh -c "$(zxfer_build_remote_backup_dir_prepare_cmd "${case_spec%%|*}" "target.example")" 2>&1)
		assertEquals "Unsafe remote directory [${case_spec%%|*}] exits 92." 92 "$?"
		assertContains "$output" "${case_spec#*|}"
	done
	assertFalse "Nothing is created below a symlinked component." "[ -e '$base/real/child' ]"

	output=$(sh -c "$(zxfer_build_remote_backup_dir_prepare_cmd "-dashed/dir" "target.example")" 2>&1)
	assertEquals "Relative dash-prefixed paths are quoted for ls and still fail closed outside a real root." 92 "$?"
}

test_build_backup_pair_write_cmd_handles_a_shared_target_without_following_symlinks() {
	dir="$TEST_TMPDIR_PHYSICAL/remote_write"
	target="$dir/backup.meta"
	decoy="$TEST_TMPDIR_PHYSICAL/remote_write_decoy"
	printf 'decoy\n' >"$decoy"
	(umask 077 && mkdir -p "$dir")

	printf '%s\n' "line one" "line two" |
		sh -c "$(zxfer_build_backup_pair_write_cmd "$target" "$target" tank/src cat)"
	assertEquals 0 "$?"
	assertEquals "line one
line two" "$(cat "$target")"
	case "$(ls -ldn "$target")" in
	-rw-------*) ;;
	*) fail "Remote writes publish 0600 files: $(ls -ldn "$target")" ;;
	esac

	rm -f "$target"
	ln -s "$decoy" "$target"
	output=$(printf 'attack\n' | sh -c "$(zxfer_build_backup_pair_write_cmd "$target" "$target" tank/src cat)" 2>&1)
	assertEquals 92 "$?"
	assertContains "$output" "Refusing to write backup metadata $target because it is a symlink."
	assertEquals "decoy" "$(cat "$decoy")"
	rm -f "$target"

	output=$(sh -c "$(zxfer_build_backup_pair_write_cmd "$target" "$target" tank/src false)" 2>&1)
	assertEquals "A failed payload helper exits 92." 92 "$?"
	assertFalse "[ -e '$target' ]"
	assertEquals "A failed payload leaves no stage file." "" "$(find "$dir" -name '.zxfer-backup-write.*')"
}

test_build_remote_backup_read_cmd_enforces_owner_mode_and_directory_checks() {
	dir="$TEST_TMPDIR_PHYSICAL/remote_read"
	mkdir -p "$dir"
	chmod 700 "$dir"
	file="$dir/backup.meta"
	printf 'payload\n' >"$file"
	chmod 600 "$file"
	g_cmd_cat="/bin/cat"

	assertEquals "payload" "$(sh -c "$(zxfer_build_remote_backup_read_cmd "$file" "origin.example")")"

	sh -c "$(zxfer_build_remote_backup_read_cmd "$dir/missing.meta" "origin.example")" >/dev/null
	assertEquals "Missing files exit 94." 94 "$?"

	chmod 644 "$file"
	sh -c "$(zxfer_build_remote_backup_read_cmd "$file" "origin.example")" >/dev/null
	assertEquals "Files that are not 0600 exit 96." 96 "$?"
	chmod 600 "$file"

	chmod 777 "$dir"
	sh -c "$(zxfer_build_remote_backup_read_cmd "$file" "origin.example")" >/dev/null
	assertEquals "A directory other users can modify exits 91." 91 "$?"
	chmod 1777 "$dir"
	assertEquals "A sticky shared directory cannot swap the entry and is accepted." \
		"payload" "$(sh -c "$(zxfer_build_remote_backup_read_cmd "$file" "origin.example")")"
	chmod 700 "$dir"

	ln -s "$dir" "$TEST_TMPDIR_PHYSICAL/remote_read_link"
	output=$(sh -c "$(zxfer_build_remote_backup_read_cmd "$TEST_TMPDIR_PHYSICAL/remote_read_link/backup.meta" "origin.example")" 2>&1)
	assertEquals "Symlinked components exit 98." 98 "$?"
	assertContains "$output" "Refusing to use backup metadata $TEST_TMPDIR_PHYSICAL/remote_read_link/backup.meta because path component $TEST_TMPDIR_PHYSICAL/remote_read_link is a symlink."

	# Root-owned directories are trusted, so the directory must belong to
	# neither root nor the ssh user; a fake id reports the ssh user as the
	# directory owner's uid plus one. The FreeBSD and OmniOS CI guests run the
	# suite as root: there the root-owned case is checked first, then the
	# directory is given to uid 4320.
	test_uid=$(id -u)
	dir_owner=$test_uid
	[ "$test_uid" -ne 0 ] || dir_owner=4320
	fake_bin="$TEST_TMPDIR/remote_read_other_owner"
	mkdir -p "$fake_bin"
	cat >"$fake_bin/id" <<EOF
#!/bin/sh
printf '%s\n' $((dir_owner + 1))
EOF
	chmod +x "$fake_bin/id"
	other_user_cmd=$(
		g_zxfer_secure_path="$fake_bin:$ZXFER_DEFAULT_SECURE_PATH"
		zxfer_build_remote_backup_read_cmd "$file" "origin.example"
	)
	if [ "$test_uid" -eq 0 ]; then
		assertEquals "A root-owned directory and file are trusted for any ssh user." \
			"payload" "$(sh -c "$other_user_cmd")"
		chown "$dir_owner" "$dir" || fail "Could not give $dir to uid $dir_owner."
	fi
	sh -c "$other_user_cmd" >/dev/null
	assertEquals "When the ssh user is neither root nor the directory owner the directory check exits 91." 91 "$?"
}

test_build_remote_backup_storage_listing_cmd_lists_the_root_chain_and_descendants() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/listing_store"
	(umask 077 && mkdir -p "$g_backup_storage_root/tank/src/a/.zxfer_backup_info.v2/h/0a" \
		"$g_backup_storage_root/tank/src/b/c" "$g_backup_storage_root/other")
	: >"$g_backup_storage_root/tank/src/.zxfer_backup_info.src.k1.2"
	ln -s "$g_backup_storage_root/other" "$g_backup_storage_root/tank/src/link"

	output=$(sh -c "$(zxfer_build_remote_backup_storage_listing_cmd tank/src origin.example)" | sort -u)
	assertEquals "The chain, descendant directories and symlinks are listed; metadata trees are not." \
		"tank
tank/src
tank/src/a
tank/src/b
tank/src/b/c
tank/src/link" "$output"

	output=$(sh -c "$(zxfer_build_remote_backup_storage_listing_cmd tank/src/b/c/d origin.example)" | sort -u)
	assertEquals "Only existing directories of a deeper root's chain are listed." \
		"tank
tank/src
tank/src/b
tank/src/b/c" "$output"

	ln -s "$g_backup_storage_root/other" "$g_backup_storage_root/pool"
	output=$(sh -c "$(zxfer_build_remote_backup_storage_listing_cmd pool/src origin.example)" 2>&1)
	assertEquals "A symlinked path component exits 92." 92 "$?"
	assertContains "$output" "path component $g_backup_storage_root/pool is a symlink."

	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/listing_store_missing"
	output=$(sh -c "$(zxfer_build_remote_backup_storage_listing_cmd tank/src origin.example)")
	assertEquals "A missing store lists nothing and succeeds." 0 "$?"
	assertEquals "" "$output"
}

# An -O -k run lists the origin's store once. find(1) searches only the
# source root's own storage directory, so a store that is missing or holds
# only an ancestor needs no find; once the root has a directory, a missing
# find is the remote helper dependency error. Regression: every -O -k run,
# a first hop included, required find.
test_list_remote_backup_storage_dirs_needs_find_only_for_an_existing_root_directory() {
	no_find_bin="$TEST_TMPDIR/listing_no_find_bin"
	mkdir -p "$no_find_bin"
	g_initial_source="tank/src"
	g_option_O_origin_host="origin.example"
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/listing_find_store"

	for store_dir in "" tank tank/src; do
		[ -z "$store_dir" ] || (umask 077 && mkdir -p "$g_backup_storage_root/$store_dir")
		output=$(
			(
				g_zxfer_secure_path=$no_find_bin
				zxfer_build_remote_sh_c_command() {
					g_zxfer_remote_sh_c_command_result=$1
					printf '%s\n' "$1"
				}
				zxfer_invoke_ssh_shell_command_for_host() { sh -c "$2"; }
				zxfer_list_remote_backup_storage_dirs
				printf 'listed=%s listing=%s\n' "$g_zxfer_backup_forwarded_listed" \
					"$g_zxfer_backup_forwarded_listing"
			) 2>&1
		)
		status=$?
		if [ "$store_dir" != tank/src ]; then
			assertEquals "Store [${store_dir:-missing}] lists without find." 0 "$status"
			assertEquals "listed=1 listing=$store_dir" "$output"
			continue
		fi
		assertEquals "A root directory without find on the origin fails closed." 1 "$status"
		assertContains "$output" "Required dependency \"find\" not found on host origin.example in secure PATH ($no_find_bin)."
		assertContains "$output" "Required remote backup-metadata helper dependency not found on host origin.example in secure PATH ($no_find_bin)."
	done
}

test_read_remote_backup_file_maps_program_statuses_to_documented_errors() {
	# remote status | throw or return:<status> | expected message
	for case_spec in \
		"91|throw|Refusing to use backup metadata /tmp/backup.meta on origin.example because its directory is not a private directory owned by root or the ssh user." \
		"95|throw|Refusing to use backup metadata /tmp/backup.meta on origin.example because it is not owned by root or the ssh user." \
		"96|throw|Refusing to use backup metadata /tmp/backup.meta on origin.example because its permissions are not 0600." \
		"97|throw|Cannot determine ownership or permissions for backup metadata /tmp/backup.meta on origin.example." \
		"94|return:4|" \
		"98|return:1|Refusing to use backup metadata /tmp/backup.meta because path component /tmp/link is a symlink." \
		"255|throw|Failed to contact host origin.example while reading backup metadata /tmp/backup.meta." \
		"3|return:5|"; do
		remote_status=${case_spec%%|*}
		rest=${case_spec#*|}
		expected_status=${rest%%|*}
		expected_message=${rest#*|}
		output=$(
			(
				REMOTE_STATUS=$remote_status
				zxfer_invoke_ssh_shell_command_for_host() {
					# No case statement here: bash 3.2 (macOS /bin/sh)
					# mis-parses case patterns inside command substitution.
					if [ "$REMOTE_STATUS" -eq 98 ]; then
						printf 'Refusing to use backup metadata /tmp/backup.meta because path component /tmp/link is a symlink.\n' >&2
					elif [ "$REMOTE_STATUS" -eq 255 ]; then
						printf 'ssh: connect refused\n' >&2
					fi
					return "$REMOTE_STATUS"
				}
				zxfer_read_remote_backup_file "origin.example" "/tmp/backup.meta" source >/dev/null
				printf 'status=%s\n' "$?"
			) 2>&1
		)
		case "$expected_status" in
		throw) assertEquals "Remote status $remote_status throws." 1 "$?" ;;
		*) assertContains "Remote status $remote_status maps to ${expected_status#return:}." "$output" "status=${expected_status#return:}" ;;
		esac
		[ -z "$expected_message" ] || assertContains "$output" "$expected_message"
	done

	result=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() {
				printf 'remote payload'
			}
			zxfer_read_remote_backup_file "origin.example" "/tmp/backup.meta" source >/dev/null
			printf '%s' "$g_zxfer_backup_file_read_result"
		)
	)
	assertEquals "Successful reads publish the payload." "remote payload" "$result"
}

test_remote_backup_protocol_renderers_match_readable_golden_output() {
	actual_script="$TEST_TMPDIR/remote_backup_protocol_scripts.actual"
	golden_script="$ZXFER_ROOT/tests/golden/remote_backup_protocol_scripts.golden"

	(
		g_zxfer_secure_path="/secure/bin:/usr/bin"
		g_cmd_cat="/bin/cat"
		printf '%s\n' "### prelude-with-symlink-guard"
		zxfer_build_remote_backup_script_prelude "target.example doas" \
			"/var/db/zxfer/tank/src/backup.meta" metadata id ls awk
		printf '%s\n' "### directory-prepare"
		zxfer_build_remote_backup_dir_prepare_cmd \
			"/var/db/zxfer/tank/src" "target.example doas"
		# The -T write program exactly as zxfer_write_backup_properties
		# composes and sends it.
		printf '%s\n' "### write"
		(
			g_option_T_target_host="target.example doas"
			g_backup_storage_root="/var/db/zxfer"
			g_initial_source="tank/src"
			g_initial_source_had_trailing_slash=1
			g_destination="backup/dst"
			g_backup_file_contents=".	compression=lz4=local"
			zxfer_get_backup_metadata_filename() { printf '%s\n' backup.meta; }
			zxfer_resolve_cli_command_safe() { g_zxfer_resolved_cli_command_result="cat"; }
			zxfer_run_remote_backup_script() { printf '%s\n' "$2"; }
			zxfer_write_backup_properties
		)
		printf '%s\n' "### read"
		zxfer_build_remote_backup_read_cmd \
			"/var/db/zxfer/tank/src/backup.meta" "origin.example doas"
		printf '%s\n' "### storage-listing"
		g_backup_storage_root="/var/db/zxfer"
		zxfer_build_remote_backup_storage_listing_cmd "tank/src" "origin.example doas"
	) >"$actual_script"

	if ! cmp -s "$golden_script" "$actual_script"; then
		diff -u "$golden_script" "$actual_script" >&2
		fail "Readable remote backup protocol renderers drifted from their exact-output golden."
	fi
	assertTrue "Every rendered program is valid POSIX sh." \
		"grep -v '^###' '$actual_script' | sh -n"
}

test_remote_backup_renderers_treat_host_and_path_metacharacters_as_data() {
	marker="$TEST_TMPDIR/renderer_metachar_marker"
	host="target.example; touch $marker #"
	path="$TEST_TMPDIR_PHYSICAL/quote' dir;*/it's.meta"
	helper="$TEST_TMPDIR_PHYSICAL/quote' cat helper"
	cat >"$helper" <<'EOF'
#!/bin/sh
exec /bin/cat "$@"
EOF
	chmod +x "$helper"
	g_cmd_cat=$helper
	zxfer_render_shell_command_from_argv "$helper"
	cat_command=$g_zxfer_shell_command_result

	rendered=$(
		zxfer_build_remote_backup_dir_prepare_cmd "${path%/*}" "$host" mktemp mv rm &&
			zxfer_build_backup_pair_write_cmd "$path" "$path" tank/src "$cat_command"
	)
	assertContains "Hosts are single-quoted data inside the rendered program." \
		"$rendered" "'target.example; touch $marker #'"
	assertContains "Paths with quotes are escaped for the remote shell." \
		"$rendered" "'$TEST_TMPDIR_PHYSICAL/quote'\\'' dir;*/it'\\''s.meta'"
	printf 'quoted payload\n' | sh -c "$rendered"
	assertEquals 0 "$?"
	assertEquals "quoted payload" "$(cat "$path")"
	assertFalse "Host metacharacters never execute." "[ -e '$marker' ]"
	assertEquals "The read program handles the same quoting." \
		"quoted payload" "$(sh -c "$(zxfer_build_remote_backup_read_cmd "$path" "$host")")"
}

test_backup_listing_and_collapsed_programs_keep_quoted_arguments_and_stdin() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/store' with spaces"
	root="tank/child' with spaces"
	path="$g_backup_storage_root/$root/backup.meta"
	rendered=$(
		zxfer_build_remote_backup_dir_prepare_cmd "${path%/*}" "target.example doas" mktemp mv rm &&
			zxfer_build_backup_pair_write_cmd "$path" "$path" "$root" cat
	)
	# Dry-run rendering removes newlines; the same program must still parse
	# and read payload stdin instead of consuming it while initializing argv.
	collapsed=$(printf '%s\n' "$rendered" | tr '\n' ' ')
	printf 'collapsed payload\n' | sh -c "$collapsed"
	status=$?
	assertEquals "The collapsed program publishes its stdin." 0 "$status"
	assertEquals "collapsed payload" "$(cat "$path")"
	listing=$(sh -c "$(zxfer_build_remote_backup_storage_listing_cmd "$root" origin.example)")
	status=$?
	assertEquals "Quoted ancestor arguments survive the listing prefix." 0 "$status"
	assertEquals "The chain precedes find's root entry." "tank
$root
$root" "$listing"
}
