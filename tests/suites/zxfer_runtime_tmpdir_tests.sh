#!/bin/sh
# Effective TMPDIR, temp-file and cleanup-PID tests for src/zxfer_runtime.sh.
# Run by tests/test_zxfer_runtime.sh under the exec fixture. The unsafe-TMPDIR
# fallback of a whole run is pinned in tests/test_contract_failures.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_get_temp_file_uses_physical_tmpdir_for_symlinked_tmpdir_paths() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_tmp="$physical_tmpdir/tmp_real"
	link_tmp="$physical_tmpdir/tmp_link"
	mkdir -p "$real_tmp"
	ln -s "$real_tmp" "$link_tmp"
	TMPDIR="$link_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	file=$(zxfer_get_temp_file && printf '%s' "$g_zxfer_temp_file_result")

	case "$file" in
	"$real_tmp"/*) inside_real=0 ;;
	*) inside_real=1 ;;
	esac
	case "$file" in
	"$link_tmp"/*) inside_link=0 ;;
	*) inside_link=1 ;;
	esac

	assertEquals "Temp files should use the physical TMPDIR target instead of the symlinked path." 0 "$inside_real"
	assertEquals "Temp files should not be created through the symlinked TMPDIR path itself." 1 "$inside_link"
	assertTrue "Temp file should exist under the physical TMPDIR path." "[ -f \"$file\" ]"

	rm -f "$file"
	TMPDIR="$TEST_TMPDIR"
}

test_get_temp_file_throws_when_mktemp_fails() {
	set +e
	output=$(
		(
			mktemp() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_temp_file
		)
	)
	status=$?

	assertEquals "Temporary-file allocation failures should abort." 1 "$status"
	assertContains "Temporary-file allocation failures should use the documented error." \
		"$output" "Error creating temporary file."
}

test_zxfer_cleanup_pid_helpers_ignore_invalid_inputs_in_current_shell() {
	# The entry file's helper returns once the child runs its own program, so
	# the TERM below cannot reach a copy of this shell and its traps.
	zxfer_runtime_spawn_live_child tracked ||
		fail "Unable to start the live child."
	tracked_pid=$g_zxfer_runtime_live_child_pid
	zxfer_register_cleanup_pid "$tracked_pid" "tracked cleanup helper"

	zxfer_register_cleanup_pid ""
	zxfer_register_cleanup_pid "abc"
	assertEquals "Cleanup PID registration should ignore empty and non-numeric inputs." \
		"$tracked_pid	tracked cleanup helper	pid" "$g_zxfer_cleanup_pid_records"

	zxfer_unregister_cleanup_pid ""
	zxfer_unregister_cleanup_pid "abc"
	zxfer_unregister_cleanup_pid "$tracked_pid"
	assertEquals "Cleanup PID unregistration should ignore invalid inputs and drop only the requested row." \
		"" "$g_zxfer_cleanup_pid_records"

	output=$(
		(
			l_stub_pid=7001
			g_zxfer_cleanup_pid_records="abc	forged helper
$l_stub_pid	tracked cleanup helper
$$	self helper"
			g_test_cleanup_abort_calls=""
			# This synthetic PID must not inherit a real host process's liveness.
			kill() { return 1; }
			zxfer_abort_cleanup_pid() {
				g_test_cleanup_abort_calls="${g_test_cleanup_abort_calls}${g_test_cleanup_abort_calls:+ }$1:$2"
				return 0
			}
			zxfer_kill_registered_cleanup_pids
			printf 'abort_calls=<%s>\n' "$g_test_cleanup_abort_calls"
			printf 'remaining=<%s>\n' "$g_zxfer_cleanup_pid_records"
		)
	)

	kill -s TERM "$tracked_pid" >/dev/null 2>&1 || true
	wait "$tracked_pid" 2>/dev/null || true
	assertContains "Cleanup termination should still delegate teardown for the validated helper when invalid entries are present in the PID list." \
		"$output" "abort_calls=<7001:TERM>"
	assertNotContains "Cleanup termination should ignore rows with a non-numeric PID." \
		"$output" "abc:"
	assertContains "Cleanup termination should remove the processed helper without treating malformed or self rows as signal targets." \
		"$output" "remaining=<abc	forged helper
$$	self helper>"
}

# The candidates stand in for a memory-backed default listed first and a
# disk-backed one.
test_zxfer_try_get_effective_tmpdir_prefers_a_safe_tmpdir_then_the_first_safe_default() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	explicit_tmp="$physical_tmpdir/effective_tmp_explicit"
	insecure_tmp="$physical_tmpdir/effective_tmp_insecure"
	ram_tmp="$physical_tmpdir/effective_tmp_ram"
	disk_tmp="$physical_tmpdir/effective_tmp_disk"
	mkdir -p "$explicit_tmp" "$insecure_tmp" "$ram_tmp" "$disk_tmp"
	chmod 0777 "$insecure_tmp"
	output=$(
		(
			zxfer_list_default_tmpdir_candidates() {
				printf '%s\n' "$ram_tmp" "$disk_tmp"
			}
			# Each row is LABEL:TMPDIR; an empty TMPDIR means unset.
			for tmpdir_row in "unset:" "explicit:$explicit_tmp" \
				"unsafe:$insecure_tmp"; do
				if [ -n "${tmpdir_row#*:}" ]; then
					TMPDIR=${tmpdir_row#*:}
				else
					unset TMPDIR
				fi
				g_zxfer_effective_tmpdir=""
				g_zxfer_effective_tmpdir_requested=""
				zxfer_try_get_effective_tmpdir
				printf '%s=%s <%s> key=<%s>\n' "${tmpdir_row%%:*}" "$?" \
					"$g_zxfer_effective_tmpdir" "$g_zxfer_effective_tmpdir_requested"
			done
		)
	)
	chmod 0700 "$insecure_tmp"

	assertEquals "An unset TMPDIR takes the first safe default under the default key, a safe TMPDIR wins over the defaults, and an unsafe one falls back to the first safe default." \
		"unset=0 <$ram_tmp> key=<__ZXFER_DEFAULT_TMPDIR__>
explicit=0 <$explicit_tmp> key=<$explicit_tmp>
unsafe=0 <$ram_tmp> key=<$insecure_tmp>" "$output"
}

test_zxfer_unsafe_tmpdir_fallback_note_is_held_until_option_parsing_emits_it() {
	# The eager run temp root decides TMPDIR safety in session startup,
	# BEFORE -V parsing; the advisory must be held and replayed once option
	# parsing knows the verbosity state instead of being silently dropped.
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	insecure_tmp="$physical_tmpdir/effective_tmp_note_insecure"
	safe_tmp="$physical_tmpdir/effective_tmp_note_safe"
	mkdir -p "$insecure_tmp" "$safe_tmp"
	chmod 0777 "$insecure_tmp"
	pre_parse_stderr="$TEST_TMPDIR/tmpdir_note_pre_parse.stderr"
	post_parse_stderr="$TEST_TMPDIR/tmpdir_note_post_parse.stderr"
	immediate_stderr="$TEST_TMPDIR/tmpdir_note_immediate.stderr"
	(
		TMPDIR="$insecure_tmp"
		g_option_V_very_verbose=0
		g_zxfer_effective_tmpdir=""
		g_zxfer_effective_tmpdir_requested=""
		g_zxfer_tmpdir_fallback_note=""
		zxfer_list_default_tmpdir_candidates() {
			printf '%s\n' "$safe_tmp"
		}
		zxfer_try_get_effective_tmpdir >/dev/null 2>"$pre_parse_stderr" || exit $?
		g_option_V_very_verbose=1
		zxfer_emit_pending_tmpdir_fallback_note 2>"$post_parse_stderr"
		# A second replay must stay silent: the note is consumed on emission.
		zxfer_emit_pending_tmpdir_fallback_note 2>>"$post_parse_stderr"
	)
	held_status=$?
	(
		TMPDIR="$insecure_tmp"
		g_option_V_very_verbose=1
		g_zxfer_effective_tmpdir=""
		g_zxfer_effective_tmpdir_requested=""
		g_zxfer_tmpdir_fallback_note=""
		zxfer_list_default_tmpdir_candidates() {
			printf '%s\n' "$safe_tmp"
		}
		zxfer_try_get_effective_tmpdir >/dev/null 2>"$immediate_stderr" || exit $?
	)
	immediate_status=$?
	chmod 0700 "$insecure_tmp"

	assertEquals "The held-advisory fallback path should still resolve the temp root cleanly." \
		0 "$held_status"
	assertEquals "No advisory should print while -V state is still unknown." \
		"" "$(cat "$pre_parse_stderr")"
	assertEquals "The held advisory should replay exactly once under -V after option parsing." \
		"Ignoring unsafe TMPDIR $insecure_tmp; using $safe_tmp instead." \
		"$(cat "$post_parse_stderr")"
	assertEquals "The immediate-advisory fallback path should still resolve the temp root cleanly." \
		0 "$immediate_status"
	assertEquals "The advisory should print at decision time when -V is already live." \
		"Ignoring unsafe TMPDIR $insecure_tmp; using $safe_tmp instead." \
		"$(cat "$immediate_stderr")"
}

test_zxfer_try_get_effective_tmpdir_reuses_cached_value_in_current_shell() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	cached_tmp="$physical_tmpdir/effective_tmp_cached"
	mkdir -p "$cached_tmp"
	TMPDIR="$cached_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	first_out="$TEST_TMPDIR/effective_tmp_first.out"
	second_out="$TEST_TMPDIR/effective_tmp_second.out"
	zxfer_try_get_effective_tmpdir && printf '%s\n' "$g_zxfer_effective_tmpdir" >"$first_out"
	first_status=$?
	zxfer_try_get_effective_tmpdir && printf '%s\n' "$g_zxfer_effective_tmpdir" >"$second_out"
	second_status=$?

	assertEquals "The first effective TMPDIR lookup should succeed for a trusted directory." \
		0 "$first_status"
	assertEquals "Repeated effective TMPDIR lookups should reuse the cached value." \
		0 "$second_status"
	assertEquals "The first lookup should return the trusted TMPDIR path." \
		"$cached_tmp" "$(cat "$first_out")"
	assertEquals "The cached lookup should return the same TMPDIR path." \
		"$cached_tmp" "$(cat "$second_out")"
	assertEquals "The cached TMPDIR path should remain stored in the current shell." \
		"$cached_tmp" "$g_zxfer_effective_tmpdir"
	assertEquals "The cached TMPDIR request key should remain stored in the current shell." \
		"$cached_tmp" "$g_zxfer_effective_tmpdir_requested"

	TMPDIR="$TEST_TMPDIR"
}

test_zxfer_create_private_temp_dir_returns_failure_when_effective_tmpdir_lookup_fails_in_current_shell() {
	zxfer_try_get_effective_tmpdir() {
		return 1
	}

	zxfer_create_private_temp_dir "zxfer_private_tmp" >"$TEST_TMPDIR/private_temp_dir.out" 2>/dev/null
	status=$?

	zxfer_source_runtime_modules_through "zxfer_runtime.sh"

	assertEquals "Private temp directory creation should fail when the effective temp root cannot be determined." \
		1 "$status"
}
