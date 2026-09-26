#!/bin/sh
# Tests for src/zxfer_runtime.sh, run by tests/test_zxfer_exec.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_get_temp_file_creates_unique_file() {
	# zxfer_get_temp_file should provide unique temp files so concurrent options do
	# not collide or overwrite each other.
	file_one=$(zxfer_get_temp_file && printf '%s' "$g_zxfer_temp_file_result")
	file_two=$(zxfer_get_temp_file && printf '%s' "$g_zxfer_temp_file_result")

	assertTrue "First temp file should exist." "[ -f \"$file_one\" ]"
	assertTrue "Second temp file should exist." "[ -f \"$file_two\" ]"
	assertNotEquals "Two consecutive temp file names should be unique." "$file_one" "$file_two"

	rm -f "$file_one" "$file_two"
}

test_get_temp_file_honors_tmpdir_variable() {
	# Honor the TMPDIR override so tests or CLI invocations can direct
	# scratch files to a specific filesystem, but use the validated
	# physical directory path rather than a logical symlinked alias.
	custom_tmp="$TEST_TMPDIR/custom"
	mkdir -p "$custom_tmp"
	physical_custom_tmp=$(cd -P "$custom_tmp" && pwd)
	TMPDIR="$custom_tmp"

	file=$(zxfer_get_temp_file && printf '%s' "$g_zxfer_temp_file_result")

	case "$file" in
	"$physical_custom_tmp"/*) inside=0 ;;
	*) inside=1 ;;
	esac

	assertEquals "Temp file should be created inside the validated TMPDIR root." 0 "$inside"
	assertTrue "Temp file should exist." "[ -f \"$file\" ]"

	rm -f "$file"
	TMPDIR="$TEST_TMPDIR"
}

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

test_get_temp_file_rejects_non_sticky_world_writable_tmpdir_and_falls_back_to_system_tmp() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	insecure_tmp="$physical_tmpdir/insecure_tmp"
	mkdir -p "$insecure_tmp"
	chmod 0777 "$insecure_tmp"
	TMPDIR="$insecure_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	file=$(zxfer_get_temp_file && printf '%s' "$g_zxfer_temp_file_result")
	status=$?

	assertEquals "Non-sticky world-writable TMPDIR values should not prevent temporary file creation." 0 "$status"
	case "$file" in
	"$insecure_tmp"/*) inside_insecure=0 ;;
	*) inside_insecure=1 ;;
	esac
	assertEquals "Non-sticky world-writable TMPDIR values should be rejected." 1 "$inside_insecure"
	assertTrue "Fallback temp file should exist." "[ -f \"$file\" ]"

	rm -f "$file"
	chmod 0700 "$insecure_tmp"
	TMPDIR="$TEST_TMPDIR"
}

test_get_temp_file_allows_sticky_world_writable_tmpdir() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	sticky_tmp="$physical_tmpdir/sticky_tmp"
	mkdir -p "$sticky_tmp"
	chmod 1777 "$sticky_tmp"
	TMPDIR="$sticky_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	file=$(zxfer_get_temp_file && printf '%s' "$g_zxfer_temp_file_result")

	case "$file" in
	"$sticky_tmp"/*) inside_sticky=0 ;;
	*) inside_sticky=1 ;;
	esac

	assertEquals "Sticky world-writable TMPDIR values should remain usable." 0 "$inside_sticky"
	assertTrue "Sticky TMPDIR temp file should exist." "[ -f \"$file\" ]"

	rm -f "$file"
	chmod 0700 "$sticky_tmp"
	TMPDIR="$TEST_TMPDIR"
}

test_get_temp_file_ignores_relative_tmpdir_and_falls_back_to_system_tmp() {
	old_pwd=$(pwd)
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	mkdir -p "$physical_tmpdir/relative_tmp_root"
	cd "$physical_tmpdir" || fail "Unable to cd into physical tempdir."
	TMPDIR="relative_tmp_root"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	file=$(zxfer_get_temp_file && printf '%s' "$g_zxfer_temp_file_result")
	status=$?

	cd "$old_pwd" || fail "Unable to restore working directory."

	assertEquals "Relative TMPDIR values should not prevent temporary file creation." 0 "$status"
	case "$file" in
	"$physical_tmpdir"/relative_tmp_root/*) inside_relative=0 ;;
	*) inside_relative=1 ;;
	esac
	assertEquals "Relative TMPDIR values should be ignored instead of being used directly." 1 "$inside_relative"
	assertTrue "Fallback temp file should exist." "[ -f \"$file\" ]"

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

test_zxfer_kill_registered_cleanup_pids_only_terminates_registered_pids() {
	output=$(
		(
			unrelated_pid=60101
			g_zxfer_cleanup_pid_records="50101	registered cleanup helper"
			g_test_cleanup_abort_calls=""
			zxfer_abort_cleanup_pid() {
				g_test_cleanup_abort_calls="${g_test_cleanup_abort_calls}${g_test_cleanup_abort_calls:+ }$1:$2"
				return 0
			}
			zxfer_kill_registered_cleanup_pids
			printf 'abort_calls=<%s>\n' "$g_test_cleanup_abort_calls"
			printf 'remaining=<%s>\n' "$g_zxfer_cleanup_pid_records"
			printf 'unrelated=<%s>\n' "$unrelated_pid"
		)
	)

	assertContains "Cleanup should delegate validated teardown only for tracked helper PIDs." \
		"$output" "abort_calls=<50101:TERM>"
	assertNotContains "Cleanup should not delegate teardown for unrelated helper PIDs." \
		"$output" "60101:"
	assertContains "Cleanup PID tracking should be cleared after termination." \
		"$output" "remaining=<>"
}

test_zxfer_cleanup_pid_helpers_ignore_invalid_inputs_in_current_shell() {
	sleep 30 &
	tracked_pid=$!
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

test_zxfer_try_get_effective_tmpdir_resolves_symlinked_tmpdir_to_physical_path() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_tmp="$physical_tmpdir/effective_tmp_real"
	link_tmp="$physical_tmpdir/effective_tmp_link"
	mkdir -p "$real_tmp"
	ln -s "$real_tmp" "$link_tmp"
	TMPDIR="$link_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	result=$(zxfer_try_get_effective_tmpdir && printf '%s' "$g_zxfer_effective_tmpdir")
	status=$?

	assertEquals "Symlinked TMPDIR values should still resolve successfully when their physical target is trusted." 0 "$status"
	assertEquals "Effective TMPDIR resolution should return the physical directory path." "$real_tmp" "$result"
	TMPDIR="$TEST_TMPDIR"
}

test_zxfer_try_get_effective_tmpdir_prefers_memory_backed_default_candidates_in_current_shell() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	ram_tmp="$physical_tmpdir/default_tmp_ram"
	disk_tmp="$physical_tmpdir/default_tmp_disk"
	mkdir -p "$ram_tmp" "$disk_tmp"
	output=$(
		(
			unset TMPDIR
			g_zxfer_effective_tmpdir=""
			g_zxfer_effective_tmpdir_requested=""
			output_file="$TEST_TMPDIR/effective_tmp_default_current_shell.out"

			zxfer_list_default_tmpdir_candidates() {
				printf '%s\n' "$ram_tmp"
				printf '%s\n' "$disk_tmp"
			}

			zxfer_try_get_effective_tmpdir && printf '%s\n' "$g_zxfer_effective_tmpdir" >"$output_file" || exit $?
			result=$(cat "$output_file")
			printf 'result=%s\n' "$result"
			printf 'request=%s\n' "$g_zxfer_effective_tmpdir_requested"
		)
	)
	status=$?

	assertEquals "Unset TMPDIR should prefer the first validated default temp-root candidate, which lets zxfer prefer memory-backed roots when available." \
		0 "$status"
	assertContains "Unset TMPDIR should resolve to the preferred memory-backed default candidate." \
		"$output" "result=$ram_tmp"
	assertContains "Default-tempdir selections should cache under the synthetic default request key." \
		"$output" "request=__ZXFER_DEFAULT_TMPDIR__"
}

test_zxfer_try_get_effective_tmpdir_prefers_explicit_tmpdir_over_default_candidates_in_current_shell() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	explicit_tmp="$physical_tmpdir/effective_tmp_explicit"
	ram_tmp="$physical_tmpdir/effective_tmp_default_ram"
	mkdir -p "$explicit_tmp" "$ram_tmp"
	output=$(
		(
			TMPDIR="$explicit_tmp"
			g_zxfer_effective_tmpdir=""
			g_zxfer_effective_tmpdir_requested=""
			output_file="$TEST_TMPDIR/effective_tmp_explicit_current_shell.out"

			zxfer_list_default_tmpdir_candidates() {
				printf '%s\n' "$ram_tmp"
				printf '%s\n' "/tmp"
			}

			zxfer_try_get_effective_tmpdir && printf '%s\n' "$g_zxfer_effective_tmpdir" >"$output_file" || exit $?
			result=$(cat "$output_file")
			printf 'result=%s\n' "$result"
			printf 'request=%s\n' "$g_zxfer_effective_tmpdir_requested"
		)
	)
	status=$?

	assertEquals "A valid explicit TMPDIR should still win over the default memory-backed candidate list." \
		0 "$status"
	assertContains "A valid explicit TMPDIR should remain the effective temp root." \
		"$output" "result=$explicit_tmp"
	assertContains "The cache key should still reflect the explicit TMPDIR request." \
		"$output" "request=$explicit_tmp"
}

test_zxfer_try_get_effective_tmpdir_falls_back_to_preferred_default_candidate_when_tmpdir_is_unsafe() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	insecure_tmp="$physical_tmpdir/effective_tmp_insecure_preferred"
	ram_tmp="$physical_tmpdir/effective_tmp_fallback_ram"
	disk_tmp="$physical_tmpdir/effective_tmp_fallback_disk"
	mkdir -p "$insecure_tmp" "$ram_tmp" "$disk_tmp"
	chmod 0777 "$insecure_tmp"
	output=$(
		(
			TMPDIR="$insecure_tmp"
			g_zxfer_effective_tmpdir=""
			g_zxfer_effective_tmpdir_requested=""

			zxfer_list_default_tmpdir_candidates() {
				printf '%s\n' "$ram_tmp"
				printf '%s\n' "$disk_tmp"
			}

			result=$(zxfer_try_get_effective_tmpdir && printf '%s' "$g_zxfer_effective_tmpdir") || exit $?
			printf 'result=%s\n' "$result"
		)
	)
	status=$?
	chmod 0700 "$insecure_tmp"

	assertEquals "Unsafe TMPDIR values should still resolve cleanly by falling back to the preferred validated default temp root." \
		0 "$status"
	assertContains "Unsafe TMPDIR values should fall back to the preferred validated default candidate before disk-backed fallbacks." \
		"$output" "result=$ram_tmp"
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

test_zxfer_try_get_effective_tmpdir_rejects_non_sticky_world_writable_tmpdir() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	insecure_tmp="$physical_tmpdir/effective_tmp_insecure"
	mkdir -p "$insecure_tmp"
	chmod 0777 "$insecure_tmp"
	TMPDIR="$insecure_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	result=$(zxfer_try_get_effective_tmpdir && printf '%s' "$g_zxfer_effective_tmpdir")
	status=$?

	assertEquals "Unsafe world-writable TMPDIR values should still resolve by falling back to the system temp root." 0 "$status"
	assertNotEquals "Unsafe world-writable TMPDIR values should not remain selected." "$insecure_tmp" "$result"

	chmod 0700 "$insecure_tmp"
	TMPDIR="$TEST_TMPDIR"
}

test_zxfer_create_secure_staging_dir_for_path_returns_failure_when_parent_lookup_fails() {
	stage_path="$TEST_TMPDIR/create_secure_staging_parent_lookup/backup.meta"

	zxfer_test_capture_subshell "
		zxfer_get_path_parent_dir() {
			return 1
		}
		zxfer_create_secure_staging_dir_for_path \"$stage_path\" >/dev/null
	"

	assertEquals "Secure same-directory staging should fail closed when the parent-path lookup fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_zxfer_create_secure_staging_dir_for_path_returns_failure_when_parent_validation_fails() {
	stage_root="$TEST_TMPDIR/create_secure_staging_parent_validation"
	stage_path="$stage_root/backup.meta"
	mkdir -p "$stage_root"

	zxfer_test_capture_subshell "
		zxfer_validate_temp_root_candidate() {
			return 1
		}
		zxfer_create_secure_staging_dir_for_path \"$stage_path\" >/dev/null
	"

	assertEquals "Secure same-directory staging should fail closed when the parent directory is not a trusted temp-root candidate." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_zxfer_create_secure_staging_dir_for_path_uses_unpredictable_mktemp_names() {
	# Staging parents may be shared sticky directories, so the staged name
	# must be mktemp-randomized: predictable pid+attempt slots are squat-able
	# by a local process-table reader.
	stage_root=$(cd -P "$TEST_TMPDIR" && pwd)/create_secure_staging_random
	stage_path="$stage_root/backup.meta"
	mkdir -p "$stage_root"

	zxfer_create_secure_staging_dir_for_path "$stage_path" >/dev/null
	stage_status=$?
	stage_dir=$g_zxfer_secure_staging_dir_result
	zxfer_create_secure_staging_dir_for_path "$stage_path" >/dev/null
	second_stage_dir=$g_zxfer_secure_staging_dir_result

	case "${stage_dir##*/}" in
	".zxfer.stage.$$."*)
		stage_name_randomized=no
		;;
	.zxfer.stage.??????)
		stage_name_randomized=yes
		;;
	*)
		stage_name_randomized=no
		;;
	esac

	assertEquals "Secure same-directory staging should succeed under a validated parent." \
		0 "$stage_status"
	assertEquals "Secure same-directory staging should use the randomized mktemp template, not pid+attempt slots." \
		yes "$stage_name_randomized"
	assertTrue "Secure same-directory staging should create the staged directory." \
		"[ -d \"$stage_dir\" ]"
	assertNotEquals "Consecutive staging directories should never reuse a name." \
		"$stage_dir" "$second_stage_dir"
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
