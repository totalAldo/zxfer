#!/bin/sh
# Tests for src/zxfer_snapshot_state.sh, run by tests/test_zxfer_exec.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

# Print what a destination probe published: "status=N result=R error=E".
print_destination_probe() {
	l_print_probe_status=0
	zxfer_probe_destination_existence "$@" || l_print_probe_status=$?
	printf 'status=%s result=%s error=%s\n' "$l_print_probe_status" \
		"$g_zxfer_destination_exists_result" "$g_zxfer_destination_exists_error"
}

test_probe_destination_existence_reports_present_through_the_real_runner() {
	# The real runner executes g_cmd_zfs; `true` answers "exists".
	old_g_cmd_zfs=${g_cmd_zfs-}
	g_cmd_zfs=true

	output=$(print_destination_probe "pool/fs")

	assertEquals "Destination should exist when the probe command succeeds." \
		"status=0 result=1 error=" "$output"

	g_cmd_zfs=$old_g_cmd_zfs
}

test_destination_parent_missing_confirmed_by_ancestor_listing_covers_listing_outcomes() {
	present_status=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "backup/dst/src"
				return 0
			}
			set +e
			zxfer_destination_parent_missing_confirmed_by_ancestor_listing "backup/dst/src"
			printf '%s\n' "$?"
		)
	)
	unrelated_status=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "unrelated/dataset"
				return 0
			}
			set +e
			zxfer_destination_parent_missing_confirmed_by_ancestor_listing "backup/dst/src"
			printf '%s\n' "$?"
		)
	)
	missing_output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "cannot open 'backup/dst': dataset does not exist" >&2
				return 1
			}
			set +e
			zxfer_destination_parent_missing_confirmed_by_ancestor_listing "backup/dst/src"
			printf 'status=%s\n' "$?"
			zxfer_lookup_destination_existence_cache "backup/dst/src"
			printf 'cached=%s\n' "$g_zxfer_destination_existence_cache_entry_result"
		)
	)

	assertEquals "Ancestor listings that still contain the missing dataset should not confirm absence." \
		"1" "$present_status"
	assertEquals "Ancestor listings that lack both datasets should not confirm absence." \
		"1" "$unrelated_status"
	assertContains "Ancestor listings that report a missing ancestor should confirm absence." \
		"$missing_output" "status=0"
	assertContains "Confirmed missing parents should seed the destination existence cache as absent." \
		"$missing_output" "cached=0"
}

test_probe_destination_existence_skips_probe_render_when_not_very_verbose() {
	render_count_file="$TEST_TMPDIR/exists_quiet.renders"
	printf '%s\n' 0 >"$render_count_file"

	result=$(
		(
			RENDER_COUNT_FILE="$render_count_file"
			zxfer_render_destination_zfs_command() {
				printf '%s\n' 1 >>"$RENDER_COUNT_FILE"
				printf '%s\n' "rendered"
			}
			zxfer_run_destination_zfs_cmd() {
				return 0
			}
			# -v alone prints no probe trace, so nothing is rendered.
			g_option_v_verbose=1
			g_option_V_very_verbose=0
			print_destination_probe "pool/fs" live
		)
	)

	assertEquals "Probes without -V should still report existence." "status=0 result=1 error=" "$result"
	assertEquals "Probes without -V should not render the probe command for display." \
		"0" "$(cat "$render_count_file")"
}

test_probe_destination_existence_renders_probe_display_when_very_verbose() {
	stderr_file="$TEST_TMPDIR/exists_verbose.err"

	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				return 0
			}
			g_option_T_target_host=""
			g_cmd_zfs="/sbin/zfs"
			g_option_V_very_verbose=1
			print_destination_probe "pool/fs" live 2>"$stderr_file"
		)
	)

	assertEquals "Very-verbose destination probes should still report existence." \
		"status=0 result=1 error=" "$result"
	assertEquals "Very-verbose destination probes should keep the current operator line text." \
		"Checking if destination exists: '/sbin/zfs' 'list' '-H' 'pool/fs'" \
		"$(cat "$stderr_file")"
}

test_probe_destination_existence_maps_missing_dataset_diagnostics_to_absent() {
	for missing_case in "stderr|dataset does not exist" "stdout|dataset does not exist" \
		"stderr|no such pool or dataset"; do
		output=$(
			(
				MISSING_CASE=$missing_case
				zxfer_run_destination_zfs_cmd() {
					if [ "${MISSING_CASE%%|*}" = stderr ]; then
						printf '%s\n' "cannot open 'pool/fs': ${MISSING_CASE#*|}" >&2
					else
						printf '%s\n' "cannot open 'pool/fs': ${MISSING_CASE#*|}"
					fi
					return 1
				}
				print_destination_probe "pool/fs"
			)
		)

		assertEquals "Missing-dataset diagnostics should map to destination absent [$missing_case]." \
			"status=0 result=0 error=" "$output"
	done
}

test_probe_destination_existence_reports_probe_failures() {
	for failure_case in "ssh: permission denied" ""; do
		output=$(
			(
				FAILURE_MESSAGE=$failure_case
				zxfer_run_destination_zfs_cmd() {
					[ -z "$FAILURE_MESSAGE" ] || printf '%s\n' "$FAILURE_MESSAGE" >&2
					return 1
				}
				print_destination_probe "pool/fs"
			)
		)

		if [ -n "$failure_case" ]; then
			assertEquals "Operational probe failures should fail with the destination context and the diagnostic." \
				"status=1 result= error=Failed to determine whether destination dataset [pool/fs] exists: ssh: permission denied" \
				"$output"
		else
			assertEquals "Silent probe failures should still fail with the destination context." \
				"status=1 result= error=Failed to determine whether destination dataset [pool/fs] exists." \
				"$output"
		fi
	done
}

test_probe_destination_existence_uses_cached_exact_result_without_reprobing() {
	output=$(
		(
			zxfer_set_destination_existence_cache_entry "pool/fs" 1
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "probe should not run" >&2
				return 1
			}
			print_destination_probe "pool/fs"
		)
	)

	assertEquals "Exact cached destination results should be returned without another zfs probe." \
		"status=0 result=1 error=" "$output"
}

test_probe_destination_existence_infers_missing_descendants_from_seeded_tree() {
	output=$(
		(
			zxfer_seed_destination_existence_cache_from_recursive_list "backup/dst" "backup/dst
backup/dst/existing"
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "probe should not run" >&2
				return 1
			}
			print_destination_probe "backup/dst/missing"
		)
	)

	assertEquals "Datasets omitted from a seeded destination subtree should be treated as missing without another zfs probe." \
		"status=0 result=0 error=" "$output"
}

test_probe_destination_existence_live_bypasses_cache_and_refreshes_exact_entry() {
	output=$(
		(
			zxfer_mark_destination_root_missing_in_cache "backup/dst"
			zxfer_run_destination_zfs_cmd() {
				return 0
			}
			printf 'live: '
			print_destination_probe "backup/dst/child" live
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "probe should not run" >&2
				return 1
			}
			printf 'cached: '
			print_destination_probe "backup/dst/child"
		)
	)

	assertContains "Live destination probes should bypass cached subtree-missing state." \
		"$output" "live: status=0 result=1 error="
	assertContains "Successful live probes should refresh the exact cache entry for later callers." \
		"$output" "cached: status=0 result=1 error="
}

test_probe_destination_existence_uses_parent_recursive_listing_for_ambiguous_omnios_child_probes() {
	output=$(
		(
			g_destination_operating_system="SunOS"
			g_option_V_very_verbose=1
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					printf '%s\n' "backup/dst/src"
					printf '%s\n' "backup/dst/src/child"
					return 0
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			print_destination_probe "backup/dst/src/child" live 2>/dev/null
		)
	)

	assertEquals "A parent recursive listing that contains an ambiguous OmniOS child should report that it exists." \
		"status=0 result=1 error=" "$output"
}

test_probe_destination_existence_uses_parent_recursive_listing_to_confirm_missing_omnios_child() {
	output=$(
		(
			g_destination_operating_system="SunOS"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					printf '%s\n' "backup/dst/src"
					return 0
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			print_destination_probe "backup/dst/src/child" live
		)
	)

	assertEquals "A parent recursive listing that omits the child should report it missing." \
		"status=0 result=0 error=" "$output"
}

test_probe_destination_existence_reports_parent_recursive_listing_failures_for_ambiguous_omnios_child_probe() {
	output=$(
		(
			g_destination_operating_system="SunOS"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					printf '%s\n' "permission denied" >&2
					return 1
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			print_destination_probe "backup/dst/src/child" live
		)
	)

	assertEquals "Ambiguous OmniOS child probes should fail closed with the child and parent context when the parent recursive fallback errors." \
		"status=1 result= error=Failed to determine whether destination dataset [backup/dst/src/child] exists: parent recursive listing for [backup/dst/src] failed: permission denied" \
		"$output"
}

test_probe_destination_existence_reports_parent_recursive_listing_without_parent_dataset_for_ambiguous_omnios_child_probe() {
	output=$(
		(
			g_destination_operating_system="SunOS"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					printf '%s\n' "backup/dst/other"
					return 0
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			print_destination_probe "backup/dst/src/child" live
		)
	)

	assertEquals "A parent recursive fallback that does not list the parent dataset should fail closed." \
		"status=1 result= error=Failed to determine whether destination dataset [backup/dst/src/child] exists: parent recursive listing for [backup/dst/src] did not contain the parent dataset." \
		"$output"
}

test_probe_destination_existence_parent_recursive_listing_treats_missing_parent_as_missing_child_for_ambiguous_omnios_probe() {
	output=$(
		(
			g_destination_operating_system="SunOS"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					printf '%s\n' "cannot open 'backup/dst/src': no such pool or dataset" >&2
					return 1
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			print_destination_probe "backup/dst/src/child" live
		)
	)

	assertEquals "A missing parent discovered through the recursive fallback should map to a missing child." \
		"status=0 result=0 error=" "$output"
}

test_probe_destination_existence_parent_recursive_listing_treats_silent_missing_parent_as_missing_child_when_ancestor_confirms_absence() {
	output=$(
		(
			g_destination_operating_system="SunOS"
			g_option_V_very_verbose=1
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst" ]; then
					printf '%s\n' "backup/dst"
					return 0
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			print_destination_probe "backup/dst/src/child" live 2>/dev/null
		)
	)

	assertEquals "A silent SunOS parent-listing failure should map to missing only when an ancestor listing proves the parent is absent." \
		"status=0 result=0 error=" "$output"
}

test_probe_destination_existence_reports_silent_parent_recursive_listing_failures_for_ambiguous_omnios_child_probe() {
	output=$(
		(
			g_destination_operating_system="SunOS"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst" ]; then
					return 1
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			print_destination_probe "backup/dst/src/child" live
		)
	)

	assertEquals "Silent parent recursive fallback failures should still fail closed with the dedicated recursive-listing error." \
		"status=1 result= error=Failed to determine whether destination dataset [backup/dst/src/child] exists: parent recursive listing for [backup/dst/src] failed." \
		"$output"
}

test_probe_destination_existence_cached_hits_do_not_increment_probe_counter() {
	output=$(
		(
			g_option_V_very_verbose=1
			g_zxfer_profile_exists_destination_calls=0
			zxfer_set_destination_existence_cache_entry "pool/fs" 1
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "probe should not run" >&2
				return 1
			}
			zxfer_probe_destination_existence "pool/fs" 2>/dev/null
			printf 'cached_calls=%s\n' "$g_zxfer_profile_exists_destination_calls"
			zxfer_run_destination_zfs_cmd() {
				return 0
			}
			zxfer_probe_destination_existence "pool/other" live 2>/dev/null
			printf 'live_calls=%s\n' "$g_zxfer_profile_exists_destination_calls"
		)
	)

	assertContains "Cached destination answers should not count as live destination probes." \
		"$output" "cached_calls=0"
	assertContains "Live destination probes should still increment the destination-probe profile counter." \
		"$output" "live_calls=1"
}
