#!/bin/sh
# Property transfer fragment: source collection and -e restore, the
# destination existence decision and creation, destination command
# rendering/execution, the child-inheritance awk, and apply. Run by
# tests/test_zxfer_property_transfer.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

################################################################################
# SOURCE COLLECTION
################################################################################

test_collect_source_props_publishes_raw_and_effective_lists() {
	(
		zxfer_load_normalized_dataset_properties() {
			printf '%s\n' "$*" >>"$TEST_TMPDIR/collect_side.log"
			g_zxfer_normalized_dataset_properties="compression=lz4=local,readonly=on=local"
		}
		zxfer_collect_source_props "tank/src" "backup/dst"
		printf 'raw=%s\neffective=%s\n' "$g_zxfer_source_pvs_raw" "$g_zxfer_source_pvs_effective"
	) >"$TEST_TMPDIR/collect_plain.out"
	assertEquals "raw=compression=lz4=local,readonly=on=local
effective=compression=lz4=local,readonly=on=local" "$(cat "$TEST_TMPDIR/collect_plain.out")"
	assertEquals "Source properties are read through the source side." \
		"tank/src source" "$(cat "$TEST_TMPDIR/collect_side.log")"
}

test_collect_source_props_propagates_lookup_failures_with_diagnostic() {
	(
		zxfer_load_normalized_dataset_properties() {
			g_zxfer_property_error_result="cannot open tank/src: permission denied"
			return 3
		}
		zxfer_collect_source_props "tank/src" "backup/dst"
		printf 'status=%s raw=<%s> error=%s\n' "$?" "$g_zxfer_source_pvs_raw" "$g_zxfer_property_error_result"
	) >"$TEST_TMPDIR/collect_failure.out"
	assertEquals "status=3 raw=<> error=cannot open tank/src: permission denied" \
		"$(cat "$TEST_TMPDIR/collect_failure.out")"
}

test_collect_source_props_uses_backup_restore() {
	(
		zxfer_load_normalized_dataset_properties() {
			g_zxfer_normalized_dataset_properties="compression=lz4=local,readonly=on=local"
		}
		g_option_e_restore_property_mode=1
		ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
		ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
		g_restored_backup_file_contents=$(zxfer_test_render_current_backup_metadata_contents \
			"$(zxfer_test_backup_metadata_row "." "readonly=on=local,compression=lz4=local")")
		zxfer_collect_source_props "tank/src" "backup/dst"
		printf 'raw=%s\neffective=%s\n' "$g_zxfer_source_pvs_raw" "$g_zxfer_source_pvs_effective"
	) >"$TEST_TMPDIR/collect_restore.out"
	assertEquals "raw=compression=lz4=local,readonly=on=local
effective=readonly=on=local,compression=lz4=local" "$(cat "$TEST_TMPDIR/collect_restore.out")"
}

test_collect_source_props_fails_when_backup_entry_missing() {
	set +e
	output=$(
		(
			zxfer_load_normalized_dataset_properties() {
				g_zxfer_normalized_dataset_properties="compression=lz4=local"
			}
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_e_restore_property_mode=1
			ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
			ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
			g_restored_backup_file_contents=$(zxfer_test_render_current_backup_metadata_contents \
				"$(zxfer_test_backup_metadata_row "other" "compression=lz4=local")")
			zxfer_collect_source_props "tank/src" "backup/dst"
		)
	)
	status=$?
	assertEquals 2 "$status"
	assertEquals "Can't find the properties for the filesystem tank/src and destination backup/dst" "$output"
}

test_collect_source_props_restore_mode_requires_restored_contents() {
	set +e
	output=$(
		(
			zxfer_load_normalized_dataset_properties() {
				g_zxfer_normalized_dataset_properties="compression=lz4=local"
			}
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_e_restore_property_mode=1
			g_restored_backup_file_contents=""
			zxfer_collect_source_props "tank/src" "backup/dst"
		)
	)
	status=$?
	assertEquals 2 "$status"
	assertEquals "Can't find the properties for the filesystem tank/src and destination backup/dst" "$output"
}

################################################################################
# DESTINATION CREATE
################################################################################

test_property_destination_exists_answers_listed_destinations_without_probing() {
	set +e
	(
		g_recursive_dest_list="backup/dst
backup/dst/child"
		zxfer_probe_destination_existence() {
			printf 'unexpected probe\n' >&2
			exit 99
		}
		zxfer_property_destination_exists "backup/dst/child"
	)
	assertEquals "Listed destinations exist and need diffing." 0 "$?"
}

test_property_destination_exists_live_probes_unlisted_destinations_and_notes_existing_ones() {
	set +e
	(
		g_recursive_dest_list="backup/dst"
		zxfer_probe_destination_existence() {
			printf '%s %s\n' "$1" "${2:-cache}" >>"$TEST_TMPDIR/exists_probe.log"
			g_zxfer_destination_exists_result=1
		}
		zxfer_property_destination_exists "backup/dst/child"
		printf 'status=%s\n' "$?" >>"$TEST_TMPDIR/exists_probe.log"
		printf 'list=%s\n' "$(printf '%s' "$g_recursive_dest_list" | tr '\n' ' ')" >>"$TEST_TMPDIR/exists_probe.log"
		zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=0; }
		zxfer_property_destination_exists "backup/dst/other"
		printf 'missing_status=%s\n' "$?" >>"$TEST_TMPDIR/exists_probe.log"
		printf 'list=%s\n' "$(printf '%s' "$g_recursive_dest_list" | tr '\n' ' ')" >>"$TEST_TMPDIR/exists_probe.log"
	)
	assertEquals "backup/dst/child live
status=0
list=backup/dst backup/dst/child
missing_status=1
list=backup/dst backup/dst/child" "$(cat "$TEST_TMPDIR/exists_probe.log")"
}

test_property_destination_exists_rethrows_live_probe_failures() {
	set +e
	output=$(
		(
			zxfer_probe_destination_existence() {
				g_zxfer_destination_exists_error="Failed to determine whether destination dataset [backup/dst/child] exists: timeout"
				return 7
			}
			zxfer_throw_error() {
				printf '%s|%s\n' "$1" "$2"
				exit "$2"
			}
			zxfer_property_destination_exists "backup/dst/child"
		)
	)
	status=$?
	assertEquals 7 "$status"
	assertEquals "Failed to determine whether destination dataset [backup/dst/child] exists: timeout|7" "$output"
}

zxfer_property_test_log_destination_zfs() {
	printf '%s\n' "$*" >>"$CREATE_LOG"
}

# Existence fakes, defined at top level because a case statement inside
# "$(...)" is not portable across shells: only the parent backup/dst exists;
# or the parent probe fails operationally.
zxfer_property_test_parent_exists() {
	case "$1" in
	backup/dst) g_zxfer_destination_exists_result=1 ;;
	*) g_zxfer_destination_exists_result=0 ;;
	esac
}

zxfer_property_test_parent_probe_fails() {
	g_zxfer_destination_exists_error="Failed to determine whether destination dataset [backup/dst] exists: permission denied"
	return 1
}

test_create_destination_dataset_initial_source_precreates_missing_parent_then_applies_override_list() {
	CREATE_LOG="$TEST_TMPDIR/create_initial.log"
	: >"$CREATE_LOG"
	(
		zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=0; }
		zxfer_run_destination_zfs_cmd() { zxfer_property_test_log_destination_zfs "$@"; }
		zxfer_create_destination_dataset 1 "compression=lz4=local,atime=off=override" "ignored=1=local" \
			filesystem "" "backup/dst/child" "readonly"
		printf 'status=%s\n' "$?" >>"$CREATE_LOG"
	)
	assertEquals "create -p backup/dst
create -o compression=lz4 -o atime=off backup/dst/child
status=0" "$(cat "$CREATE_LOG")"
}

test_create_destination_dataset_uses_parent_create_when_no_properties_apply() {
	CREATE_LOG="$TEST_TMPDIR/create_parents.log"
	: >"$CREATE_LOG"
	(
		zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=0; }
		zxfer_run_destination_zfs_cmd() { zxfer_property_test_log_destination_zfs "$@"; }
		zxfer_create_destination_dataset 1 "" "" filesystem "" "backup/dst/child" "readonly"
	)
	assertEquals "create -p backup/dst/child" "$(cat "$CREATE_LOG")"
}

test_create_destination_dataset_child_uses_creation_properties_and_volume_size() {
	CREATE_LOG="$TEST_TMPDIR/create_child.log"
	: >"$CREATE_LOG"
	(
		zxfer_probe_destination_existence() { zxfer_property_test_parent_exists "$@"; }
		zxfer_run_destination_zfs_cmd() { zxfer_property_test_log_destination_zfs "$@"; }
		zxfer_create_destination_dataset 0 "compression=lz4=local" "compression=lz4=local,volblocksize=8192=-" \
			volume 1073741824 "backup/dst/vol" "readonly"
	)
	assertEquals "create -V 1073741824 -o compression=lz4 -o volblocksize=8192 backup/dst/vol" "$(cat "$CREATE_LOG")"
}

test_create_destination_dataset_child_omits_parent_matching_override_creation_properties() {
	CREATE_LOG="$TEST_TMPDIR/create_child_override.log"
	: >"$CREATE_LOG"
	(
		g_option_I_ignore_properties="atime"
		zxfer_probe_destination_existence() { zxfer_property_test_parent_exists "$@"; }
		zxfer_run_destination_zfs_cmd() { zxfer_property_test_log_destination_zfs "$@"; }
		zxfer_load_normalized_dataset_properties() {
			printf 'parent=%s %s\n' "$1" "$2" >>"$CREATE_LOG"
			g_zxfer_normalized_dataset_properties="compression=lz4=local,mountpoint=/mnt=local,atime=off=local,quota=1G=local"
		}
		zxfer_create_destination_dataset 0 "" \
			"compression=lz4=override,mountpoint=/mnt=override,quota=1G=override,atime=off=override,checksum=sha256=local" \
			filesystem "" "backup/dst/child" "readonly,mountpoint"
	)
	assertEquals "The parent is read only when override-sourced creation entries exist; inheritable matches are dropped, quota (noninheritable), readonly/ignored parent entries, and non-override entries stay." \
		"parent=backup/dst destination
create -o mountpoint=/mnt -o quota=1G -o atime=off -o checksum=sha256 backup/dst/child" "$(cat "$CREATE_LOG")"
}

test_create_destination_dataset_reports_parent_property_read_and_create_failures() {
	set +e
	output=$(
		(
			zxfer_probe_destination_existence() { zxfer_property_test_parent_exists "$@"; }
			zxfer_load_normalized_dataset_properties() { return 3; }
			zxfer_throw_error() {
				printf '%s|%s\n' "$1" "$2"
				exit "$2"
			}
			zxfer_create_destination_dataset 0 "" "compression=lz4=override" filesystem "" "backup/dst/child" "readonly"
		)
	)
	assertEquals "Failed to retrieve parent destination properties for [backup/dst].|3" "$output"

	output=$(
		(
			zxfer_probe_destination_existence() { zxfer_property_test_parent_exists "$@"; }
			zxfer_run_destination_zfs_cmd() { return 5; }
			zxfer_throw_error() {
				printf '%s|%s\n' "$1" "$2"
				exit "$2"
			}
			zxfer_create_destination_dataset 0 "" "compression=lz4=local" filesystem "" "backup/dst/child" "readonly"
		)
	)
	status=$?
	assertEquals 5 "$status"
	assertEquals "Error when creating destination filesystem.|5" "$output"
}

test_create_destination_dataset_stops_before_any_create_when_the_child_override_filter_fails() {
	l_failing_awk="$TEST_TMPDIR/failing-awk"
	cat >"$l_failing_awk" <<'EOF'
#!/bin/sh
exit 6
EOF
	chmod 755 "$l_failing_awk"
	set +e
	output=$(
		(
			g_cmd_awk=$l_failing_awk
			zxfer_probe_destination_existence() { zxfer_property_test_parent_exists "$@"; }
			zxfer_load_normalized_dataset_properties() {
				g_zxfer_normalized_dataset_properties="compression=lz4=local"
			}
			zxfer_run_destination_zfs_cmd() { printf 'unexpected zfs %s\n' "$*"; }
			zxfer_throw_error() {
				printf '%s|%s\n' "$1" "$2"
				exit "$2"
			}
			zxfer_create_destination_dataset 0 "" "compression=lz4=override" filesystem "" \
				"backup/dst/child" "readonly"
		)
	)
	status=$?
	assertEquals 6 "$status"
	assertEquals "A failed child override filter must stop the run before any create." \
		"Failed to filter child creation override properties.|6" "$output"
}

test_create_destination_dataset_reports_parent_probe_failures() {
	set +e
	output=$(
		(
			zxfer_probe_destination_existence() { zxfer_property_test_parent_probe_fails "$@"; }
			zxfer_test_stub_throw_error_to_stdout
			zxfer_create_destination_dataset 1 "compression=lz4=local" "" filesystem "" "backup/dst/child" "readonly"
		)
	)
	status=$?
	assertEquals 1 "$status"
	assertEquals "Failed to determine whether destination dataset [backup/dst] exists: permission denied" "$output"
}

test_create_destination_dataset_marks_created_hierarchy_and_invalidates_destination_table_when_live() {
	(
		zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=0; }
		zxfer_run_destination_zfs_cmd() { :; }
		zxfer_property_test_table_add destination "backup/dst" "compression=stale=local"
		zxfer_property_test_table_add destination "backup/other" "compression=lz4=local"
		zxfer_create_destination_dataset 1 "compression=lz4=local" "" filesystem "" "backup/dst/child" "readonly"
		printf 'list=%s\n' "$(printf '%s' "$g_recursive_dest_list" | tr '\n' ' ')"
		printf 'parent_row=%s\n' "$(zxfer_property_table_find_dataset destination backup/dst && echo warm || echo cold)"
		printf 'other_row=%s\n' "$(zxfer_property_table_find_dataset destination backup/other && echo warm || echo cold)"
	) >"$TEST_TMPDIR/create_cache.out"
	assertEquals "list=backup/dst backup/dst/child
parent_row=cold
other_row=warm" "$(cat "$TEST_TMPDIR/create_cache.out")"
}

test_create_destination_dataset_dry_run_renders_creates_without_touching_caches() {
	(
		g_option_n_dryrun=1
		g_option_T_target_host=""
		g_cmd_zfs="/sbin/zfs"
		zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=0; }
		zxfer_run_destination_zfs_cmd() {
			printf 'unexpected live create\n' >&2
			exit 99
		}
		zxfer_create_destination_dataset 1 "compression=lz4=local" "" filesystem "" "backup/dst/child" "readonly"
		printf 'list=<%s>\n' "$g_recursive_dest_list"
	) >"$TEST_TMPDIR/create_dryrun.out"
	assertEquals "'/sbin/zfs' 'create' '-p' 'backup/dst'
'/sbin/zfs' 'create' '-o' 'compression=lz4' 'backup/dst/child'
list=<>" "$(cat "$TEST_TMPDIR/create_dryrun.out")"
}

test_run_zfs_create_with_properties_rejects_unsafe_shapes() {
	set +e
	output=$(zxfer_run_zfs_create_with_properties yes filesystem "" "compression=lz4" "backup/dst" 2>&1)
	status=$?
	assertEquals "Parent hierarchy creates must never carry -o properties." 1 "$status"
	assertEquals "" "$output"
	zxfer_run_zfs_create_with_properties no volume "" "compression=lz4" "backup/dst" >/dev/null 2>&1
	assertEquals "Volume creates need the source volsize." 1 "$?"
}

test_run_zfs_create_with_properties_decodes_delimiter_heavy_assignments_for_exec() {
	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "$#"
				printf '%s\n' "$@"
			}
			zxfer_run_zfs_create_with_properties no filesystem "" \
				"user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi=local,user:multi=a%0Ab" "backup/dst"
		)
	)
	assertEquals "$(printf '6\ncreate\n-o\nuser:note=value,with,commas=and;semi\n-o\nuser:multi=a\nb\nbackup/dst')" "$result"
}

test_run_zfs_create_with_properties_keeps_soh_values_in_one_argument() {
	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "$#"
				printf '[%s]\n' "$@"
			}
			zxfer_run_zfs_create_with_properties no filesystem "" \
				"$(printf 'com.x:note=a\001-o\001mountpoint%%3D/etc=local')" "backup/dst"
		)
	)
	assertEquals "$(printf '4\n[create]\n[-o]\n[com.x:note=a\001-o\001mountpoint=/etc]\n[backup/dst]')" "$result"
}

test_run_zfs_create_with_properties_renders_dry_run_command() {
	g_option_n_dryrun=1
	g_option_T_target_host=""
	g_cmd_zfs="/sbin/zfs"
	assertEquals "'/sbin/zfs' 'create' '-o' 'compression=lz4' '-o' 'quota=1G' 'backup/dst'" \
		"$(zxfer_run_zfs_create_with_properties no filesystem "" "compression=lz4,quota=1G" "backup/dst")"
}

test_run_zfs_create_with_properties_dry_run_ends_the_remote_line() {
	output=$(
		(
			g_option_n_dryrun=1
			g_option_T_target_host="backup@example.com"
			g_target_cmd_zfs="/remote/bin/zfs"
			zxfer_run_zfs_create_with_properties no filesystem "" "compression=lz4" "backup/dst"
			printf 'next\n'
		)
	)
	assertContains "$(printf '%s\n' "$output" | sed -n 1p)" "'compression=lz4'"
	assertEquals "A -T dry-run create ends its own line." "next" "$(printf '%s\n' "$output" | sed -n 2p)"
}

################################################################################
# DESTINATION COMMANDS
################################################################################

test_zxfer_build_destination_zfs_command_renders_the_local_zfs_path() {
	g_option_T_target_host=""
	g_cmd_zfs="/sbin/zfs"
	assertEquals "'/sbin/zfs' 'set' 'quota=1G' 'backup/dst'" \
		"$(zxfer_build_destination_zfs_command set quota=1G backup/dst)"
}

test_zxfer_build_destination_zfs_command_routes_remote_targets_through_ssh() {
	rendered=$(
		(
			g_option_T_target_host="backup@example.com"
			g_target_cmd_zfs="/remote/bin/zfs"
			zxfer_build_destination_zfs_command set quota=1G backup/dst
		)
	)
	assertContains "$rendered" "backup@example.com"
	assertContains "$rendered" "/remote/bin/zfs"
	assertContains "$rendered" "'quota=1G'"
}

test_zxfer_build_destination_zfs_command_escapes_control_bytes_for_remote_targets() {
	l_printable="com.x:note=it's \\033 fine"
	l_hostile=$(printf 'com.x:note=ok\033[2J\007 cut\r\nnext')
	(
		g_option_T_target_host="backup@example.com"
		g_target_cmd_zfs="/remote/bin/zfs"
		zxfer_render_destination_zfs_command set "$l_printable" backup/dst >"$TEST_TMPDIR/remote_render.out"
		zxfer_build_destination_zfs_command set "$l_printable" backup/dst >"$TEST_TMPDIR/remote_build.out"
		zxfer_build_destination_zfs_command set "$l_hostile" backup/dst >"$TEST_TMPDIR/remote_hostile.out"
	)
	assertEquals "A printable -T command displays exactly as rendered." \
		"$(cat "$TEST_TMPDIR/remote_render.out")" "$(cat "$TEST_TMPDIR/remote_build.out")"
	l_hostile_display=$(cat "$TEST_TMPDIR/remote_hostile.out")
	assertEquals "The -T display holds no raw control byte." \
		0 "$(zxfer_property_test_count_control_bytes "$l_hostile_display")"
	assertEquals "The -T display is one newline-terminated line." \
		1 "$(wc -l <"$TEST_TMPDIR/remote_hostile.out" | tr -d ' ')"
	# Not assertContains: it pipes through echo, which expands backslashes.
	case $l_hostile_display in
	*'com.x:note=ok\x1B[2J\x07 cut\r'*) ;;
	*) fail "The -T display should show the value escaped: $l_hostile_display" ;;
	esac
}

# Purpose: Run one `zfs inherit` through the shared property verb runner.
# Usage: zxfer_property_test_inherit PROPERTY DESTINATION
zxfer_property_test_inherit() {
	zxfer_run_destination_property_verb inherit \
		"Error when inheriting properties on destination filesystem." "$2" "$1"
}

# Purpose: Run one `zfs set` through the shared property verb runner.
# Usage: zxfer_property_test_set ASSIGNMENT DESTINATION
zxfer_property_test_set() {
	zxfer_run_destination_property_verb set \
		"Error when setting properties on destination filesystem." "$2" "$1"
}

test_zxfer_run_destination_property_verb_renders_display_lines_when_verbose() {
	output=$(
		(
			g_option_n_dryrun=0
			g_option_v_verbose=1
			g_option_T_target_host=""
			g_cmd_zfs="/sbin/zfs"
			zxfer_run_destination_zfs_cmd() { :; }
			zxfer_property_test_set quota=1G backup/dst
			zxfer_property_test_inherit quota backup/dst
		)
	)
	assertEquals "'/sbin/zfs' 'set' 'quota=1G' 'backup/dst'
'/sbin/zfs' 'inherit' 'quota' 'backup/dst'" "$output"
}

test_zxfer_run_destination_property_verb_dry_run_emits_newline_terminated_remote_lines() {
	output=$(
		(
			g_option_n_dryrun=1
			g_option_T_target_host="backup@example.com"
			g_target_cmd_zfs="/remote/bin/zfs"
			zxfer_property_test_set quota=1G backup/dst
			zxfer_property_test_inherit quota backup/dst
		)
	)
	assertEquals 2 "$(printf '%s\n' "$output" | grep -c "backup@example.com")"
	assertContains "$(printf '%s\n' "$output" | sed -n 1p)" "quota=1G"
	assertContains "$(printf '%s\n' "$output" | sed -n 2p)" "inherit"
}

test_zxfer_run_destination_property_verb_set_handles_dry_run_and_failures() {
	g_option_n_dryrun=1
	g_option_T_target_host=""
	g_cmd_zfs="/remote/zfs"
	assertEquals "'/remote/zfs' 'set' 'quota=1G' 'backup/dst'" "$(zxfer_property_test_set quota=1G backup/dst)"

	set +e
	output=$(
		(
			zxfer_run_destination_zfs_cmd() { return 1; }
			zxfer_test_stub_throw_error_to_stdout
			g_option_n_dryrun=0
			zxfer_property_test_set quota=1G backup/dst
		)
	)
	status=$?
	assertEquals 1 "$status"
	assertEquals "Error when setting properties on destination filesystem." "$output"
}

test_zxfer_run_destination_property_verb_invalidates_only_after_live_success() {
	log="$TEST_TMPDIR/set_invalidation.log"
	: >"$log"
	(
		zxfer_invalidate_destination_property_mutation_cache() { printf 'invalidated=%s\n' "$*" >>"$log"; }
		g_option_n_dryrun=1
		zxfer_property_test_set quota=1G backup/dst >/dev/null
		g_option_n_dryrun=0
		zxfer_run_destination_zfs_cmd() { return 1; }
		zxfer_throw_error() { exit 1; }
		zxfer_property_test_set quota=1G backup/dst
	)
	assertEquals "Dry runs and failed sets must not invalidate destination rows." "" "$(cat "$log")"
	(
		zxfer_invalidate_destination_property_mutation_cache() { printf 'invalidated=%s\n' "$*" >>"$log"; }
		zxfer_run_destination_zfs_cmd() { :; }
		zxfer_property_test_set quota=1G backup/dst
		zxfer_property_test_inherit quota backup/dst
	)
	assertEquals "Sets and inherits drop the whole destination subtree (the default scope)." \
		"invalidated=backup/dst
invalidated=backup/dst" "$(cat "$log")"
}

test_apply_property_changes_batches_decoded_set_assignments_for_local_exec() {
	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "$#"
				printf '%s\n' "$@"
			}
			zxfer_apply_property_changes "backup/dst" 1 \
				"quota=1G,user:note=a%2Cb%3Dc,user:multi=line1%0Aline2,,atime=off=local" "" ""
		)
	)
	assertEquals "$(printf '6\nset\nquota=1G\nuser:note=a,b=c\nuser:multi=line1\nline2\natime=off\nbackup/dst')" "$result"
}

test_apply_property_changes_keeps_soh_set_values_in_one_argument() {
	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "$#"
				printf '[%s]\n' "$@"
			}
			zxfer_apply_property_changes "backup/dst" 1 \
				"$(printf 'com.x:note=a\001sharenfs%%3Drw=local')" "" ""
		)
	)
	assertEquals "$(printf '3\n[set]\n[com.x:note=a\001sharenfs=rw]\n[backup/dst]')" "$result"
}

test_apply_property_changes_runs_no_set_for_a_list_of_empty_items() {
	(
		zxfer_run_destination_zfs_cmd() {
			printf 'unexpected set\n' >&2
			exit 99
		}
		zxfer_apply_property_changes "backup/dst" 1 ",," "" ""
	)
	assertEquals 0 "$?"
}

test_apply_property_changes_preserves_literal_set_assignment_for_remote_exec() {
	fake_ssh="$TEST_TMPDIR/fake_ssh_join_exec_set"
	remote_zfs="$TEST_TMPDIR/fake_remote_zfs_set"
	ssh_log="$TEST_TMPDIR/fake_ssh_join_exec_set.log"
	remote_log="$TEST_TMPDIR/fake_remote_zfs_set.log"
	l_property="user:test\$\\\`\"\\\\"
	l_value="value with spaces \$\\\`\"\\\\"
	old_g_cmd_ssh=${g_cmd_ssh-}
	old_target_host=$g_option_T_target_host
	old_target_cmd_zfs=${g_target_cmd_zfs-}

	cat >"$fake_ssh" <<'EOF'
#!/bin/sh
while [ $# -gt 0 ]; do
	case "$1" in
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
host=$1
shift
remote_cmd=""
for arg in "$@"; do
	if [ "$remote_cmd" = "" ]; then
		remote_cmd=$arg
	else
		remote_cmd="$remote_cmd $arg"
	fi
done
if [ -n "${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "$host" >>"$FAKE_SSH_LOG"
	printf '%s\n' "$remote_cmd" >>"$FAKE_SSH_LOG"
fi
if ! eval "set -- $remote_cmd"; then
	exit 1
fi
"$@"
EOF
	chmod +x "$fake_ssh"

	cat >"$remote_zfs" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" >"$ZXFER_REMOTE_ZFS_LOG"
EOF
	chmod +x "$remote_zfs"

	FAKE_SSH_LOG="$ssh_log"
	ZXFER_REMOTE_ZFS_LOG="$remote_log"
	export FAKE_SSH_LOG ZXFER_REMOTE_ZFS_LOG

	g_option_n_dryrun=0
	g_cmd_ssh="$fake_ssh"
	g_option_T_target_host="target.example"
	g_target_cmd_zfs="$remote_zfs"

	# The value is already serialized: none of its characters needs encoding.
	zxfer_apply_property_changes "backup/dst" 1 "$l_property=$l_value" "" ""

	unset FAKE_SSH_LOG ZXFER_REMOTE_ZFS_LOG
	g_cmd_ssh=$old_g_cmd_ssh
	g_option_T_target_host=$old_target_host
	g_target_cmd_zfs=$old_target_cmd_zfs

	assertEquals "Remote property sets should preserve the literal assignment after ssh joins the remote command into a shell string." \
		"$(printf '%s\n' "set" "$l_property=$l_value" "backup/dst")" "$(cat "$remote_log")"
	assertEquals "target.example" "$(sed -n '1p' "$ssh_log")"
}

test_zxfer_run_destination_property_verb_inherit_handles_dry_run_and_failures() {
	g_option_n_dryrun=1
	g_option_T_target_host=""
	g_cmd_zfs="/remote/zfs"
	assertEquals "'/remote/zfs' 'inherit' 'quota' 'backup/dst'" "$(zxfer_property_test_inherit quota backup/dst)"

	set +e
	output=$(
		(
			zxfer_run_destination_zfs_cmd() { return 1; }
			zxfer_test_stub_throw_error_to_stdout
			g_option_n_dryrun=0
			zxfer_property_test_inherit quota backup/dst
		)
	)
	status=$?
	assertEquals 1 "$status"
	assertEquals "Error when inheriting properties on destination filesystem." "$output"
}

test_zxfer_run_destination_property_verb_fails_closed_when_a_dry_run_line_cannot_be_rendered() {
	set +e
	output=$(
		(
			g_option_n_dryrun=1
			zxfer_build_destination_zfs_command() { return 4; }
			zxfer_throw_error() {
				printf '%s|%s\n' "$1" "$2"
				exit "$2"
			}
			zxfer_property_test_set quota=1G backup/dst
			printf 'continued\n'
		)
	)
	status=$?
	assertEquals 4 "$status"
	assertEquals "Error when setting properties on destination filesystem.|4" "$output"
}

################################################################################
# CHILD INHERIT ADJUSTMENT
################################################################################

zxfer_property_test_adjust_with_parent() {
	l_parent_pvs=$1
	shift
	(
		zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=1; }
		zxfer_load_normalized_dataset_properties() {
			g_zxfer_normalized_dataset_properties=$l_parent_pvs
			g_zxfer_normalized_dataset_properties_cache_hit=1
		}
		zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" "$@" "$ZXFER_BASE_READONLY_PROPERTIES"
		printf '%s\n%s\n' "$g_zxfer_adjusted_set_list" "$g_zxfer_adjusted_inherit_list"
	)
}

test_adjust_child_inherit_to_match_parent_promotes_mismatched_parent_values_to_sets() {
	assertEquals "quota=32M,atime=off
checksum=sha256" "$(zxfer_property_test_adjust_with_parent "checksum=sha256=local,atime=on=local" \
		"checksum=sha256=inherited,atime=off=inherited" "quota=32M" "checksum=sha256,atime=off")"
}

test_adjust_child_inherit_to_match_parent_preserves_inherit_when_parent_matches() {
	assertEquals "
checksum=sha256,atime=off" "$(zxfer_property_test_adjust_with_parent "checksum=sha256=local,atime=off=local" \
		"checksum=sha256=inherited,atime=off=inherited" "" "checksum=sha256,atime=off")"
}

test_adjust_child_inherit_to_match_parent_moves_inherited_source_properties_out_of_set_list_when_parent_matches() {
	assertEquals "compression=lz4
checksum=sha256" "$(zxfer_property_test_adjust_with_parent "checksum=sha256=local,compression=lz4=local" \
		"checksum=sha256=inherited,compression=lz4=local" "checksum=sha256,compression=lz4" "")"
}

test_adjust_child_inherit_to_match_parent_keeps_matching_inheritable_overrides_inherited() {
	assertEquals "quota=1G
checksum=sha256,atime=off" "$(zxfer_property_test_adjust_with_parent "checksum=sha256=local" \
		"checksum=sha256=override,atime=off=override,quota=1G=override" \
		"checksum=sha256,quota=1G" "atime=off")"
}

test_adjust_child_inherit_to_match_parent_filters_parent_properties_with_the_readonly_and_ignore_lists() {
	result=$(
		(
			g_option_I_ignore_properties="copies"
			zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=1; }
			zxfer_load_normalized_dataset_properties() {
				g_zxfer_normalized_dataset_properties="readonly=on=local,copies=2=local,atime=off=local"
			}
			zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
				"readonly=on=inherited,copies=2=inherited,atime=off=inherited" "" \
				"readonly=on,copies=2,atime=off" "readonly"
			printf '%s\n%s\n' "$g_zxfer_adjusted_set_list" "$g_zxfer_adjusted_inherit_list"
		)
	)
	assertEquals "Read-only and -I parent entries are not visible, so the child must set them locally." \
		"readonly=on,copies=2
atime=off" "$result"
}

test_adjust_child_inherit_to_match_parent_returns_unchanged_lists_without_work_root_or_parent() {
	result=$(
		(
			zxfer_probe_destination_existence() {
				printf 'unexpected probe\n' >&2
				exit 99
			}
			zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" "compression=lz4=local" "" "" "readonly"
			printf 'empty=<%s|%s>\n' "$g_zxfer_adjusted_set_list" "$g_zxfer_adjusted_inherit_list"
			zxfer_adjust_child_inherit_to_match_parent "backup" "compression=lz4=local" "compression=lz4" "atime=off" "readonly"
			printf 'root=<%s|%s>\n' "$g_zxfer_adjusted_set_list" "$g_zxfer_adjusted_inherit_list"
			zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=0; }
			zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" "compression=lz4=local" "compression=lz4" "atime=off" "readonly"
			printf 'missing=<%s|%s>\n' "$g_zxfer_adjusted_set_list" "$g_zxfer_adjusted_inherit_list"
		)
	)
	assertEquals "empty=<|>
root=<compression=lz4|atime=off>
missing=<compression=lz4|atime=off>" "$result"
}

test_adjust_child_inherit_to_match_parent_reports_parent_probe_and_load_failures() {
	set +e
	output=$(
		(
			zxfer_probe_destination_existence() {
				g_zxfer_destination_exists_error="Failed to determine whether destination dataset [backup/dst] exists: timeout"
				return 1
			}
			zxfer_test_stub_throw_error_to_stdout
			zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" "compression=lz4=inherited" "" "compression=lz4" "readonly"
		)
	)
	assertEquals "Failed to determine whether destination dataset [backup/dst] exists: timeout" "$output"

	(
		zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=1; }
		zxfer_load_normalized_dataset_properties() { return 6; }
		zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" "compression=lz4=inherited" "" "compression=lz4" "readonly"
	)
	assertEquals "A parent property read failure returns its status for the caller to report." 6 "$?"
}

test_adjust_child_inherit_to_match_parent_uses_prefetched_destination_table_and_counts_live_parent_reads() {
	zxfer_property_test_table_add destination "backup/dst" "compression=lz4=local"
	result=$(
		(
			g_option_V_very_verbose=1
			zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=1; }
			zxfer_run_zfs_cmd_for_role() {
				printf 'unexpected zfs call\n' >&2
				exit 99
			}
			zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" "compression=lz4=inherited" "" "compression=lz4" "readonly"
			printf '%s|%s|%s\n' "$g_zxfer_adjusted_set_list" "$g_zxfer_adjusted_inherit_list" \
				"${g_zxfer_profile_parent_destination_property_reads:-0}"
		)
	)
	assertEquals "|compression=lz4|0" "$result"
}

test_adjust_child_inherit_to_match_parent_reports_awk_failures() {
	set +e
	output=$(
		(
			zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=1; }
			zxfer_load_normalized_dataset_properties() {
				g_zxfer_normalized_dataset_properties="compression=lz4=local"
			}
			g_cmd_awk="$TEST_TMPDIR/missing-awk"
			zxfer_test_stub_throw_error_to_stdout
			zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" "compression=lz4=inherited" "" "compression=lz4" "readonly" 2>/dev/null
		)
	)
	status=$?
	assertEquals 1 "$status"
	assertEquals "Failed to reconcile child property inheritance." "$output"
}

################################################################################
# APPLY
################################################################################

zxfer_property_test_apply() {
	(
		# Join argv with explicit spaces: "$*" would use the caller's IFS.
		zxfer_run_destination_zfs_cmd() {
			l_apply_line=""
			for l_apply_arg in "$@"; do
				l_apply_line="$l_apply_line $l_apply_arg"
			done
			printf '%s\n' "${l_apply_line# }"
		}
		zxfer_apply_property_changes "$@"
	)
}

test_apply_property_changes_batches_sets_and_inherits_per_entry_for_children() {
	assertEquals "set compression=lz4 atime=off backup/dst/child
inherit checksum backup/dst/child
inherit copies backup/dst/child" \
		"$(zxfer_property_test_apply "backup/dst/child" 0 "quota=1G" "compression=lz4,atime=off" "checksum=sha256,copies=2")"
}

test_apply_property_changes_uses_initial_set_list_and_ignores_inherits_for_the_initial_source() {
	assertEquals "set quota=1G backup/dst" \
		"$(zxfer_property_test_apply "backup/dst" 1 "quota=1G" "compression=lz4" "checksum=sha256")"
	assertEquals "" "$(zxfer_property_test_apply "backup/dst" 1 "" "compression=lz4" "checksum=sha256")"
}

test_apply_property_changes_fails_closed_when_the_set_fails() {
	set +e
	output=$(
		(
			zxfer_run_destination_zfs_cmd() { return 3; }
			zxfer_throw_error() {
				printf '%s|%s\n' "$1" "$2"
				exit "$2"
			}
			zxfer_apply_property_changes "backup/dst" 1 "quota=1G" "" ""
		)
	)
	status=$?
	assertEquals 3 "$status"
	assertEquals "Error when setting properties on destination filesystem.|3" "$output"
}

test_apply_property_changes_logs_decoded_lists_only_when_verbose() {
	quiet=$(
		(
			zxfer_run_destination_zfs_cmd() { :; }
			g_cmd_awk="$TEST_TMPDIR/missing-awk"
			zxfer_apply_property_changes "backup/dst/child" 0 "" "compression=lz4" "atime=off"
		)
	)
	assertEquals "Quiet runs render no display lines and spawn no decoder." "" "$quiet"

	verbose=$(
		(
			g_option_v_verbose=1
			g_option_T_target_host=""
			g_cmd_zfs="/sbin/zfs"
			zxfer_run_destination_zfs_cmd() { :; }
			zxfer_apply_property_changes "backup/dst/child" 0 "" "user:note=a%2Cb" "atime=off"
		)
	)
	assertEquals "Setting properties/sources on destination filesystem \"backup/dst/child\".
Property set list: user:note=a,b
Property inherit list: atime=off
'/sbin/zfs' 'set' 'user:note=a,b' 'backup/dst/child'
'/sbin/zfs' 'inherit' 'atime' 'backup/dst/child'" "$verbose"
}

test_apply_property_changes_escapes_control_bytes_in_verbose_lines() {
	l_raw=$(printf 'ok\033[2J\033]0;owned\007 lit=\\033[31m \\c cut\r\nProperty set list: forged')
	l_encoded=$(printf 'ok\033[2J\033]0%%3Bowned\007 lit%%3D\\033[31m \\c cut%%0D%%0AProperty set list: forged')
	l_shown='com.x:note=ok\x1B[2J\x1B]0;owned\x07 lit=\\033[31m \\c cut\r\nProperty set list: forged'
	l_argv_log="$TEST_TMPDIR/apply_escape.argv"
	: >"$l_argv_log"

	verbose=$(
		(
			g_option_v_verbose=1
			g_option_T_target_host=""
			g_cmd_zfs="/sbin/zfs"
			zxfer_run_destination_zfs_cmd() { printf '[%s]\n' "$@" >>"$l_argv_log"; }
			zxfer_apply_property_changes "backup/dst/child" 0 "" "com.x:note=$l_encoded" \
				"$(printf 'com.x:tag=a\033b')"
		)
	)

	assertEquals "Setting properties/sources on destination filesystem \"backup/dst/child\".
Property set list: $l_shown
Property inherit list: com.x:tag=a\\x1Bb
'/sbin/zfs' 'set' '$l_shown' 'backup/dst/child'
'/sbin/zfs' 'inherit' 'com.x:tag' 'backup/dst/child'" "$verbose"
	assertEquals "-v output holds no control byte but LF." \
		0 "$(zxfer_property_test_count_control_bytes "$verbose")"
	assertEquals "zfs still gets the raw value as one argument." "[set]
[com.x:note=$l_raw]
[backup/dst/child]
[inherit]
[com.x:tag]
[backup/dst/child]" "$(cat "$l_argv_log")"
}

test_property_transfer_helpers_preserve_caller_ifs_and_globbing() {
	l_saved_ifs=$IFS
	IFS=","
	set -f
	result=$(zxfer_property_test_apply "backup/dst/child" 0 "" "compression=lz4,atime=off" "checksum=sha256")
	zxfer_plan_property_changes "compression=lz4=local,atime=off=local" "" 1 filesystem "" "" "" \
		"compression=off=local"
	l_globbing=$(zxfer_property_test_report_globbing_state after)
	l_ifs_after=$IFS
	set +f
	IFS=$l_saved_ifs
	assertEquals "set compression=lz4 atime=off backup/dst/child
inherit checksum backup/dst/child" "$result"
	assertEquals "compression=lz4,atime=off" "$g_zxfer_plan_initial_set_result"
	assertEquals "after_globbing=disabled" "$l_globbing"
	assertEquals "," "$l_ifs_after"
}
