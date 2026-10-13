#!/bin/sh
# Property transfer fragment: the guards the launcher never reaches or the
# fault injector cannot trip: create shapes, the parent probe, the child
# override filter's awk, the dry-run rendering (-n stops before any property
# work), the child-inheritance awk, and the caller's IFS and globbing. Run by
# tests/test_zxfer_property_transfer.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

# The raw source view must survive destination reads, planning, and execution:
# -e changes the effective view, -o changes the plan, but neither changes -k.
test_property_operation_owns_raw_and_effective_views_across_result_changes() {
	(
		g_option_e_restore_property_mode=1
		g_option_P_transfer_property=1
		g_zxfer_property_override_policy="user:note=override=override"
		zxfer_load_normalized_dataset_properties() {
			case $2 in
			source) g_zxfer_normalized_dataset_properties="type=volume=-,volsize=16384=-,user:note=original=local" ;;
			destination)
				g_zxfer_normalized_dataset_properties="type=volume=-,volsize=16384=-,user:note=old=local"
				g_zxfer_property_row_payload_result=clobbered
				;;
			esac
		}
		zxfer_find_restored_backup_properties() {
			g_zxfer_property_row_payload_result="type=volume=-,volsize=16384=-,user:note=restored=local"
		}
		zxfer_property_destination_exists() { return 0; }
		zxfer_execute_property_plan() {
			printf 'apply=%s\n' "$(zxfer_property_plan_list "$2" set)"
			g_zxfer_normalized_dataset_properties=clobbered
			g_zxfer_required_properties_result=clobbered
			g_zxfer_property_plan_file=clobbered
			g_zxfer_property_plan_summary_result=clobbered
			g_zxfer_property_row_payload_result=clobbered
		}
		zxfer_capture_backup_metadata_for_completed_transfer() { printf 'backup=%s\n' "$2"; }
		zxfer_transfer_properties tank/src
		g_zxfer_property_override_policy=""
		zxfer_transfer_properties tank/src
	) >"$TEST_TMPDIR/owned-property-views.out"
	assertEquals "Overrides and restore only affect the applied view; each backup retains the source." \
		"apply=user:note=override
backup=type=volume=-,volsize=16384=-,user:note=original=local
apply=user:note=restored
backup=type=volume=-,volsize=16384=-,user:note=original=local" \
		"$(cat "$TEST_TMPDIR/owned-property-views.out")"
}

################################################################################
# DESTINATION CREATE
################################################################################

# The launcher notes a seeded dataset again after its seed, so only this test
# sees that a destination found live is added to the destination list.
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

test_create_destination_dataset_uses_parent_create_when_no_properties_apply() {
	CREATE_LOG="$TEST_TMPDIR/create_parents.log"
	: >"$CREATE_LOG"
	(
		zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=0; }
		zxfer_run_destination_zfs_cmd() { zxfer_property_test_log_destination_zfs "$@"; }
		zxfer_property_test_create 1 "" "" filesystem "" "backup/dst/child" "readonly"
	)
	assertEquals "create -p backup/dst/child" "$(cat "$CREATE_LOG")"
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
			zxfer_property_test_create 0 "" "compression=lz4=override" filesystem "" \
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
			zxfer_property_test_create 1 "compression=lz4=local" "" filesystem "" "backup/dst/child" "readonly"
		)
	)
	status=$?
	assertEquals 1 "$status"
	assertEquals "Failed to determine whether destination dataset [backup/dst] exists: permission denied" "$output"
}

test_run_zfs_create_with_properties_rejects_unsafe_shapes() {
	set +e
	output=$(zxfer_property_test_run_create yes filesystem "" "compression=lz4" "backup/dst" 2>&1)
	status=$?
	assertEquals "Parent hierarchy creates must never carry -o properties." 1 "$status"
	assertEquals "" "$output"
	zxfer_property_test_run_create no volume "" "compression=lz4" "backup/dst" >/dev/null 2>&1
	assertEquals "Volume creates need the source volsize." 1 "$?"
}

################################################################################
# DRY RUNS
################################################################################

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

# -n previews without any property work, so the launcher never reaches these
# dry-run branches: a create, set or inherit prints its command instead of
# running it, touches no cache, ends each -T line, and fails closed when its
# line cannot be rendered.
test_destination_property_commands_render_instead_of_running_on_dry_runs() {
	(
		g_option_n_dryrun=1
		g_option_R_recursive="tank/src"
		g_option_T_target_host=""
		g_cmd_zfs="/sbin/zfs"
		zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=0; }
		zxfer_run_destination_zfs_cmd() {
			printf 'unexpected live command\n' >&2
			exit 99
		}
		zxfer_invalidate_destination_property_mutation_cache() { printf 'unexpected invalidation\n'; }
		zxfer_property_test_create 1 "compression=lz4=local" "" filesystem "" "backup/dst/child" "readonly"
		printf 'list=<%s>\n' "$g_recursive_dest_list"
		zxfer_property_test_run_create no filesystem "" "compression=lz4,quota=1G" "backup/dst"
		zxfer_property_test_set quota=1G backup/dst
		zxfer_property_test_inherit quota backup/dst
	) >"$TEST_TMPDIR/dry_run_local.out" 2>"$TEST_TMPDIR/dry_run_local.err"
	assertFalse "dry-run property creation must not print a creation-attempt notice" \
		"[ -s '$TEST_TMPDIR/dry_run_local.err' ]"
	assertEquals "'/sbin/zfs' 'create' '-p' 'backup/dst'
'/sbin/zfs' 'create' '-o' 'compression=lz4' 'backup/dst/child'
list=<>
'/sbin/zfs' 'create' '-o' 'compression=lz4' '-o' 'quota=1G' 'backup/dst'
'/sbin/zfs' 'set' 'quota=1G' 'backup/dst'
'/sbin/zfs' 'inherit' 'quota' 'backup/dst'" "$(cat "$TEST_TMPDIR/dry_run_local.out")"

	output=$(
		(
			g_option_n_dryrun=1
			g_option_T_target_host="backup@example.com"
			g_target_cmd_zfs="/remote/bin/zfs"
			zxfer_property_test_run_create no filesystem "" "compression=lz4" "backup/dst"
			zxfer_property_test_set quota=1G backup/dst
			zxfer_property_test_inherit quota backup/dst
			printf 'next\n'
		)
	)
	assertEquals "Each -T line names the host." 3 \
		"$(printf '%s\n' "$output" | grep -c "backup@example.com")"
	assertContains "$(printf '%s\n' "$output" | sed -n 1p)" "'compression=lz4'"
	assertContains "$(printf '%s\n' "$output" | sed -n 2p)" "quota=1G"
	assertContains "$(printf '%s\n' "$output" | sed -n 3p)" "inherit"
	assertEquals "Each -T line ends its own line." "next" "$(printf '%s\n' "$output" | sed -n 4p)"

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
		zxfer_property_test_write_actions set "$2" "$3" "$1"
		zxfer_resolve_parent_property_plan "$g_zxfer_property_plan_file" "$l_parent_pvs" "$ZXFER_BASE_READONLY_PROPERTIES" apply
		zxfer_property_plan_list "$g_zxfer_property_parent_plan_file" set
		zxfer_property_plan_list "$g_zxfer_property_parent_plan_file" inherit
	)
}

# The child inherit adjustment against the destination parent (its list
# filtered with the readonly and -I lists first): an inherit stays only where
# the parent provides the value or it is a matching inheritable -o value, and
# is set on the child otherwise; a noninheritable -o value is always set. The
# plan's child sets are local or noninheritable, so its set-list branches are
# pinned here only.
test_adjust_child_inherit_to_match_parent_keeps_inherits_only_the_parent_provides() {
	assertEquals "A mismatched parent value turns an inherit into a set." "quota=32M,atime=off
checksum=sha256" "$(zxfer_property_test_adjust_with_parent "checksum=sha256=local,atime=on=local" \
		"checksum=sha256=inherited,atime=off=inherited" "quota=32M" "checksum=sha256,atime=off")"
	assertEquals "A matching parent keeps the inherits." "
checksum=sha256,atime=off" "$(zxfer_property_test_adjust_with_parent "checksum=sha256=local,atime=off=local" \
		"checksum=sha256=inherited,atime=off=inherited" "" "checksum=sha256,atime=off")"
	assertEquals "A set of an inherited source value becomes an inherit when the parent matches." \
		"compression=lz4
checksum=sha256" "$(zxfer_property_test_adjust_with_parent "checksum=sha256=local,compression=lz4=local" \
			"checksum=sha256=inherited,compression=lz4=local" "checksum=sha256,compression=lz4" "")"
	assertEquals "A matching inheritable override stays inherited; a noninheritable one is set." \
		"quota=1G
checksum=sha256,atime=off" "$(zxfer_property_test_adjust_with_parent "checksum=sha256=local" \
			"checksum=sha256=override,atime=off=override,quota=1G=override" \
			"checksum=sha256,quota=1G" "atime=off")"
	assertEquals "Read-only and -I parent entries are not visible, so the child must set them locally." \
		"readonly=on,copies=2
atime=off" "$(
			g_option_I_ignore_properties="copies"
			zxfer_property_test_adjust_with_parent "readonly=on=local,copies=2=local,atime=off=local" \
				"readonly=on=inherited,copies=2=inherited,atime=off=inherited" "" \
				"readonly=on,copies=2,atime=off"
		)"
}

# The shared resolver preserves creation's underlying status, reconciliation
# diagnostics, and checked publication when a producer stops short.
test_parent_resolution_preserves_failure_statuses_and_rejects_partial_output() {
	zxfer_property_test_write_actions set "compression=lz4" "" "compression=lz4=inherited"
	l_parent_input=$g_zxfer_property_plan_file
	(
		g_cmd_awk="$TEST_TMPDIR/missing-parent-awk"
		zxfer_resolve_parent_property_plan "$l_parent_input" "compression=lz4=local" "" create 2>/dev/null
	) >"$TEST_TMPDIR/parent-failure.out"
	l_parent_status=$?
	assertEquals "Creation preserves the underlying command status." 127 "$l_parent_status"
	(
		g_cmd_awk="$TEST_TMPDIR/missing-parent-awk"
		zxfer_test_stub_throw_error_to_stdout
		zxfer_resolve_parent_property_plan "$l_parent_input" "compression=lz4=local" "" apply 2>/dev/null
	) >"$TEST_TMPDIR/parent-failure.out"
	l_parent_status=$?
	assertEquals "Reconciliation retains its original diagnostic and status." 1 "$l_parent_status"
	assertEquals "Failed to reconcile child property inheritance." "$(cat "$TEST_TMPDIR/parent-failure.out")"
	cat >"$TEST_TMPDIR/short-parent-awk" <<'EOF'
#!/bin/sh
printf 'set\tcompression=lz4=inherited=lz4\n' >"$ZXFER_PROPERTY_PLAN_OUTPUT"
EOF
	chmod 755 "$TEST_TMPDIR/short-parent-awk"
	(
		g_cmd_awk="$TEST_TMPDIR/short-parent-awk"
		zxfer_resolve_parent_property_plan "$l_parent_input" "compression=lz4=local" "" create
		l_parent_status=$?
		printf '%s|<%s>\n' "$l_parent_status" "$g_zxfer_property_plan_summary_result"
	) >"$TEST_TMPDIR/parent-short.out"
	assertEquals "A partial plan never publishes a count summary." '1|<>' "$(cat "$TEST_TMPDIR/parent-short.out")"
}

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
		zxfer_property_test_write_actions set "$4" "$5"
		zxfer_execute_property_plan apply "$g_zxfer_property_plan_file" "" "" "$1"
	)
}

test_property_transfer_helpers_preserve_caller_ifs_and_globbing() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"
	PROBE_LOG=/dev/null
	l_saved_ifs=$IFS
	IFS=","
	set -f
	zxfer_read_override_properties "compression=lz4,user:glob=*"
	l_override=$g_zxfer_override_properties_result
	result=$(zxfer_property_test_apply "backup/dst/child" 0 "" "compression=lz4,atime=off" "checksum=sha256")
	zxfer_property_test_plan 1 "compression=lz4=local,atime=off=local" "" 1 filesystem "" "" "" \
		"compression=off=local"
	l_initial_set=$(zxfer_property_plan_list "$g_zxfer_property_plan_file" set)
	l_unsupported=$(
		zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=1; }
		zxfer_run_source_zfs_cmd() { zxfer_property_test_fake_unsupported_scan source "$@"; }
		zxfer_run_destination_zfs_cmd() { zxfer_property_test_fake_unsupported_scan destination "$@"; }
		zxfer_calculate_unsupported_properties
		printf '%s\n' "$g_zxfer_unsupported_filesystem_properties"
	)
	l_globbing=$(zxfer_property_test_report_globbing_state after)
	l_ifs_after=$IFS
	set +f
	IFS=$l_saved_ifs
	assertEquals "compression=lz4=override,user:glob=*=override" "$l_override"
	assertEquals "set compression=lz4 atime=off backup/dst/child
inherit checksum backup/dst/child" "$result"
	assertEquals "compression=lz4,atime=off" "$l_initial_set"
	assertEquals "overlay,volmode" "$l_unsupported"
	assertEquals "after_globbing=disabled" "$l_globbing"
	assertEquals "," "$l_ifs_after"
}

# No mutation may precede detection of a short or malformed action file.
test_property_executor_refuses_partial_or_wrong_mode_plans_before_mutation() {
	zxfer_property_test_write_actions set 'compression=lz4'
	l_execute_test_plan=$g_zxfer_property_plan_file
	(
		zxfer_run_destination_zfs_cmd() { printf 'unexpected command\n'; }
		zxfer_execute_property_plan create "$l_execute_test_plan" filesystem "" backup/dst
		l_execute_test_status=$?
		printf 'wrong-mode=%s\n' "$l_execute_test_status"
		printf 'set\tcompression=lz4=local=\n' >|"$l_execute_test_plan"
		zxfer_execute_property_plan apply "$l_execute_test_plan" "" "" backup/dst
		l_execute_test_status=$?
		printf 'partial=%s\n' "$l_execute_test_status"
	) >"$TEST_TMPDIR/executor-invalid.out"
	assertEquals 'wrong-mode=1
partial=1' "$(cat "$TEST_TMPDIR/executor-invalid.out")"
}

# These cases exercise the operation owner after the old parent coordinator
# was removed: no unnecessary parent reads, and original probe/read statuses.
test_property_owner_preserves_parent_guards_and_failure_ordering() {
	for l_owner_test_mode in empty pool_root missing probe_failure read_failure; do
		(
			g_option_P_transfer_property=1
			g_actual_dest=backup/dst/child
			[ "$l_owner_test_mode" != pool_root ] || g_actual_dest=backup
			zxfer_property_destination_exists() { return 0; }
			zxfer_load_normalized_dataset_properties() {
				g_zxfer_normalized_dataset_properties_cache_hit=1
				if [ "$2" = source ]; then
					g_zxfer_normalized_dataset_properties='type=volume=-,volsize=8192=-,compression=lz4=inherited'
				elif [ "$1" = "$g_actual_dest" ]; then
					g_zxfer_normalized_dataset_properties='type=volume=-,volsize=8192=-,compression=off=local'
					[ "$l_owner_test_mode" != empty ] || g_zxfer_normalized_dataset_properties='type=volume=-,volsize=8192=-,compression=lz4=inherited'
				else
					printf 'parent-read\n'
					return 6
				fi
			}
			zxfer_probe_destination_existence() {
				printf 'parent-probe\n'
				g_zxfer_destination_exists_error='parent probe failed'
				[ "$l_owner_test_mode" != probe_failure ] || return 7
				g_zxfer_destination_exists_result=0
				[ "$l_owner_test_mode" != read_failure ] || g_zxfer_destination_exists_result=1
			}
			zxfer_run_destination_zfs_cmd() { printf '%s\n' "$*"; }
			zxfer_capture_backup_metadata_for_completed_transfer() { :; }
			zxfer_throw_error() {
				printf '%s|%s\n' "$1" "$2"
				exit "$2"
			}
			zxfer_transfer_properties tank/src/child
		) >"$TEST_TMPDIR/owner-$l_owner_test_mode.out"
		l_owner_test_status=$?
		case $l_owner_test_mode in
		empty)
			assertEquals 0 "$l_owner_test_status"
			assertEquals "No actions require no parent probe." '' "$(cat "$TEST_TMPDIR/owner-empty.out")"
			;;
		pool_root)
			assertEquals 0 "$l_owner_test_status"
			assertEquals 'inherit compression backup' "$(cat "$TEST_TMPDIR/owner-pool_root.out")"
			;;
		missing)
			assertEquals 0 "$l_owner_test_status"
			assertEquals 'parent-probe
inherit compression backup/dst/child' "$(cat "$TEST_TMPDIR/owner-missing.out")"
			;;
		probe_failure)
			assertEquals 7 "$l_owner_test_status"
			assertEquals 'parent-probe
parent probe failed|7' "$(cat "$TEST_TMPDIR/owner-probe_failure.out")"
			;;
		read_failure)
			assertEquals 6 "$l_owner_test_status"
			assertEquals 'parent-probe
parent-read
Failed to reconcile inherited child properties for destination [backup/dst/child].|6' "$(cat "$TEST_TMPDIR/owner-read_failure.out")"
			;;
		esac
	done
}

# A staged-file read failure belongs to the operation: never continue into
# capture or the next dataset when the consumer cannot read its retained file.
test_property_owner_stops_before_capture_when_the_action_file_disappears() {
	(
		g_option_P_transfer_property=1
		zxfer_property_destination_exists() { return 0; }
		zxfer_load_normalized_dataset_properties() {
			g_zxfer_normalized_dataset_properties='type=volume=-,volsize=8192=-,compression=lz4=local'
		}
		zxfer_show_property_plan() { rm -f "$1"; }
		zxfer_run_destination_zfs_cmd() { printf 'unexpected mutation\n'; }
		zxfer_capture_backup_metadata_for_completed_transfer() { printf 'unexpected capture\n'; }
		zxfer_throw_error() {
			printf '%s|%s\n' "$1" "$2"
			exit "$2"
		}
		zxfer_transfer_properties tank/src
	) >"$TEST_TMPDIR/missing-action-file.out"
	l_owner_test_status=$?
	assertEquals 1 "$l_owner_test_status"
	assertEquals 'Failed to read dataset property plan.|1' "$(cat "$TEST_TMPDIR/missing-action-file.out")"
}
