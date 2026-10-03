#!/bin/sh
# Property transfer fragment: the guards the launcher never reaches or the
# fault injector cannot trip: create shapes, the parent probe, the child
# override filter's awk, the dry-run rendering (-n stops before any property
# work), the child-inheritance awk, and the caller's IFS and globbing. Run by
# tests/test_zxfer_property_transfer.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

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
		zxfer_create_destination_dataset 1 "" "" filesystem "" "backup/dst/child" "readonly"
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

test_run_zfs_create_with_properties_rejects_unsafe_shapes() {
	set +e
	output=$(zxfer_run_zfs_create_with_properties yes filesystem "" "compression=lz4" "backup/dst" 2>&1)
	status=$?
	assertEquals "Parent hierarchy creates must never carry -o properties." 1 "$status"
	assertEquals "" "$output"
	zxfer_run_zfs_create_with_properties no volume "" "compression=lz4" "backup/dst" >/dev/null 2>&1
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
		zxfer_create_destination_dataset 1 "compression=lz4=local" "" filesystem "" "backup/dst/child" "readonly"
		printf 'list=<%s>\n' "$g_recursive_dest_list"
		zxfer_run_zfs_create_with_properties no filesystem "" "compression=lz4,quota=1G" "backup/dst"
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
			zxfer_run_zfs_create_with_properties no filesystem "" "compression=lz4" "backup/dst"
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
		zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=1; }
		zxfer_load_normalized_dataset_properties() {
			g_zxfer_normalized_dataset_properties=$l_parent_pvs
			g_zxfer_normalized_dataset_properties_cache_hit=1
		}
		zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" "$@" "$ZXFER_BASE_READONLY_PROPERTIES"
		printf '%s\n%s\n' "$g_zxfer_adjusted_set_list" "$g_zxfer_adjusted_inherit_list"
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

# The adjustment leaves the lists alone for an empty plan, a pool root or a
# missing parent (none of which the transfer passes), throws when the parent
# probe fails, returns the parent read status for the caller to report, and
# fails closed when awk cannot run.
test_adjust_child_inherit_to_match_parent_leaves_lists_alone_or_fails_closed_without_a_parent() {
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
# CALLER SHELL STATE
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
	zxfer_plan_property_changes "compression=lz4=local,atime=off=local" "" 1 filesystem "" "" "" \
		"compression=off=local"
	{
		IFS= read -r l_unused_override
		IFS= read -r l_unused_creation
		IFS= read -r l_unused_dest
		IFS= read -r l_initial_set
	} <<EOF
$g_zxfer_property_plan_result
EOF
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
