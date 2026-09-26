#!/bin/sh
# Tests for src/zxfer_property_*.sh, run by tests/test_zxfer_exec.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

fake_property_set_runner() {
	FAKE_SET_CALLS="${FAKE_SET_CALLS}${1}@${2};"
}

fake_property_inherit_runner() {
	FAKE_INHERIT_CALLS="${FAKE_INHERIT_CALLS}${1}@${2};"
}

sort_property_list() {
	l_list=$1
	echo "$l_list" | tr ',' '\n' | sort | tr '\n' ',' | sed 's/,$//'
}

test_derive_override_lists_handles_overrides_only() {
	zxfer_derive_override_lists "compression=lz4=local" "compression=lzjb" 0 "filesystem"

	assertEquals "Override list should reflect -o values with override sources." "compression=lzjb=override" "$g_zxfer_override_pvs_result"
	assertEquals "Creation list should keep explicit overrides for source-local properties." "compression=lzjb=override" "$g_zxfer_creation_pvs_result"
}

test_derive_override_lists_includes_local_props_for_creation() {
	source_pvs="compression=lz4=local,refreservation=4G=received,quota=none=local"
	override_opts="quota=8G"
	zxfer_derive_override_lists "$source_pvs" "$override_opts" 1 "volume"

	expected_override="compression=lz4=local,quota=8G=override,refreservation=4G=received"
	assertEquals "Overrides should include source properties with user overrides applied." "$(sort_property_list "$expected_override")" "$(sort_property_list "$g_zxfer_override_pvs_result")"
	expected_creation="compression=lz4=local,quota=8G=override,refreservation=4G=received"
	assertEquals "Creation list should keep local props, explicit local overrides, and zvol refreservation even if not local." "$(sort_property_list "$expected_creation")" "$(sort_property_list "$g_zxfer_creation_pvs_result")"
}

test_diff_properties_separates_set_and_inherit_lists() {
	override_pvs="compression=lz4=local,atime=off=received"
	dest_pvs="compression=lzjb=local,atime=on=local"
	zxfer_diff_properties "$override_pvs" "$dest_pvs" "casesensitivity,normalization,jailed,utf8only"

	assertEquals "Initial pass should require setting every diverging property." "compression=lz4,atime=off" "$g_zxfer_diff_initial_set_result"
	assertEquals "Child dataset should only set properties sourced locally on the parent." "compression=lz4" "$g_zxfer_diff_child_set_result"
	assertEquals "Child dataset should inherit properties whose source is not local." "atime=off" "$g_zxfer_diff_inherit_result"
}

test_apply_property_changes_skips_inherit_for_initial_source() {
	result=$(
		(
			zxfer_run_zfs_set_properties() { fake_property_set_runner "$@"; }
			zxfer_run_destination_property_verb() { fake_property_inherit_runner "$4" "$3"; }
			FAKE_SET_CALLS=""
			FAKE_INHERIT_CALLS=""
			zxfer_apply_property_changes "pool/src" 1 "compression=lz4" "" ""
			printf 'set=%s inherit=%s\n' "$FAKE_SET_CALLS" "$FAKE_INHERIT_CALLS"
		)
	)

	assertEquals "Initial source should call the set runner once with the full initial diff list and never inherit." \
		"set=compression=lz4@pool/src; inherit=" "$result"
}

test_apply_property_changes_invokes_inherit_runner_for_children() {
	result=$(
		(
			zxfer_run_zfs_set_properties() { fake_property_set_runner "$@"; }
			zxfer_run_destination_property_verb() { fake_property_inherit_runner "$4" "$3"; }
			FAKE_SET_CALLS=""
			FAKE_INHERIT_CALLS=""
			zxfer_apply_property_changes "pool/src" 0 "" "compression=lz4" "atime=off"
			printf 'set=%s inherit=%s\n' "$FAKE_SET_CALLS" "$FAKE_INHERIT_CALLS"
		)
	)

	assertEquals "Child dataset should apply the full child set list in one runner call and inherit the requested properties." \
		"set=compression=lz4@pool/src; inherit=atime@pool/src;" "$result"
}

test_sanitize_property_list_drops_requested_entries() {
	l_oldifs=$IFS
	IFS=","
	zxfer_sanitize_property_list "compression=lz4=local,atime=off=local" "" "atime"
	IFS=$l_oldifs
	assertEquals "compression=lz4=local" "$g_zxfer_sanitized_property_list_result"
}

test_parse_property_views_prefers_human_none_values() {
	printf 'compression\natime\n' >"$TEST_TMPDIR/views.skeleton"
	printf 'compression\tlz4\tlocal\natime\ton\tlocal\n' >"$TEST_TMPDIR/views.machine"
	printf 'compression\tlz4\tlocal\natime\tnone\tlocal\n' >"$TEST_TMPDIR/views.human"
	result=$(zxfer_parse_property_views merge "$TEST_TMPDIR/views.skeleton" \
		"$TEST_TMPDIR/views.machine" "$TEST_TMPDIR/views.human")
	assertEquals "compression=lz4=local,atime=none=local" "$result"
}

test_derive_override_lists_with_transfer_all_preserves_sources() {
	zxfer_derive_override_lists "compression=lz4=local,atime=off=local" "compression=lz4" 1 filesystem
	assertEquals "compression=lz4=override,atime=off=local" "$g_zxfer_override_pvs_result"
	assertEquals "compression=lz4=override,atime=off=local" "$g_zxfer_creation_pvs_result"
}

test_derive_override_lists_without_transfer_all_uses_overrides_only() {
	zxfer_derive_override_lists "compression=lz4=local" "atime=off" 0 filesystem
	assertEquals "atime=off=override" "$g_zxfer_override_pvs_result"
	assertEquals "atime=off=override" "$g_zxfer_creation_pvs_result"
}

test_sanitize_property_list_removes_readonly_and_ignored_sets() {
	zxfer_sanitize_property_list "compression=lz4=local,atime=off=local" "compression" "atime"
	assertEquals "" "$g_zxfer_sanitized_property_list_result"
}

test_diff_properties_returns_expected_set_and_inherit_lists() {
	zxfer_diff_properties "compression=lz4=local,atime=off=received" "compression=lz4=local,atime=on=local" ""
	assertEquals "atime=off" "$g_zxfer_diff_initial_set_result"
	assertEquals "" "$g_zxfer_diff_child_set_result"
	assertEquals "atime=off" "$g_zxfer_diff_inherit_result"
}

test_apply_property_changes_uses_initial_set_list_for_root_dataset() {
	log="$TEST_TMPDIR/property_apply_initial.log"
	: >"$log"
	(
		zxfer_run_zfs_set_properties() { printf 'set %s %s\n' "$1" "$2" >>"$log"; }
		zxfer_run_destination_property_verb() { printf 'inherit %s %s\n' "$4" "$3" >>"$log"; }
		zxfer_apply_property_changes "tank/dst" 1 "compression=lz4,atime=off" "copies=2" "checksum"
	)
	result=$(cat "$log")
	expected="set compression=lz4,atime=off tank/dst"
	assertEquals "$expected" "$result"
	rm -f "$log"
}

test_apply_property_changes_sets_and_inherits_on_children() {
	log="$TEST_TMPDIR/property_apply_child.log"
	: >"$log"
	(
		zxfer_run_zfs_set_properties() { printf 'set %s %s\n' "$1" "$2" >>"$log"; }
		zxfer_run_destination_property_verb() { printf 'inherit %s %s\n' "$4" "$3" >>"$log"; }
		zxfer_apply_property_changes "tank/dst/child" 0 "compression=lz4" "atime=off" "encryption"
	)
	result=$(cat "$log")
	expected="set atime=off tank/dst/child
inherit encryption tank/dst/child"
	assertEquals "$expected" "$result"
	rm -f "$log"
}
