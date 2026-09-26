#!/bin/sh
# The unit fixture shared by the property-state and property-transfer
# entries: reset owner state, a trimmed readonly list, the tank/src ->
# backup/dst roots and a run root, plus small property helpers.
# shellcheck disable=SC2034,SC2317,SC2329

# Purpose: Append one "dataset<TAB>list" row to one side's property table.
# Usage: zxfer_property_test_table_add source|destination DATASET LIST
zxfer_property_test_table_add() {
	l_table_row=$2$ZXFER_TAB$3
	case $1 in
	source)
		g_zxfer_source_property_table=${g_zxfer_source_property_table:+$g_zxfer_source_property_table$ZXFER_LF}$l_table_row
		;;
	destination)
		g_zxfer_destination_property_table=${g_zxfer_destination_property_table:+$g_zxfer_destination_property_table$ZXFER_LF}$l_table_row
		;;
	esac
}

# Purpose: Print how many control bytes other than LF (C0 or DEL) TEXT holds.
# Usage: zxfer_property_test_count_control_bytes TEXT
zxfer_property_test_count_control_bytes() {
	printf '%s' "$1" | LC_ALL=C tr -dc '\001-\011\013-\037\177' | wc -c | tr -d ' '
}

# Purpose: Print LABEL_globbing=enabled or =disabled for the current shell.
# Usage: zxfer_property_test_report_globbing_state LABEL
zxfer_property_test_report_globbing_state() {
	l_globbing_label=$1
	case $- in
	*f*) printf '%s_globbing=disabled\n' "$l_globbing_label" ;;
	*) printf '%s_globbing=enabled\n' "$l_globbing_label" ;;
	esac
}

# Purpose: Record the host ssh path once TEST_TMPDIR exists.
# Usage: zxfer_test_property_fixture_one_time_setup, from oneTimeSetUp.
zxfer_test_property_fixture_one_time_setup() {
	TEST_SSH_PATH=$(command -v ssh 2>/dev/null || printf '%s\n' ssh)
}

# Purpose: Reset owner state and the property roots before a case.
# Usage: zxfer_test_property_fixture_setup || return, from setUp.
zxfer_test_property_fixture_setup() {
	zxfer_test_reset_all_owner_state || return
	unset FAKE_REMOTE_PATH FAKE_SSH_LOG ZXFER_REMOTE_ZFS_LOG
	ZXFER_BASE_READONLY_PROPERTIES="readonly,mountpoint"
	ZXFER_FREEBSD_READONLY_PROPERTIES="aclmode"
	g_cmd_zfs="/sbin/zfs"
	g_cmd_ssh=$TEST_SSH_PATH
	g_initial_source="tank/src"
	g_destination="backup/dst"
	g_actual_dest="backup/dst"
	zxfer_test_allocate_runtime_root "$TEST_TMPDIR"
}
