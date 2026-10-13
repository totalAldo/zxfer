#!/bin/sh
# The unit fixture shared by the property-state and property-transfer
# entries: reset owner state, a trimmed readonly list, the tank/src ->
# backup/dst roots and a run root, plus small property helpers.
# shellcheck disable=SC2034,SC2154,SC2317,SC2329

# Purpose: Store LIST as the newest row of one side's property table, as a
# live read does: a row file in the row store and an index line ahead of the
# older ones.
# Usage: zxfer_property_test_table_add source|destination DATASET LIST; the
# suite's setUp must have allocated a run root.
zxfer_property_test_table_add() {
	zxfer_prepare_property_read_files || return 1
	zxfer_store_property_row "$3" || return 1
	l_table_line=$ZXFER_LF$g_zxfer_property_row_result$ZXFER_TAB$2
	case $1 in
	source)
		g_zxfer_source_property_table=$l_table_line${g_zxfer_source_property_table:-$ZXFER_LF}
		;;
	destination)
		g_zxfer_destination_property_table=$l_table_line${g_zxfer_destination_property_table:-$ZXFER_LF}
		;;
	esac
}

# Purpose: Print one side's table as "dataset<TAB>list" lines, newest first,
# with "-" for a tombstone or a row that cannot be read.
# Usage: zxfer_property_test_table_dump source|destination
zxfer_property_test_table_dump() {
	case $1 in
	source) l_dump_index=${g_zxfer_source_property_table:-} ;;
	*) l_dump_index=${g_zxfer_destination_property_table:-} ;;
	esac
	printf '%s' "$l_dump_index" | while IFS=$ZXFER_TAB read -r l_dump_row l_dump_dataset; do
		[ -n "$l_dump_row" ] || continue
		if zxfer_read_property_row "$l_dump_row"; then
			printf '%s\t%s\n' "$l_dump_dataset" "$g_zxfer_property_row_payload_result"
		else
			printf '%s\t-\n' "$l_dump_dataset"
		fi
	done
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

# Purpose: Prepare policy for a planner case, preserving the fixture's policy.
# Usage: zxfer_property_test_plan IS_INITIAL SOURCE OVERRIDE TRANSFER TYPE
# READONLY IGNORE UNSUPPORTED [DESTINATION]; an explicit empty destination exists.
zxfer_property_test_plan() {
	l_test_plan_override=$g_zxfer_property_override_policy
	l_test_plan_transfer=$g_option_P_transfer_property
	l_test_plan_readonly=$g_zxfer_property_readonly_policy
	l_test_plan_ignore=$g_option_I_ignore_properties
	g_zxfer_property_override_policy=$3
	g_option_P_transfer_property=$4
	g_zxfer_property_readonly_policy=$6
	g_option_I_ignore_properties=$7
	l_test_plan_exists=0
	[ "$#" -lt 9 ] || l_test_plan_exists=1
	l_test_plan_status=0
	zxfer_plan_property_changes "$1" "$l_test_plan_exists" "$5" "$2" "${9:-}" "$8" || l_test_plan_status=$?
	g_zxfer_property_override_policy=$l_test_plan_override
	g_option_P_transfer_property=$l_test_plan_transfer
	g_zxfer_property_readonly_policy=$l_test_plan_readonly
	g_option_I_ignore_properties=$l_test_plan_ignore
	return "$l_test_plan_status"
}

# Purpose: Stage a small action fixture, independently of the policy planner.
# Usage: zxfer_property_test_write_actions VERB SET_OR_CREATE [INHERIT] [SOURCE_PVS].
# SOURCE_PVS supplies first-value provenance for parent reconciliation cases.
zxfer_property_test_write_actions() {
	zxfer_stage_property_inputs "actions$ZXFER_TAB$2" "inherits$ZXFER_TAB${3:-}" "source$ZXFER_TAB${4:-}" || return
	zxfer_ensure_runtime_artifact_file "$g_zxfer_property_plan_file" property-test-plan || return
	g_zxfer_property_plan_file=$g_zxfer_runtime_artifact_path_result
	# shellcheck disable=SC2016  # Fixture AWK uses literal fields.
	ZXFER_PROPERTY_INPUT_FILE=$g_zxfer_property_input_file awk -v verb="$1" \
		"$ZXFER_PROPERTY_INPUT_AWK$ZXFER_PROPERTY_AWK_LIB"'
function rows(list, action, count, items, i, fields, source, desired) {
	count = split(list, items, ",")
	for (i = 1; i <= count; i++) {
		if (items[i] == "") continue
		split(items[i], fields, "=")
		source = (fields[1] in sources) ? sources[fields[1]] : (fields[3] != "" ? fields[3] : "local")
		desired = (fields[1] in values) ? values[fields[1]] : fields[2]
		print action "\t" fields[1] "=" fields[2] "=" source "=" desired
	}
}
BEGIN {
	first_values(property_input["source"], values, sources)
	rows(property_input["actions"], verb)
	rows(property_input["inherits"], "inherit")
	print "__ZXFER_PROPERTY_ACTIONS__"
}' >|"$g_zxfer_property_plan_file" || return
	zxfer_validate_property_plan "$g_zxfer_property_plan_file"
}

# Purpose: Retain the existing policy-case expectations with the new file API.
# Usage: zxfer_property_test_filter_child_creation CREATION PARENT READONLY.
zxfer_property_test_filter_child_creation() {
	zxfer_property_test_write_actions create "$1" || return
	zxfer_resolve_parent_property_plan "$g_zxfer_property_plan_file" "$2" "$3" create || return
	awk -F '\t' '$1 == "create" { split($2, f, "="); printf "%s%s=%s=%s", sep, f[1], f[2], f[3]; sep="," } END { print "" }' \
		"$g_zxfer_property_parent_plan_file"
}

# Purpose: Preserve create fixtures while exercising the retained operation.
# Usage: zxfer_property_test_create INITIAL OVERRIDE CREATION TYPE SIZE DEST READONLY.
zxfer_property_test_create() {
	if [ "$1" -eq 1 ]; then
		zxfer_property_test_write_actions create "$2" || return
	else
		zxfer_property_test_write_actions create "$3" || return
	fi
	zxfer_create_destination_dataset "$g_zxfer_property_plan_file" "$1" "$4" "$5" "$6" "$7"
}

# Purpose: Exercise direct command construction guards and dry-run rendering.
# Usage: zxfer_property_test_run_create yes|no TYPE SIZE LIST DEST.
zxfer_property_test_run_create() {
	zxfer_property_test_write_actions create "$4" || return
	zxfer_execute_property_plan create "$g_zxfer_property_plan_file" "$2" "$3" "$5" "$1"
}
