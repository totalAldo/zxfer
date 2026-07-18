#!/bin/sh
# Shared property-backup metadata fixture renderers.
# shellcheck disable=SC2154,SC2317,SC2329

zxfer_test_render_current_backup_metadata_contents() {
	l_format_version=${ZXFER_BACKUP_METADATA_FORMAT_VERSION:-2}
	l_header_line=${ZXFER_BACKUP_METADATA_HEADER_LINE:-#zxfer property backup file}
	l_source_root=${ZXFER_TEST_BACKUP_SOURCE_ROOT:-}
	l_destination_root=${ZXFER_TEST_BACKUP_DESTINATION_ROOT:-}
	l_first_row=""

	for l_line in "$@"; do
		[ -n "$l_line" ] || continue
		l_first_row=$l_line
		break
	done

	if [ -n "$l_first_row" ]; then
		if [ -z "$l_source_root" ]; then
			case "$l_first_row" in
			*,*)
				l_source_root=${l_first_row%%,*}
				;;
			*)
				l_source_root=${g_initial_source:-}
				;;
			esac
		fi
		if [ -z "$l_destination_root" ]; then
			case "$l_first_row" in
			*,*)
				l_row_remainder=${l_first_row#*,}
				if [ "$l_row_remainder" != "$l_first_row" ]; then
					l_destination_root=${l_row_remainder%%,*}
				fi
				;;
			*)
				l_destination_root=${g_destination:-}
				;;
			esac
		fi
	fi

	if [ -z "$l_source_root" ]; then
		l_source_root=${g_initial_source:-}
	fi
	if [ -z "$l_destination_root" ]; then
		l_destination_root=${g_destination:-}
	fi

	printf '%s\n' "$l_header_line"
	printf '%s\n' "#format_version:$l_format_version"
	printf '%s\n' "#version:test-version"
	if [ -n "$l_source_root" ]; then
		printf '%s\n' "#source_root:$l_source_root"
	fi
	if [ -n "$l_destination_root" ]; then
		printf '%s\n' "#destination_root:$l_destination_root"
	fi

	for l_line in "$@"; do
		[ -n "$l_line" ] || continue
		printf '%s\n' "$l_line"
	done
}

zxfer_test_backup_metadata_row() {
	l_relative_path=$1
	l_properties=$2

	printf '%s\t%s\n' "$l_relative_path" "$l_properties"
}
