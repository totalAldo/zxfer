#!/bin/sh
#
# Shared helpers for zxfer shunit2 test suites.
#

if [ -n "${TESTS_DIR:-}" ]; then
	ZXFER_ROOT=$TESTS_DIR/..
else
	case "$0" in
	/*)
		ZXFER_ROOT=$(dirname "$0")/..
		;;
	*)
		ZXFER_ROOT=${PWD:-.}/$(dirname "$0")/..
		;;
	esac
fi
SHUNIT2_BIN="$ZXFER_ROOT/tests/shunit2/shunit2"

if [ ! -r "$SHUNIT2_BIN" ]; then
	echo "Missing shunit2 dependency at $SHUNIT2_BIN" >&2
	exit 1
fi

# shellcheck source=src/zxfer_common.sh
. "$ZXFER_ROOT/src/zxfer_common.sh"

zxfer_source_runtime_modules_through() {
	l_last_module=$1
	l_root=${2:-$ZXFER_ROOT}

	# shellcheck source=src/zxfer_globals.sh
	. "$l_root/src/zxfer_globals.sh"
	[ "$l_last_module" = "zxfer_globals.sh" ] && return 0

	# shellcheck source=src/zxfer_secure_paths.sh
	. "$l_root/src/zxfer_secure_paths.sh"
	[ "$l_last_module" = "zxfer_secure_paths.sh" ] && return 0

	# shellcheck source=src/zxfer_remote_cli.sh
	. "$l_root/src/zxfer_remote_cli.sh"
	[ "$l_last_module" = "zxfer_remote_cli.sh" ] && return 0

	# shellcheck source=src/zxfer_backup_metadata.sh
	. "$l_root/src/zxfer_backup_metadata.sh"
	[ "$l_last_module" = "zxfer_backup_metadata.sh" ] && return 0

	# shellcheck source=src/zxfer_property_cache.sh
	. "$l_root/src/zxfer_property_cache.sh"
	[ "$l_last_module" = "zxfer_property_cache.sh" ] && return 0

	# shellcheck source=src/zxfer_transfer_properties.sh
	. "$l_root/src/zxfer_transfer_properties.sh"
	[ "$l_last_module" = "zxfer_transfer_properties.sh" ] && return 0

	# shellcheck source=src/zxfer_zfs_mode.sh
	. "$l_root/src/zxfer_zfs_mode.sh"
	[ "$l_last_module" = "zxfer_zfs_mode.sh" ] && return 0

	# shellcheck source=src/zxfer_get_zfs_list.sh
	. "$l_root/src/zxfer_get_zfs_list.sh"
	[ "$l_last_module" = "zxfer_get_zfs_list.sh" ] && return 0

	# shellcheck source=src/zxfer_zfs_send_receive.sh
	. "$l_root/src/zxfer_zfs_send_receive.sh"
	[ "$l_last_module" = "zxfer_zfs_send_receive.sh" ] && return 0

	# shellcheck source=src/zxfer_inspect_delete_snap.sh
	. "$l_root/src/zxfer_inspect_delete_snap.sh"
	[ "$l_last_module" = "zxfer_inspect_delete_snap.sh" ] && return 0

	printf 'Unknown zxfer module for test runtime load: %s\n' "$l_last_module" >&2
	return 1
}

# Provide sane defaults for globals that zxfer_common expects.
: "${g_option_n_dryrun:=0}"
: "${g_option_v_verbose:=0}"
: "${g_option_V_very_verbose:=0}"
: "${g_option_b_beep_always:=0}"
: "${g_option_B_beep_on_success:=0}"
