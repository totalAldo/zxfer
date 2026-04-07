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

# Provide sane defaults for globals that zxfer_common expects.
: "${g_option_n_dryrun:=0}"
: "${g_option_v_verbose:=0}"
: "${g_option_V_very_verbose:=0}"
: "${g_option_b_beep_always:=0}"
: "${g_option_B_beep_on_success:=0}"
