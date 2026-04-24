#!/bin/sh
#
# Run the guest-local integration harness from a vmactions VM step while
# preserving the harness status for a later host-side failure check.
#

set -u

l_guest_shell=${1:-/bin/sh}
l_tmpdir=${TMPDIR:-}
l_status_file=
l_prepare_file=
l_status=0

if [ -z "$l_tmpdir" ]; then
	printf '%s\n' "ERROR: TMPDIR must point at the vmactions artifact directory." >&2
	exit 1
fi

l_status_file=$l_tmpdir/harness.exit-status
l_prepare_file=$l_tmpdir/prepare.sh

mkdir -p "$l_tmpdir" || {
	printf '%s\n' "ERROR: Unable to create artifact directory: $l_tmpdir" >&2
	exit 1
}
rm -f "$l_status_file" "$l_prepare_file" || {
	printf '%s\n' "ERROR: Unable to clear stale harness status file: $l_status_file" >&2
	exit 1
}

if [ -n "${ZXFER_CI_GUEST_PREPARE:-}" ]; then
	{
		printf '%s\n' "#!/bin/sh"
		printf '%s\n' "set -eu"
		printf '%s\n' "$ZXFER_CI_GUEST_PREPARE"
	} >"$l_prepare_file" || {
		printf '%s\n' "ERROR: Unable to write guest preparation script: $l_prepare_file" >&2
		exit 1
	}
	chmod 700 "$l_prepare_file" || {
		printf '%s\n' "ERROR: Unable to chmod guest preparation script: $l_prepare_file" >&2
		exit 1
	}
	"$l_guest_shell" "$l_prepare_file" || l_status=$?
fi

if [ "$l_status" -eq 0 ]; then
	"$l_guest_shell" ./tests/run_integration_zxfer.sh --yes --keep-going || l_status=$?
fi

printf '%s\n' "$l_status" >"$l_status_file" || {
	printf '%s\n' "ERROR: Unable to write harness status file: $l_status_file" >&2
	exit 1
}
printf 'zxfer integration guest exit status: %s\n' "$l_status"

# Return success so vmactions reaches its copyback phase. A host-side workflow
# step reads harness.exit-status and restores the real guest result.
exit 0
