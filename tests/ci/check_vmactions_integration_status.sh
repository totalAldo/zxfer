#!/bin/sh
#
# Restore a guest-local vmactions integration harness status after copyback.
#

set -eu

l_artifact_dir=${1:-}
l_label=${2:-Guest}
l_status_file=
l_status=

if [ -z "$l_artifact_dir" ]; then
	printf '%s\n' "::error title=$l_label integration status missing::Artifact directory argument is required."
	exit 1
fi

l_status_file=$l_artifact_dir/harness.exit-status
if [ ! -s "$l_status_file" ]; then
	printf '%s\n' "::error title=$l_label integration status missing::No harness status file was copied back from $l_status_file."
	exit 1
fi

l_status=$(sed -n '1p' "$l_status_file" | tr -d ' 	\r\n')
case "$l_status" in
'' | *[!0-9]*)
	printf '%s\n' "::error title=$l_label integration status invalid::Invalid harness status [$l_status] in $l_status_file."
	exit 1
	;;
0)
	printf '%s\n' "$l_label integration harness passed."
	exit 0
	;;
*)
	printf '%s\n' "::error title=$l_label integration failed::$l_label integration failed inside the VM with exit status $l_status. See the uploaded integration artifact for harness logs."
	exit "$l_status"
	;;
esac
