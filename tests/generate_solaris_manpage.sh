#!/bin/sh
#
# Render the Solaris/illumos section 1M page from the canonical section 8 page.
#

set -eu

ZXFER_ROOT=$(cd "$(dirname "$0")/.." && pwd)
SOURCE_FILE=${ZXFER_MANPAGE_SOURCE:-"$ZXFER_ROOT/man/zxfer.8"}
DESTINATION_FILE=${ZXFER_MANPAGE_DESTINATION:-"$ZXFER_ROOT/man/zxfer.1m"}
MODE=check

print_usage() {
	cat <<'EOF'
Usage: tests/generate_solaris_manpage.sh [--check | --stdout | --write]

Render man/zxfer.1m deterministically from canonical man/zxfer.8.

  --check   fail when the checked-in Solaris/illumos page is stale (default)
  --stdout  print the generated page
  --write   replace man/zxfer.1m with the generated page
EOF
}

case "${1:-}" in
'') ;;
--check)
	MODE=check
	;;
--stdout)
	MODE=stdout
	;;
--write)
	MODE='write'
	;;
-h | --help)
	print_usage
	exit 0
	;;
*)
	printf 'Unknown argument: %s\n' "$1" >&2
	exit 1
	;;
esac
[ "$#" -le 1 ] || {
	print_usage >&2
	exit 1
}

[ -r "$SOURCE_FILE" ] || {
	printf 'Canonical man page is not readable: %s\n' "$SOURCE_FILE" >&2
	exit 1
}

render_solaris_manpage() {
	awk '
		NR == 1 {
			print ".\\\" Generated from man/zxfer.8 by tests/generate_solaris_manpage.sh; do not edit."
			next
		}
		/^\.TH / {
			field_count = split($0, quoted_fields, "\\\"")
			if (field_count < 7 || quoted_fields[6] == "") {
				print "Unable to parse canonical .TH date: " $0 > "/dev/stderr"
				exit 2
			}
			printf ".TH \"ZXFER\" \"1M\" \"%s\" \"zxfer\" \"System Administration Commands\"\n", quoted_fields[6]
			next
		}
		{ print }
	' "$SOURCE_FILE"
}

case "$MODE" in
stdout)
	render_solaris_manpage
	;;
check | write)
	TEMP_FILE=$(mktemp "${TMPDIR:-/tmp}/zxfer.manpage.XXXXXX")
	trap 'rm -f "$TEMP_FILE"' EXIT HUP INT TERM
	render_solaris_manpage >"$TEMP_FILE"
	if [ "$MODE" = check ]; then
		if ! cmp -s "$TEMP_FILE" "$DESTINATION_FILE"; then
			printf '%s\n' "Generated Solaris/illumos man page is stale." >&2
			printf '%s\n' "Run: ./tests/generate_solaris_manpage.sh --write" >&2
			exit 1
		fi
	else
		chmod 0644 "$TEMP_FILE"
		mv "$TEMP_FILE" "$DESTINATION_FILE"
		trap - EXIT HUP INT TERM
	fi
	;;
esac
