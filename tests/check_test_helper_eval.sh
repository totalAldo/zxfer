#!/bin/sh
# Reject new eval commands in the shared test-helper layer.

set -eu

ZXFER_ROOT=${1:-$(cd "$(dirname "$0")/.." && pwd)}
l_policy_file=$ZXFER_ROOT/tests/test_helper_eval_policy.tsv

if [ ! -r "$l_policy_file" ]; then
	printf 'Missing test-helper eval policy: %s\n' "$l_policy_file" >&2
	exit 1
fi

# Shared helper files live at the compatibility entry or under tests/helpers.
# Keep top-level *_helper.sh files in scope so future shared scaffolding cannot
# bypass the rule by living beside test_helper.sh.
set --
for l_helper_file in \
	"$ZXFER_ROOT"/tests/*_helper.sh \
	"$ZXFER_ROOT"/tests/helpers/*.sh; do
	[ -f "$l_helper_file" ] || continue
	set -- "$@" "$l_helper_file"
done

if [ "$#" -eq 0 ]; then
	printf 'No shared test-helper files found under %s/tests.\n' "$ZXFER_ROOT" >&2
	exit 1
fi

awk -v l_policy_file="$l_policy_file" -v l_root_prefix="$ZXFER_ROOT/" '
	function report(message) {
		print "test-helper eval policy: " message
		failures++
	}

	FILENAME == l_policy_file {
		if ($0 ~ /^[[:space:]]*($|#)/) {
			next
		}
		if (NF < 2 || $1 == "" || $2 == "") {
			report("malformed inventory row " FNR " in " l_policy_file)
			next
		}
		key = $1 SUBSEP $2
		if (key in allowed) {
			report("duplicate inventory entry for " $1 ":" $2)
			next
		}
		allowed[key] = 1
		next
	}

	FNR == 1 {
		current_function = ""
	}

	{
		relative_path = FILENAME
		if (index(relative_path, l_root_prefix) == 1) {
			relative_path = substr(relative_path, length(l_root_prefix) + 1)
		}

		if ($0 ~ /^[A-Za-z_][A-Za-z0-9_]*\(\)[[:space:]]*\{/) {
			current_function = $0
			sub(/\(.*/, "", current_function)
		}

		trimmed = $0
		sub(/^[[:space:]]*/, "", trimmed)
		if (trimmed ~ /^#/) {
			next
		}
		if ($0 !~ /(^|[^[:alnum:]_])eval([^[:alnum:]_]|$)/) {
			next
		}

		key = relative_path SUBSEP current_function
		if (!(key in allowed)) {
			report(relative_path ":" FNR " has an uninventoried eval command in " current_function)
		} else {
			seen[key]++
		}
	}

	END {
		for (key in allowed) {
			if (!(key in seen)) {
				split(key, parts, SUBSEP)
				report("inventoried eval capture was not found: " parts[1] ":" parts[2])
			} else if (seen[key] != 1) {
				split(key, parts, SUBSEP)
				report("inventoried eval capture appeared " seen[key] " times: " parts[1] ":" parts[2])
			}
		}
		exit failures != 0
	}
' "$l_policy_file" "$@"
