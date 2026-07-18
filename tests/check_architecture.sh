#!/bin/sh
#
# Check zxfer's canonical manifest, module graph, and internal call boundaries.
# This check is offline and sources only the deliberately pure loader manifest.
#

set -eu

if [ -n "${ZXFER_ARCHITECTURE_ROOT:-}" ]; then
	ZXFER_ROOT=$(cd "$ZXFER_ARCHITECTURE_ROOT" && pwd)
else
	ZXFER_ROOT=$(cd "$(dirname "$0")/.." && pwd)
fi
POLICY_FILE=$ZXFER_ROOT/tests/architecture_policy.tsv
EVAL_POLICY_FILE=$ZXFER_ROOT/tests/architecture_eval_policy.tsv
EVAL_SITE_AWK=$ZXFER_ROOT/tests/extract_eval_sites.awk
GLOBAL_WRITER_AWK=$ZXFER_ROOT/tests/extract_global_writes.awk
FUNCTION_SCRATCH_AWK=$ZXFER_ROOT/tests/extract_function_scratch.awk
FUNCTION_SCRATCH_BASELINE=$ZXFER_ROOT/tests/function_scratch_baseline.tsv
TAB=$(printf '\t')
VIOLATIONS=0

architecture_tmp=$(mktemp -d "${TMPDIR:-/tmp}/zxfer-architecture.XXXXXX") || exit 1
trap 'rm -rf "$architecture_tmp"' 0 HUP INT TERM

report_architecture_violation() {
	printf 'architecture: %s\n' "$*" >&2
	VIOLATIONS=$((VIOLATIONS + 1))
}

[ -r "$POLICY_FILE" ] || {
	echo "Missing architecture policy: $POLICY_FILE" >&2
	exit 1
}
[ -r "$EVAL_POLICY_FILE" ] || {
	echo "Missing production eval policy: $EVAL_POLICY_FILE" >&2
	exit 1
}
[ -r "$EVAL_SITE_AWK" ] || {
	echo "Missing eval-site extractor: $EVAL_SITE_AWK" >&2
	exit 1
}
[ -r "$GLOBAL_WRITER_AWK" ] || {
	echo "Missing global-writer extractor: $GLOBAL_WRITER_AWK" >&2
	exit 1
}
[ -r "$FUNCTION_SCRATCH_AWK" ] || {
	echo "Missing function-scratch extractor: $FUNCTION_SCRATCH_AWK" >&2
	exit 1
}
[ -r "$FUNCTION_SCRATCH_BASELINE" ] || {
	echo "Missing function-scratch baseline: $FUNCTION_SCRATCH_BASELINE" >&2
	exit 1
}

ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT
export ZXFER_SOURCE_MODULES_ROOT
# shellcheck source=src/zxfer_modules.sh
. "$ZXFER_ROOT/src/zxfer_modules.sh"

printf '%s\n' "$ZXFER_SOURCE_MODULE_MANIFEST" | awk 'NF { print "src/" $0 }' >"$architecture_tmp/manifest"
sort "$architecture_tmp/manifest" >"$architecture_tmp/manifest-sorted"

# Manifest names must be unique and resolve to real files.
l_duplicate_modules=$(sort "$architecture_tmp/manifest" | uniq -d)
if [ -n "$l_duplicate_modules" ]; then
	report_architecture_violation "duplicate manifest entries: $(printf '%s' "$l_duplicate_modules" | tr '\n' ' ')"
fi
while IFS= read -r l_module; do
	[ -f "$ZXFER_ROOT/$l_module" ] || report_architecture_violation "manifest entry does not exist: $l_module"
done <"$architecture_tmp/manifest"

awk -F "$TAB" '$1 == "standalone" { print $2 }' "$POLICY_FILE" >"$architecture_tmp/standalone"
(
	cd "$ZXFER_ROOT"
	for l_module in src/zxfer_*.sh; do
		[ "$l_module" != src/zxfer_modules.sh ] || continue
		printf '%s\n' "$l_module"
	done
) | sort >"$architecture_tmp/source-modules"
sort "$architecture_tmp/manifest" "$architecture_tmp/standalone" | uniq >"$architecture_tmp/declared-modules"

l_missing_manifest=$(comm -23 "$architecture_tmp/source-modules" "$architecture_tmp/declared-modules")
if [ -n "$l_missing_manifest" ]; then
	report_architecture_violation "production modules missing from manifest/policy: $(printf '%s' "$l_missing_manifest" | tr '\n' ' ')"
fi
l_unknown_declared=$(comm -13 "$architecture_tmp/source-modules" "$architecture_tmp/declared-modules")
if [ -n "$l_unknown_declared" ]; then
	report_architecture_violation "manifest/policy names unknown production modules: $(printf '%s' "$l_unknown_declared" | tr '\n' ' ')"
fi

# Dynamic shell evaluation can conceal indirect writes and command execution
# from token-based ownership checks. Inventory every remaining literal eval by
# module, owning function, and exact trimmed source line. This makes any added,
# moved, changed, or removed site an explicit architecture-policy decision.
while IFS= read -r l_module; do
	awk -v module="$l_module" -f "$EVAL_SITE_AWK" "$ZXFER_ROOT/$l_module"
done <"$architecture_tmp/source-modules" >"$architecture_tmp/eval-sites"
awk -v module="zxfer" -f "$EVAL_SITE_AWK" "$ZXFER_ROOT/zxfer" \
	>>"$architecture_tmp/eval-sites"
cut -f1-3 "$architecture_tmp/eval-sites" | sort >"$architecture_tmp/eval-site-tuples"
awk -F "$TAB" '
	$0 !~ /^[[:space:]]*#/ && $0 != "" &&
		(NF < 4 || $1 == "" || $2 == "" || $3 == "" || $4 == "") {
		printf "line %d must contain module, function, exact source line, and purpose: %s\n", FNR, $0
	}
' "$EVAL_POLICY_FILE" >"$architecture_tmp/eval-policy-invalid"
if [ -s "$architecture_tmp/eval-policy-invalid" ]; then
	while IFS= read -r l_violation; do
		report_architecture_violation "invalid production eval policy: $l_violation"
	done <"$architecture_tmp/eval-policy-invalid"
fi
awk -F "$TAB" '
	$0 !~ /^[[:space:]]*#/ && $0 != "" &&
		NF >= 4 && $1 != "" && $2 != "" && $3 != "" && $4 != "" {
		printf "%s\t%s\t%s\n", $1, $2, $3
	}
' "$EVAL_POLICY_FILE" | sort >"$architecture_tmp/eval-policy-tuples"
comm -23 "$architecture_tmp/eval-site-tuples" \
	"$architecture_tmp/eval-policy-tuples" >"$architecture_tmp/eval-sites-uninventoried"
if [ -s "$architecture_tmp/eval-sites-uninventoried" ]; then
	while IFS= read -r l_violation; do
		report_architecture_violation "uninventoried production eval: $l_violation"
	done <"$architecture_tmp/eval-sites-uninventoried"
fi
comm -13 "$architecture_tmp/eval-site-tuples" \
	"$architecture_tmp/eval-policy-tuples" >"$architecture_tmp/eval-sites-stale"
if [ -s "$architecture_tmp/eval-sites-stale" ]; then
	while IFS= read -r l_violation; do
		report_architecture_violation "stale production eval inventory: $l_violation"
	done <"$architecture_tmp/eval-sites-stale"
fi

# Mutable globals have exactly one source-module owner. The extractor records
# every direct assignment/read/unset site; this pass deliberately deduplicates
# repeat writes inside the owner before looking for cross-module writers.
while IFS= read -r l_module; do
	awk -v module="$l_module" -f "$GLOBAL_WRITER_AWK" "$ZXFER_ROOT/$l_module"
done <"$architecture_tmp/source-modules" >"$architecture_tmp/global-writes"
awk -v module="zxfer" -f "$GLOBAL_WRITER_AWK" "$ZXFER_ROOT/zxfer" \
	>"$architecture_tmp/launcher-global-writes"
if [ -s "$architecture_tmp/launcher-global-writes" ]; then
	while IFS= read -r l_launcher_write; do
		report_architecture_violation "launcher writes owner-module mutable global: $l_launcher_write"
	done <"$architecture_tmp/launcher-global-writes"
fi
awk -F "$TAB" '
	!seen[$1 SUBSEP $2]++ {
		writer_count[$1]++
		writers[$1] = writers[$1] (writers[$1] ? ", " : "") $2
	}
	END {
		for (name in writer_count) {
			if (writer_count[name] > 1)
				printf "%s: %s\n", name, writers[name]
		}
	}
' "$architecture_tmp/global-writes" | sort >"$architecture_tmp/global-owner-violations"
if [ -s "$architecture_tmp/global-owner-violations" ]; then
	while IFS= read -r l_violation; do
		report_architecture_violation "mutable global has multiple module writers: $l_violation"
	done <"$architecture_tmp/global-owner-violations"
fi

# Parsed CLI option state is immutable outside the CLI owner. This is stricter
# than single-writer ownership: moving the only g_option_* write to another
# module must not silently redefine the option-state boundary.
awk -F "$TAB" '
	$1 ~ /^g_option_[A-Za-z0-9_]+$/ && $2 != "src/zxfer_cli.sh" {
		printf "%s written by %s:%s (%s)\n", $1, $2, $3, $4
	}
' "$architecture_tmp/global-writes" | sort -u >"$architecture_tmp/option-owner-violations"
if [ -s "$architecture_tmp/option-owner-violations" ]; then
	while IFS= read -r l_violation; do
		report_architecture_violation "parsed option write outside src/zxfer_cli.sh: $l_violation"
	done <"$architecture_tmp/option-owner-violations"
fi

# Build the internal function-owner table only from canonical modules.
while IFS= read -r l_module; do
	awk -v module="$l_module" '
		/^[A-Za-z_][A-Za-z0-9_]*\(\)[[:space:]]*\{/ {
			name = $1
			sub(/\(.*/, "", name)
			printf "%s\t%s\n", name, module
		}
	' "$ZXFER_ROOT/$l_module"
done <"$architecture_tmp/manifest" >"$architecture_tmp/function-owners"

l_duplicate_functions=$(cut -f1 "$architecture_tmp/function-owners" | sort | uniq -d)
if [ -n "$l_duplicate_functions" ]; then
	report_architecture_violation "duplicate internal function definitions: $(printf '%s' "$l_duplicate_functions" | tr '\n' ' ')"
fi

# POSIX sh has no function-local variables. A direct in-process call can
# therefore overwrite a caller's l_* scratch value when both functions assign
# the same name. Compare exact caller/callee/name tuples against the documented
# baseline so new overlaps fail while existing debt can ratchet downward.
while IFS= read -r l_module; do
	awk -f "$FUNCTION_SCRATCH_AWK" \
		"$architecture_tmp/function-owners" "$ZXFER_ROOT/$l_module"
done <"$architecture_tmp/manifest" >"$architecture_tmp/function-scratch-records"
awk -F "$TAB" '
	$1 == "write" {
		writes[$2 SUBSEP $3] = 1
		variables[$3] = 1
		next
	}
	$1 == "call" {
		calls[$2 SUBSEP $3] = 1
	}
	END {
		for (call_key in calls) {
			split(call_key, pair, SUBSEP)
			for (variable in variables) {
				if (writes[pair[1] SUBSEP variable] && writes[pair[2] SUBSEP variable])
					printf "%s\t%s\t%s\n", pair[1], pair[2], variable
			}
		}
	}
' "$architecture_tmp/function-scratch-records" | sort -u >"$architecture_tmp/function-scratch-collisions"

awk -F "$TAB" '
	$0 !~ /^[[:space:]]*#/ && $0 != "" && (NF < 3 || $1 == "" || $2 == "" || $3 == "") {
		printf "line %d must contain caller, callee, and scratch name: %s\n", FNR, $0
	}
' "$FUNCTION_SCRATCH_BASELINE" >"$architecture_tmp/function-scratch-baseline-invalid"
if [ -s "$architecture_tmp/function-scratch-baseline-invalid" ]; then
	while IFS= read -r l_violation; do
		report_architecture_violation "invalid function-scratch baseline: $l_violation"
	done <"$architecture_tmp/function-scratch-baseline-invalid"
fi
awk -F "$TAB" '
	$0 !~ /^[[:space:]]*#/ && $0 != "" && NF >= 3 && $1 != "" && $2 != "" && $3 != "" {
		printf "%s\t%s\t%s\n", $1, $2, $3
	}
' "$FUNCTION_SCRATCH_BASELINE" | sort >"$architecture_tmp/function-scratch-baseline"
l_duplicate_scratch_baseline=$(uniq -d "$architecture_tmp/function-scratch-baseline")
if [ -n "$l_duplicate_scratch_baseline" ]; then
	report_architecture_violation "duplicate function-scratch baseline entries: $(printf '%s' "$l_duplicate_scratch_baseline" | tr '\n' ' ')"
fi
comm -23 "$architecture_tmp/function-scratch-collisions" \
	"$architecture_tmp/function-scratch-baseline" >"$architecture_tmp/function-scratch-new"
if [ -s "$architecture_tmp/function-scratch-new" ]; then
	while IFS="$TAB" read -r l_caller l_callee l_scratch_name; do
		report_architecture_violation "new function scratch collision: $l_caller -> $l_callee both write $l_scratch_name"
	done <"$architecture_tmp/function-scratch-new"
fi
comm -13 "$architecture_tmp/function-scratch-collisions" \
	"$architecture_tmp/function-scratch-baseline" >"$architecture_tmp/function-scratch-stale"
if [ -s "$architecture_tmp/function-scratch-stale" ]; then
	while IFS="$TAB" read -r l_caller l_callee l_scratch_name; do
		report_architecture_violation "stale function-scratch baseline entry: $l_caller -> $l_callee no longer both write $l_scratch_name"
	done <"$architecture_tmp/function-scratch-stale"
fi

# Resolve internal call edges. Function-like tokens in operator strings are
# intentionally included: generated commands may not smuggle an upward zxfer
# dependency around the graph merely by quoting it.
while IFS= read -r l_module; do
	awk -F "$TAB" -v caller="$l_module" '
		NR == FNR { owner[$1] = $2; next }
		$0 ~ /^[[:space:]]*#/ { next }
		{
			line = $0
			gsub(/[^A-Za-z0-9_]/, " ", line)
			count = split(line, token, / +/)
			for (i = 1; i <= count; i++) {
				if (token[i] in owner && owner[token[i]] != caller)
					printf "%s\t%s\n", caller, owner[token[i]]
			}
		}
	' "$architecture_tmp/function-owners" "$ZXFER_ROOT/$l_module"
done <"$architecture_tmp/manifest" | sort -u >"$architecture_tmp/edges"

awk -F "$TAB" '$1 == "layer" { print $2 "\t" $3 "\t" $4 }' "$POLICY_FILE" >"$architecture_tmp/layers"
cut -f1 "$architecture_tmp/layers" | sort >"$architecture_tmp/layer-modules"
l_missing_layers=$(comm -23 "$architecture_tmp/manifest-sorted" "$architecture_tmp/layer-modules")
if [ -n "$l_missing_layers" ]; then
	report_architecture_violation "manifest modules missing layer ownership: $(printf '%s' "$l_missing_layers" | tr '\n' ' ')"
fi
l_unknown_layers=$(comm -13 "$architecture_tmp/source-modules" "$architecture_tmp/layer-modules")
if [ -n "$l_unknown_layers" ]; then
	report_architecture_violation "layer policy names unknown production modules: $(printf '%s' "$l_unknown_layers" | tr '\n' ' ')"
fi

awk -F "$TAB" '
	NR == FNR {
		rank[$1] = $2
		layer[$1] = $3
		next
	}
	{
		caller = $1
		callee = $2
		if (!(caller in rank) || !(callee in rank))
			next
		if (rank[callee] > rank[caller]) {
			printf "upward dependency: %s (%s) -> %s (%s)\n", caller, layer[caller], callee, layer[callee]
			violations++
		}
	}
	END { exit(violations > 0 ? 1 : 0) }
' "$architecture_tmp/layers" "$architecture_tmp/edges" >"$architecture_tmp/layer-violations" || :
if [ -s "$architecture_tmp/layer-violations" ]; then
	while IFS= read -r l_violation; do
		report_architecture_violation "$l_violation"
	done <"$architecture_tmp/layer-violations"
fi

# A same-layer dependency is legal only when the complete graph remains
# acyclic. Print one concrete back edge for each detected DFS cycle.
awk -F "$TAB" '
	{
		edge[$1 SUBSEP $2] = 1
		node[$1] = 1
		node[$2] = 1
	}
	function visit(current,    key, pair, next_node) {
		state[current] = 1
		for (key in edge) {
			split(key, pair, SUBSEP)
			if (pair[1] != current)
				continue
			next_node = pair[2]
			if (state[next_node] == 1) {
				printf "dependency cycle back edge: %s -> %s\n", current, next_node
				cycles++
				continue
			}
			if (state[next_node] == 0)
				visit(next_node)
		}
		state[current] = 2
	}
	END {
		for (current in node) {
			if (state[current] == 0)
				visit(current)
		}
		exit(cycles > 0 ? 1 : 0)
	}
' "$architecture_tmp/edges" >"$architecture_tmp/cycle-violations" || :
if [ -s "$architecture_tmp/cycle-violations" ]; then
	while IFS= read -r l_violation; do
		report_architecture_violation "$l_violation"
	done <"$architecture_tmp/cycle-violations"
fi

l_internal_probes=$(awk '
	$0 !~ /^[[:space:]]*#/ && $0 ~ /command[[:space:]]+-v[[:space:]]+zxfer_/ {
		printf "%s:%d:%s\n", FILENAME, FNR, $0
	}
' "$ZXFER_ROOT"/src/*.sh)
if [ -n "$l_internal_probes" ]; then
	report_architecture_violation "internal function-existence probes remain: $l_internal_probes"
fi

if [ "$VIOLATIONS" -gt 0 ]; then
	printf 'architecture check failed: %s violation(s)\n' "$VIOLATIONS" >&2
	exit 1
fi

l_module_count=$(wc -l <"$architecture_tmp/manifest" | tr -d ' \t')
l_edge_count=$(wc -l <"$architecture_tmp/edges" | tr -d ' \t')
l_global_count=$(cut -f1 "$architecture_tmp/global-writes" | sort -u | wc -l | tr -d ' \t')
l_scratch_count=$(wc -l <"$architecture_tmp/function-scratch-collisions" | tr -d ' \t')
printf 'architecture check passed: %s canonical modules, %s acyclic dependency edges, %s singly owned mutable globals, %s baselined function-scratch overlaps\n' \
	"$l_module_count" "$l_edge_count" "$l_global_count" "$l_scratch_count"
