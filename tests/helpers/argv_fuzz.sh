#!/bin/sh
#
# Seeded argv-boundary fuzz: generate a case, run the real ./zxfer against the
# fake zfs in tests/helpers/argv_fuzz.awk in every mode, and check what
# reached zfs. Used by tests/run_argv_fuzz.sh and tests/test_run_argv_fuzz.sh.
# Source it after tests/helpers/blackbox.sh, which supplies the mock ssh and
# the GNU-parallel-faithful mock parallel.
#
# Modes, one zxfer run each per case:
#   R        -R                   plain recursive replication
#   P        -P -R                with the property pass
#   L        -P -R                with every recursive zfs get refused, so
#                                 each dataset's properties are read alone;
#                                 it must mutate exactly as P does
#   j        -j 2 -P -R           parallel discovery and send jobs
#   O        -O localhost -P -R   source commands over the mock ssh
#   T        -T localhost -P -R   destination commands over the mock ssh
#   invalid  -P -R, one operand holding a control byte: must fail closed
# The mock ssh joins its remote argv with spaces and runs it with sh -c, as
# sshd does, so -O and -T exercise zxfer's remote quoting.
#
# A case directory holds the generated files (see the gen role in the awk
# file) and one directory per mode with mockbin/ (fake zfs, mock ssh, mock
# parallel), state/ (the model the fake zfs updates, argv.log with one
# escaped argv per line, violations, and mode L's refuse_recursive_get),
# tmp/ (zxfer's TMPDIR), command, stdout, stderr and problems. A black-box
# pin may also give state/ a race file (see start_race in the awk file).
#
# ZXFER_ARGV_FUZZ_FAULT (self-test only) puts a bug between zxfer and the fake
# zfs: "split" re-splits every argument on whitespace, "truncate" drops the
# last byte of every property value, "inject" adds readonly=on to every
# zfs set, and "prefetch" makes every recursive zfs get report local sources
# as received.
#
# shellcheck shell=sh disable=SC2034,SC2154

ARGV_FUZZ_AWK="$ZXFER_ROOT/tests/helpers/argv_fuzz.awk"
ARGV_FUZZ_MODES="R P L j O T invalid"
ARGV_FUZZ_LF='
'

# Purpose: Write one seeded case into DIR, which must not exist yet.
# Usage: argv_fuzz_generate_case SEED CASE DIR
argv_fuzz_generate_case() {
	mkdir "$3" || return 1
	LC_ALL=C ARGV_FUZZ_SEED=$1 ARGV_FUZZ_CASE=$2 ARGV_FUZZ_CASE_DIR=$3 \
		awk -v role=gen -f "$ARGV_FUZZ_AWK"
}

# Purpose: Write a run's mock bin: the fake zfs bound to STATE_DIR (behind
# the ZXFER_ARGV_FUZZ_FAULT shim when one is set), the mock ssh and the mock
# parallel.
# Usage: argv_fuzz_write_mockbin MOCKBIN_DIR STATE_DIR
argv_fuzz_write_mockbin() {
	l_mockbin_dir=$1
	l_mockbin_state=$2

	# The paths are embedded in single quotes below.
	case $l_mockbin_dir$l_mockbin_state$ARGV_FUZZ_AWK in
	*"'"* | *"$ARGV_FUZZ_LF"*)
		printf 'argv_fuzz: unsupported character in a work path: %s\n' "$l_mockbin_dir" >&2
		return 1
		;;
	esac
	l_mockbin_awk=$(zxfer_mockbin_resolve_host_tool awk) || return 1
	mkdir -p "$l_mockbin_dir" || return 1
	l_mockbin_fake=$l_mockbin_dir/zfs
	[ -z "${ZXFER_ARGV_FUZZ_FAULT:-}" ] || l_mockbin_fake=$l_mockbin_dir/zfs.fake
	cat >"$l_mockbin_fake" <<EOF || return 1
#!/bin/sh
# Fake zfs for the argv fuzz; tests/helpers/argv_fuzz.awk does the work.
ARGV_FUZZ_STATE='$l_mockbin_state'
LC_ALL=C
export ARGV_FUZZ_STATE LC_ALL
exec '$l_mockbin_awk' -v role=zfs -f '$ARGV_FUZZ_AWK' -- "\$@"
EOF
	case ${ZXFER_ARGV_FUZZ_FAULT:-} in
	'') ;;
	split)
		cat >"$l_mockbin_dir/zfs" <<EOF || return 1
#!/bin/sh
set -f
exec '$l_mockbin_fake' \$*
EOF
		;;
	truncate)
		cat >"$l_mockbin_dir/zfs" <<EOF || return 1
#!/bin/sh
case \${1:-} in
set | create)
	l_first=1
	for l_arg do
		[ "\$l_first" -eq 0 ] || set --
		l_first=0
		case \$l_arg in
		?*=?*) l_arg=\${l_arg%?} ;;
		esac
		set -- "\$@" "\$l_arg"
	done
	;;
esac
exec '$l_mockbin_fake' "\$@"
EOF
		;;
	inject)
		cat >"$l_mockbin_dir/zfs" <<EOF || return 1
#!/bin/sh
if [ "\${1:-}" = set ]; then
	shift
	set -- set readonly=on "\$@"
fi
exec '$l_mockbin_fake' "\$@"
EOF
		;;
	prefetch)
		cat >"$l_mockbin_dir/zfs" <<EOF || return 1
#!/bin/sh
case "\${1:-} \${2:-}" in
"get -r")
	'$l_mockbin_fake' "\$@" >"\$0.\$\$" || exit
	sed 's/	local\$/	received/' "\$0.\$\$"
	rm -f "\$0.\$\$"
	exit 0
	;;
esac
exec '$l_mockbin_fake' "\$@"
EOF
		;;
	*)
		printf 'argv_fuzz: unknown ZXFER_ARGV_FUZZ_FAULT: %s\n' "$ZXFER_ARGV_FUZZ_FAULT" >&2
		return 1
		;;
	esac
	chmod +x "$l_mockbin_dir/zfs" "$l_mockbin_fake" &&
		planning_write_socket_mock_ssh "$l_mockbin_dir/ssh" &&
		planning_write_mock_parallel "$l_mockbin_dir/parallel"
}

# Purpose: Run ./zxfer for one mode of a generated case, then check the run.
# Usage: argv_fuzz_run_mode CASE_DIR MODE; the run lives in CASE_DIR/MODE.
# Returns 0 when the run passed, 1 when CASE_DIR/MODE/problems lists
# problems, and 2 when the run could not be set up.
argv_fuzz_run_mode() {
	l_run_case=$1
	l_run_mode=$2
	l_run_dir=$l_run_case/$l_run_mode

	mkdir -p "$l_run_dir/state" "$l_run_dir/tmp" &&
		chmod 700 "$l_run_dir/tmp" &&
		cp "$l_run_case/model" "$l_run_dir/state/model" &&
		: >"$l_run_dir/state/argv.log" &&
		: >"$l_run_dir/state/violations" &&
		argv_fuzz_write_mockbin "$l_run_dir/mockbin" "$l_run_dir/state" ||
		return 2
	{
		IFS= read -r l_run_source &&
			IFS= read -r l_run_destination &&
			IFS= read -r l_run_broken_operand &&
			IFS= read -r l_run_broken_text
	} <"$l_run_case/operands" || return 2

	case $l_run_mode in
	R) set -- -R ;;
	P) set -- -P -R ;;
	L)
		set -- -P -R
		: >"$l_run_dir/state/refuse_recursive_get" || return 2
		;;
	j) set -- -j 2 -P -R ;;
	O) set -- -O localhost -P -R ;;
	T) set -- -T localhost -P -R ;;
	invalid)
		set -- -P -R
		# printf %b turns the \0ooo escape into the control byte; the x
		# keeps a trailing newline from being stripped.
		l_run_broken=$(printf '%bx' "$l_run_broken_text")
		if [ "$l_run_broken_operand" = source ]; then
			l_run_source=${l_run_broken%x}
		else
			l_run_destination=${l_run_broken%x}
		fi
		;;
	*) return 2 ;;
	esac
	if [ "$l_run_mode" = invalid ]; then
		printf 'zxfer %s, %s operand [%s] (printf %%b form)\n' "$*" \
			"$l_run_broken_operand" "$l_run_broken_text"
	else
		printf 'zxfer %s [%s] [%s]\n' "$*" "$l_run_source" "$l_run_destination"
	fi >"$l_run_dir/command"

	(
		TMPDIR=$l_run_dir/tmp
		PATH=$(zxfer_mockbin_secure_path_env "$l_run_dir/mockbin")
		export TMPDIR PATH
		zxfer_mockbin_run_zxfer "$l_run_dir/mockbin" "$l_run_dir/state" "" \
			"$@" "$l_run_source" "$l_run_destination"
	) >"$l_run_dir/stdout" 2>"$l_run_dir/stderr" </dev/null
	l_run_status=$?

	LC_ALL=C ARGV_FUZZ_CASE_DIR=$l_run_case ARGV_FUZZ_STATE=$l_run_dir/state \
		ARGV_FUZZ_MODE=$l_run_mode ARGV_FUZZ_STATUS=$l_run_status \
		ARGV_FUZZ_STDERR=$l_run_dir/stderr \
		awk -v role=check -f "$ARGV_FUZZ_AWK" >"$l_run_dir/problems"
	case $? in
	0) return 0 ;;
	1) return 1 ;;
	*) return 2 ;;
	esac
}

# Purpose: Print the escaped argv log of a run, one "[arg] [arg]" call per
# line, indented for a failure report.
# Usage: argv_fuzz_render_argv_log ARGV_LOG
argv_fuzz_render_argv_log() {
	awk -F '\t' '{
		line = ""
		for (i = 1; i <= NF; i++)
			line = line (i > 1 ? " " : "") "[" $i "]"
		print "      " line
	}' "$1"
}
