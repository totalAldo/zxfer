#!/bin/sh
#
# Integration tests for unusual dataset names (spaces and other legal
# punctuation) and property values (control characters, quotes and shell
# syntax), locally, with GNU parallel, and over mock -O / -T localhost.
# Sourced by tests/run_integration_zxfer.sh; the registry owns execution order.
# A name or value this platform's zfs refuses when building the source fixture
# is skipped with a log line. A zxfer failure fails the test.

hostile_dataset_names_replication_test() {
	log "Starting hostile dataset names replication test"

	mock_path="$WORKDIR/mock_hostile_names"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	src_root="$SRC_POOL/hn names src"
	expected="$WORKDIR/hn_names.expected"
	actual="$WORKDIR/hn_names.actual"
	modes="local jobs origin target origin_jobs"
	failures=

	for l_mode in $modes; do
		destroy_test_datasets_if_present "$DEST_POOL/hn names $l_mode"
	done
	destroy_test_datasets_if_present "$src_root"
	zfs create "$src_root" || fail "Unable to create $src_root."
	# "a", "a b" and "a  b" are decoys for one another: a name split on spaces,
	# or re-joined with one space, lands on another real dataset.
	for l_child in 'a' 'a b' 'a  b' 'a b/c  d' '-lead' ' lead space' \
		'trail space ' 'dots.and:colons_under-score'; do
		zfs create "$src_root/$l_child" 2>/dev/null ||
			log "Skipping dataset name [$l_child]: this platform's zfs rejected it"
	done

	for l_mode in $modes; do
		case $l_mode in
		local) set -- -v ;;
		jobs) set -- -v -j 2 ;;
		origin) set -- -v -O localhost ;;
		target) set -- -v -T localhost ;;
		origin_jobs) set -- -v -j 2 -O localhost ;;
		esac
		case $l_mode in
		*jobs)
			if ! has_parallel; then
				log "Skipping hostile names mode $l_mode (parallel not available)"
				continue
			fi
			;;
		esac
		dest_root="$DEST_POOL/hn names $l_mode"
		zfs create "$dest_root" || fail "Unable to create $dest_root."

		# The first run seeds every dataset; the second sends incrementally.
		for l_snapshot in "hn 1 $l_mode" "hn:2.$l_mode"; do
			zfs snap -r "$src_root@$l_snapshot" ||
				fail "Unable to snapshot $src_root@$l_snapshot."
			status=0
			output=$(ZXFER_SECURE_PATH="$secure_path" run_zxfer "$@" -R "$src_root" "$dest_root" 2>&1) ||
				status=$?
			# Expect exactly the source tree, renamed under the destination root.
			{
				printf '%s\n' "$dest_root"
				zfs list -H -o name -t filesystem,snapshot -r "$src_root" |
					awk -v src="$src_root" -v dst="$dest_root/${src_root##*/}" \
						'{ print dst substr($0, length(src) + 1) }'
			} | LC_ALL=C sort >"$expected"
			zfs list -H -o name -t filesystem,snapshot -r "$dest_root" |
				LC_ALL=C sort >"$actual"
			if [ "$status" -ne 0 ] || ! cmp -s "$expected" "$actual"; then
				failures="$failures
[$l_mode, @$l_snapshot] zxfer exit $status; expected (<) vs destination (>) listing:
$(diff "$expected" "$actual")
zxfer output:
$output"
				break
			fi
		done
		destroy_test_datasets_if_present "$dest_root"
	done
	destroy_test_datasets_if_present "$src_root"

	[ -z "$failures" ] || fail "Hostile dataset names did not replicate intact:$failures"
	log "Hostile dataset names replication test passed"
}

hostile_dataset_names_delete_test() {
	log "Starting hostile dataset names -d test"

	src_root="$SRC_POOL/hn delete src"
	src_before="$WORKDIR/hn_delete_src.before"
	src_after="$WORKDIR/hn_delete_src.after"
	expected="$WORKDIR/hn_delete.expected"
	actual="$WORKDIR/hn_delete.actual"
	failures=

	destroy_test_datasets_if_present "$DEST_POOL/hn delete j1" "$DEST_POOL/hn delete j2" "$src_root"
	zfs create "$src_root" || fail "Unable to create $src_root."
	if ! zfs create "$src_root/sp ace" 2>/dev/null; then
		log "Skipping hostile dataset names -d test: this platform's zfs rejected a name with a space"
		destroy_test_datasets_if_present "$src_root"
		return
	fi
	zfs create "$src_root/sp" || fail "Unable to create $src_root/sp."
	zfs create "$src_root/sp  ace" || fail "Unable to create $src_root/sp  ace."
	zfs snap -r "$src_root@base 1" || fail "Unable to snapshot $src_root@base 1."
	# Only "sp ace" will hold "drop 2" as a destination-only snapshot. The
	# decoys "sp" (its first word) and "sp  ace" (its double-space twin) hold
	# "drop 2" on both sides, so -d must keep theirs.
	zfs snap "$src_root/sp@drop 2" || fail "Unable to snapshot $src_root/sp@drop 2."
	zfs snap "$src_root/sp  ace@drop 2" || fail "Unable to snapshot $src_root/sp  ace@drop 2."
	zfs list -H -o name -t filesystem,snapshot -r "$src_root" | LC_ALL=C sort >"$src_before"

	for l_jobs in 1 2; do
		if [ "$l_jobs" -eq 2 ] && ! has_parallel; then
			log "Skipping hostile dataset names -d -j 2 (parallel not available)"
			continue
		fi
		dest_root="$DEST_POOL/hn delete j$l_jobs"
		target="$dest_root/${src_root##*/}/sp ace@drop 2"
		zfs create "$dest_root" || fail "Unable to create $dest_root."
		status=0
		output=$(run_zxfer -v -R "$src_root" "$dest_root" 2>&1) || status=$?
		[ "$status" -eq 0 ] || fail "Seeding $dest_root failed with exit $status. Output: $output"
		zfs snap "$target" || fail "Unable to snapshot $target."
		zfs list -H -o name -t filesystem,snapshot -r "$dest_root" |
			awk -v target="$target" '$0 != target' | LC_ALL=C sort >"$expected"

		status=0
		output=$(run_zxfer -v -d -j "$l_jobs" -R "$src_root" "$dest_root" 2>&1) || status=$?
		zfs list -H -o name -t filesystem,snapshot -r "$dest_root" | LC_ALL=C sort >"$actual"
		zfs list -H -o name -t filesystem,snapshot -r "$src_root" | LC_ALL=C sort >"$src_after"
		if [ "$status" -ne 0 ] || ! cmp -s "$expected" "$actual" ||
			! cmp -s "$src_before" "$src_after"; then
			failures="$failures
[-d -j $l_jobs] zxfer exit $status; only [$target] should be gone. Destination expected (<) vs actual (>):
$(diff "$expected" "$actual")
Source before (<) vs after (>):
$(diff "$src_before" "$src_after")
zxfer output:
$output"
		fi
		destroy_test_datasets_if_present "$dest_root"
	done
	destroy_test_datasets_if_present "$src_root"

	[ -z "$failures" ] || fail "zxfer -d did not destroy exactly the destination-only snapshot:$failures"
	log "Hostile dataset names -d test passed"
}

hostile_property_values_test() {
	log "Starting hostile property values test"

	mock_path="$WORKDIR/mock_hostile_props"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	src_root="$SRC_POOL/hn_props_src"
	canary="$WORKDIR/hn_props.canary"
	src_file="$WORKDIR/hn_props.src"
	dest_file="$WORKDIR/hn_props.dest"
	modes="local origin target"
	tab=$(printf '\t')
	soh=$(printf '\001')
	lf='
'
	props=
	failures=

	for l_mode in $modes; do
		destroy_test_datasets_if_present "$DEST_POOL/hn_props_$l_mode"
	done
	destroy_test_datasets_if_present "$src_root"
	safe_rm_f "$canary"
	zfs create "$src_root" || fail "Unable to create $src_root."
	zfs create "$src_root/child" || fail "Unable to create $src_root/child."
	# Each value breaks a naive parse or render: a tab or line feed splits
	# `zfs get -H` records, \001 once split one value into extra argv (hn:soh
	# carries a would-be extra property), `%` and `,` `=` `;` meet zxfer's own
	# list encoding, and $(...) or backticks run if ever re-evaluated.
	# shellcheck disable=SC1003,SC2016  # Backslashes, $ and backticks are literal values.
	for l_prop in hn:tab hn:lf hn:lf-edges hn:soh hn:quotes hn:backslash \
		hn:subst hn:backtick hn:percent hn:csv hn:glob hn:dash hn:utf8 hn:child; do
		l_target=$src_root
		case $l_prop in
		hn:tab) l_value="tab${tab}separated${tab}value" ;;
		hn:lf) l_value="first line${lf}second line" ;;
		hn:lf-edges) l_value="${lf}blank line next${lf}${lf}last line${lf}" ;;
		hn:soh) l_value="before${soh}hn:injected=soh${soh}after" ;;
		hn:quotes) l_value="it's a \"double\" and 'single' quote" ;;
		hn:backslash) l_value='C:\new\table \001 \\ ends with\' ;;
		hn:subst) l_value="\$(touch $canary) \$HOME" ;;
		hn:backtick) l_value="\`touch $canary\`" ;;
		hn:percent) l_value='100% %25 %0A %09 %2C %3D %s %n %%' ;;
		hn:csv) l_value='a,b=c;d==e,,=' ;;
		hn:glob) l_value='* ? [a-z] ~ {a,b}' ;;
		hn:dash) l_value='-o canmount=off -- -' ;;
		hn:utf8) l_value=$(printf 'caf\303\251 \342\234\223') ;;
		hn:child)
			l_value="child${tab}value${lf}second line"
			l_target=$src_root/child
			;;
		esac
		if zfs set "$l_prop=$l_value" "$l_target" 2>/dev/null; then
			props="$props $l_prop"
		else
			log "Skipping property value $l_prop: this platform's zfs rejected it"
		fi
	done
	[ -n "$props" ] || fail "zfs rejected every hostile property value fixture."
	zfs snap -r "$src_root@hp1" || fail "Unable to snapshot $src_root@hp1."

	for l_mode in $modes; do
		case $l_mode in
		local) set -- -v -P ;;
		origin) set -- -v -P -O localhost ;;
		target) set -- -v -P -T localhost ;;
		esac
		dest_root="$DEST_POOL/hn_props_$l_mode"
		dest_dataset="$dest_root/${src_root##*/}"
		zfs create "$dest_root" || fail "Unable to create $dest_root."

		# The create pass builds the destination with the properties; the set
		# pass must restore them after the destination values are changed.
		for l_pass in create set; do
			if [ "$l_pass" = set ]; then
				for l_prop in $props; do
					l_target=$dest_dataset
					[ "$l_prop" != hn:child ] || l_target=$dest_dataset/child
					zfs set "$l_prop=changed" "$l_target" ||
						fail "Unable to change $l_prop on $l_target."
				done
			fi
			status=0
			output=$(ZXFER_SECURE_PATH="$secure_path" run_zxfer "$@" -R "$src_root" "$dest_root" 2>&1) ||
				status=$?
			mismatch=
			[ "$status" -eq 0 ] || mismatch="$lf zxfer exit $status"
			for l_dataset in '' /child; do
				[ "$status" -eq 0 ] || break
				for l_prop in $props; do
					zfs get -Hpo value "$l_prop" "$src_root$l_dataset" >"$src_file" 2>&1
					zfs get -Hpo value "$l_prop" "$dest_dataset$l_dataset" >"$dest_file" 2>&1
					cmp -s "$src_file" "$dest_file" || mismatch="$mismatch
 $l_prop on ${l_dataset:-the root}, source bytes:
$(od -c "$src_file")
 destination bytes:
$(od -c "$dest_file")"
				done
				# The effective user property names must match: an extra one means
				# a value was split. (The root may hold inherited ones locally.)
				zfs get -H -o property all "$src_root$l_dataset" |
					grep : | LC_ALL=C sort >"$src_file"
				zfs get -H -o property all "$dest_dataset$l_dataset" |
					grep : | LC_ALL=C sort >"$dest_file"
				cmp -s "$src_file" "$dest_file" || mismatch="$mismatch
 user property names on ${l_dataset:-the root}, source (<) vs destination (>):
$(diff "$src_file" "$dest_file")"
			done
			[ ! -e "$canary" ] || mismatch="$mismatch
 a property value was executed as shell code ($canary exists)"
			if [ -n "$mismatch" ]; then
				failures="$failures
[$l_mode, $l_pass pass]$mismatch
zxfer output:
$output"
				break
			fi
		done
		destroy_test_datasets_if_present "$dest_root"
	done
	destroy_test_datasets_if_present "$src_root"

	[ -z "$failures" ] || fail "Hostile property values did not transfer byte-identical:$failures"
	log "Hostile property values test passed"
}

hostile_property_override_test() {
	log "Starting hostile property override test"

	src_root="$SRC_POOL/hn_override_src"
	dest_root="$DEST_POOL/hn_override_dest"
	dest_dataset="$dest_root/${src_root##*/}"
	canary="$WORKDIR/hn_override.canary"
	expected="$WORKDIR/hn_override.expected"
	actual="$WORKDIR/hn_override.actual"
	tab=$(printf '\t')
	soh=$(printf '\001')
	lf='
'
	# In a -o list "\," is a literal comma; every other byte is literal.
	note_head="tab${tab}lf${lf}soh${soh}hn:injected=o 'sq' \"dq\" C:\\new \$(touch $canary) \`touch $canary\` 100% %2C = "
	note_value="${note_head}a,b"
	other_value="\$HOME * %0A${tab}=="
	failures=

	destroy_test_datasets_if_present "$dest_root" "$src_root"
	safe_rm_f "$canary"
	zfs create "$src_root" || fail "Unable to create $src_root."
	zfs create "$src_root/child" || fail "Unable to create $src_root/child."
	# The values reach zfs only through -o, so first check that this platform's
	# zfs accepts them at all.
	if ! zfs set "hn:note=$note_value" "$src_root" 2>/dev/null ||
		! zfs set "hn:other=$other_value" "$src_root" 2>/dev/null; then
		log "Skipping hostile property override test: this platform's zfs rejected a value"
		destroy_test_datasets_if_present "$dest_root" "$src_root"
		return
	fi
	zfs set hn:note=source "$src_root" || fail "Unable to set hn:note on $src_root."
	zfs set hn:other=source "$src_root" || fail "Unable to set hn:other on $src_root."
	zfs snap -r "$src_root@ho1" || fail "Unable to snapshot $src_root@ho1."
	zfs create "$dest_root" || fail "Unable to create $dest_root."

	# The create pass builds the destination with the overrides; the set pass
	# must restore them after the destination values are changed.
	for l_pass in create set; do
		if [ "$l_pass" = set ]; then
			zfs set hn:note=changed "$dest_dataset" || fail "Unable to change hn:note on $dest_dataset."
			zfs set hn:other=changed "$dest_dataset" || fail "Unable to change hn:other on $dest_dataset."
		fi
		status=0
		output=$(run_zxfer -v -P -o "hn:note=${note_head}a\\,b,hn:other=$other_value" \
			-R "$src_root" "$dest_root" 2>&1) || status=$?
		[ "$status" -eq 0 ] || failures="$failures
[$l_pass pass] zxfer exit $status"
		# The child inherits both overrides from the replicated root.
		for l_dataset in "$dest_dataset" "$dest_dataset/child"; do
			[ "$status" -eq 0 ] || break
			for l_prop in hn:note hn:other hn:injected; do
				case $l_prop in
				hn:note) printf '%s\n' "$note_value" >"$expected" ;;
				hn:other) printf '%s\n' "$other_value" >"$expected" ;;
				hn:injected) printf '%s\n' "-" >"$expected" ;;
				esac
				zfs get -Hpo value "$l_prop" "$l_dataset" >"$actual" 2>&1
				cmp -s "$expected" "$actual" || failures="$failures
[$l_pass pass] $l_prop on $l_dataset, expected bytes:
$(od -c "$expected")
 actual bytes:
$(od -c "$actual")"
			done
		done
		[ ! -e "$canary" ] || failures="$failures
[$l_pass pass] an override value was executed as shell code ($canary exists)"
		if [ -n "$failures" ]; then
			failures="$failures
zxfer output:
$output"
			break
		fi
	done
	destroy_test_datasets_if_present "$dest_root" "$src_root"

	[ -z "$failures" ] || fail "Hostile -o override values did not apply byte-identical:$failures"
	log "Hostile property override test passed"
}

hostile_property_backup_restore_test() {
	log "Starting hostile property backup/restore test"

	src_dataset="$SRC_POOL/hn_backup_src"
	dest_root="$DEST_POOL/hn_backup_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"
	backup_dir="$WORKDIR/hn_backup_dir"
	original="$WORKDIR/hn_backup.original"
	actual="$WORKDIR/hn_backup.actual"
	tab=$(printf '\t')
	soh=$(printf '\001')
	lf='
'
	props=
	failures=

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	safe_rm_rf "$backup_dir"
	zfs create "$src_dataset" || fail "Unable to create $src_dataset."
	zfs create "$dest_root" || fail "Unable to create $dest_root."
	for l_prop in hn:lf hn:mixed; do
		case $l_prop in
		hn:lf) l_value="first line${lf}second line${lf}" ;;
		hn:mixed) l_value="tab${tab}soh${soh}lf${lf}%0A %25 C:\\new 'sq' \"dq\"" ;;
		esac
		if zfs set "$l_prop=$l_value" "$src_dataset" 2>/dev/null; then
			props="$props $l_prop"
		else
			log "Skipping property value $l_prop: this platform's zfs rejected it"
		fi
	done
	[ -n "$props" ] || fail "zfs rejected every hostile backup property value fixture."
	zfs snap -r "$src_dataset@hb1" || fail "Unable to snapshot $src_dataset@hb1."

	status=0
	output=$(ZXFER_BACKUP_DIR="$backup_dir" run_zxfer -v -k -R "$src_dataset" "$dest_root" 2>&1) ||
		status=$?
	[ "$status" -eq 0 ] || fail "Backup (-k) run failed with exit $status. Output: $output"
	[ -n "$(find_backup_metadata_file_for_exact_pair "$backup_dir" "$src_dataset" "$dest_dataset")" ] ||
		fail "Backup (-k) run wrote no metadata for $src_dataset -> $dest_dataset under $backup_dir."

	# Change both sides, so only the backup still holds the original values.
	for l_prop in $props; do
		zfs get -Hpo value "$l_prop" "$src_dataset" >"$original.${l_prop#hn:}" ||
			fail "Unable to read $l_prop on $src_dataset."
		zfs set "$l_prop=changed source" "$src_dataset" || fail "Unable to change $l_prop on $src_dataset."
		zfs set "$l_prop=changed destination" "$dest_dataset" ||
			fail "Unable to change $l_prop on $dest_dataset."
	done
	status=0
	output=$(ZXFER_BACKUP_DIR="$backup_dir" run_zxfer -v -e -R "$src_dataset" "$dest_root" 2>&1) ||
		status=$?
	[ "$status" -eq 0 ] || fail "Restore (-e) run failed with exit $status. Output: $output"
	for l_prop in $props; do
		zfs get -Hpo value "$l_prop" "$dest_dataset" >"$actual" 2>&1
		cmp -s "$original.${l_prop#hn:}" "$actual" || failures="$failures
 $l_prop on $dest_dataset, original bytes:
$(od -c "$original.${l_prop#hn:}")
 restored bytes:
$(od -c "$actual")"
	done
	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	[ -z "$failures" ] || fail "Hostile property values did not survive the -k/-e round trip:$failures"
	log "Hostile property backup/restore test passed"
}

hostile_property_record_shaped_value_test() {
	log "Starting record-shaped property value test"

	mock_path="$WORKDIR/mock_hostile_record"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	src_root="$SRC_POOL/hn_record_src"
	dest_parent="$DEST_POOL/hn_record_dest"
	src_file="$WORKDIR/hn_record.src"
	dest_file="$WORKDIR/hn_record.dest"
	tab=$(printf '\t')
	lf='
'
	failures=

	destroy_test_datasets_if_present "$dest_parent" "$src_root"
	zfs create "$src_root" || fail "Unable to create $src_root."
	zfs create "$dest_parent" || fail "Unable to create $dest_parent."
	# zxfer leaves out a user property removed before its lone re-read
	# because zfs answers that read with NAME<TAB>-<TAB>- and exit 0.
	status=0
	output=$(zfs get -Hpo property,value,source -- hn:absent "$src_root" 2>&1) || status=$?
	[ "$status" -eq 0 ] && [ "$output" = "hn:absent${tab}-${tab}-" ] ||
		fail "zfs read the missing user property hn:absent as [$output] with exit $status, not [hn:absent<TAB>-<TAB>-] with exit 0."
	# `zfs get -H` prints the second line of each value exactly like another
	# property's record, so that output alone is ambiguous: a fake user
	# property, a native one, the dataset column of a recursive listing, or a
	# user property the dataset really holds (hn:real). zxfer must exit 0 with
	# every value byte-identical and no property added, cut or changed.
	for l_shape in user native named real; do
		l_source="$src_root/$l_shape"
		case $l_shape in
		user) l_value="x${tab}local${lf}hn:fake${tab}y" ;;
		native) l_value="x${tab}local${lf}readonly${tab}on" ;;
		named) l_value="x${tab}local${lf}$l_source${tab}hn:fake${tab}y" ;;
		real) l_value="x${tab}local${lf}hn:real${tab}forged" ;;
		esac
		zfs create "$l_source" || fail "Unable to create $l_source."
		if [ "$l_shape" = real ]; then
			zfs set hn:real=genuine "$l_source" || fail "Unable to set hn:real on $l_source."
		fi
		if ! zfs set "hn:record=$l_value" "$l_source" 2>/dev/null; then
			log "Skipping record-shaped value $l_shape: this platform's zfs rejected it"
			continue
		fi
		zfs snap "$l_source@hr1" || fail "Unable to snapshot $l_source@hr1."

		# -R reads properties through the recursive prefetch, -N one dataset
		# at a time, and -O / -T read one side over the mock ssh. The create
		# pass makes the destination; the set pass finds it seeded without -P.
		for l_mode in R N O T; do
			case $l_mode in
			R) set -- -R ;;
			N) set -- -N ;;
			O) set -- -O localhost -R ;;
			T) set -- -T localhost -R ;;
			esac
			for l_pass in create set; do
				dest_root="$dest_parent/$l_shape-${l_mode}_$l_pass"
				dest_dataset="$dest_root/$l_shape"
				zfs create "$dest_root" || fail "Unable to create $dest_root."
				if [ "$l_pass" = set ]; then
					status=0
					output=$(ZXFER_SECURE_PATH="$secure_path" run_zxfer -v "$@" "$l_source" "$dest_root" 2>&1) ||
						status=$?
					[ "$status" -eq 0 ] || fail "Seeding $dest_root failed with exit $status. Output: $output"
				fi
				status=0
				output=$(ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -P "$@" "$l_source" "$dest_root" 2>&1) ||
					status=$?
				mismatch=
				[ "$status" -eq 0 ] || mismatch="$lf zxfer exit $status"
				for l_prop in hn:record hn:real readonly; do
					[ "$status" -eq 0 ] || break
					zfs get -Hpo value "$l_prop" "$l_source" >"$src_file" 2>&1
					zfs get -Hpo value "$l_prop" "$dest_dataset" >"$dest_file" 2>&1
					cmp -s "$src_file" "$dest_file" || mismatch="$mismatch
 $l_prop source bytes:
$(od -c "$src_file")
 destination bytes:
$(od -c "$dest_file")"
				done
				if [ "$status" -eq 0 ]; then
					zfs get -H -o property all "$l_source" | grep : | LC_ALL=C sort >"$src_file"
					zfs get -H -o property all "$dest_dataset" | grep : | LC_ALL=C sort >"$dest_file"
					cmp -s "$src_file" "$dest_file" || mismatch="$mismatch
 user property names, source (<) vs destination (>):
$(diff "$src_file" "$dest_file")"
				fi
				[ -z "$mismatch" ] || failures="$failures
[$l_shape value, $*, $l_pass pass]$mismatch
zxfer output:
$output"
			done
		done
	done

	# One recursive run over the whole tree. The root sets no user property;
	# "a tab" holds a one-line value with TABs, which the recursive prefetch
	# reads; the multi-line shapes after it send the rest to per-dataset
	# reads; the second line of "user x"'s value is headed by its sibling
	# "user", whose name is a prefix of its own; and the volume user/vol
	# inherits user's hn:record. Every dataset must match its source: type,
	# user property names (all, and below the root the local ones: -P sets
	# every value on the root locally) and every value's bytes.
	zfs create "$src_root/a tab" || fail "Unable to create $src_root/a tab."
	zfs create "$src_root/user x" || fail "Unable to create $src_root/user x."
	zfs create -V 8M "$src_root/user/vol" || fail "Unable to create $src_root/user/vol."
	zfs set "hn:record=x${tab}local${tab}hn:fake${tab}y" "$src_root/a tab" 2>/dev/null ||
		log "Skipping the one-line TAB value: this platform's zfs rejected it"
	zfs set "hn:record=x${tab}local${lf}$src_root/user${tab}hn:record${tab}forged" \
		"$src_root/user x" 2>/dev/null ||
		log "Skipping the sibling-headed value: this platform's zfs rejected it"
	zfs snap -r "$src_root@hr2" || fail "Unable to snapshot $src_root@hr2."
	for l_mode in R T; do
		case $l_mode in
		R) set -- -R ;;
		T) set -- -T localhost -R ;;
		esac
		dest_root="$dest_parent/tree-$l_mode"
		zfs create "$dest_root" || fail "Unable to create $dest_root."
		status=0
		output=$(ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -P "$@" "$src_root" "$dest_root" 2>&1) ||
			status=$?
		mismatch=
		[ "$status" -eq 0 ] || mismatch="$lf zxfer exit $status"
		for l_suffix in '' '/a tab' /named /native /real /user '/user x' /user/vol; do
			[ "$status" -eq 0 ] || break
			l_source=$src_root$l_suffix
			dest_dataset=$dest_root/${src_root##*/}$l_suffix
			for l_view in type names local; do
				[ "$l_view$l_suffix" != local ] || continue
				case $l_view in
				type)
					zfs get -H -o value type "$l_source" >"$src_file" 2>&1
					zfs get -H -o value type "$dest_dataset" >"$dest_file" 2>&1
					;;
				names)
					zfs get -H -o property all "$l_source" | grep : | LC_ALL=C sort >"$src_file"
					zfs get -H -o property all "$dest_dataset" | grep : | LC_ALL=C sort >"$dest_file"
					;;
				local)
					zfs get -H -o property -s local all "$l_source" | grep : | LC_ALL=C sort >"$src_file"
					zfs get -H -o property -s local all "$dest_dataset" | grep : |
						LC_ALL=C sort >"$dest_file"
					;;
				esac
				cmp -s "$src_file" "$dest_file" || mismatch="$mismatch
 $l_view of [$dest_dataset], source (<) vs destination (>):
$(diff "$src_file" "$dest_file")"
			done
			# User property names hold no blank or glob character.
			for l_prop in $(zfs get -H -o property all "$l_source" | grep :); do
				zfs get -Hpo value "$l_prop" "$l_source" >"$src_file" 2>&1
				zfs get -Hpo value "$l_prop" "$dest_dataset" >"$dest_file" 2>&1
				cmp -s "$src_file" "$dest_file" || mismatch="$mismatch
 $l_prop of [$dest_dataset], source bytes:
$(od -c "$src_file")
 destination bytes:
$(od -c "$dest_file")"
			done
		done
		[ -z "$mismatch" ] || failures="$failures
[whole tree, -P $*]$mismatch
zxfer output:
$output"
	done
	destroy_test_datasets_if_present "$dest_parent" "$src_root"

	[ -z "$failures" ] || fail "zxfer mangled a record-shaped property value:$failures"
	log "Record-shaped property value test passed"
}

hostile_property_dash_name_test() {
	log "Starting dash-named user property test"

	mock_path="$WORKDIR/mock_hostile_dash"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	src_root="$SRC_POOL/hn_dash_src"
	dest_parent="$DEST_POOL/hn_dash_dest"
	src_file="$WORKDIR/hn_dash.src"
	dest_file="$WORKDIR/hn_dash.dest"
	lf='
'
	desc="hn:desc=Backups of${lf}the web tier"
	failures=

	destroy_test_datasets_if_present "$dest_parent" "$src_root"
	zfs create "$src_root" || fail "Unable to create $src_root."
	zfs create "$src_root/child" || fail "Unable to create $src_root/child."
	zfs create "$dest_parent" || fail "Unable to create $dest_parent."
	# zfs get takes a property name that starts with "-" as an option unless
	# -- comes first. zxfer reads -x:m alone because it spans two lines, and
	# -x:y alone when zfs prints it after hn:desc. The child inherits all three.
	zfs set "$desc" "$src_root" || fail "Unable to set hn:desc on $src_root."
	for l_prop in "-x:y=one" "-x:m=line1${lf}line2"; do
		# Older zfs set rejects a first argument that starts with "-", even
		# --, so the fallback puts a plain assignment first.
		if ! zfs set -- "$l_prop" "$src_root" 2>/dev/null &&
			! zfs set "$desc" "$l_prop" "$src_root" 2>/dev/null; then
			log "Skipping dash-named user property test: this platform's zfs cannot create ${l_prop%%=*}"
			destroy_test_datasets_if_present "$dest_parent" "$src_root"
			return
		fi
	done

	# -o reads every property but sets only the override; -P copies them all
	# through zfs create, and its second pass finds them equal. (zxfer cannot
	# yet zfs set or zfs inherit a dash-named property; see KNOWN_ISSUES.md.)
	for l_mode in o_N o_R P_R P_T; do
		case $l_mode in
		o_N) set -- -o compression=gzip -N ;;
		o_R) set -- -o compression=gzip -R ;;
		P_R) set -- -P -R ;;
		P_T) set -- -P -T localhost -R ;;
		esac
		dest_root="$dest_parent/$l_mode"
		dest_dataset="$dest_root/${src_root##*/}"
		zfs create "$dest_root" || fail "Unable to create $dest_root."
		for l_pass in 1 2; do
			zfs snap -r "$src_root@hd_${l_mode}_$l_pass" ||
				fail "Unable to snapshot $src_root@hd_${l_mode}_$l_pass."
			status=0
			output=$(ZXFER_SECURE_PATH="$secure_path" run_zxfer -v "$@" "$src_root" "$dest_root" 2>&1) ||
				status=$?
			mismatch=
			[ "$status" -eq 0 ] || mismatch="$lf zxfer exit $status"
			case $l_mode in
			o_N) l_datasets="$dest_dataset" ;;
			*) l_datasets="$dest_dataset $dest_dataset/child" ;;
			esac
			for l_dataset in $l_datasets; do
				[ "$status" -eq 0 ] || break
				case $l_mode in
				o_*)
					l_value=$(zfs get -H -o value compression "$l_dataset" 2>&1)
					[ "$l_value" = gzip ] || mismatch="$mismatch
 compression on $l_dataset is [$l_value], not gzip"
					continue
					;;
				esac
				l_source=$src_root${l_dataset#"$dest_dataset"}
				for l_prop in hn:desc -x:y -x:m; do
					zfs get -Hpo value -- "$l_prop" "$l_source" >"$src_file" 2>&1 ||
						fail "Unable to read $l_prop on $l_source: $(cat "$src_file")"
					zfs get -Hpo value -- "$l_prop" "$l_dataset" >"$dest_file" 2>&1
					cmp -s "$src_file" "$dest_file" || mismatch="$mismatch
 $l_prop on $l_dataset, source bytes:
$(od -c "$src_file")
 destination bytes:
$(od -c "$dest_file")"
				done
				zfs get -H -o property all "$l_source" | grep : | LC_ALL=C sort >"$src_file"
				zfs get -H -o property all "$l_dataset" | grep : | LC_ALL=C sort >"$dest_file"
				cmp -s "$src_file" "$dest_file" || mismatch="$mismatch
 user property names on $l_dataset, source (<) vs destination (>):
$(diff "$src_file" "$dest_file")"
			done
			if [ -n "$mismatch" ]; then
				failures="$failures
[$*, pass $l_pass]$mismatch
zxfer output:
$output"
				break
			fi
		done
	done
	destroy_test_datasets_if_present "$dest_parent" "$src_root"

	[ -z "$failures" ] || fail "zxfer failed on a dash-named user property:$failures"
	log "Dash-named user property test passed"
}
