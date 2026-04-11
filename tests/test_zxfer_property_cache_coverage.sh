#!/bin/sh
#
# Additional shunit2 coverage for zxfer_property_cache.sh branches that are hard
# to credit through subshell-heavy property reconcile tests under bash xtrace.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_property_cache_coverage"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"
	TMPDIR=$TEST_TMPDIR
	export TMPDIR
	g_cmd_awk=${g_cmd_awk:-$(command -v awk 2>/dev/null || printf '%s\n' awk)}
	g_LZFS="/sbin/zfs"
	g_RZFS="/sbin/zfs"
	g_option_R_recursive=""
	g_option_P_transfer_property=0
	g_option_o_override_property=""
	g_option_V_very_verbose=0
	g_initial_source="tank/src"
	g_destination="backup/dst"
	g_recursive_source_dataset_list=""
	g_recursive_source_list=""
	g_recursive_dest_list=""
	g_zxfer_profile_normalized_property_reads_source=0
	g_zxfer_profile_normalized_property_reads_destination=0
	g_zxfer_profile_normalized_property_reads_other=0
	g_zxfer_profile_required_property_backfill_gets=0
	g_zxfer_property_stage_file_read_result=""
	zxfer_reset_property_iteration_caches
}

zxfer_test_write_property_cache_object() {
	l_cache_path=$1
	l_cache_kind=$2
	l_cache_payload=$3

	mkdir -p "${l_cache_path%/*}" || fail "Unable to create cache-object parent directory."
	zxfer_write_cache_object_file_atomically \
		"$l_cache_path" "$l_cache_kind" "" "$l_cache_payload" >/dev/null ||
		fail "Unable to write property cache test fixture."
}

test_zxfer_property_cache_capture_and_read_helpers_cover_success_paths_in_current_shell() {
	cache_path="$TEST_TMPDIR/read-cache.entry"

	zxfer_capture_serialized_property_records "$(printf '%s\n' \
		"compression	lz4	local" \
		"readonly	off	inherited")"
	capture_status=$?

	zxfer_test_write_property_cache_object \
		"$cache_path" \
		"$ZXFER_PROPERTY_CACHE_OBJECT_KIND_NORMALIZED" \
		"compression=lz4=local"
	zxfer_read_property_cache_file "$cache_path" "$ZXFER_PROPERTY_CACHE_OBJECT_KIND_NORMALIZED"
	read_status=$?

	assertEquals "Serialized property capture should convert tab-delimited records into the cache encoding format." \
		"0" "$capture_status"
	assertEquals "Serialized property capture should preserve the encoded property list in the shared scratch result." \
		"compression=lz4=local,readonly=off=inherited" "$g_zxfer_serialized_property_records_result"
	assertEquals "Property-cache reads should succeed for readable cache entries." \
		"0" "$read_status"
	assertEquals "Successful property-cache reads should populate the shared cache-read scratch result." \
		"compression=lz4=local" "$g_zxfer_property_cache_read_result"
}

test_zxfer_capture_serialized_property_records_reports_tempfile_failures_in_current_shell() {
	zxfer_get_temp_file() {
		return 1
	}

	set +e
	zxfer_capture_serialized_property_records "compression	lz4	local" >/dev/null 2>&1
	status=$?
	set -e

	assertEquals "Serialized property capture should fail closed when it cannot allocate a staging file." \
		"1" "$status"
}

test_zxfer_capture_serialized_property_records_reports_readback_failures_in_current_shell() {
	serialized_output_file="$TEST_TMPDIR/serialized_property_readback_failure.out"
	err_log="$TEST_TMPDIR/serialized_property_readback_failure.err"
	g_zxfer_serialized_property_records_result="stale-serialized"

	zxfer_get_temp_file() {
		g_zxfer_temp_file_result="$serialized_output_file"
		: >"$g_zxfer_temp_file_result"
	}

	cat() {
		if [ "$1" = "$serialized_output_file" ]; then
			printf '%s\n' "serialized readback failed" >&2
			printf '%s\n' "compression=lz4=local"
			return 26
		fi
		command cat "$@"
	}

	set +e
	zxfer_capture_serialized_property_records "compression	lz4	local" >/dev/null 2>"$err_log"
	status=$?
	set -e

	unset -f zxfer_get_temp_file
	unset -f cat

	assertEquals "Serialized property capture should fail closed when the staged serializer output cannot be read back." \
		"26" "$status"
	assertEquals "Serialized property capture should not publish stale or partial serializer scratch after a readback failure." \
		"" "$g_zxfer_serialized_property_records_result"
	assertEquals "Serialized property capture should preserve the staged readback diagnostic." \
		"serialized readback failed" "$(cat "$err_log")"
}

test_zxfer_property_cache_path_and_invalidation_helpers_cover_current_shell_paths() {
	l_dataset="tank/src"
	g_zxfer_property_cache_dir="$TEST_TMPDIR/property-cache"
	mkdir -p "$g_zxfer_property_cache_dir/normalized/source" \
		"$g_zxfer_property_cache_dir/normalized/destination" \
		"$g_zxfer_property_cache_dir/required/source" \
		"$g_zxfer_property_cache_dir/required/destination"

	zxfer_property_cache_dataset_path normalized source "$l_dataset" >/dev/null
	source_normalized_path=$g_zxfer_property_cache_path
	zxfer_property_cache_property_path required source "$l_dataset" "compression" >/dev/null
	source_required_path=$g_zxfer_property_cache_path
	zxfer_property_cache_dataset_path normalized destination "$l_dataset" >/dev/null
	destination_normalized_path=$g_zxfer_property_cache_path
	zxfer_property_cache_property_path required destination "$l_dataset" "compression" >/dev/null
	destination_required_path=$g_zxfer_property_cache_path
	zxfer_test_write_property_cache_object \
		"$source_normalized_path" \
		"$ZXFER_PROPERTY_CACHE_OBJECT_KIND_NORMALIZED" \
		"cached"
	zxfer_test_write_property_cache_object \
		"$source_required_path" \
		"$ZXFER_PROPERTY_CACHE_OBJECT_KIND_REQUIRED" \
		"cached"
	zxfer_test_write_property_cache_object \
		"$destination_normalized_path" \
		"$ZXFER_PROPERTY_CACHE_OBJECT_KIND_NORMALIZED" \
		"cached"
	zxfer_test_write_property_cache_object \
		"$destination_required_path" \
		"$ZXFER_PROPERTY_CACHE_OBJECT_KIND_REQUIRED" \
		"cached"

	zxfer_invalidate_dataset_property_cache source "$l_dataset"
	g_zxfer_destination_property_tree_prefetch_state=1
	zxfer_reset_destination_property_iteration_cache
	source_normalized_exists=0
	source_required_exists=0
	destination_normalized_exists=0
	destination_required_exists=0
	if [ -e "$source_normalized_path" ]; then
		source_normalized_exists=1
	fi
	if [ -e "$source_required_path" ]; then
		source_required_exists=1
	fi
	if [ -e "$destination_normalized_path" ]; then
		destination_normalized_exists=1
	fi
	if [ -e "$destination_required_path" ]; then
		destination_required_exists=1
	fi

	assertEquals "Source dataset cache invalidation should remove the matching normalized-property cache entry." \
		"0" "$source_normalized_exists"
	assertEquals "Source dataset cache invalidation should remove the matching required-property cache subtree." \
		"0" "$source_required_exists"
	assertEquals "Destination iteration cache resets should clear the destination normalized-property cache subtree." \
		"0" "$destination_normalized_exists"
	assertEquals "Destination iteration cache resets should clear the destination required-property cache subtree." \
		"0" "$destination_required_exists"
	assertEquals "Destination iteration cache resets should rearm destination property-tree prefetch for the next dataset." \
		"0" "${g_zxfer_destination_property_tree_prefetch_state:-1}"
}

test_zxfer_property_cache_property_path_reports_property_key_failures_in_current_shell() {
	g_zxfer_property_cache_dir="$TEST_TMPDIR/property-cache-failure"
	zxfer_property_cache_encode_key() {
		if [ "$1" = "compression" ]; then
			return 1
		fi
		printf '%s\n' "encoded"
	}

	set +e
	zxfer_property_cache_property_path required source "tank/src" "compression" >/dev/null 2>&1
	status=$?
	set -e

	assertEquals "Required-property cache path helpers should fail closed when property key encoding fails." \
		"1" "$status"
}

test_zxfer_property_tree_prefetch_list_mark_and_cleanup_helpers_cover_current_shell_paths() {
	source_dataset_file="$TEST_TMPDIR/source-datasets.out"
	source_list_file="$TEST_TMPDIR/source-list.out"
	source_root_file="$TEST_TMPDIR/source-root.out"
	destination_list_file="$TEST_TMPDIR/destination-list.out"
	first_cleanup_file="$TEST_TMPDIR/cleanup-1"
	second_cleanup_file="$TEST_TMPDIR/cleanup-2"

	g_recursive_source_dataset_list="tank/src tank/src/child"
	zxfer_get_property_tree_prefetch_dataset_list source >"$source_dataset_file"

	g_recursive_source_dataset_list=""
	g_recursive_source_list="tank/src tank/src/child"
	zxfer_get_property_tree_prefetch_dataset_list source >"$source_list_file"

	g_recursive_source_list=""
	g_initial_source="tank/src"
	zxfer_get_property_tree_prefetch_dataset_list source >"$source_root_file"

	g_recursive_dest_list="backup/dst backup/dst/child"
	zxfer_get_property_tree_prefetch_dataset_list destination >"$destination_list_file"

	set +e
	zxfer_get_property_tree_prefetch_dataset_list other >/dev/null 2>&1
	invalid_status=$?
	set -e

	: >"$first_cleanup_file"
	: >"$second_cleanup_file"
	zxfer_cleanup_recursive_property_prefetch_stage_files "$first_cleanup_file" "$second_cleanup_file"
	zxfer_mark_recursive_property_prefetch_failed source
	zxfer_mark_recursive_property_prefetch_failed destination
	first_cleanup_exists=0
	second_cleanup_exists=0
	if [ -e "$first_cleanup_file" ]; then
		first_cleanup_exists=1
	fi
	if [ -e "$second_cleanup_file" ]; then
		second_cleanup_exists=1
	fi

	assertEquals "Explicit recursive source dataset lists should be preferred when building prefetch targets." \
		"tank/src
tank/src/child" "$(cat "$source_dataset_file")"
	assertEquals "Recursive source-list fallbacks should preserve newline-delimited dataset names." \
		"tank/src
tank/src/child" "$(cat "$source_list_file")"
	assertEquals "Property-tree prefetch dataset selection should finally fall back to the initial source root." \
		"tank/src" "$(cat "$source_root_file")"
	assertEquals "Recursive destination lists should be converted to newline-delimited dataset names for destination prefetch." \
		"backup/dst
backup/dst/child" "$(cat "$destination_list_file")"
	assertEquals "Unknown property-tree prefetch dataset-list sides should fail immediately." \
		"1" "$invalid_status"
	assertEquals "Recursive property prefetch cleanup helpers should remove every staged file they are handed." \
		"0" "$first_cleanup_exists"
	assertEquals "Recursive property prefetch cleanup helpers should remove later staged files too." \
		"0" "$second_cleanup_exists"
	assertEquals "Source-side recursive property prefetch failures should disable source prefetch for the iteration." \
		"2" "${g_zxfer_source_property_tree_prefetch_state:-0}"
	assertEquals "Destination-side recursive property prefetch failures should disable destination prefetch for the iteration." \
		"2" "${g_zxfer_destination_property_tree_prefetch_state:-0}"
}

test_zxfer_group_recursive_property_tree_by_dataset_covers_success_paths_in_current_shell() {
	filter_file="$TEST_TMPDIR/property-filter.list"
	tree_file="$TEST_TMPDIR/property-tree.tsv"
	output_file="$TEST_TMPDIR/property-grouped.tsv"

	printf '%s\n' "tank/src" "tank/src/child" >"$filter_file"
	cat >"$tree_file" <<'EOF'
tank/src	compression	lz4	local
tank/src	readonly	off	local
tank/src/child	compression	gzip	inherited
tank/src/child	readonly	off	inherited
tank/skip	compression	off	local
EOF

	zxfer_group_recursive_property_tree_by_dataset "$filter_file" "$tree_file" >"$output_file"
	status=$?

	assertEquals "Recursive property grouping should succeed for filtered property trees." \
		"0" "$status"
	assertEquals "Recursive property grouping should retain dataset order and merge encoded property records per dataset." \
		"tank/src	compression=lz4=local,readonly=off=local
tank/src/child	compression=gzip=inherited,readonly=off=inherited" "$(cat "$output_file")"
}

test_zxfer_prefetch_recursive_normalized_properties_source_success_in_current_shell() {
	log="$TEST_TMPDIR/prefetch-source.calls"
	: >"$log"
	g_option_V_very_verbose=1
	g_zxfer_source_property_tree_prefetch_root="tank/src"
	g_zxfer_source_property_tree_prefetch_zfs_cmd="/sbin/zfs"
	g_zxfer_source_property_tree_prefetch_state=0
	g_recursive_source_dataset_list="tank/src

tank/src/child"

	zxfer_run_zfs_cmd_for_spec() {
		printf '%s %s %s %s %s %s\n' "$1" "$2" "$3" "$4" "$5" "$6" >>"$log"
		case "$4" in
		-Hpo)
			printf '%s\n' \
				"tank/src	compression	lz4	local" \
				"tank/src	readonly	off	local" \
				"tank/src/child	compression	gzip	inherited" \
				"tank/src/child	readonly	off	inherited"
			return 0
			;;
		-Ho)
			printf '%s\n' \
				"tank/src	compression	lz4	local" \
				"tank/src	readonly	off	local" \
				"tank/src/child	compression	gzip	inherited" \
				"tank/src/child	readonly	off	inherited"
			return 0
			;;
		esac
		return 1
	}

	zxfer_prefetch_recursive_normalized_properties source
	status=$?
	zxfer_property_cache_dataset_path normalized source "tank/src" >/dev/null
	root_cache_path=$g_zxfer_property_cache_path
	zxfer_property_cache_dataset_path normalized source "tank/src/child" >/dev/null
	child_cache_path=$g_zxfer_property_cache_path
	zxfer_read_property_cache_file "$root_cache_path" "$ZXFER_PROPERTY_CACHE_OBJECT_KIND_NORMALIZED"
	root_cache_payload=$g_zxfer_property_cache_read_result
	zxfer_read_property_cache_file "$child_cache_path" "$ZXFER_PROPERTY_CACHE_OBJECT_KIND_NORMALIZED"
	child_cache_payload=$g_zxfer_property_cache_read_result

	assertEquals "Recursive source property-tree prefetch should succeed for matching source dataset trees." \
		"0" "$status"
	assertEquals "Successful recursive source property-tree prefetch should leave source prefetch ready for reuse." \
		"1" "${g_zxfer_source_property_tree_prefetch_state:-0}"
	assertEquals "Successful recursive source property-tree prefetch should count as a single source normalized-property read." \
		"1" "${g_zxfer_profile_normalized_property_reads_source:-0}"
	assertEquals "Recursive source property-tree prefetch should store the root dataset's normalized property list in the cache." \
		"compression=lz4=local,readonly=off=local" "$root_cache_payload"
	assertEquals "Recursive source property-tree prefetch should also store descendant normalized property lists in the cache." \
		"compression=gzip=inherited,readonly=off=inherited" "$child_cache_payload"
	assertEquals "Recursive source property-tree prefetch should use exactly one machine-readable and one human-readable recursive zfs get." \
		"2" "$(awk 'END {print NR + 0}' "$log")"
}

test_zxfer_prefetch_recursive_normalized_properties_destination_success_in_current_shell() {
	log="$TEST_TMPDIR/prefetch-destination.calls"
	: >"$log"
	g_option_V_very_verbose=1
	g_zxfer_destination_property_tree_prefetch_root="backup/dst"
	g_zxfer_destination_property_tree_prefetch_zfs_cmd="/sbin/zfs"
	g_zxfer_destination_property_tree_prefetch_state=0
	g_recursive_dest_list="backup/dst
backup/dst/child"

	zxfer_run_zfs_cmd_for_spec() {
		printf '%s %s %s %s %s %s\n' "$1" "$2" "$3" "$4" "$5" "$6" >>"$log"
		case "$4" in
		-Hpo)
			printf '%s\n' \
				"backup/dst	compression	lz4	local" \
				"backup/dst	readonly	off	local" \
				"backup/dst/child	compression	gzip	inherited" \
				"backup/dst/child	readonly	off	inherited"
			return 0
			;;
		-Ho)
			printf '%s\n' \
				"backup/dst	compression	lz4	local" \
				"backup/dst	readonly	off	local" \
				"backup/dst/child	compression	gzip	inherited" \
				"backup/dst/child	readonly	off	inherited"
			return 0
			;;
		esac
		return 1
	}

	zxfer_prefetch_recursive_normalized_properties destination
	status=$?
	zxfer_property_cache_dataset_path normalized destination "backup/dst/child" >/dev/null
	child_cache_path=$g_zxfer_property_cache_path
	zxfer_read_property_cache_file "$child_cache_path" "$ZXFER_PROPERTY_CACHE_OBJECT_KIND_NORMALIZED"
	child_cache_payload=$g_zxfer_property_cache_read_result

	assertEquals "Recursive destination property-tree prefetch should succeed for matching destination dataset trees." \
		"0" "$status"
	assertEquals "Successful recursive destination property-tree prefetch should leave destination prefetch ready for reuse." \
		"1" "${g_zxfer_destination_property_tree_prefetch_state:-0}"
	assertEquals "Successful recursive destination property-tree prefetch should count as a single destination normalized-property read." \
		"1" "${g_zxfer_profile_normalized_property_reads_destination:-0}"
	assertEquals "Recursive destination property-tree prefetch should store descendant normalized property lists in the cache." \
		"compression=gzip=inherited,readonly=off=inherited" "$child_cache_payload"
	assertEquals "Recursive destination property-tree prefetch should use exactly one machine-readable and one human-readable recursive zfs get." \
		"2" "$(awk 'END {print NR + 0}' "$log")"
}

test_zxfer_maybe_prefetch_recursive_normalized_properties_covers_success_and_failure_paths_in_current_shell() {
	source_cache_path="$TEST_TMPDIR/maybe-source.cache"
	destination_cache_path="$TEST_TMPDIR/maybe-destination.cache"
	log="$TEST_TMPDIR/maybe-prefetch.calls"
	: >"$log"

	g_zxfer_source_property_tree_prefetch_root="tank/src"
	g_zxfer_source_property_tree_prefetch_zfs_cmd="/source/zfs"
	g_recursive_source_dataset_list="tank/src"
	g_zxfer_destination_property_tree_prefetch_root="backup/dst"
	g_zxfer_destination_property_tree_prefetch_zfs_cmd="/dest/zfs"
	g_recursive_dest_list="backup/dst"

	zxfer_prefetch_recursive_normalized_properties() {
		printf '%s\n' "$1" >>"$log"
		case "$1" in
		source)
			printf '%s\n' "compression=lz4=local" >"$source_cache_path"
			;;
		destination)
			printf '%s\n' "compression=lz4=local" >"$destination_cache_path"
			;;
		esac
		return 0
	}
	zxfer_property_cache_dataset_path() {
		case "$2" in
		source)
			g_zxfer_property_cache_path=$source_cache_path
			;;
		destination)
			g_zxfer_property_cache_path=$destination_cache_path
			;;
		esac
		printf '%s\n' "$g_zxfer_property_cache_path"
	}

	zxfer_maybe_prefetch_recursive_normalized_properties "tank/src" "/source/zfs" source >/dev/null 2>&1
	source_status=$?
	zxfer_maybe_prefetch_recursive_normalized_properties "backup/dst" "/dest/zfs" destination >/dev/null 2>&1
	destination_status=$?

	set +e
	zxfer_maybe_prefetch_recursive_normalized_properties "backup/dst" "/dest/zfs" other >/dev/null 2>&1
	invalid_side_status=$?
	zxfer_prefetch_recursive_normalized_properties() {
		return 1
	}
	zxfer_maybe_prefetch_recursive_normalized_properties "tank/src" "/source/zfs" source >/dev/null 2>&1
	prefetch_failure_status=$?
	zxfer_prefetch_recursive_normalized_properties() {
		return 0
	}
	zxfer_property_cache_dataset_path() {
		return 1
	}
	zxfer_maybe_prefetch_recursive_normalized_properties "backup/dst" "/dest/zfs" destination >/dev/null 2>&1
	cache_path_failure_status=$?
	set -e

	assertEquals "Source-side recursive property prefetch should succeed when the requested dataset stays within the recursive source tree." \
		"0" "$source_status"
	assertEquals "Destination-side recursive property prefetch should succeed when the requested dataset stays within the recursive destination tree." \
		"0" "$destination_status"
	assertEquals "Unknown recursive property prefetch sides should fail immediately." \
		"1" "$invalid_side_status"
	assertEquals "Recursive property prefetch lookups should fail closed when the prefetch pass itself fails." \
		"1" "$prefetch_failure_status"
	assertEquals "Recursive property prefetch lookups should fail closed when the cache path cannot be derived after prefetch." \
		"1" "$cache_path_failure_status"
	assertEquals "Successful maybe-prefetch lookups should invoke the recursive source and destination prefetch helpers once each." \
		"source
destination" "$(cat "$log")"
}

test_zxfer_load_normalized_dataset_properties_covers_cache_prefetch_and_live_paths_in_current_shell() {
	cache_hit_path="$TEST_TMPDIR/normalized-cache-hit.entry"
	prefetch_hit_path="$TEST_TMPDIR/normalized-prefetch-hit.entry"
	live_log="$TEST_TMPDIR/normalized-live.calls"
	: >"$live_log"
	zxfer_test_write_property_cache_object \
		"$cache_hit_path" \
		"$ZXFER_PROPERTY_CACHE_OBJECT_KIND_NORMALIZED" \
		"compression=cached=local"
	g_option_V_very_verbose=1

	zxfer_property_cache_dataset_path() {
		case "$3" in
		tank/cache)
			g_zxfer_property_cache_path=$cache_hit_path
			;;
		tank/prefetch)
			g_zxfer_property_cache_path=$prefetch_hit_path
			;;
		*)
			g_zxfer_property_cache_path="$TEST_TMPDIR/$(printf '%s' "$3" | tr '/:' '__').cache"
			;;
		esac
		printf '%s\n' "$g_zxfer_property_cache_path"
	}
	zxfer_maybe_prefetch_recursive_normalized_properties() {
		if [ "$1" = "tank/prefetch" ]; then
			zxfer_test_write_property_cache_object \
				"$prefetch_hit_path" \
				"$ZXFER_PROPERTY_CACHE_OBJECT_KIND_NORMALIZED" \
				"compression=prefetched=local"
			return 0
		fi
		return 1
	}
	zxfer_run_zfs_cmd_for_spec() {
		printf '%s|%s|%s\n' "$1" "$4" "$6" >>"$live_log"
		printf '%s\n' "compression	lz4	local" "readonly	off	local"
		return 0
	}

	zxfer_load_normalized_dataset_properties "tank/cache" "/sbin/zfs" destination
	cache_props=$g_zxfer_normalized_dataset_properties
	cache_hit=$g_zxfer_normalized_dataset_properties_cache_hit

	zxfer_load_normalized_dataset_properties "tank/prefetch" "/sbin/zfs" source
	prefetch_props=$g_zxfer_normalized_dataset_properties
	prefetch_hit=$g_zxfer_normalized_dataset_properties_cache_hit

	g_LZFS="/default/zfs"
	zxfer_load_normalized_dataset_properties "tank/live-source" "" source
	live_source_props=$g_zxfer_normalized_dataset_properties

	zxfer_load_normalized_dataset_properties "backup/live-destination" "/dest/zfs" destination
	live_destination_props=$g_zxfer_normalized_dataset_properties

	zxfer_load_normalized_dataset_properties "tank/live-other" "/other/zfs" other
	live_other_props=$g_zxfer_normalized_dataset_properties

	assertEquals "Normalized-property cache hits should return the cached property payload without live probes." \
		"compression=cached=local" "$cache_props"
	assertEquals "Normalized-property cache hits should mark the lookup as a cache hit." \
		"1" "$cache_hit"
	assertEquals "Prefetched normalized-property cache entries should be returned after recursive prefetch materializes the requested dataset." \
		"compression=prefetched=local" "$prefetch_props"
	assertEquals "Prefetched normalized-property cache entries should also mark the lookup as a cache hit." \
		"1" "$prefetch_hit"
	assertEquals "Live source normalized-property reads should parse and preserve the human-readable property set." \
		"compression=lz4=local,readonly=off=local" "$live_source_props"
	assertEquals "Live destination normalized-property reads should parse and preserve the human-readable property set." \
		"compression=lz4=local,readonly=off=local" "$live_destination_props"
	assertEquals "Live non-source, non-destination normalized-property reads should still parse and preserve the property set." \
		"compression=lz4=local,readonly=off=local" "$live_other_props"
	assertEquals "Live normalized-property reads should increment the source-side profile counter exactly once." \
		"1" "${g_zxfer_profile_normalized_property_reads_source:-0}"
	assertEquals "Live normalized-property reads should increment the destination-side profile counter exactly once." \
		"1" "${g_zxfer_profile_normalized_property_reads_destination:-0}"
	assertEquals "Live normalized-property reads should increment the generic profile counter exactly once." \
		"1" "${g_zxfer_profile_normalized_property_reads_other:-0}"
	assertEquals "Live normalized-property reads should issue one machine-readable and one human-readable zfs get per uncached lookup." \
		"6" "$(awk 'END {print NR + 0}' "$live_log")"
}

test_zxfer_get_required_property_probe_covers_cache_live_and_unsupported_paths_in_current_shell() {
	cache_hit_path="$TEST_TMPDIR/required-cache-hit.entry"
	calls_log="$TEST_TMPDIR/required-live.calls"
	: >"$calls_log"
	zxfer_test_write_property_cache_object \
		"$cache_hit_path" \
		"$ZXFER_PROPERTY_CACHE_OBJECT_KIND_REQUIRED" \
		"casesensitivity=sensitive=local"
	g_option_V_very_verbose=1

	zxfer_property_cache_property_path() {
		case "$3:$4" in
		tank/cache:casesensitivity)
			g_zxfer_property_cache_path=$cache_hit_path
			;;
		*)
			g_zxfer_property_cache_path="$TEST_TMPDIR/$(printf '%s_%s' "$3" "$4" | tr '/:' '__').cache"
			;;
		esac
		printf '%s\n' "$g_zxfer_property_cache_path"
	}
	zxfer_run_zfs_cmd_for_spec() {
		printf '%s|%s|%s\n' "$1" "$5" "$6" >>"$calls_log"
		if [ "$5" = "casesensitivity" ]; then
			printf '%s\n' "casesensitivity	sensitive	local"
			return 0
		fi
		printf '%s\n' "property does not apply to datasets of this type"
		return 1
	}

	zxfer_get_required_property_probe "tank/cache" "casesensitivity" "/sbin/zfs" source
	cache_result=$g_zxfer_required_property_probe_result

	g_LZFS="/default/zfs"
	zxfer_get_required_property_probe "tank/live" "casesensitivity" "" source
	live_result=$g_zxfer_required_property_probe_result

	zxfer_get_required_property_probe "tank/vol" "utf8only" "/sbin/zfs" other
	unsupported_result=$g_zxfer_required_property_probe_result

	assertEquals "Required-property cache hits should return the cached property payload without live probes." \
		"casesensitivity=sensitive=local" "$cache_result"
	assertEquals "Required-property probes should fall back to g_LZFS when no zfs command is supplied." \
		"casesensitivity=sensitive=local" "$live_result"
	assertEquals "Unsupported required-property probes should use the unsupported sentinel result." \
		"__ZXFER_REQUIRED_PROPERTY_UNSUPPORTED__" "$unsupported_result"
	assertEquals "Live required-property probes should increment the must-create backfill counter once per uncached probe." \
		"2" "${g_zxfer_profile_required_property_backfill_gets:-0}"
	assertEquals "Live required-property probes should only invoke zfs for uncached lookups." \
		"2" "$(awk 'END {print NR + 0}' "$calls_log")"
	assertContains "Explicit required-property probes should honor the default local zfs command fallback." \
		"$(cat "$calls_log")" "/default/zfs|casesensitivity|tank/live"
}

test_zxfer_property_cache_wrapper_helpers_cover_current_shell_paths() {
	normalized_out="$TEST_TMPDIR/wrapper-normalized.out"
	required_out="$TEST_TMPDIR/wrapper-required.out"
	destination_out="$TEST_TMPDIR/wrapper-destination.out"

	zxfer_get_required_property_probe() {
		case "$2" in
		casesensitivity)
			g_zxfer_required_property_probe_result="casesensitivity=sensitive=local"
			return 0
			;;
		utf8only)
			g_zxfer_required_property_probe_result="__ZXFER_REQUIRED_PROPERTY_UNSUPPORTED__"
			return 0
			;;
		esac
		return 1
	}
	zxfer_populate_required_properties_present "tank/src" "compression=lz4=local" "/sbin/zfs" "casesensitivity,utf8only" source
	populated_required_properties=$g_zxfer_required_properties_result

	unset -f zxfer_get_required_property_probe
	zxfer_load_normalized_dataset_properties() {
		g_zxfer_normalized_dataset_properties="compression=lz4=local"
		return 0
	}
	zxfer_get_normalized_dataset_properties "tank/src" "/sbin/zfs" source >"$normalized_out"
	zxfer_load_destination_props "backup/dst" ""
	destination_props=$g_zxfer_destination_pvs_raw
	zxfer_collect_destination_props "backup/dst" "" >"$destination_out"
	unset -f zxfer_load_normalized_dataset_properties

	zxfer_populate_required_properties_present() {
		g_zxfer_required_properties_result="compression=lz4=local,casesensitivity=sensitive=local"
		return 0
	}
	zxfer_ensure_required_properties_present "tank/src" "compression=lz4=local" "/sbin/zfs" "casesensitivity" source >"$required_out"
	unset -f zxfer_populate_required_properties_present

	assertEquals "Required-property population should append supported creation-time properties while skipping unsupported ones." \
		"compression=lz4=local,casesensitivity=sensitive=local" "$populated_required_properties"
	assertEquals "Normalized-property wrapper helpers should print the loaded normalized property list." \
		"compression=lz4=local" "$(cat "$normalized_out")"
	assertEquals "Destination-property loaders should store the normalized destination property payload in the shared raw property scratch result." \
		"compression=lz4=local" "$destination_props"
	assertEquals "Destination-property collection wrappers should print the stored raw destination property payload." \
		"compression=lz4=local" "$(cat "$destination_out")"
	assertEquals "Required-property wrapper helpers should print the populated required-property list." \
		"compression=lz4=local,casesensitivity=sensitive=local" "$(cat "$required_out")"
}

test_zxfer_property_cache_wrapper_helpers_preserve_exact_failure_statuses() {
	set +e
	populate_output=$(
		(
			zxfer_get_required_property_probe() {
				printf '%s\n' "populate failed"
				return 5
			}
			zxfer_populate_required_properties_present \
				"tank/src" "compression=lz4=local" "/sbin/zfs" "casesensitivity" source
		)
	)
	populate_status=$?
	load_output=$(
		(
			zxfer_load_normalized_dataset_properties() {
				printf '%s\n' "normalized failed"
				return 7
			}
			zxfer_load_destination_props "backup/dst" ""
		)
	)
	load_status=$?
	collect_output=$(
		(
			zxfer_load_destination_props() {
				printf '%s\n' "collect failed"
				return 9
			}
			zxfer_collect_destination_props "backup/dst" ""
		)
	)
	collect_status=$?
	required_output=$(
		(
			zxfer_populate_required_properties_present() {
				printf '%s\n' "required failed"
				return 11
			}
			zxfer_ensure_required_properties_present \
				"tank/src" "compression=lz4=local" "/sbin/zfs" "casesensitivity" source
		)
	)
	required_status=$?

	assertEquals "Required-property population should preserve the exact failure status from required-property probes." \
		5 "$populate_status"
	assertEquals "Required-property population should preserve required-property probe failure output." \
		"populate failed" "$populate_output"
	assertEquals "Destination property loading should preserve the exact failure status from normalized-property loading." \
		7 "$load_status"
	assertEquals "Destination property loading should preserve normalized-property failure output." \
		"normalized failed" "$load_output"
	assertEquals "Destination property collection should preserve the exact failure status from destination-property loading." \
		9 "$collect_status"
	assertEquals "Destination property collection should preserve destination-property loader failure output." \
		"collect failed" "$collect_output"
	assertEquals "Required-property wrapper helpers should preserve the exact failure status from required-property population." \
		11 "$required_status"
	assertEquals "Required-property wrapper helpers should preserve required-property population failure output." \
		"required failed" "$required_output"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
