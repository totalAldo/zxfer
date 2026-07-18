#!/bin/sh
# Property destination creation, reconciliation, and apply behavior tests.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_ensure_destination_exists_returns_one_when_dataset_already_exists() {
	set +e
	zxfer_ensure_destination_exists 1 1 "" "" filesystem "" "backup/dst" "$ZXFER_BASE_READONLY_PROPERTIES" ""
	status=$?

	assertEquals "Existing destinations should skip creation and return 1." 1 "$status"
}

test_ensure_destination_exists_initial_source_adds_parents_when_missing() {
	result=$(
		(
			zxfer_exists_destination() {
				printf '0\n'
			}
			create_runner() {
				printf '%s|%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" "$5"
			}
			zxfer_ensure_destination_exists 0 1 "compression=lz4=local,atime=off=override" "" filesystem "" "backup/dst/child" "$ZXFER_BASE_READONLY_PROPERTIES" create_runner
		)
	)

	assertEquals "Initial-source creation should create missing parents separately before applying target properties without -p." \
		"yes|filesystem|||backup/dst
no|filesystem||compression=lz4,atime=off|backup/dst/child" "$result"
}

test_ensure_destination_exists_reports_parent_probe_failures() {
	set +e
	output=$(
		(
			zxfer_exists_destination() {
				printf '%s\n' "Failed to determine whether destination dataset [backup/dst] exists: permission denied"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_destination_exists 0 1 "compression=lz4=local" "" filesystem "" "backup/dst/child" "$ZXFER_BASE_READONLY_PROPERTIES" create_runner
		)
	)
	status=$?

	assertEquals "Parent destination probe failures should abort creation planning." 1 "$status"
	assertContains "Parent destination probe failures should surface the probe error." \
		"$output" "Failed to determine whether destination dataset [backup/dst] exists: permission denied"
}

test_ensure_destination_exists_child_uses_creation_properties() {
	result=$(
		(
			zxfer_exists_destination() {
				printf '1\n'
			}
			create_runner() {
				printf '%s|%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" "$5"
			}
			g_option_I_ignore_properties="mountpoint"
			zxfer_ensure_destination_exists 0 0 "" "mountpoint=/mnt=local,readonly=off=local,compression=lz4=local" filesystem "" "backup/dst/child" "readonly" create_runner
		)
	)

	assertEquals "Child dataset creation should use the supplied readonly list and filtered creation properties without -p when the parent exists." \
		"no|filesystem||compression=lz4|backup/dst/child" "$result"
}

test_ensure_destination_exists_child_omits_parent_matching_override_creation_properties() {
	result=$(
		(
			zxfer_exists_destination() {
				printf '1\n'
			}
			zxfer_collect_destination_props() {
				printf '%s\n' "quota=32M=local,checksum=sha256=local,compression=lz4=local"
			}
			create_runner() {
				printf '%s|%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" "$5"
			}
			zxfer_ensure_destination_exists 0 0 "" "quota=32M=override,checksum=sha256=override,compression=gzip=override,casesensitivity=sensitive=local" filesystem "" "backup/dst/child" "" create_runner
		)
	)

	assertEquals "Missing child creates should inherit parent-matching recursive overrides only when the property can inherit." \
		"no|filesystem||quota=32M,compression=gzip,casesensitivity=sensitive|backup/dst/child" "$result"
}

test_ensure_destination_exists_child_precreates_missing_parent_before_property_create() {
	result=$(
		(
			zxfer_exists_destination() {
				printf '0\n'
			}
			create_runner() {
				printf '%s|%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" "$5"
			}
			zxfer_ensure_destination_exists 0 0 "" "casesensitivity=sensitive=local,compression=lz4=local" filesystem "" "backup/dst/child" "" create_runner
		)
	)

	assertEquals "Missing child creates with properties should create parent hierarchy first, then create the target without -p so create-time properties are honored." \
		"yes|filesystem|||backup/dst
no|filesystem||casesensitivity=sensitive,compression=lz4|backup/dst/child" "$result"
}

test_ensure_destination_exists_reports_create_failures() {
	set +e
	output=$(
		(
			create_runner() {
				return 1
			}
			zxfer_exists_destination() {
				printf '0\n'
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_destination_exists 0 1 "compression=lz4=local" "" filesystem "" "backup/dst" "$ZXFER_BASE_READONLY_PROPERTIES" create_runner
		)
	)
	status=$?

	assertEquals "Create-runner failures should abort destination creation." 1 "$status"
	assertContains "Create-runner failures should use the destination-creation error." \
		"$output" "Error when creating destination filesystem."
}

test_ensure_destination_exists_uses_default_runner_when_unspecified_in_current_shell() {
	log="$TEST_TMPDIR/default_create_runner.log"
	zxfer_run_zfs_create_with_properties() {
		printf '%s|%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" "$5" >"$log"
	}
	zxfer_exists_destination() {
		printf '1\n'
	}
	ZXFER_BASE_READONLY_PROPERTIES=""
	g_option_I_ignore_properties=""

	zxfer_ensure_destination_exists 0 0 "" "readonly=off=local,compression=lz4=local" filesystem "" "backup/dst/child" "readonly" ""
	status=$?

	unset -f zxfer_run_zfs_create_with_properties
	unset -f zxfer_exists_destination

	assertEquals "Blank create-runner arguments should fall back to the default zfs create helper." 0 "$status"
	assertEquals "Default create-runner selection should sanitize creation properties using the supplied readonly list before invocation." \
		"no|filesystem||compression=lz4|backup/dst/child" "$(cat "$log")"
}

test_ensure_destination_exists_marks_created_hierarchy_in_cache() {
	# shellcheck disable=SC2030
	output=$(
		g_recursive_dest_list=""
		zxfer_mark_destination_root_missing_in_cache "backup/dst"
		zxfer_exists_destination() {
			printf '0\n'
		}
		create_runner() {
			return 0
		}
		zxfer_ensure_destination_exists 0 1 "compression=lz4=local" "" filesystem "" "backup/dst/child" "$ZXFER_BASE_READONLY_PROPERTIES" create_runner
		printf 'root=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst")"
		printf 'child=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst/child")"
		printf 'sibling=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst/sibling")"
		printf 'dests=%s\n' "$g_recursive_dest_list"
	)

	assertContains "Successful destination creates should mark the destination root as existing in the cache." \
		"$output" "root=1"
	assertContains "Successful destination creates should mark the created dataset as existing in the cache." \
		"$output" "child=1"
	assertContains "Uncreated siblings should still be inferred missing under the authoritative root cache." \
		"$output" "sibling=0"
	assertContains "Successful destination creates should append separately-created parents and the created dataset to the in-memory recursive destination list." \
		"$output" "dests=backup/dst
backup/dst/child"
}

test_ensure_destination_exists_appends_created_dataset_without_whitespace_prefix_when_root_already_tracked() {
	# shellcheck disable=SC2030
	output=$(
		g_recursive_dest_list="backup/dst"
		zxfer_mark_destination_root_missing_in_cache "backup/dst"
		zxfer_set_destination_existence_cache_entry "backup/dst" 1
		zxfer_exists_destination() {
			printf '1\n'
		}
		create_runner() {
			return 0
		}
		zxfer_ensure_destination_exists 0 1 "compression=lz4=local" "" filesystem "" "backup/dst/child" "$ZXFER_BASE_READONLY_PROPERTIES" create_runner
		printf 'dests=%s\n' "$g_recursive_dest_list"
	)

	assertContains "Appending a created dataset under an already-tracked destination root should preserve exact newline-delimited dataset names without leading whitespace." \
		"$output" "dests=backup/dst
backup/dst/child"
}

test_collect_destination_props_defaults_to_g_rzfs() {
	output_file="$TEST_TMPDIR/collect_destination_props_default.out"

	zxfer_load_destination_props() {
		g_zxfer_destination_pvs_raw="$1|${2:-$g_RZFS}"
	}
	g_RZFS="/remote/zfs"
	zxfer_collect_destination_props "backup/dst" "" >"$output_file"
	# shellcheck source=src/zxfer_property_state.sh
	. "$ZXFER_ROOT/src/zxfer_property_state.sh"

	assertEquals "Destination property collection should default to g_RZFS." \
		"backup/dst|/remote/zfs" "$(cat "$output_file")"
}

test_load_destination_props_defaults_to_g_rzfs_and_records_raw_props() {
	output_file="$TEST_TMPDIR/load_destination_props_default.out"

	zxfer_load_normalized_dataset_properties() {
		printf 'dataset=%s|zfs=%s|side=%s\n' "$1" "$2" "$3" >"$output_file"
		g_zxfer_normalized_dataset_properties="compression=lz4=local"
	}
	g_RZFS="/remote/zfs"
	zxfer_load_destination_props "backup/dst" ""
	printf 'raw=%s\n' "$g_zxfer_destination_pvs_raw" >>"$output_file"
	# shellcheck source=src/zxfer_property_state.sh
	. "$ZXFER_ROOT/src/zxfer_property_state.sh"

	assertEquals "Destination property loading should default to g_RZFS, use the destination cache side, and store the raw normalized properties." \
		"dataset=backup/dst|zfs=/remote/zfs|side=destination
raw=compression=lz4=local" "$(cat "$output_file")"
}

test_load_destination_props_propagates_lookup_failures() {
	set +e
	output=$(
		(
			zxfer_load_normalized_dataset_properties() {
				printf '%s\n' "ssh timeout"
				return 1
			}
			zxfer_load_destination_props "backup/dst" ""
		)
	)
	status=$?

	assertEquals "Destination property loading should fail when normalized inspection fails." \
		"1" "$status"
	assertEquals "Destination property loading should preserve normalized inspection errors." \
		"ssh timeout" "$output"
}

test_zxfer_build_destination_zfs_command_uses_local_zfs_path_when_rzfs_matches_cmd() {
	g_option_T_target_host=""
	g_cmd_zfs="/sbin/zfs"
	g_RZFS=$g_cmd_zfs

	assertEquals "Local destination command rendering should still include the resolved local zfs path when g_RZFS already matches g_cmd_zfs." \
		"'/sbin/zfs' 'set' 'quota=1G' 'backup/dst'" \
		"$(zxfer_build_destination_zfs_command set quota=1G backup/dst)"
}

test_zxfer_build_destination_zfs_command_routes_remote_targets_through_ssh() {
	rendered=$(
		(
			g_option_T_target_host="backup@example.com"
			g_target_cmd_zfs="/remote/bin/zfs"
			zxfer_build_destination_zfs_command set quota=1G backup/dst
		)
	)

	assertContains "Remote destination command rendering should route through ssh." \
		"$rendered" "backup@example.com"
	assertContains "Remote destination command rendering should use the resolved remote zfs path." \
		"$rendered" "/remote/bin/zfs"
	assertContains "Remote destination command rendering should keep the property assignment quoted." \
		"$rendered" "'quota=1G'"
}

test_zxfer_run_zfs_set_assignments_and_inherit_render_display_lines_when_verbose() {
	set_output=$(
		(
			g_option_n_dryrun=0
			g_option_v_verbose=1
			g_option_T_target_host=""
			g_cmd_zfs="/sbin/zfs"
			g_RZFS=$g_cmd_zfs
			zxfer_run_destination_zfs_cmd() {
				:
			}
			zxfer_invalidate_destination_property_mutation_cache() {
				:
			}
			zxfer_run_zfs_set_assignments backup/dst quota=1G
		)
	)
	inherit_output=$(
		(
			g_option_n_dryrun=0
			g_option_v_verbose=1
			g_option_T_target_host=""
			g_cmd_zfs="/sbin/zfs"
			g_RZFS=$g_cmd_zfs
			zxfer_run_destination_zfs_cmd() {
				:
			}
			zxfer_invalidate_destination_property_mutation_cache() {
				:
			}
			zxfer_run_zfs_inherit_property quota backup/dst
		)
	)

	assertEquals "Verbose live property sets should keep the current operator line text." \
		"'/sbin/zfs' 'set' 'quota=1G' 'backup/dst'" "$set_output"
	assertEquals "Verbose live property inherits should keep the current operator line text." \
		"'/sbin/zfs' 'inherit' 'quota' 'backup/dst'" "$inherit_output"
}

test_zxfer_run_zfs_set_and_inherit_dry_run_emit_newline_terminated_remote_lines() {
	output=$(
		(
			g_option_n_dryrun=1
			g_option_T_target_host="backup@example.com"
			g_target_cmd_zfs="/remote/bin/zfs"
			zxfer_run_zfs_set_assignments backup/dst quota=1G
			zxfer_run_zfs_inherit_property quota backup/dst
		)
	)

	assertEquals "Remote dry-run set and inherit previews must stay newline-separated instead of concatenating." \
		2 "$(printf '%s\n' "$output" | grep -c "backup@example.com")"
	assertContains "Remote dry-run previews should emit the set command on the first line." \
		"$(printf '%s\n' "$output" | sed -n 1p)" "quota=1G"
	assertContains "Remote dry-run previews should emit the inherit command on the second line." \
		"$(printf '%s\n' "$output" | sed -n 2p)" "inherit"
}

test_ensure_destination_exists_invalidates_destination_cache_after_live_create() {
	log="$TEST_TMPDIR/create_invalidation.log"
	: >"$log"

	(
		zxfer_exists_destination() {
			printf '1\n'
		}
		create_runner() {
			return 0
		}
		zxfer_invalidate_destination_property_mutation_cache() {
			printf '%s\n' "$1" >>"$log"
		}
		zxfer_ensure_destination_exists 0 1 "compression=lz4=local" "" filesystem "" "backup/dst" "$ZXFER_BASE_READONLY_PROPERTIES" create_runner
	)

	assertEquals "Successful live destination creation should invalidate destination property mutation caches for that dataset." \
		"backup/dst" "$(cat "$log")"
}

test_try_property_transfer_destination_create_live_probes_unlisted_existing_child() {
	log="$TEST_TMPDIR/property_create_live_probe_existing_child.log"
	: >"$log"
	g_actual_dest="backup/dst/child"
	g_recursive_dest_list="backup/dst"

	zxfer_exists_destination() {
		printf 'probe=%s mode=%s\n' "$1" "${2:-}" >>"$log"
		printf '%s\n' 1
	}
	zxfer_note_destination_dataset_exists() {
		printf 'noted=%s\n' "$1" >>"$log"
	}
	zxfer_ensure_destination_exists() {
		printf 'ensure=%s\n' "$1" >>"$log"
		return 1
	}

	set +e
	zxfer_try_property_transfer_destination_create \
		"tank/src/child" 0 0 "compression=lz4=local" "compression=lz4=local" \
		filesystem "" "$ZXFER_BASE_READONLY_PROPERTIES"
	status=$?
	set -e

	assertEquals "Existing but unlisted destination children should continue into property diffing instead of create." \
		1 "$status"
	assertEquals "Property create planning should live-probe children missing from the recursive destination list and record existing results." \
		"probe=backup/dst/child mode=live
noted=backup/dst/child
ensure=1" "$(cat "$log")"

	unset -f zxfer_exists_destination
	unset -f zxfer_note_destination_dataset_exists
	unset -f zxfer_ensure_destination_exists
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"
}

test_try_property_transfer_destination_create_rethrows_live_probe_failures() {
	g_actual_dest="backup/dst/child"
	g_recursive_dest_list="backup/dst"

	set +e
	output=$(
		(
			zxfer_exists_destination() {
				printf '%s\n' "probe failed"
				return 42
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit "${2:-1}"
			}
			zxfer_try_property_transfer_destination_create \
				"tank/src/child" 0 0 "compression=lz4=local" "compression=lz4=local" \
				filesystem "" "$ZXFER_BASE_READONLY_PROPERTIES"
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Destination live-probe failures should preserve the probe status." \
		42 "$status"
	assertContains "Destination live-probe failures should surface the probe diagnostic." \
		"$output" "probe failed"
}

test_zxfer_run_zfs_set_assignments_handles_dry_run_and_failures() {
	g_option_n_dryrun=1
	g_RZFS="/remote/zfs"
	assertEquals "Dry-run property sets should render the destination command." \
		"/remote/zfs 'set' 'quota=1G' 'backup/dst'" \
		"$(zxfer_run_zfs_set_assignments backup/dst quota=1G)"

	set +e
	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_option_n_dryrun=0
			zxfer_run_zfs_set_assignments backup/dst quota=1G
		)
	)
	status=$?

	assertEquals "Live property-set failures should abort." 1 "$status"
	assertContains "Live property-set failures should surface the set error." \
		"$output" "Error when setting properties on destination filesystem."
}

test_zxfer_run_zfs_set_assignments_invalidates_only_after_live_success() {
	log="$TEST_TMPDIR/set_property_no_false_invalidation.log"
	: >"$log"

	(
		zxfer_invalidate_destination_property_mutation_cache() {
			printf 'invalidated=%s\n' "$1" >>"$log"
		}
		g_option_n_dryrun=1
		zxfer_run_zfs_set_assignments backup/dst quota=1G >/dev/null
	)
	assertEquals "Dry-run property sets should not invalidate destination mutation caches." \
		"" "$(cat "$log")"

	set +e
	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				return 37
			}
			zxfer_invalidate_destination_property_mutation_cache() {
				printf '%s\n' "invalidated"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			g_option_n_dryrun=0
			zxfer_run_zfs_set_assignments backup/dst quota=1G
		)
	)
	status=$?
	set -e

	assertEquals "Failed live property sets should preserve the set status." \
		37 "$status"
	assertNotContains "Failed live property sets should not invalidate destination mutation caches as if the mutation succeeded." \
		"$output" "invalidated"
}

test_zxfer_run_zfs_set_assignments_preserves_literal_assignment_for_local_exec() {
	log="$TEST_TMPDIR/set_property_local.log"
	l_property="user:test\$\\\`\"\\\\"
	l_value="value with spaces \$\\\`\"\\\\"

	zxfer_run_destination_zfs_cmd() {
		printf '%s\n' "$@" >"$log"
	}

	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_RZFS="/sbin/zfs"
	zxfer_run_zfs_set_assignments "backup/dst" "$l_property=$l_value"

	unset -f zxfer_run_destination_zfs_cmd

	assertEquals "Live local property sets should pass the literal assignment to zfs without shell-style escaping." \
		"$(printf '%s\n' "set" "$l_property=$l_value" "backup/dst")" "$(cat "$log")"
}

test_zxfer_run_zfs_set_assignments_invalidates_destination_cache_after_live_set() {
	log="$TEST_TMPDIR/set_invalidation.log"
	: >"$log"

	(
		zxfer_run_destination_zfs_cmd() {
			return 0
		}
		zxfer_invalidate_destination_property_mutation_cache() {
			printf '%s\n' "$1" >>"$log"
		}

		g_option_n_dryrun=0
		zxfer_run_zfs_set_assignments "backup/dst" quota=1G
	)

	assertEquals "Successful live property sets should invalidate destination property mutation caches for that dataset." \
		"backup/dst" "$(cat "$log")"
}

test_zxfer_run_zfs_set_properties_batches_assignments_for_local_exec() {
	log="$TEST_TMPDIR/set_properties_local.log"

	zxfer_run_destination_zfs_cmd() {
		printf '%s\n' "$@" >"$log"
	}

	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_RZFS="/sbin/zfs"
	zxfer_run_zfs_set_properties "compression=lz4,atime=off" "backup/dst"

	unset -f zxfer_run_destination_zfs_cmd

	assertEquals "Live local batched property sets should pass every assignment to a single zfs set invocation." \
		"$(printf '%s\n' "set" "compression=lz4" "atime=off" "backup/dst")" "$(cat "$log")"
}

test_zxfer_run_zfs_set_properties_renders_single_dry_run_command() {
	g_option_n_dryrun=1
	g_RZFS="/remote/zfs"

	assertEquals "Dry-run batched property sets should render one readable destination command per dataset." \
		"/remote/zfs 'set' 'compression=lz4' 'atime=off' 'backup/dst'" \
		"$(zxfer_run_zfs_set_properties "compression=lz4,atime=off" backup/dst)"
}

test_zxfer_run_zfs_set_properties_invalidates_destination_cache_once_after_live_set() {
	log="$TEST_TMPDIR/set_properties_invalidation.log"
	: >"$log"

	(
		zxfer_run_destination_zfs_cmd() {
			return 0
		}
		zxfer_invalidate_destination_property_mutation_cache() {
			printf '%s\n' "$1" >>"$log"
		}

		g_option_n_dryrun=0
		zxfer_run_zfs_set_properties "compression=lz4,atime=off" "backup/dst"
	)

	assertEquals "Successful live batched property sets should invalidate destination property mutation caches once for the dataset." \
		"backup/dst" "$(cat "$log")"
}

test_zxfer_run_zfs_set_properties_skips_empty_assignments_when_batching() {
	log="$TEST_TMPDIR/set_properties_skip_empty.log"

	(
		zxfer_run_zfs_set_assignments() {
			printf '%s\n' "$@" >"$log"
		}
		zxfer_run_zfs_set_properties "compression=lz4,,atime=off," "backup/dst"
	)

	assertEquals "Batched property sets should drop empty assignments before invoking the set runner." \
		"$(printf '%s\n' "backup/dst" "compression=lz4" "atime=off")" "$(cat "$log")"
}

test_zxfer_run_zfs_set_assignments_returns_success_without_assignments() {
	zxfer_run_zfs_set_assignments "backup/dst"
	status=$?

	assertEquals "Batched assignment execution should no-op successfully when there are no assignments." \
		"0" "$status"
}

test_zxfer_run_zfs_set_assignments_fuzz_preserves_delimiter_heavy_values_for_local_exec() {
	current_log=""
	l_property="user:zxfer.fuzz"
	case_file="$TEST_TMPDIR/set_property_local_fuzz_cases.txt"
	cat >"$case_file" <<'EOF'
value,with,commas|backup/dst.child-01
value=with=equals|backup/dst.tail-02
value;with:semicolon|backup/dst.tail-03
value,=; mixed|backup/dst.tail-04
EOF

	zxfer_run_destination_zfs_cmd() {
		printf '%s\n' "$@" >"$current_log"
	}

	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_RZFS="/sbin/zfs"
	case_index=0
	while IFS='|' read -r l_value l_destination || [ -n "$l_value$l_destination" ]; do
		[ -n "$l_destination" ] || continue
		case_index=$((case_index + 1))
		current_log="$TEST_TMPDIR/set_property_local_fuzz_$case_index.log"
		zxfer_run_zfs_set_assignments "$l_destination" "$l_property=$l_value"
		assertEquals "Local property fuzz case $case_index should preserve the literal assignment and dataset tail." \
			"$(printf '%s\n' "set" "$l_property=$l_value" "$l_destination")" "$(cat "$current_log")"
	done <"$case_file"

	unset -f zxfer_run_destination_zfs_cmd
}

test_zxfer_run_zfs_set_assignments_preserves_literal_assignment_for_remote_exec() {
	fake_ssh="$TEST_TMPDIR/fake_ssh_join_exec_set"
	remote_zfs="$TEST_TMPDIR/fake_remote_zfs_set"
	ssh_log="$TEST_TMPDIR/fake_ssh_join_exec_set.log"
	remote_log="$TEST_TMPDIR/fake_remote_zfs_set.log"
	l_property="user:test\$\\\`\"\\\\"
	l_value="value with spaces \$\\\`\"\\\\"
	old_g_cmd_ssh=${g_cmd_ssh-}
	old_target_host=$g_option_T_target_host
	old_target_cmd_zfs=${g_target_cmd_zfs-}

	cat >"$fake_ssh" <<'EOF'
#!/bin/sh
while [ $# -gt 0 ]; do
	case "$1" in
	-o | -S | -O)
		shift 2
		;;
	-M | -N | -fN)
		shift
		;;
	--)
		shift
		break
		;;
	-*)
		shift
		;;
	*)
		break
		;;
	esac
done
host=$1
shift
remote_cmd=""
for arg in "$@"; do
	if [ "$remote_cmd" = "" ]; then
		remote_cmd=$arg
	else
		remote_cmd="$remote_cmd $arg"
	fi
done
if [ -n "${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "$host" >>"$FAKE_SSH_LOG"
	printf '%s\n' "$remote_cmd" >>"$FAKE_SSH_LOG"
fi
if ! eval "set -- $remote_cmd"; then
	exit 1
fi
"$@"
EOF
	chmod +x "$fake_ssh"

	cat >"$remote_zfs" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" >"$ZXFER_REMOTE_ZFS_LOG"
EOF
	chmod +x "$remote_zfs"

	FAKE_SSH_LOG="$ssh_log"
	ZXFER_REMOTE_ZFS_LOG="$remote_log"
	export FAKE_SSH_LOG ZXFER_REMOTE_ZFS_LOG

	g_option_n_dryrun=0
	g_cmd_ssh="$fake_ssh"
	g_option_T_target_host="target.example"
	g_target_cmd_zfs="$remote_zfs"

	zxfer_run_zfs_set_assignments "backup/dst" "$l_property=$l_value"

	unset FAKE_SSH_LOG ZXFER_REMOTE_ZFS_LOG
	g_cmd_ssh=$old_g_cmd_ssh
	g_option_T_target_host=$old_target_host
	g_target_cmd_zfs=$old_target_cmd_zfs

	assertEquals "Remote property sets should preserve the literal assignment after ssh joins the remote command into a shell string." \
		"$(printf '%s\n' "set" "$l_property=$l_value" "backup/dst")" "$(cat "$remote_log")"
	assertEquals "Remote property sets should keep the target host as the ssh destination." \
		"target.example" "$(sed -n '1p' "$ssh_log")"
}

test_zxfer_run_destination_zfs_property_command_passes_destination_profile_side_when_hosts_match() {
	fake_ssh="$TEST_TMPDIR/fake_ssh_destination_profile_side"
	ssh_log="$TEST_TMPDIR/fake_ssh_destination_profile_side.log"
	old_g_cmd_ssh=${g_cmd_ssh-}
	old_origin_host=$g_option_O_origin_host
	old_target_host=$g_option_T_target_host
	old_target_cmd_zfs=${g_target_cmd_zfs-}
	old_very_verbose=${g_option_V_very_verbose-}
	old_total_ssh=${g_zxfer_profile_ssh_shell_invocations-}
	old_source_ssh=${g_zxfer_profile_source_ssh_shell_invocations-}
	old_destination_ssh=${g_zxfer_profile_destination_ssh_shell_invocations-}

	cat >"$fake_ssh" <<'EOF'
#!/bin/sh
while [ $# -gt 0 ]; do
	case "$1" in
	-o | -S | -O)
		shift 2
		;;
	-M | -N | -fN)
		shift
		;;
	--)
		shift
		break
		;;
	-*)
		shift
		;;
	*)
		break
		;;
	esac
done
host=$1
printf '%s\n' "$host" >"$FAKE_SSH_LOG"
exit 0
EOF
	chmod +x "$fake_ssh"

	FAKE_SSH_LOG="$ssh_log"
	export FAKE_SSH_LOG

	g_cmd_ssh="$fake_ssh"
	g_option_O_origin_host="shared.example"
	g_option_T_target_host="shared.example"
	g_target_cmd_zfs="/remote/zfs"
	g_option_V_very_verbose=1
	g_zxfer_profile_ssh_shell_invocations=0
	g_zxfer_profile_source_ssh_shell_invocations=0
	g_zxfer_profile_destination_ssh_shell_invocations=0

	zxfer_run_destination_zfs_property_command set "user:test=value" "backup/dst"

	result_total_ssh=${g_zxfer_profile_ssh_shell_invocations:-0}
	result_source_ssh=${g_zxfer_profile_source_ssh_shell_invocations:-0}
	result_destination_ssh=${g_zxfer_profile_destination_ssh_shell_invocations:-0}

	unset FAKE_SSH_LOG
	g_cmd_ssh=$old_g_cmd_ssh
	g_option_O_origin_host=$old_origin_host
	g_option_T_target_host=$old_target_host
	g_target_cmd_zfs=$old_target_cmd_zfs
	g_option_V_very_verbose=$old_very_verbose
	g_zxfer_profile_ssh_shell_invocations=$old_total_ssh
	g_zxfer_profile_source_ssh_shell_invocations=$old_source_ssh
	g_zxfer_profile_destination_ssh_shell_invocations=$old_destination_ssh

	assertEquals "Destination-side property commands should keep the shared host as the ssh destination." \
		"shared.example" "$(cat "$ssh_log")"
	assertEquals "Destination-side property commands should increment the total ssh shell counter." \
		"1" "$result_total_ssh"
	assertEquals "Destination-side property commands should not increment the source-side ssh shell counter when origin and target share the same host spec." \
		"0" "$result_source_ssh"
	assertEquals "Destination-side property commands should increment the destination-side ssh shell counter when origin and target share the same host spec." \
		"1" "$result_destination_ssh"
}

test_zxfer_run_zfs_set_assignments_fuzz_preserves_delimiter_heavy_values_for_remote_exec() {
	fake_ssh="$TEST_TMPDIR/fake_ssh_join_exec_set_fuzz"
	fake_doas="$TEST_TMPDIR/doas"
	remote_zfs="$TEST_TMPDIR/fake_remote_zfs_set_fuzz"
	ssh_log="$TEST_TMPDIR/fake_ssh_join_exec_set_fuzz.log"
	remote_log="$TEST_TMPDIR/fake_remote_zfs_set_fuzz.log"
	case_file="$TEST_TMPDIR/set_property_remote_fuzz_cases.txt"
	l_property="user:zxfer.fuzz"
	old_g_cmd_ssh=${g_cmd_ssh-}
	old_target_host=$g_option_T_target_host
	old_target_cmd_zfs=${g_target_cmd_zfs-}
	old_fake_remote_path=${FAKE_REMOTE_PATH-}

	cat >"$fake_ssh" <<'EOF'
#!/bin/sh
while [ $# -gt 0 ]; do
	case "$1" in
	-o | -S | -O)
		shift 2
		;;
	-M | -N | -fN)
		shift
		;;
	--)
		shift
		break
		;;
	-*)
		shift
		;;
	*)
		break
		;;
	esac
done
host=$1
shift
remote_cmd=""
for arg in "$@"; do
	if [ "$remote_cmd" = "" ]; then
		remote_cmd=$arg
	else
		remote_cmd="$remote_cmd $arg"
	fi
done
if [ -n "${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "$host" >>"$FAKE_SSH_LOG"
	printf '%s\n' "$remote_cmd" >>"$FAKE_SSH_LOG"
fi
PATH=${FAKE_REMOTE_PATH:-$PATH}
export PATH
if ! eval "set -- $remote_cmd"; then
	exit 1
fi
"$@"
EOF
	chmod +x "$fake_ssh"

	cat >"$fake_doas" <<'EOF'
#!/bin/sh
exec "$@"
EOF
	chmod +x "$fake_doas"

	cat >"$remote_zfs" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" >"$ZXFER_REMOTE_ZFS_LOG"
EOF
	chmod +x "$remote_zfs"

	cat >"$case_file" <<'EOF'
value,with,commas|backup/dst.child-01
value=with=equals|backup/dst.tail-02
value;with:semicolon|backup/dst.tail-03
value,=; mixed|backup/dst.tail-04
EOF

	FAKE_SSH_LOG="$ssh_log"
	FAKE_REMOTE_PATH="$TEST_TMPDIR:$PATH"
	ZXFER_REMOTE_ZFS_LOG="$remote_log"
	export FAKE_SSH_LOG FAKE_REMOTE_PATH ZXFER_REMOTE_ZFS_LOG

	g_option_n_dryrun=0
	g_cmd_ssh="$fake_ssh"
	g_option_T_target_host="target.example doas"
	g_target_cmd_zfs="$remote_zfs"
	case_index=0
	while IFS='|' read -r l_value l_destination || [ -n "$l_value$l_destination" ]; do
		[ -n "$l_destination" ] || continue
		case_index=$((case_index + 1))
		: >"$ssh_log"
		zxfer_run_zfs_set_assignments "$l_destination" "$l_property=$l_value"
		assertEquals "Remote property fuzz case $case_index should preserve the literal assignment after ssh joins the remote command." \
			"$(printf '%s\n' "set" "$l_property=$l_value" "$l_destination")" "$(cat "$remote_log")"
		assertEquals "Remote property fuzz case $case_index should keep the target host separate from wrapper tokens." \
			"target.example" "$(sed -n '1p' "$ssh_log")"
		assertContains "Remote property fuzz case $case_index should preserve the doas wrapper in the remote command string." \
			"$(sed -n '2p' "$ssh_log")" "'doas'"
	done <"$case_file"

	unset FAKE_SSH_LOG ZXFER_REMOTE_ZFS_LOG
	if [ -n "${old_fake_remote_path:+set}" ]; then
		FAKE_REMOTE_PATH=$old_fake_remote_path
		export FAKE_REMOTE_PATH
	else
		unset FAKE_REMOTE_PATH
	fi
	g_cmd_ssh=$old_g_cmd_ssh
	g_option_T_target_host=$old_target_host
	g_target_cmd_zfs=$old_target_cmd_zfs
}

test_zxfer_run_zfs_set_properties_preserves_literal_assignments_for_remote_exec() {
	fake_ssh="$TEST_TMPDIR/fake_ssh_join_exec_set_properties"
	remote_zfs="$TEST_TMPDIR/fake_remote_zfs_set_properties"
	ssh_log="$TEST_TMPDIR/fake_ssh_join_exec_set_properties.log"
	remote_log="$TEST_TMPDIR/fake_remote_zfs_set_properties.log"
	old_g_cmd_ssh=${g_cmd_ssh-}
	old_target_host=$g_option_T_target_host
	old_target_cmd_zfs=${g_target_cmd_zfs-}

	cat >"$fake_ssh" <<'EOF'
#!/bin/sh
while [ $# -gt 0 ]; do
	case "$1" in
	-o | -S | -O)
		shift 2
		;;
	-M | -N | -fN)
		shift
		;;
	--)
		shift
		break
		;;
	-*)
		shift
		;;
	*)
		break
		;;
	esac
done
host=$1
shift
remote_cmd=""
for arg in "$@"; do
	if [ "$remote_cmd" = "" ]; then
		remote_cmd=$arg
	else
		remote_cmd="$remote_cmd $arg"
	fi
done
	if [ -n "${FAKE_SSH_LOG:-}" ]; then
		printf '%s\n' "$host" >>"$FAKE_SSH_LOG"
		printf '%s\n' "$remote_cmd" >>"$FAKE_SSH_LOG"
	fi
if ! eval "set -- $remote_cmd"; then
	exit 1
fi
"$@"
EOF
	chmod +x "$fake_ssh"

	cat >"$remote_zfs" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" >"$ZXFER_REMOTE_ZFS_LOG"
EOF
	chmod +x "$remote_zfs"

	FAKE_SSH_LOG="$ssh_log"
	ZXFER_REMOTE_ZFS_LOG="$remote_log"
	export FAKE_SSH_LOG ZXFER_REMOTE_ZFS_LOG

	g_option_n_dryrun=0
	g_cmd_ssh="$fake_ssh"
	g_option_T_target_host="target.example"
	g_target_cmd_zfs="$remote_zfs"

	zxfer_run_zfs_set_properties "user:first=value with spaces,user:second=keep=equals" "backup/dst"

	unset FAKE_SSH_LOG ZXFER_REMOTE_ZFS_LOG
	g_cmd_ssh=$old_g_cmd_ssh
	g_option_T_target_host=$old_target_host
	g_target_cmd_zfs=$old_target_cmd_zfs

	assertEquals "Remote batched property sets should preserve each literal assignment after ssh joins the remote command into a shell string." \
		"$(printf '%s\n' "set" "user:first=value with spaces" "user:second=keep=equals" "backup/dst")" "$(cat "$remote_log")"
	assertEquals "Remote batched property sets should keep the target host as the ssh destination." \
		"target.example" "$(sed -n '1p' "$ssh_log")"
}

test_zxfer_run_zfs_set_properties_decodes_delimiter_heavy_assignments_for_local_exec() {
	log="$TEST_TMPDIR/set_properties_local_decoded.log"

	zxfer_run_destination_zfs_cmd() {
		printf '%s\n' "$@" >"$log"
	}

	g_option_n_dryrun=0
	g_option_T_target_host=""
	zxfer_run_zfs_set_properties "user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi" "backup/dst"

	assertEquals "Batched property sets should decode delimiter-heavy values before invoking zfs set." \
		"$(printf '%s\n' "set" "user:note=value,with,commas=and;semi" "backup/dst")" "$(cat "$log")"

	unset -f zxfer_run_destination_zfs_cmd
}

test_zxfer_run_zfs_set_properties_keeps_decoded_line_feed_value_in_one_argument() {
	log="$TEST_TMPDIR/set_properties_local_linefeed.log"

	zxfer_run_destination_zfs_cmd() {
		printf 'argc=%s\n' "$#" >"$log"
		printf 'assignment=%s\n' "$2" >>"$log"
	}

	g_option_n_dryrun=0
	g_option_T_target_host=""
	zxfer_run_zfs_set_properties "user:note=line1%0Aline2" "backup/dst"

	assertEquals "Batched property sets should decode line-feed values without splitting one assignment into multiple argv entries." \
		"$(printf 'argc=3\nassignment=user:note=line1\nline2')" "$(cat "$log")"

	unset -f zxfer_run_destination_zfs_cmd
}

test_zxfer_run_zfs_inherit_property_handles_dry_run_and_failures() {
	g_option_n_dryrun=1
	g_RZFS="/remote/zfs"
	assertEquals "Dry-run inherit operations should render the destination command." \
		"/remote/zfs 'inherit' 'quota' 'backup/dst'" \
		"$(zxfer_run_zfs_inherit_property quota backup/dst)"

	set +e
	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_option_n_dryrun=0
			zxfer_run_zfs_inherit_property quota backup/dst
		)
	)
	status=$?

	assertEquals "Live inherit failures should abort." 1 "$status"
	assertContains "Live inherit failures should surface the inherit error." \
		"$output" "Error when inheriting properties on destination filesystem."
}

test_zxfer_run_zfs_inherit_property_invalidates_destination_cache_after_live_inherit() {
	log="$TEST_TMPDIR/inherit_invalidation.log"
	: >"$log"

	(
		zxfer_run_destination_zfs_cmd() {
			return 0
		}
		zxfer_invalidate_destination_property_mutation_cache() {
			printf '%s\n' "$1" >>"$log"
		}

		g_option_n_dryrun=0
		zxfer_run_zfs_inherit_property quota "backup/dst"
	)

	assertEquals "Successful live property inheritance should invalidate destination property mutation caches for that dataset." \
		"backup/dst" "$(cat "$log")"
}

test_zxfer_run_zfs_inherit_property_preserves_literal_property_for_local_exec() {
	log="$TEST_TMPDIR/inherit_property_local.log"
	l_property="user:test\$\\\`\"\\\\"

	zxfer_run_destination_zfs_cmd() {
		printf '%s\n' "$@" >"$log"
	}

	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_RZFS="/sbin/zfs"
	zxfer_run_zfs_inherit_property "$l_property" "backup/dst"

	unset -f zxfer_run_destination_zfs_cmd

	assertEquals "Live local property inheritance should pass the literal property name to zfs without shell-style escaping." \
		"$(printf '%s\n' "inherit" "$l_property" "backup/dst")" "$(cat "$log")"
}

test_zxfer_run_zfs_inherit_property_preserves_literal_property_for_remote_exec_with_wrapper_host_spec() {
	fake_ssh="$TEST_TMPDIR/fake_ssh_join_exec_inherit"
	fake_doas="$TEST_TMPDIR/doas"
	remote_zfs="$TEST_TMPDIR/fake_remote_zfs_inherit"
	ssh_log="$TEST_TMPDIR/fake_ssh_join_exec_inherit.log"
	remote_log="$TEST_TMPDIR/fake_remote_zfs_inherit.log"
	l_property="user:test'quote"
	old_g_cmd_ssh=${g_cmd_ssh-}
	old_target_host=$g_option_T_target_host
	old_target_cmd_zfs=${g_target_cmd_zfs-}
	old_fake_remote_path=${FAKE_REMOTE_PATH-}

	cat >"$fake_ssh" <<'EOF'
#!/bin/sh
while [ $# -gt 0 ]; do
	case "$1" in
	-o | -S | -O)
		shift 2
		;;
	-M | -N | -fN)
		shift
		;;
	--)
		shift
		break
		;;
	-*)
		shift
		;;
	*)
		break
		;;
	esac
done
host=$1
shift
remote_cmd=""
for arg in "$@"; do
	if [ "$remote_cmd" = "" ]; then
		remote_cmd=$arg
	else
		remote_cmd="$remote_cmd $arg"
	fi
done
if [ -n "${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "$host" >>"$FAKE_SSH_LOG"
	printf '%s\n' "$remote_cmd" >>"$FAKE_SSH_LOG"
fi
PATH=${FAKE_REMOTE_PATH:-$PATH}
export PATH
if ! eval "set -- $remote_cmd"; then
	exit 1
fi
"$@"
EOF
	chmod +x "$fake_ssh"

	cat >"$fake_doas" <<'EOF'
#!/bin/sh
exec "$@"
EOF
	chmod +x "$fake_doas"

	cat >"$remote_zfs" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" >"$ZXFER_REMOTE_ZFS_LOG"
EOF
	chmod +x "$remote_zfs"

	FAKE_SSH_LOG="$ssh_log"
	FAKE_REMOTE_PATH="$TEST_TMPDIR:$PATH"
	ZXFER_REMOTE_ZFS_LOG="$remote_log"
	export FAKE_SSH_LOG FAKE_REMOTE_PATH ZXFER_REMOTE_ZFS_LOG

	g_option_n_dryrun=0
	g_cmd_ssh="$fake_ssh"
	g_option_T_target_host="target.example doas"
	g_target_cmd_zfs="$remote_zfs"

	zxfer_run_zfs_inherit_property "$l_property" "backup/dst"

	unset FAKE_SSH_LOG ZXFER_REMOTE_ZFS_LOG
	if [ -n "${old_fake_remote_path:+set}" ]; then
		FAKE_REMOTE_PATH=$old_fake_remote_path
		export FAKE_REMOTE_PATH
	else
		unset FAKE_REMOTE_PATH
	fi
	g_cmd_ssh=$old_g_cmd_ssh
	g_option_T_target_host=$old_target_host
	g_target_cmd_zfs=$old_target_cmd_zfs

	assertEquals "Remote property inheritance should preserve literal property names after ssh joins the remote command into a shell string." \
		"$(printf '%s\n' "inherit" "$l_property" "backup/dst")" "$(cat "$remote_log")"
	assertEquals "Wrapper-style target specs should still keep the ssh destination host separate from wrapper tokens." \
		"target.example" "$(sed -n '1p' "$ssh_log")"
	assertContains "Wrapper-style target specs should preserve the wrapper token in the remote shell command." \
		"$(sed -n '2p' "$ssh_log")" "'doas'"
}

test_diff_properties_rejects_must_create_mismatches() {
	set +e
	output=$(
		(
			zxfer_throw_error_with_usage() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_diff_properties "casesensitivity=mixed=local" "casesensitivity=sensitive=local" "casesensitivity"
		)
	)
	status=$?

	assertEquals "Must-create property mismatches should abort." 1 "$status"
	assertContains "Must-create mismatches should explain that the property may only be set at creation time." \
		"$output" "may only be set"
}

test_diff_properties_sets_local_value_when_destination_source_is_inherited() {
	outfile="$TEST_TMPDIR/diff_set_local.out"

	zxfer_diff_properties "compression=lz4=local" "compression=lz4=inherited" "" >"$outfile"

	assertEquals "Initial-source property sets should still include matching values when the destination source is not local." \
		"compression=lz4" "$(sed -n '1p' "$outfile")"
	assertEquals "Child property sets should force local values when the destination inherits the same value." \
		"compression=lz4" "$(sed -n '2p' "$outfile")"
	assertEquals "No inherit list should be produced when the source is already local." \
		"" "$(sed -n '3p' "$outfile")"
}

test_diff_properties_inherits_value_when_destination_is_local_but_source_is_not() {
	outfile="$TEST_TMPDIR/diff_inherit_same_value.out"

	zxfer_diff_properties "compression=lz4=inherited" "compression=lz4=local" "" >"$outfile"

	assertEquals "No initial-source set list is needed when the destination already has the matching local value." \
		"" "$(sed -n '1p' "$outfile")"
	assertEquals "No child set list should be produced when the source value is inherited." \
		"" "$(sed -n '2p' "$outfile")"
	assertEquals "Child property diffs should request inheritance when the destination has a local copy of an inherited source value." \
		"compression=lz4" "$(sed -n '3p' "$outfile")"
}

test_diff_properties_treats_overrides_as_parent_sets() {
	outfile="$TEST_TMPDIR/diff_override_local.out"

	zxfer_diff_properties "checksum=sha256=override" "checksum=sha256=local" "" >"$outfile"

	assertEquals "Matching local override values should not request any additional root-level set." \
		"" "$(sed -n '1p' "$outfile")"
	assertEquals "Matching local override values should not request a child set when the child can inherit from a matching parent." \
		"" "$(sed -n '2p' "$outfile")"
	assertEquals "Matching local override values should request child inheritance so recursive overrides do not remain stamped locally on descendants." \
		"checksum=sha256" "$(sed -n '3p' "$outfile")"

	zxfer_diff_properties "checksum=sha256=override" "checksum=fletcher4=local" "" >"$outfile"

	assertEquals "Changed inheritable override values should still appear in the root set list." \
		"checksum=sha256" "$(sed -n '1p' "$outfile")"
	assertEquals "Changed inheritable override values should not be stamped locally on child datasets by default." \
		"" "$(sed -n '2p' "$outfile")"
	assertEquals "Changed inheritable override values should request child inheritance so the parent value remains authoritative." \
		"checksum=sha256" "$(sed -n '3p' "$outfile")"

	zxfer_diff_properties "quota=32M=override" "quota=32M=local" "" >"$outfile"

	assertEquals "Matching non-inheritable override values should not request a root-level set." \
		"" "$(sed -n '1p' "$outfile")"
	assertEquals "Matching non-inheritable override values should not request a child set when the child is already local." \
		"" "$(sed -n '2p' "$outfile")"
	assertEquals "Matching non-inheritable override values must not be converted into inheritance requests." \
		"" "$(sed -n '3p' "$outfile")"

	zxfer_diff_properties "quota=32M=override" "quota=none=local" "" >"$outfile"

	assertEquals "Changed non-inheritable override values should still appear in the root set list." \
		"quota=32M" "$(sed -n '1p' "$outfile")"
	assertEquals "Changed non-inheritable override values should be set locally on child datasets." \
		"quota=32M" "$(sed -n '2p' "$outfile")"
	assertEquals "Changed non-inheritable override values must not be converted into inheritance requests." \
		"" "$(sed -n '3p' "$outfile")"
}

test_diff_properties_does_not_stamp_matching_default_noninheritable_values() {
	outfile="$TEST_TMPDIR/diff_noninherit_default.out"

	zxfer_diff_properties "quota=none=default" "quota=none=default" "" >"$outfile"

	assertEquals "Matching default non-inheritable values should not request a root-level local set." \
		"" "$(sed -n '1p' "$outfile")"
	assertEquals "Matching default non-inheritable values should not request a child local set." \
		"" "$(sed -n '2p' "$outfile")"
	assertEquals "Matching default non-inheritable values must not be converted into inheritance requests." \
		"" "$(sed -n '3p' "$outfile")"

	zxfer_diff_properties "quota=none=default" "quota=none=local" "" >"$outfile"

	assertEquals "Matching effective non-inheritable values should not be restamped locally when the source is not local." \
		"" "$(sed -n '1p' "$outfile")"
	assertEquals "Matching effective non-inheritable child values should not be restamped locally when the source is not local." \
		"" "$(sed -n '2p' "$outfile")"
	assertEquals "Matching effective non-inheritable values must not be inherited from a local destination." \
		"" "$(sed -n '3p' "$outfile")"
}

test_diff_properties_sets_changed_noninheritable_values_locally() {
	outfile="$TEST_TMPDIR/diff_noninherit_changed.out"

	zxfer_diff_properties "quota=none=default" "quota=32M=local" "" >"$outfile"

	assertEquals "Changed non-inheritable values should request a root-level set even when the source is default." \
		"quota=none" "$(sed -n '1p' "$outfile")"
	assertEquals "Changed non-inheritable values should request a child local set because inheritance cannot preserve them." \
		"quota=none" "$(sed -n '2p' "$outfile")"
	assertEquals "Changed non-inheritable values must not be converted into inheritance requests." \
		"" "$(sed -n '3p' "$outfile")"
}

test_diff_properties_inherits_missing_inheritable_override_properties() {
	outfile="$TEST_TMPDIR/diff_override_missing_dest.out"

	zxfer_diff_properties "user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi=override" "compression=lz4=local" "" >"$outfile"

	assertEquals "Destination properties missing an override-managed property should still request a root-level local set." \
		"user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi" "$(sed -n '1p' "$outfile")"
	assertEquals "Missing inheritable override-managed properties should not be stamped locally on child datasets by default." \
		"" "$(sed -n '2p' "$outfile")"
	assertEquals "Missing inheritable override-managed properties should request child inheritance from the converged parent." \
		"user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi" "$(sed -n '3p' "$outfile")"
}

test_diff_properties_reports_awk_failures() {
	set +e
	output=$(
		(
			g_cmd_awk="false"
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result="$TEST_TMPDIR/diff_awk_failure.tmp"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$g_zxfer_temp_file_result"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}
			zxfer_diff_properties "compression=lz4=local" "compression=lz4=local" ""
		) 2>&1
	)
	status=$?

	assertEquals "Property diffing should fail closed when its awk helper cannot execute." \
		"1" "$status"
	assertContains "Property diffing awk failures should surface the helper failure message." \
		"$output" "Failed to diff dataset properties."
}

test_diff_properties_preserves_staged_readback_failures_without_publishing_results() {
	set +e
	output=$(
		(
			l_tmp_path="$g_zxfer_run_tmp_root/diff_readback_failure.tmp"
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result=$l_tmp_path
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$g_zxfer_temp_file_result"
			}
			zxfer_read_property_reconcile_stage_file() {
				return 1
			}
			zxfer_diff_properties "compression=lz4=local" "compression=lz4=local" "" || {
				if [ -e "$l_tmp_path" ]; then
					printf 'tmp_exists=yes\n'
				else
					printf 'tmp_exists=no\n'
				fi
				exit 1
			}
		)
	)
	status=$?

	assertEquals "Property diffing should fail closed when staged diff readback fails." \
		"1" "$status"
	assertContains "Property diff staged readback failures should still clean the staged temp file." \
		"$output" "tmp_exists=no"
}

test_diff_properties_preserves_must_create_mismatch_readback_failures_without_publishing_results() {
	set +e
	output=$(
		(
			l_tmp_path="$g_zxfer_run_tmp_root/diff_mismatch_readback_failure.tmp"
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result=$l_tmp_path
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$g_zxfer_temp_file_result"
			}
			zxfer_read_property_reconcile_stage_file() {
				return 46
			}
			zxfer_diff_properties "casesensitivity=mixed=local" "casesensitivity=sensitive=local" "casesensitivity"
			l_status=$?
			printf 'status=%s\n' "$l_status"
			if [ -e "$l_tmp_path" ]; then
				printf 'tmp_exists=yes\n'
			else
				printf 'tmp_exists=no\n'
			fi
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Must-create mismatch staging should preserve staged readback failures before surfacing a usage error." \
		46 "$status"
	assertContains "Must-create mismatch staged readback failures should still clean the staged temp file." \
		"$output" "tmp_exists=no"
}

test_diff_properties_rethrows_tempfile_allocation_failures() {
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}
			zxfer_get_temp_file() {
				zxfer_throw_error "Error creating temporary file."
			}
			zxfer_diff_properties "compression=lz4=local" "compression=lz4=local" ""
		) 2>&1
	)
	status=$?

	assertEquals "Property diffing should fail closed when temp-file allocation fails." \
		"1" "$status"
	assertEquals "Property diffing should preserve the temp-file allocation failure." \
		"Error creating temporary file." "$output"
}

test_diff_properties_preserves_nonthrowing_tempfile_status() {
	set +e
	output=$(
		(
			g_zxfer_property_reconcile_stage_file_result="stale-stage"
			zxfer_get_temp_file() {
				return 38
			}
			zxfer_diff_properties "compression=lz4=local" "compression=lz4=local" ""
			l_status=$?
			printf 'status=%s\n' "$l_status"
			printf 'stage=%s\n' "$g_zxfer_property_reconcile_stage_file_result"
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Property diffing should preserve non-throwing temp allocation statuses." \
		38 "$status"
	assertEquals "Property diffing should clear stale stage-file allocation results on failure." \
		"status=38
stage=" "$output"
}

test_adjust_child_inherit_to_match_parent_promotes_mismatched_parent_values_to_sets() {
	outfile="$TEST_TMPDIR/adjust_child_inherit.out"

	(
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_collect_destination_props() {
			printf '%s\n' "checksum=sha256=local,atime=on=local"
		}
		zxfer_sanitize_property_list() {
			printf '%s\n' "$1"
		}
		zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
			"checksum=sha256=inherited,atime=off=inherited" \
			"quota=32M" \
			"checksum=sha256,atime=off" \
			"$ZXFER_BASE_READONLY_PROPERTIES"
	) >"$outfile"

	assertEquals "Parent-matching inherited properties should remain in the inherit list." \
		"quota=32M,atime=off" "$(sed -n '1p' "$outfile")"
	assertEquals "Only properties whose parent already matches should remain inherited." \
		"checksum=sha256" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_preserves_inherit_when_parent_matches() {
	outfile="$TEST_TMPDIR/adjust_child_inherit_match.out"

	(
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_collect_destination_props() {
			printf '%s\n' "checksum=sha256=local,atime=off=local"
		}
		zxfer_sanitize_property_list() {
			printf '%s\n' "$1"
		}
		zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
			"checksum=sha256=inherited,atime=off=inherited" \
			"" \
			"checksum=sha256,atime=off" \
			"$ZXFER_BASE_READONLY_PROPERTIES"
	) >"$outfile"

	assertEquals "When the parent already has the desired values, no local sets are needed." \
		"" "$(sed -n '1p' "$outfile")"
	assertEquals "Matching parent values should preserve inheritance requests." \
		"checksum=sha256,atime=off" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_moves_inherited_source_properties_out_of_set_list_when_parent_matches() {
	outfile="$TEST_TMPDIR/adjust_child_inherit_demote.out"

	(
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_collect_destination_props() {
			printf '%s\n' "checksum=sha256=local,compression=lz4=local"
		}
		zxfer_sanitize_property_list() {
			printf '%s\n' "$1"
		}
		zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
			"checksum=sha256=inherited,compression=lz4=local" \
			"checksum=sha256,compression=lz4" \
			"" \
			"$ZXFER_BASE_READONLY_PROPERTIES"
	) >"$outfile"

	assertEquals "Inherited source properties should be removed from the child set list when the parent already provides the same value." \
		"compression=lz4" "$(sed -n '1p' "$outfile")"
	assertEquals "Inherited source properties whose parent already matches should be converted back into inherit operations." \
		"checksum=sha256" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_moves_matching_override_properties_out_of_set_list() {
	outfile="$TEST_TMPDIR/adjust_child_override_inherit_demote.out"

	(
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_collect_destination_props() {
			printf '%s\n' "quota=32M=local,checksum=sha256=local,compression=lz4=local"
		}
		zxfer_sanitize_property_list() {
			printf '%s\n' "$1"
		}
		zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
			"quota=32M=override,checksum=sha256=override,compression=lz4=override" \
			"quota=32M,checksum=sha256,compression=gzip" \
			"" \
			"$ZXFER_BASE_READONLY_PROPERTIES"
	) >"$outfile"

	assertEquals "Only inheritable recursive overrides whose parent already provides the requested value should be removed from the child set list." \
		"quota=32M,compression=gzip" "$(sed -n '1p' "$outfile")"
	assertEquals "Matching inheritable recursive overrides should become inherit operations for existing descendants." \
		"checksum=sha256" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_preserves_matching_override_inherit_without_parent_match() {
	outfile="$TEST_TMPDIR/adjust_child_override_inherit_stale_parent.out"

	(
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_collect_destination_props() {
			printf '%s\n' "checksum=fletcher4=local,atime=on=local"
		}
		zxfer_sanitize_property_list() {
			printf '%s\n' "$1"
		}
		zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
			"checksum=sha256=override,atime=off=inherited" \
			"" \
			"checksum=sha256,atime=off" \
			"$ZXFER_BASE_READONLY_PROPERTIES"
	) >"$outfile"

	assertEquals "Non-override inherited properties should still require the live parent value to match." \
		"atime=off" "$(sed -n '1p' "$outfile")"
	assertEquals "Matching inheritable recursive overrides should remain inheritance requests even when the parent probe has not observed the just-planned parent override." \
		"checksum=sha256" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_uses_supplied_readonly_list() {
	outfile="$TEST_TMPDIR/adjust_child_inherit_readonly.out"

	(
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_collect_destination_props() {
			printf '%s\n' "compression=lz4=local,atime=off=local"
		}
		ZXFER_BASE_READONLY_PROPERTIES=""
		g_option_I_ignore_properties=""
		zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
			"compression=lz4=inherited,atime=off=inherited" \
			"" \
			"compression=lz4,atime=off" \
			"atime"
	) >"$outfile"

	assertEquals "Supplied readonly properties should be removed from the parent comparison set before deciding whether a child may inherit." \
		"atime=off" "$(sed -n '1p' "$outfile")"
	assertEquals "Only properties still visible after readonly filtering should remain inherited." \
		"compression=lz4" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_returns_unchanged_lists_when_inherit_list_is_empty() {
	outfile="$TEST_TMPDIR/adjust_child_inherit_empty.out"

	(
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_collect_destination_props() {
			printf '%s\n' "compression=lz4=local"
		}
		zxfer_sanitize_property_list() {
			printf '%s\n' "$1"
		}
		zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
			"compression=lz4=local" \
			"compression=lz4" \
			"" \
			""
	) >"$outfile"

	assertEquals "Child-inherit adjustment should preserve the existing set list when there is no inherit list to reconcile." \
		"compression=lz4" "$(sed -n '1p' "$outfile")"
	assertEquals "Child-inherit adjustment should emit an empty inherit list unchanged when there is nothing to reconcile." \
		"" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_returns_empty_lists_when_nothing_needs_reconciliation() {
	outfile="$TEST_TMPDIR/adjust_child_inherit_both_empty.out"

	zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" "" "" "" "" >"$outfile"

	assertEquals "Child-inherit adjustment should leave an empty set list unchanged when there is nothing to reconcile." \
		"" "$(sed -n '1p' "$outfile")"
	assertEquals "Child-inherit adjustment should leave an empty inherit list unchanged when there is nothing to reconcile." \
		"" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_returns_unchanged_lists_for_root_dataset() {
	outfile="$TEST_TMPDIR/adjust_child_inherit_root.out"

	zxfer_adjust_child_inherit_to_match_parent "backup" \
		"compression=lz4=local" \
		"compression=lz4" \
		"quota=none" \
		"" >"$outfile"

	assertEquals "Root datasets should preserve the existing set list because there is no parent dataset to inspect." \
		"compression=lz4" "$(sed -n '1p' "$outfile")"
	assertEquals "Root datasets should preserve the inherit list because there is no parent dataset to inspect." \
		"quota=none" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_returns_unchanged_lists_when_parent_is_missing() {
	outfile="$TEST_TMPDIR/adjust_child_inherit_parent_missing.out"

	(
		zxfer_exists_destination() {
			printf '0\n'
		}
		zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
			"compression=lz4=local" \
			"compression=lz4" \
			"quota=none" \
			""
	) >"$outfile"

	assertEquals "Missing destination parents should preserve the existing set list." \
		"compression=lz4" "$(sed -n '1p' "$outfile")"
	assertEquals "Missing destination parents should preserve the existing inherit list." \
		"quota=none" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_reports_parent_probe_failures() {
	set +e
	output=$(
		(
			zxfer_exists_destination() {
				printf '%s\n' "ssh timeout"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
				"compression=lz4=inherited" \
				"" \
				"compression=lz4" \
				""
		)
	)
	status=$?

	assertEquals "Parent existence probe failures should abort child-inherit reconciliation." \
		"1" "$status"
	assertContains "Parent existence probe failures should surface the original probe error." \
		"$output" "ssh timeout"
}

test_adjust_child_inherit_to_match_parent_returns_failure_when_parent_props_cannot_be_loaded() {
	set +e
	output=$(
		(
			zxfer_exists_destination() {
				printf '1\n'
			}
			zxfer_collect_destination_props() {
				printf '%s\n' "ssh timeout"
				return 27
			}
			zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
				"compression=lz4=local" \
				"compression=lz4" \
				"quota=none" \
				""
		)
	)
	status=$?

	assertEquals "Parent-property load failures should abort child-inherit reconciliation." \
		"27" "$status"
	assertEquals "Parent-property load failures should not emit partial adjusted lists." \
		"" "$output"
}

test_adjust_child_inherit_to_match_parent_rethrows_parent_property_staged_readback_failures() {
	set +e
	output=$(
		(
			l_tmp_path="$g_zxfer_run_tmp_root/adjust_parent_readback_failure.tmp"
			g_zxfer_normalized_dataset_properties_cache_hit=1
			zxfer_exists_destination() {
				printf '1\n'
			}
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result=$l_tmp_path
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$g_zxfer_temp_file_result"
			}
			zxfer_collect_destination_props() {
				printf '%s\n' "compression=lz4=local"
			}
			zxfer_read_property_reconcile_stage_file() {
				return 47
			}
			zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
				"compression=lz4=inherited" \
				"" \
				"compression=lz4" \
				""
			l_status=$?
			printf 'status=%s\n' "$l_status"
			if [ -e "$l_tmp_path" ]; then
				printf 'tmp_exists=yes\n'
			else
				printf 'tmp_exists=no\n'
			fi
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Child-inherit reconciliation should preserve staged parent-property readback failures." \
		47 "$status"
	assertContains "Parent-property staged readback failures should still clean the staged temp file." \
		"$output" "tmp_exists=no"
}

test_adjust_child_inherit_to_match_parent_rethrows_tempfile_allocation_failures() {
	set +e
	output=$(
		(
			zxfer_exists_destination() {
				printf '1\n'
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}
			zxfer_get_temp_file() {
				zxfer_throw_error "Error creating temporary file."
			}
			zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
				"compression=lz4=inherited" \
				"" \
				"compression=lz4" \
				""
		) 2>&1
	)
	status=$?

	assertEquals "Child-inherit reconciliation should fail closed when temp-file allocation fails." \
		"1" "$status"
	assertEquals "Child-inherit reconciliation should preserve the temp-file allocation failure." \
		"Error creating temporary file." "$output"
}

test_adjust_child_inherit_to_match_parent_preserves_nonthrowing_tempfile_status() {
	set +e
	output=$(
		(
			g_zxfer_property_reconcile_stage_file_result="stale-stage"
			zxfer_exists_destination() {
				printf '1\n'
			}
			zxfer_get_temp_file() {
				return 39
			}
			zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
				"compression=lz4=inherited" \
				"" \
				"compression=lz4" \
				""
			l_status=$?
			printf 'status=%s\n' "$l_status"
			printf 'stage=%s\n' "$g_zxfer_property_reconcile_stage_file_result"
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Child-inherit reconciliation should preserve non-throwing temp allocation statuses." \
		39 "$status"
	assertEquals "Child-inherit reconciliation should clear stale stage-file allocation results on failure." \
		"status=39
stage=" "$output"
}

test_adjust_child_inherit_to_match_parent_reports_awk_failures() {
	set +e
	output=$(
		(
			g_cmd_awk="false"
			zxfer_exists_destination() {
				printf '1\n'
			}
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result="$TEST_TMPDIR/adjust_inherit_awk_failure.tmp"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$g_zxfer_temp_file_result"
			}
			zxfer_collect_destination_props() {
				printf '%s\n' "compression=lz4=local"
			}
			zxfer_sanitize_property_list() {
				printf '%s\n' "$1"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}
			zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
				"compression=lz4=inherited" \
				"" \
				"compression=lz4" \
				""
		) 2>&1
	)
	status=$?

	assertEquals "Child-inherit reconciliation should fail closed when its awk helper cannot execute." \
		"1" "$status"
	assertContains "Child-inherit reconciliation awk failures should surface the helper failure message." \
		"$output" "Failed to reconcile child property inheritance."
}

test_apply_property_changes_uses_default_runners_when_unspecified() {
	log="$TEST_TMPDIR/apply_default_runners.log"
	: >"$log"

	(
		LOG_FILE="$log"
		zxfer_run_zfs_set_properties() {
			printf 'set %s %s\n' "$1" "$2" >>"$LOG_FILE"
		}
		zxfer_run_zfs_inherit_property() {
			printf 'inherit %s %s\n' "$1" "$2" >>"$LOG_FILE"
		}
		zxfer_apply_property_changes "backup/dst" 0 "" "compression=lz4,atime=off" "quota=none" "" ""
	)

	assertEquals "Default property runners should be used when no custom runner is supplied." \
		"set compression=lz4,atime=off backup/dst
inherit quota backup/dst" "$(cat "$log")"
}

test_apply_property_changes_logs_when_child_only_inherits() {
	log="$TEST_TMPDIR/apply_inherit_only.log"
	: >"$log"

	(
		zxfer_echov() {
			printf '%s\n' "$*" >>"$log"
		}
		inherit_runner() {
			printf 'inherit %s %s\n' "$1" "$2" >>"$log"
		}

		zxfer_apply_property_changes "backup/dst" 0 "" "" "quota=none" "" inherit_runner
	)

	assertContains "Child-only inheritance changes should still log the property-update banner." \
		"$(cat "$log")" "Setting properties/sources on destination filesystem \"backup/dst\"."
	assertContains "Child-only inheritance changes should still call the inherit runner." \
		"$(cat "$log")" "inherit quota backup/dst"
}

test_apply_property_changes_logs_decoded_delimiter_heavy_values() {
	log="$TEST_TMPDIR/apply_encoded_display.log"
	: >"$log"

	(
		zxfer_echov() {
			printf '%s\n' "$*" >>"$log"
		}
		set_runner() {
			:
		}
		inherit_runner() {
			:
		}

		zxfer_apply_property_changes "backup/dst" 0 "" \
			"user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi" \
			"user:inherit=value%2Ctwo" \
			set_runner inherit_runner
	)

	assertContains "Verbose property-set summaries should decode delimiter-heavy values before logging." \
		"$(cat "$log")" "Property set list: user:note=value,with,commas=and;semi"
	assertContains "Verbose property-inherit summaries should decode delimiter-heavy values before logging." \
		"$(cat "$log")" "Property inherit list: user:inherit=value,two"
}

test_apply_property_changes_batches_multiple_child_sets_in_one_runner_call() {
	log="$TEST_TMPDIR/apply_child_batch.log"
	: >"$log"

	(
		LOG_FILE="$log"
		set_runner() {
			printf 'set %s %s\n' "$1" "$2" >>"$LOG_FILE"
		}
		inherit_runner() {
			printf 'inherit %s %s\n' "$1" "$2" >>"$LOG_FILE"
		}
		zxfer_apply_property_changes "backup/dst/child" 0 "" "compression=lz4,atime=off" "quota=none" set_runner inherit_runner
	)

	assertEquals "Child-property application should batch all set operations into one set-runner call while still inheriting properties one at a time." \
		"set compression=lz4,atime=off backup/dst/child
inherit quota backup/dst/child" "$(cat "$log")"
}

test_property_reconcile_csv_helpers_preserve_caller_ifs_and_globbing() {
	log=$TEST_TMPDIR/property_reconcile_shell_state.log
	: >"$log"
	actual=$(
		(
			cd "$TEST_TMPDIR" || exit 1
			: >'user:note=expanded'
			LOG_FILE=$log
			zxfer_run_zfs_set_assignments() {
				printf 'set dataset=%s assignments=%s|%s\n' \
					"$1" "$2" "$3" >>"$LOG_FILE"
			}
			set_runner() {
				printf 'apply-set %s %s\n' "$1" "$2" >>"$LOG_FILE"
			}
			inherit_runner() {
				printf 'apply-inherit %s %s\n' "$1" "$2" >>"$LOG_FILE"
			}

			IFS=:
			set -f
			zxfer_run_zfs_set_properties \
				'user:note=*,compression=lz4' 'backup/dst'
			printf 'custom_ifs=%s\n' "$IFS"
			zxfer_property_test_report_globbing_state custom

			unset IFS
			set +f
			zxfer_apply_property_changes 'backup/dst/child' 0 '' '' \
				'user:note=*,compression=lz4' set_runner inherit_runner
			if [ "${IFS+set}" = set ]; then
				printf '%s\n' 'unset_ifs=set'
			else
				printf '%s\n' 'unset_ifs=unset'
			fi
			zxfer_property_test_report_globbing_state unset
			cat "$LOG_FILE"
		)
	)

	assertEquals "Property-reconcile CSV parsing should not alter caller IFS/globbing or expand value globs." \
		"custom_ifs=:
custom_globbing=disabled
unset_ifs=unset
unset_globbing=enabled
set dataset=backup/dst assignments=user:note=*|compression=lz4
apply-inherit user:note backup/dst/child
apply-inherit compression backup/dst/child" "$actual"
}

# Register this fragment's tests explicitly so unfiltered shunit2 execution
# cannot depend on source scanning or evaluation.
zxfer_test_add_property_reconcile_apply_tests() {
	suite_addTest test_ensure_destination_exists_returns_one_when_dataset_already_exists
	suite_addTest test_ensure_destination_exists_initial_source_adds_parents_when_missing
	suite_addTest test_ensure_destination_exists_reports_parent_probe_failures
	suite_addTest test_ensure_destination_exists_child_uses_creation_properties
	suite_addTest test_ensure_destination_exists_child_omits_parent_matching_override_creation_properties
	suite_addTest test_ensure_destination_exists_child_precreates_missing_parent_before_property_create
	suite_addTest test_ensure_destination_exists_reports_create_failures
	suite_addTest test_ensure_destination_exists_uses_default_runner_when_unspecified_in_current_shell
	suite_addTest test_ensure_destination_exists_marks_created_hierarchy_in_cache
	suite_addTest test_ensure_destination_exists_appends_created_dataset_without_whitespace_prefix_when_root_already_tracked
	suite_addTest test_collect_destination_props_defaults_to_g_rzfs
	suite_addTest test_load_destination_props_defaults_to_g_rzfs_and_records_raw_props
	suite_addTest test_load_destination_props_propagates_lookup_failures
	suite_addTest test_zxfer_build_destination_zfs_command_uses_local_zfs_path_when_rzfs_matches_cmd
	suite_addTest test_zxfer_build_destination_zfs_command_routes_remote_targets_through_ssh
	suite_addTest test_zxfer_run_zfs_set_assignments_and_inherit_render_display_lines_when_verbose
	suite_addTest test_zxfer_run_zfs_set_and_inherit_dry_run_emit_newline_terminated_remote_lines
	suite_addTest test_ensure_destination_exists_invalidates_destination_cache_after_live_create
	suite_addTest test_try_property_transfer_destination_create_live_probes_unlisted_existing_child
	suite_addTest test_try_property_transfer_destination_create_rethrows_live_probe_failures
	suite_addTest test_zxfer_run_zfs_set_assignments_handles_dry_run_and_failures
	suite_addTest test_zxfer_run_zfs_set_assignments_invalidates_only_after_live_success
	suite_addTest test_zxfer_run_zfs_set_assignments_preserves_literal_assignment_for_local_exec
	suite_addTest test_zxfer_run_zfs_set_assignments_invalidates_destination_cache_after_live_set
	suite_addTest test_zxfer_run_zfs_set_properties_batches_assignments_for_local_exec
	suite_addTest test_zxfer_run_zfs_set_properties_renders_single_dry_run_command
	suite_addTest test_zxfer_run_zfs_set_properties_invalidates_destination_cache_once_after_live_set
	suite_addTest test_zxfer_run_zfs_set_properties_skips_empty_assignments_when_batching
	suite_addTest test_zxfer_run_zfs_set_assignments_returns_success_without_assignments
	suite_addTest test_zxfer_run_zfs_set_assignments_fuzz_preserves_delimiter_heavy_values_for_local_exec
	suite_addTest test_zxfer_run_zfs_set_assignments_preserves_literal_assignment_for_remote_exec
	suite_addTest test_zxfer_run_destination_zfs_property_command_passes_destination_profile_side_when_hosts_match
	suite_addTest test_zxfer_run_zfs_set_assignments_fuzz_preserves_delimiter_heavy_values_for_remote_exec
	suite_addTest test_zxfer_run_zfs_set_properties_preserves_literal_assignments_for_remote_exec
	suite_addTest test_zxfer_run_zfs_set_properties_decodes_delimiter_heavy_assignments_for_local_exec
	suite_addTest test_zxfer_run_zfs_set_properties_keeps_decoded_line_feed_value_in_one_argument
	suite_addTest test_zxfer_run_zfs_inherit_property_handles_dry_run_and_failures
	suite_addTest test_zxfer_run_zfs_inherit_property_invalidates_destination_cache_after_live_inherit
	suite_addTest test_zxfer_run_zfs_inherit_property_preserves_literal_property_for_local_exec
	suite_addTest test_zxfer_run_zfs_inherit_property_preserves_literal_property_for_remote_exec_with_wrapper_host_spec
	suite_addTest test_diff_properties_rejects_must_create_mismatches
	suite_addTest test_diff_properties_sets_local_value_when_destination_source_is_inherited
	suite_addTest test_diff_properties_inherits_value_when_destination_is_local_but_source_is_not
	suite_addTest test_diff_properties_treats_overrides_as_parent_sets
	suite_addTest test_diff_properties_does_not_stamp_matching_default_noninheritable_values
	suite_addTest test_diff_properties_sets_changed_noninheritable_values_locally
	suite_addTest test_diff_properties_inherits_missing_inheritable_override_properties
	suite_addTest test_diff_properties_reports_awk_failures
	suite_addTest test_diff_properties_preserves_staged_readback_failures_without_publishing_results
	suite_addTest test_diff_properties_preserves_must_create_mismatch_readback_failures_without_publishing_results
	suite_addTest test_diff_properties_rethrows_tempfile_allocation_failures
	suite_addTest test_diff_properties_preserves_nonthrowing_tempfile_status
	suite_addTest test_adjust_child_inherit_to_match_parent_promotes_mismatched_parent_values_to_sets
	suite_addTest test_adjust_child_inherit_to_match_parent_preserves_inherit_when_parent_matches
	suite_addTest test_adjust_child_inherit_to_match_parent_moves_inherited_source_properties_out_of_set_list_when_parent_matches
	suite_addTest test_adjust_child_inherit_to_match_parent_moves_matching_override_properties_out_of_set_list
	suite_addTest test_adjust_child_inherit_to_match_parent_preserves_matching_override_inherit_without_parent_match
	suite_addTest test_adjust_child_inherit_to_match_parent_uses_supplied_readonly_list
	suite_addTest test_adjust_child_inherit_to_match_parent_returns_unchanged_lists_when_inherit_list_is_empty
	suite_addTest test_adjust_child_inherit_to_match_parent_returns_empty_lists_when_nothing_needs_reconciliation
	suite_addTest test_adjust_child_inherit_to_match_parent_returns_unchanged_lists_for_root_dataset
	suite_addTest test_adjust_child_inherit_to_match_parent_returns_unchanged_lists_when_parent_is_missing
	suite_addTest test_adjust_child_inherit_to_match_parent_reports_parent_probe_failures
	suite_addTest test_adjust_child_inherit_to_match_parent_returns_failure_when_parent_props_cannot_be_loaded
	suite_addTest test_adjust_child_inherit_to_match_parent_rethrows_parent_property_staged_readback_failures
	suite_addTest test_adjust_child_inherit_to_match_parent_rethrows_tempfile_allocation_failures
	suite_addTest test_adjust_child_inherit_to_match_parent_preserves_nonthrowing_tempfile_status
	suite_addTest test_adjust_child_inherit_to_match_parent_reports_awk_failures
	suite_addTest test_apply_property_changes_uses_default_runners_when_unspecified
	suite_addTest test_apply_property_changes_logs_when_child_only_inherits
	suite_addTest test_apply_property_changes_logs_decoded_delimiter_heavy_values
	suite_addTest test_apply_property_changes_batches_multiple_child_sets_in_one_runner_call
	suite_addTest test_property_reconcile_csv_helpers_preserve_caller_ifs_and_globbing
}
