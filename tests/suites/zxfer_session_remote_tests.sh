#!/bin/sh
# Remote-host session tests for src/zxfer_session.sh: remote connection
# preparation and how zxfer_init_variables resolves each endpoint. Run by
# tests/test_zxfer_session.sh under the remote-host fixture.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

test_prepare_remote_host_connections_fails_with_dependency_class_without_ssh() {
	set +e
	output=$(
		(
			zxfer_ensure_local_ssh_command() {
				g_zxfer_resolved_local_ssh_command_result="ssh dependency missing"
				return 1
			}
			zxfer_open_ssh_control_sockets() {
				printf '%s\n' "open"
			}
			zxfer_throw_error() {
				printf 'class=%s throw=%s\n' "$g_zxfer_failure_class" "$1"
				exit 9
			}
			g_option_T_target_host="target.example"
			zxfer_prepare_remote_host_connections
		)
	)
	status=$?

	assertEquals "A missing local ssh should stop remote preparation." 9 "$status"
	assertEquals "A missing local ssh should be a dependency failure with the lookup diagnostic, before any master opens." \
		"class=dependency throw=ssh dependency missing" "$output"
}

test_prepare_remote_host_connections_opens_masters_before_preloading_capabilities() {
	log="$TEST_TMPDIR/prepare_remote_hosts.log"
	: >"$log"

	result=$(
		(
			zxfer_ssh_supports_control_sockets() {
				return 0
			}
			zxfer_open_ssh_control_sockets() {
				printf 'open %s %s\n' "$g_option_O_origin_host" "$g_option_T_target_host" >>"$log"
			}
			zxfer_preload_remote_host_capabilities() {
				printf 'preload %s %s\n' "$1" "$2" >>"$log"
			}
			# One reading per clock read, in this shell.
			clock_readings="1000 1250"
			zxfer_profile_read_clock_ms() {
				g_zxfer_profile_clock_ms=${clock_readings%% *}
				clock_readings=${clock_readings#* }
			}
			g_option_O_origin_host="origin.example pfexec"
			g_option_T_target_host="target.example doas"
			g_option_V_very_verbose=1
			g_cmd_zfs="/sbin/zfs"
			g_origin_cmd_zfs="/remote/origin/zfs"
			g_target_cmd_zfs="/remote/target/zfs"
			g_ssh_supports_control_sockets=1
			zxfer_prepare_remote_host_connections
			printf 'ssh_setup_ms=%s\n' "${g_zxfer_profile_ssh_setup_ms:-0}"
		)
	)

	assertEquals "Both masters should open before either capability probe runs over them." \
		"open origin.example pfexec target.example doas
preload origin.example pfexec source
preload target.example doas destination" "$(cat "$log")"
	assertContains "Very-verbose remote preparation should accumulate ssh setup timing." \
		"$result" "ssh_setup_ms=250"
}

test_init_variables_uses_gawk_on_sunos_when_available() {
	gawk_dir="$TEST_TMPDIR/gawk_path"
	mkdir -p "$gawk_dir"
	cat >"$gawk_dir/gawk" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$gawk_dir/gawk"

	result=$(
		(
			zxfer_get_os() {
				g_zxfer_os_result="SunOS"
				printf '%s\n' "SunOS"
			}
			g_cmd_zfs="/sbin/zfs"
			g_cmd_awk="/usr/bin/awk"
			g_zxfer_secure_path="$gawk_dir"
			zxfer_init_variables
			printf '%s\n' "$g_cmd_awk"
		)
	)

	assertEquals "SunOS initialization should prefer gawk when it is available." "$gawk_dir/gawk" "$result"
}

test_init_variables_uses_local_cat_lookup_in_restore_mode() {
	cat_dir="$TEST_TMPDIR/restore_cat_path"
	mkdir -p "$cat_dir"
	printf '#!/bin/sh\nexit 0\n' >"$cat_dir/cat"
	chmod 755 "$cat_dir/cat"

	result=$(
		(
			zxfer_get_os() {
				g_zxfer_os_result="FreeBSD"
				printf '%s\n' "FreeBSD"
			}
			g_zxfer_secure_path=$cat_dir
			g_option_e_restore_property_mode=1
			zxfer_init_variables
			printf 'cat=%s\n' "$g_cmd_cat"
		)
	)

	assertContains "Restore mode on the local host should resolve cat on the secure PATH." \
		"$result" "cat=$cat_dir/cat"
}

# zxfer_init_variables resolves each endpoint on its own host: the origin's
# operating system, zfs, compressor and restore cat, and the target's
# operating system, zfs and decompressor. Wrapper-style host specs reach each
# lookup whole.
test_init_variables_resolves_each_endpoint_on_its_own_host() {
	result=$(
		(
			zxfer_get_os() {
				g_zxfer_os_result=LocalOS
				[ -z "$1" ] || g_zxfer_os_result="OS of $1"
				printf '%s\n' "$g_zxfer_os_result"
			}
			zxfer_resolve_remote_required_tool() {
				g_zxfer_required_tool_result="$2 on $1"
			}
			zxfer_resolve_cli_command_safe() {
				g_zxfer_resolved_cli_command_result="$2 on $1"
			}
			g_option_O_origin_host="origin.example pfexec"
			g_option_T_target_host="target.example doas"
			g_option_z_compress=1
			g_option_e_restore_property_mode=1
			g_cmd_compress="zstd -T0 -9"
			g_cmd_decompress="zstd -d"
			zxfer_init_variables
			printf '%s\n' "local=$g_zxfer_local_os" \
				"source=$g_source_operating_system|$g_origin_cmd_zfs|$g_origin_cmd_compress_safe" \
				"destination=$g_destination_operating_system|$g_target_cmd_zfs|$g_target_cmd_decompress_safe" \
				"cat=$g_cmd_cat"
		)
	)

	assertEquals "Each endpoint should be resolved on its own host." \
		"local=LocalOS
source=OS of origin.example pfexec|zfs on origin.example pfexec|zstd -T0 -9 on origin.example pfexec
destination=OS of target.example doas|zfs on target.example doas|zstd -d on target.example doas
cat=cat on origin.example pfexec" "$result"
}

test_init_variables_passes_explicit_profile_sides_when_origin_and_target_match() {
	log_file="$TEST_TMPDIR/init_variables_profile_sides.log"
	: >"$log_file"

	(
		zxfer_get_os() {
			printf 'os:%s:%s\n' "$1" "${2:-}" >>"$log_file"
			g_zxfer_os_result="RemoteOS"
			printf '%s\n' "RemoteOS"
		}
		zxfer_resolve_remote_required_tool() {
			printf 'tool:%s:%s:%s:%s\n' "$1" "$2" "$3" "${4:-}" >>"$log_file"
			case "$2" in
			zfs)
				g_zxfer_required_tool_result="/remote/$2"
				;;
			cat)
				g_zxfer_required_tool_result="/remote/$2"
				;;
			esac
		}
		g_option_z_compress=0
		g_cmd_ssh="/usr/bin/ssh"
		g_cmd_zfs="/sbin/zfs"
		g_option_O_origin_host="shared.example"
		g_option_T_target_host="shared.example"
		g_option_e_restore_property_mode=1
		zxfer_init_variables
	)

	result=$(cat "$log_file")
	assertContains "Origin OS probes should be tagged as source-side even when origin and target share the same host spec." \
		"$result" "os:shared.example:source"
	assertContains "Target OS probes should be tagged as destination-side even when origin and target share the same host spec." \
		"$result" "os:shared.example:destination"
	assertContains "Origin zfs dependency probes should be tagged as source-side." \
		"$result" "tool:shared.example:zfs:zfs:source"
	assertContains "Target zfs dependency probes should be tagged as destination-side." \
		"$result" "tool:shared.example:zfs:zfs:destination"
	assertContains "Origin restore-metadata cat probes should be tagged as source-side." \
		"$result" "tool:shared.example:cat:cat:source"
}

# Each row makes one lookup of zxfer_init_variables fail, with -O and -T on
# different hosts and -z and -e so every lookup runs: the run must stop as a
# dependency failure with that lookup's message.
test_init_variables_stops_on_each_failed_lookup_as_a_dependency_error() {
	while IFS='|' read -r l_failed_lookup l_expected; do
		l_output=$(
			(
				FAILED_LOOKUP=$l_failed_lookup
				zxfer_get_os() {
					[ "os:${1:-local}" != "$FAILED_LOOKUP" ] || return 1
					g_zxfer_os_result=RemoteOS
					printf '%s\n' RemoteOS
				}
				zxfer_resolve_remote_required_tool() {
					g_zxfer_required_tool_result="/remote/$2"
					[ "tool:$1:$2" != "$FAILED_LOOKUP" ] && return 0
					g_zxfer_required_tool_result="$2 lookup failed on $1"
					return 1
				}
				zxfer_resolve_cli_command_safe() {
					g_zxfer_resolved_cli_command_result="'/remote/zstd'"
					[ "codec:$1" != "$FAILED_LOOKUP" ] && return 0
					g_zxfer_resolved_cli_command_result="codec lookup failed on $1"
					return 1
				}
				zxfer_throw_error() {
					printf 'class=%s message=%s\n' "$g_zxfer_failure_class" "$1"
					exit 1
				}
				g_option_O_origin_host=origin.example
				g_option_T_target_host=target.example
				g_option_z_compress=1
				g_option_e_restore_property_mode=1
				g_cmd_compress="zstd -3"
				g_cmd_decompress="zstd -d"
				zxfer_init_variables
			)
		)
		l_status=$?

		assertEquals "[$l_failed_lookup] must stop as a dependency failure" \
			"status=1 class=dependency message=$l_expected" "status=$l_status $l_output"
	done <<EOF
os:local|Failed to determine the local operating system.
os:origin.example|Failed to determine operating system on host origin.example.
os:target.example|Failed to determine operating system on host target.example.
tool:origin.example:zfs|zfs lookup failed on origin.example
tool:target.example:zfs|zfs lookup failed on target.example
codec:origin.example|codec lookup failed on origin.example
codec:target.example|codec lookup failed on target.example
tool:origin.example:cat|cat lookup failed on origin.example
EOF
}
