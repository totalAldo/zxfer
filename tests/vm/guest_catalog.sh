#!/bin/sh
#
# Validated guest metadata and preparation policy for the VM-backed runner.
#

zxfer_vm_guest_manifest_path() {
	printf '%s\n' "${ZXFER_VM_GUEST_MANIFEST_FILE:-$ZXFER_ROOT/tests/vm/guest_manifest.tsv}"
}

zxfer_vm_validate_guest_manifest() {
	l_manifest=${1:-$(zxfer_vm_guest_manifest_path)}
	l_tab=$(printf '\t')

	if [ ! -f "$l_manifest" ] || [ ! -r "$l_manifest" ]; then
		printf 'Invalid VM guest manifest [%s]: file is not readable.\n' "$l_manifest" >&2
		return 1
	fi

	awk -F "$l_tab" -v manifest="$l_manifest" '
		function fail(message) {
			if (!failed) {
				printf "Invalid VM guest manifest [%s]: %s\n", manifest, message
			}
			failed = 1
			exit 1
		}
		function positive_integer(value) {
			return value ~ /^[1-9][0-9]*$/
		}
		function valid_identifier(value) {
			return value ~ /^[a-z][a-z0-9_-]*$/
		}
		BEGIN {
			expected = "# guest"
			expected = expected FS "label"
			expected = expected FS "profiles"
			expected = expected FS "arch"
			expected = expected FS "image_filename"
			expected = expected FS "image_url"
			expected = expected FS "checksum_url"
			expected = expected FS "archive_compression"
			expected = expected FS "base_format"
			expected = expected FS "min_disk_size"
			expected = expected FS "guest_shell"
			expected = expected FS "seed_transport"
			expected = expected FS "ssh_ready_timeout_seconds"
			expected = expected FS "ssh_ready_probe_count"
			expected = expected FS "shunit_jobs"
			expected = expected FS "shunit_mode"
			expected = expected FS "provisioner"
			expected = expected FS "cloud_init_style"
			expected = expected FS "strict_qemu_profiles"
		}
		NR == 1 {
			if ($0 != expected) {
				fail("header does not match the 19-field guest schema")
			}
			next
		}
		{
			if ($0 == "") {
				fail("blank data rows are not allowed")
			}
			if (NF != 19) {
				fail("line " NR " has " NF " fields; expected 19")
			}
			if (!valid_identifier($1)) {
				fail("line " NR " has an invalid guest name")
			}
			if ($2 == "" || $2 ~ /[[:cntrl:]]/ ||
				$2 ~ /^[[:space:]]/ || $2 ~ /[[:space:]]$/) {
				fail("line " NR " has an invalid guest label")
			}

			profile_count = split($3, profiles, ",")
			if (profile_count < 1) {
				fail("line " NR " must name at least one profile")
			}
			for (profile_index = 1; profile_index <= profile_count; profile_index++) {
				profile = profiles[profile_index]
				if (!valid_identifier(profile)) {
					fail("line " NR " has an invalid profile name")
				}
				profile_key = NR SUBSEP profile
				if (profile_key in row_profiles) {
					fail("line " NR " repeats profile [" profile "]")
				}
				row_profiles[profile_key] = 1
			}

			if ($4 != "amd64" && $4 != "arm64") {
				fail("line " NR " has an unsupported architecture")
			}
			if ($5 !~ /^[A-Za-z0-9][-A-Za-z0-9._+]*$/) {
				fail("line " NR " has an unsafe image filename")
			}
			if ($6 !~ /^https:\/\/[^[:space:]]+$/ ||
				$7 !~ /^https:\/\/[^[:space:]]+$/) {
				fail("line " NR " has an invalid HTTPS URL")
			}
			if ($8 != "none" && $8 != "xz") {
				fail("line " NR " has an unsupported archive compression")
			}
			if ($9 != "qcow2") {
				fail("line " NR " has an unsupported base-image format")
			}
			if ($10 != "-" && $10 !~ /^[1-9][0-9]*[KMGTP]?$/) {
				fail("line " NR " has an invalid minimum disk size")
			}
			if ($11 !~ /^\/[A-Za-z0-9_+.\/-]+$/) {
				fail("line " NR " has an invalid guest shell")
			}
			if ($12 != "disk-cidata" && $12 != "smbios-nocloud-net") {
				fail("line " NR " has an unsupported seed transport")
			}
			if (!positive_integer($13) ||
				!positive_integer($14) ||
				!positive_integer($15)) {
				fail("line " NR " has an invalid positive-integer field")
			}
			if ($16 != "native" && $16 != "bash-posix") {
				fail("line " NR " has an unsupported shunit mode")
			}
			if ($17 != "apt-zfs" &&
				$17 != "freebsd-pkg" &&
				$17 != "omnios-pkg") {
				fail("line " NR " has an unsupported provisioner")
			}
			if ($18 != "default" && $18 != "root-login") {
				fail("line " NR " has an unsupported cloud-init style")
			}
			if ($19 != "-") {
				strict_count = split($19, strict_profiles, ",")
				for (strict_index = 1; strict_index <= strict_count; strict_index++) {
					strict_profile = strict_profiles[strict_index]
					if (!valid_identifier(strict_profile) ||
						!((NR SUBSEP strict_profile) in row_profiles)) {
						fail("line " NR " has an invalid strict-isolation profile")
					}
					strict_key = NR SUBSEP strict_profile
					if (strict_key in row_strict_profiles) {
						fail("line " NR " repeats a strict-isolation profile")
					}
					row_strict_profiles[strict_key] = 1
				}
			}

			guest_arch_key = $1 SUBSEP $4
			if (guest_arch_key in guest_arch_rows) {
				fail("line " NR " duplicates guest/architecture [" $1 "/" $4 "]")
			}
			guest_arch_rows[guest_arch_key] = NR

			if ($1 in guest_labels) {
				if (guest_labels[$1] != $2 ||
					guest_profiles[$1] != $3 ||
					guest_shells[$1] != $11 ||
					guest_timeouts[$1] != $13 ||
					guest_probe_counts[$1] != $14 ||
					guest_shunit_jobs[$1] != $15 ||
					guest_shunit_modes[$1] != $16 ||
					guest_provisioners[$1] != $17 ||
					guest_cloud_init_styles[$1] != $18 ||
					guest_strict_profiles[$1] != $19) {
					fail("line " NR " changes guest-level fields across architecture rows")
				}
			} else {
				guest_labels[$1] = $2
				guest_profiles[$1] = $3
				guest_shells[$1] = $11
				guest_timeouts[$1] = $13
				guest_probe_counts[$1] = $14
				guest_shunit_jobs[$1] = $15
				guest_shunit_modes[$1] = $16
				guest_provisioners[$1] = $17
				guest_cloud_init_styles[$1] = $18
				guest_strict_profiles[$1] = $19
			}
			row_count++
		}
		END {
			if (failed) {
				exit 1
			}
			if (NR == 0) {
				fail("file is empty")
			}
			if (row_count == 0) {
				fail("manifest contains no guest rows")
			}
		}
	' <"$l_manifest" >&2
}

zxfer_vm_require_guest_manifest() {
	l_manifest=$(zxfer_vm_guest_manifest_path) || return 1

	if [ "${ZXFER_VM_VALIDATED_GUEST_MANIFEST_FILE:-}" = "$l_manifest" ]; then
		return 0
	fi
	zxfer_vm_validate_guest_manifest "$l_manifest" || return 1
	ZXFER_VM_VALIDATED_GUEST_MANIFEST_FILE=$l_manifest
}

# Print the validated row for GUEST [ARCH]. Without ARCH prefer the host
# architecture, then amd64. "any" selects guest-wide metadata on any host.
zxfer_vm_guest_record() {
	zxfer_vm_require_guest_manifest || return 1
	awk -F '\t' -v guest="$1" -v arch="${2:-}" -v host="${ZXFER_VM_HOST_ARCH:-amd64}" '
		NR > 1 && $1 == guest {
			if (arch == "any" || $4 == arch || (arch == "" && $4 == host)) {
				row = $0
				exit
			}
			if (arch == "" && $4 == "amd64") row = $0
		}
		END {
			if (row == "") exit 1
			print row
		}
	' <"$ZXFER_VM_VALIDATED_GUEST_MANIFEST_FILE"
}

# Load GUEST [ARCH] once for the backend and its script renderers. Every
# validated field is nonempty, including the "-" placeholders, so splitting
# on tabs preserves labels with spaces and all 19 field positions.
# shellcheck disable=SC2034  # Fields are consumed by the selected backend.
zxfer_vm_load_guest() {
	l_guest_record=$(zxfer_vm_guest_record "$@") || return 1
	IFS="$(printf '\t')" read -r g_vm_guest_name g_vm_guest_label g_vm_guest_profiles g_vm_guest_arch \
		g_vm_guest_image_filename g_vm_guest_image_url g_vm_guest_checksum_url g_vm_guest_archive_compression \
		g_vm_guest_base_format g_vm_guest_min_disk_size g_vm_guest_shell g_vm_guest_seed_transport \
		g_vm_guest_ssh_ready_timeout_seconds g_vm_guest_ssh_ready_probe_count g_vm_guest_shunit_jobs \
		g_vm_guest_shunit_mode g_vm_guest_provisioner g_vm_guest_cloud_init_style g_vm_guest_strict_qemu_profiles <<EOF || return 1
$l_guest_record
EOF
	[ "$g_vm_guest_label" != "-" ] || g_vm_guest_label=
	[ "$g_vm_guest_min_disk_size" != "-" ] || g_vm_guest_min_disk_size=
	[ "$g_vm_guest_strict_qemu_profiles" != "-" ] || g_vm_guest_strict_qemu_profiles=
	g_vm_guest_base_image_name=$g_vm_guest_image_filename
	if [ "$g_vm_guest_archive_compression" = xz ]; then
		g_vm_guest_base_image_name=${g_vm_guest_image_filename%.xz}
	fi
}

zxfer_vm_guest_names() {
	l_manifest=
	l_tab=$(printf '\t')

	zxfer_vm_require_guest_manifest || return 1
	l_manifest=$ZXFER_VM_VALIDATED_GUEST_MANIFEST_FILE
	awk -F "$l_tab" 'NR > 1 && !seen[$1]++ { print $1 }' <"$l_manifest"
}

zxfer_vm_profile_names() {
	l_manifest=
	l_tab=$(printf '\t')

	zxfer_vm_require_guest_manifest || return 1
	l_manifest=$ZXFER_VM_VALIDATED_GUEST_MANIFEST_FILE
	awk -F "$l_tab" '
		NR > 1 {
			count = split($3, profiles, ",")
			for (profile_index = 1; profile_index <= count; profile_index++) {
				profile = profiles[profile_index]
				if (!seen[profile]++) {
					print profile
				}
			}
		}
	' <"$l_manifest"
}

zxfer_vm_profile_guests() {
	l_profile=$1
	l_manifest=
	l_tab=$(printf '\t')

	zxfer_vm_require_guest_manifest || return 1
	l_manifest=$ZXFER_VM_VALIDATED_GUEST_MANIFEST_FILE
	awk -F "$l_tab" -v requested_profile="$l_profile" '
		NR > 1 && !seen_guest[$1]++ {
			count = split($3, profiles, ",")
			for (profile_index = 1; profile_index <= count; profile_index++) {
				if (profiles[profile_index] == requested_profile) {
					if (result == "") {
						result = $1
					} else {
						result = result " " $1
					}
					found = 1
					break
				}
			}
		}
		END {
			if (!found) {
				exit 1
			}
			print result
		}
	' <"$l_manifest"
}

# Render preparation commands for the loaded guest, backend, and test layer.
zxfer_vm_guest_prepare_script() {
	case "$2/$1/$g_vm_guest_provisioner" in
	integration/ci-managed/apt-zfs | perf/ci-managed/apt-zfs | perf-compare/ci-managed/apt-zfs)
		cat <<'EOF'
export PATH="/usr/sbin:/usr/bin:/sbin:/bin:$PATH"
apt-get update
apt-get install -y csh zfsutils-linux parallel zstd
modprobe zfs
zfs version
zpool version
EOF
		;;
	integration/ci-managed/freebsd-pkg | perf/ci-managed/freebsd-pkg | perf-compare/ci-managed/freebsd-pkg)
		cat <<'EOF'
pkg install -y parallel zstd
kldload zfs || true
EOF
		;;
	integration/ci-managed/omnios-pkg | perf/ci-managed/omnios-pkg | perf-compare/ci-managed/omnios-pkg)
		cat <<'EOF'
PKG_SUCCESS_ON_NOP=1 pkg install zstd
EOF
		;;
	integration/qemu/apt-zfs | perf/qemu/apt-zfs | perf-compare/qemu/apt-zfs)
		cat <<'EOF'
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y csh zfsutils-linux parallel zstd
modprobe zfs
EOF
		;;
	integration/qemu/freebsd-pkg | perf/qemu/freebsd-pkg | perf-compare/qemu/freebsd-pkg)
		cat <<'EOF'
ASSUME_ALWAYS_YES=yes pkg bootstrap -f
pkg install -y parallel zstd
kldload zfs || true
EOF
		;;
	integration/qemu/omnios-pkg | perf/qemu/omnios-pkg | perf-compare/qemu/omnios-pkg)
		cat <<'EOF'
PKG_SUCCESS_ON_NOP=1 pkg install zstd
EOF
		;;
	shunit2/ci-managed/apt-zfs | shunit2/qemu/apt-zfs)
		cat <<'EOF'
:
EOF
		;;
	shunit2/ci-managed/freebsd-pkg | shunit2/qemu/freebsd-pkg)
		cat <<'EOF'
ASSUME_ALWAYS_YES=yes pkg bootstrap -f
pkg install -y bash git
EOF
		;;
	shunit2/ci-managed/omnios-pkg | shunit2/qemu/omnios-pkg)
		cat <<'EOF'
PKG_SUCCESS_ON_NOP=1 pkg install bash git
EOF
		;;
	*)
		return 1
		;;
	esac
}
