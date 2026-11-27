#!/bin/sh
#
# QEMU backend for local disposable guest execution.
#
# shellcheck disable=SC2154  # Guest fields are loaded by zxfer_vm_load_guest.

zxfer_vm_backend_qemu_check_host() {
	case "$ZXFER_VM_HOST_OS" in
	linux | darwin | wsl2) ;;
	*)
		zxfer_vm_die "The qemu backend does not support host OS [$ZXFER_VM_HOST_OS]"
		;;
	esac

	zxfer_vm_require_command curl
	zxfer_vm_require_command python3
	zxfer_vm_require_command qemu-img
	zxfer_vm_require_command ssh
	zxfer_vm_require_command ssh-keygen
	zxfer_vm_require_command ssh-keyscan
	zxfer_vm_require_command tar
	if [ "$ZXFER_VM_TEST_LAYER" = "perf-compare" ]; then
		zxfer_vm_require_command git
	fi

	l_required_qemu_commands=
	for l_guest in $ZXFER_VM_SELECTED_GUESTS; do
		zxfer_vm_load_guest "$l_guest" ||
			zxfer_vm_die "No qemu guest architecture is defined for guest [$l_guest]"
		l_qemu_cmd=$(zxfer_vm_qemu_system_binary "$g_vm_guest_arch") ||
			zxfer_vm_die "No qemu system binary is defined for guest architecture [$g_vm_guest_arch]"
		if ! zxfer_vm_list_contains "$l_required_qemu_commands" "$l_qemu_cmd"; then
			zxfer_vm_require_command "$l_qemu_cmd"
			l_required_qemu_commands=$(zxfer_vm_append_word "$l_required_qemu_commands" "$l_qemu_cmd")
		fi
		if [ "$g_vm_guest_arch" = "arm64" ]; then
			zxfer_vm_qemu_resolve_aarch64_efi >/dev/null
		fi
		if [ "$g_vm_guest_seed_transport" = "disk-cidata" ]; then
			zxfer_vm_qemu_require_seed_image_builder
		fi
		case "$g_vm_guest_archive_compression" in
		xz)
			zxfer_vm_require_command xz
			;;
		esac
	done

	if command -v xz >/dev/null 2>&1; then
		:
	else
		zxfer_vm_warn "xz is not installed; FreeBSD guest downloads will fail until it is available."
	fi
}

zxfer_vm_qemu_require_seed_image_builder() {
	case "$ZXFER_VM_HOST_OS" in
	darwin)
		zxfer_vm_require_command hdiutil
		;;
	linux | wsl2)
		if command -v xorriso >/dev/null 2>&1 ||
			command -v genisoimage >/dev/null 2>&1 ||
			command -v mkisofs >/dev/null 2>&1; then
			return 0
		fi
		zxfer_vm_die "FreeBSD qemu guests require xorriso, genisoimage, or mkisofs to build a cidata seed image on $ZXFER_VM_HOST_OS hosts"
		;;
	*)
		zxfer_vm_die "Unsupported host OS for qemu seed-image creation: $ZXFER_VM_HOST_OS"
		;;
	esac
}

zxfer_vm_qemu_guest_state_cleanup() {
	l_preserve=${ZXFER_VM_QEMU_PRESERVE_STATE:-0}
	l_state_dir=${ZXFER_VM_QEMU_STATE_DIR:-}
	l_http_pid=${ZXFER_VM_QEMU_HTTP_PID:-}
	l_qemu_pid_file=${ZXFER_VM_QEMU_PID_FILE:-}

	if [ -n "$l_http_pid" ]; then
		kill "$l_http_pid" >/dev/null 2>&1 || true
		wait "$l_http_pid" >/dev/null 2>&1 || true
	fi

	if [ -n "$l_qemu_pid_file" ] && [ -r "$l_qemu_pid_file" ]; then
		l_qemu_pid=$(cat "$l_qemu_pid_file" 2>/dev/null || printf '%s\n' "")
		if [ -n "$l_qemu_pid" ]; then
			kill "$l_qemu_pid" >/dev/null 2>&1 || true
			wait "$l_qemu_pid" >/dev/null 2>&1 || true
		fi
	fi

	if [ "$l_preserve" != "1" ] && [ -n "$l_state_dir" ]; then
		rm -rf "$l_state_dir"
	fi

	ZXFER_VM_QEMU_HTTP_PID=
	ZXFER_VM_QEMU_PID_FILE=
	ZXFER_VM_QEMU_STATE_DIR=
	ZXFER_VM_QEMU_PRESERVE_STATE=0
}

zxfer_vm_qemu_handle_signal() {
	l_signal=$1
	l_status=$(zxfer_vm_signal_exit_status "$l_signal")

	ZXFER_VM_QEMU_PRESERVE_STATE=$ZXFER_VM_PRESERVE_FAILED_GUESTS
	trap - EXIT HUP INT TERM
	zxfer_vm_qemu_guest_state_cleanup
	exit "$l_status"
}

zxfer_vm_qemu_system_binary() {
	case "$1" in
	amd64)
		printf '%s\n' "qemu-system-x86_64"
		;;
	arm64)
		printf '%s\n' "qemu-system-aarch64"
		;;
	*)
		return 1
		;;
	esac
}

zxfer_vm_qemu_resolve_aarch64_efi() {
	if [ -n "${ZXFER_VM_QEMU_AARCH64_EFI:-}" ]; then
		[ -r "$ZXFER_VM_QEMU_AARCH64_EFI" ] ||
			zxfer_vm_die "ZXFER_VM_QEMU_AARCH64_EFI is not readable: $ZXFER_VM_QEMU_AARCH64_EFI"
		printf '%s\n' "$ZXFER_VM_QEMU_AARCH64_EFI"
		return 0
	fi

	for l_candidate in \
		/opt/homebrew/share/qemu/edk2-aarch64-code.fd \
		/usr/local/share/qemu/edk2-aarch64-code.fd \
		/opt/homebrew/share/qemu/edk2-aarch64-code.bin \
		/usr/local/share/qemu/edk2-aarch64-code.bin \
		/usr/share/qemu/edk2-aarch64-code.fd \
		/usr/share/qemu/edk2-aarch64-code.bin \
		/usr/share/qemu-efi-aarch64/QEMU_EFI.fd \
		/usr/share/edk2/aarch64/QEMU_EFI.fd \
		/usr/share/AAVMF/AAVMF_CODE.fd; do
		if [ -r "$l_candidate" ]; then
			printf '%s\n' "$l_candidate"
			return 0
		fi
	done

	zxfer_vm_die "Unable to locate a readable aarch64 UEFI firmware file for qemu; install QEMU firmware or set ZXFER_VM_QEMU_AARCH64_EFI"
}

zxfer_vm_qemu_select_accel() {
	l_guest_arch=$1

	if zxfer_vm_host_can_accelerate_guest_arch "$l_guest_arch"; then
		case "$ZXFER_VM_HOST_OS" in
		linux)
			printf '%s\n' "kvm"
			;;
		darwin)
			printf '%s\n' "hvf"
			;;
		*)
			printf '%s\n' "tcg"
			;;
		esac
	else
		printf '%s\n' "tcg"
	fi
}

zxfer_vm_qemu_machine_arg() {
	l_guest_arch=$1
	l_accel=$2

	case "$l_guest_arch" in
	amd64)
		printf 'q35,accel=%s\n' "$l_accel"
		;;
	arm64)
		# Keep QEMU's default highmem=on layout for aarch64 guests so
		# Apple Silicon local runs can place 4 GiB above the 32-bit
		# boundary instead of failing at launch.
		printf 'virt,accel=%s\n' "$l_accel"
		;;
	*)
		return 1
		;;
	esac
}

zxfer_vm_qemu_start_guest() {
	l_guest_arch=$1
	l_accel=$2
	l_http_port=$3
	l_ssh_port=$4
	l_overlay_path=$5
	l_artifact_dir=$6
	l_seed_transport=$7
	l_seed_image_path=${8:-}

	l_qemu_bin=$(zxfer_vm_qemu_system_binary "$l_guest_arch") ||
		zxfer_vm_die "Unsupported qemu guest architecture: $l_guest_arch"
	l_machine=$(zxfer_vm_qemu_machine_arg "$l_guest_arch" "$l_accel") ||
		zxfer_vm_die "Unsupported qemu machine selection for guest architecture: $l_guest_arch"

	set -- "$l_qemu_bin" \
		-daemonize \
		-pidfile "$ZXFER_VM_QEMU_PID_FILE" \
		-display none \
		-monitor none \
		-serial file:"$l_artifact_dir/serial.log" \
		-machine "$l_machine" \
		-cpu max \
		-smp 4 \
		-m 4096
	if [ "$l_guest_arch" = "arm64" ]; then
		l_efi_firmware=$(zxfer_vm_qemu_resolve_aarch64_efi) || return 1
		set -- "$@" -bios "$l_efi_firmware"
	fi
	set -- "$@" \
		-device virtio-rng-pci \
		-nic user,model=virtio-net-pci,hostfwd=tcp:127.0.0.1:"$l_ssh_port"-:22 \
		-drive if=virtio,format=qcow2,file="$l_overlay_path"
	case "$l_seed_transport" in
	disk-cidata)
		set -- "$@" -drive if=virtio,media=cdrom,format=raw,readonly=on,file="$l_seed_image_path"
		;;
	*)
		set -- "$@" -smbios type=1,serial="ds=nocloud-net;s=http://10.0.2.2:$l_http_port/"
		;;
	esac
	"$@"
}

zxfer_vm_qemu_pid_file_has_dead_process() {
	l_pid_file=${1:-}
	l_pid=

	[ -n "$l_pid_file" ] || return 1
	[ -r "$l_pid_file" ] || return 1
	IFS= read -r l_pid <"$l_pid_file" || return 1
	case "$l_pid" in
	"" | *[!0123456789]*)
		return 1
		;;
	esac

	kill -s 0 "$l_pid" >/dev/null 2>&1 && return 1
	return 0
}

# Guest SSH trust: the harness pins the guest's ed25519 host key in a per-run
# known_hosts file, and every later ssh runs with StrictHostKeyChecking=yes
# against that file, so the key it trusts is the key it connects with. A key
# change during first boot (cloud-init may regenerate host keys) is logged
# and re-pinned while waiting for readiness; after readiness a changed key
# stops the run. A scan matches when the pinned file still holds its line
# (ssh may append other key types there under `UpdateHostKeys yes`).

# Purpose: Scan the guest's ed25519 host key into FILE as one normalized
# known_hosts line ("[host]:port ssh-ed25519 KEY").
# Usage: zxfer_vm_qemu_scan_host_key HOST PORT FILE; returns 1 and leaves no
# FILE when the scan printed no key (sshd down, refused or timed out).
zxfer_vm_qemu_scan_host_key() {
	l_scan_host=$1
	l_scan_port=$2
	l_scan_file=$3

	# One key type is one connection per scan (sshd's PerSourcePenalties
	# charge each unauthenticated one) and output whose order cannot vary.
	# Comment lines, other key types and trailing fields are dropped.
	ssh-keyscan -T 5 -t ed25519 -p "$l_scan_port" "$l_scan_host" 2>/dev/null |
		awk '$1 !~ /^[#;]/ && $2 == "ssh-ed25519" && $3 != "" { print $1, $2, $3 }' |
		sort -u >"$l_scan_file"
	[ -s "$l_scan_file" ] && return 0
	rm -f "$l_scan_file"
	return 1
}

# Purpose: Wait until the guest accepts the harness key over SSH on a stable,
# pinned host key.
# Usage: zxfer_vm_qemu_wait_for_ssh HOST PORT KNOWN_HOSTS IDENTITY
#   [TIMEOUT_SECONDS] [LOG_PREFIX] [MIN_SUCCESSES]; KNOWN_HOSTS is replaced.
# Returns 0 after MIN_SUCCESSES consecutive probes on one key, else 1 with
# ZXFER_VM_QEMU_WAIT_FAILURE_REASON set to timeout or qemu_exited.
zxfer_vm_qemu_wait_for_ssh() {
	l_wait_host=$1
	l_wait_port=$2
	l_wait_known_hosts=$3
	l_wait_identity=$4
	l_wait_timeout=${5:-1800}
	l_wait_log_prefix=${6:-}
	l_wait_min_successes=${7:-1}
	l_wait_scan_file=$l_wait_known_hosts.scan
	l_wait_elapsed=0
	l_wait_delay=0
	l_wait_next_progress=30
	l_wait_successes=0
	# shellcheck disable=SC2034  # Read by the qemu backend after this helper fails.
	ZXFER_VM_QEMU_WAIT_FAILURE_REASON=timeout

	[ "$l_wait_min_successes" -gt 0 ] 2>/dev/null || l_wait_min_successes=1

	rm -f "$l_wait_known_hosts" "$l_wait_scan_file"
	while [ "$l_wait_elapsed" -lt "$l_wait_timeout" ]; do
		# Scan only before the first probe and after a failed one; an empty
		# scan is not a key change and leaves the pinned key in place.
		if [ "$l_wait_successes" -eq 0 ] &&
			zxfer_vm_qemu_scan_host_key "$l_wait_host" "$l_wait_port" "$l_wait_scan_file"; then
			if [ -f "$l_wait_known_hosts" ] &&
				! grep -q -x -F -f "$l_wait_scan_file" "$l_wait_known_hosts" &&
				[ -n "$l_wait_log_prefix" ]; then
				zxfer_vm_log "==> [$l_wait_log_prefix] guest SSH host key changed during first boot; pinning the new key and restarting the readiness count"
			fi
			mv "$l_wait_scan_file" "$l_wait_known_hosts"
		fi
		if [ -f "$l_wait_known_hosts" ] &&
			zxfer_vm_qemu_ssh_probe "$l_wait_host" "$l_wait_port" "$l_wait_known_hosts" "$l_wait_identity"; then
			l_wait_successes=$((l_wait_successes + 1))
			if [ "$l_wait_successes" -ge "$l_wait_min_successes" ]; then
				[ -z "$l_wait_log_prefix" ] ||
					zxfer_vm_log "==> [$l_wait_log_prefix] SSH ready after ${l_wait_elapsed}s of waiting"
				return 0
			fi
			l_wait_delay=5
		else
			# Back off (doubling from 5 s, at most 20 s) so failed connections from
			# the one QEMU user-network address stay under the rate that
			# makes sshd's PerSourcePenalties refuse it.
			l_wait_successes=0
			l_wait_delay=$((l_wait_delay * 2))
			[ "$l_wait_delay" -ge 5 ] || l_wait_delay=5
			[ "$l_wait_delay" -le 20 ] || l_wait_delay=20
		fi
		if zxfer_vm_qemu_pid_file_has_dead_process "${ZXFER_VM_QEMU_PID_FILE:-}"; then
			# shellcheck disable=SC2034  # Read by the qemu backend after this helper fails.
			ZXFER_VM_QEMU_WAIT_FAILURE_REASON=qemu_exited
			if [ -n "$l_wait_log_prefix" ]; then
				zxfer_vm_warn "Guest [$l_wait_log_prefix] qemu process exited before SSH readiness."
			fi
			return 1
		fi
		sleep "$l_wait_delay"
		l_wait_elapsed=$((l_wait_elapsed + l_wait_delay))
		if [ -n "$l_wait_log_prefix" ] && [ "$l_wait_elapsed" -ge "$l_wait_next_progress" ]; then
			zxfer_vm_log "==> [$l_wait_log_prefix] still waiting for SSH readiness (${l_wait_elapsed}s elapsed)"
			l_wait_next_progress=$((l_wait_elapsed + 30))
		fi
	done

	return 1
}

# Purpose: Run ssh against a QEMU guest using the pinned per-run known_hosts
# and generated ed25519 identity.
# Usage: zxfer_vm_qemu_ssh PORT KNOWN_HOSTS IDENTITY [SSH_ARG...]
zxfer_vm_qemu_ssh() {
	l_qemu_ssh_port=$1
	l_qemu_ssh_known_hosts=$2
	l_qemu_ssh_identity=$3
	shift 3

	ssh -i "$l_qemu_ssh_identity" \
		-o BatchMode=yes \
		-o IdentitiesOnly=yes \
		-o StrictHostKeyChecking=yes \
		-o UserKnownHostsFile="$l_qemu_ssh_known_hosts" \
		-p "$l_qemu_ssh_port" \
		"$@"
}

# Purpose: Check that the guest runs a command over SSH on the pinned key.
# Usage: zxfer_vm_qemu_ssh_probe HOST PORT KNOWN_HOSTS IDENTITY
zxfer_vm_qemu_ssh_probe() {
	zxfer_vm_qemu_ssh "$2" "$3" "$4" -o ConnectTimeout=10 root@"$1" true >/dev/null 2>&1
}

# Purpose: Before a remote step, wait for SSH on the pinned host key (sshd may
# be restarting); a different key is refused, never re-pinned.
# Usage: zxfer_vm_qemu_prepare_remote_ssh_step HOST PORT KNOWN_HOSTS IDENTITY
#   [LOG_PREFIX] [STEP_LABEL] [TIMEOUT_SECONDS]; returns 1 on timeout or at
#   once when the guest presents another host key.
zxfer_vm_qemu_prepare_remote_ssh_step() {
	l_step_host=$1
	l_step_port=$2
	l_step_known_hosts=$3
	l_step_identity=$4
	l_step_log_prefix=${5:-}
	l_step_label=${6:-remote step}
	l_step_timeout=${7:-30}
	l_step_scan_file=$l_step_known_hosts.scan
	l_step_elapsed=0

	while [ "$l_step_elapsed" -lt "$l_step_timeout" ]; do
		if zxfer_vm_qemu_ssh_probe "$l_step_host" "$l_step_port" "$l_step_known_hosts" "$l_step_identity"; then
			if [ "$l_step_elapsed" -gt 0 ] && [ -n "$l_step_log_prefix" ]; then
				zxfer_vm_log "==> [$l_step_log_prefix] SSH readiness recovered for $l_step_label"
			fi
			return 0
		fi
		if zxfer_vm_qemu_scan_host_key "$l_step_host" "$l_step_port" "$l_step_scan_file" &&
			! grep -q -x -F -f "$l_step_scan_file" "$l_step_known_hosts"; then
			rm -f "$l_step_scan_file"
			zxfer_vm_warn "[$l_step_log_prefix] the guest SSH host key changed after readiness; refusing to trust the new key before $l_step_label"
			return 1
		fi
		rm -f "$l_step_scan_file"
		sleep 5
		l_step_elapsed=$((l_step_elapsed + 5))
		if [ -n "$l_step_log_prefix" ] && [ $((l_step_elapsed % 15)) -eq 0 ]; then
			zxfer_vm_log "==> [$l_step_log_prefix] still waiting for SSH readiness before $l_step_label (${l_step_elapsed}s elapsed)"
		fi
	done

	return 1
}

zxfer_vm_qemu_render_cloud_init() {
	l_seed_dir=$1
	l_public_key_file=$2
	l_public_key=

	l_public_key=$(cat "$l_public_key_file") ||
		zxfer_vm_die "Unable to read generated SSH public key: $l_public_key_file"
	zxfer_vm_mkdir_p "$l_seed_dir"
	cat <<EOF >"$l_seed_dir/meta-data"
instance-id: zxfer-$g_vm_guest_name
local-hostname: zxfer-$g_vm_guest_name
EOF
	case "$g_vm_guest_cloud_init_style" in
	root-login)
		cat <<EOF >"$l_seed_dir/user-data"
#cloud-config
ssh_pwauth: false
users:
  - default
ssh_authorized_keys:
  - $l_public_key
runcmd:
  - |
    set -eu
    mkdir -p /root/.ssh
    chmod 700 /root/.ssh
    cat <<'KEY_EOF' >/root/.ssh/authorized_keys
    $l_public_key
    KEY_EOF
    chmod 600 /root/.ssh/authorized_keys
    chown -R root:wheel /root/.ssh
    if grep -Eq '^[[:space:]]*#?[[:space:]]*PermitRootLogin[[:space:]]+' /etc/ssh/sshd_config; then
      sed -i '' -E 's|^[[:space:]]*#?[[:space:]]*PermitRootLogin[[:space:]]+.*$|PermitRootLogin yes|' /etc/ssh/sshd_config
    else
      printf '%s\n' 'PermitRootLogin yes' >>/etc/ssh/sshd_config
    fi
    service sshd restart
EOF
		;;
	default)
		cat <<EOF >"$l_seed_dir/user-data"
#cloud-config
disable_root: false
ssh_pwauth: false
write_files:
  - path: /root/.ssh/authorized_keys
    owner: root:root
    permissions: '0600'
    content: |
      $l_public_key
runcmd:
  - mkdir -p /root/.ssh
  - chmod 700 /root/.ssh
  - chown root:root /root/.ssh
EOF
		;;
	*)
		zxfer_vm_die "Unsupported cloud-init style [$g_vm_guest_cloud_init_style] for guest [$g_vm_guest_name]"
		;;
	esac
}

zxfer_vm_qemu_create_seed_image() {
	l_seed_dir=$1
	l_seed_image_path=$2

	rm -f "$l_seed_image_path"
	case "$ZXFER_VM_HOST_OS" in
	darwin)
		hdiutil makehybrid \
			-quiet \
			-iso \
			-joliet \
			-default-volume-name cidata \
			-o "$l_seed_image_path" \
			"$l_seed_dir" >/dev/null ||
			zxfer_vm_die "Failed to build cidata seed image with hdiutil"
		;;
	linux | wsl2)
		if command -v xorriso >/dev/null 2>&1; then
			xorriso -as mkisofs \
				-quiet \
				-volid cidata \
				-joliet \
				-rock \
				-output "$l_seed_image_path" \
				"$l_seed_dir" >/dev/null 2>&1 ||
				zxfer_vm_die "Failed to build cidata seed image with xorriso"
		elif command -v genisoimage >/dev/null 2>&1; then
			genisoimage \
				-quiet \
				-volid cidata \
				-joliet \
				-rock \
				-output "$l_seed_image_path" \
				"$l_seed_dir" >/dev/null 2>&1 ||
				zxfer_vm_die "Failed to build cidata seed image with genisoimage"
		elif command -v mkisofs >/dev/null 2>&1; then
			mkisofs \
				-quiet \
				-volid cidata \
				-joliet \
				-rock \
				-o "$l_seed_image_path" \
				"$l_seed_dir" >/dev/null 2>&1 ||
				zxfer_vm_die "Failed to build cidata seed image with mkisofs"
		else
			zxfer_vm_die "No supported cidata seed-image builder is available on $ZXFER_VM_HOST_OS"
		fi
		;;
	*)
		zxfer_vm_die "Unsupported host OS for cidata seed-image creation: $ZXFER_VM_HOST_OS"
		;;
	esac
}

zxfer_vm_qemu_write_repo_archive() {
	if [ "$ZXFER_VM_HOST_OS" = "darwin" ]; then
		COPYFILE_DISABLE=1 \
			COPY_EXTENDED_ATTRIBUTES_DISABLE=1 \
			tar --no-mac-metadata --no-xattrs --exclude .git -cf - -C "$ZXFER_ROOT" .
	else
		tar --exclude .git -cf - -C "$ZXFER_ROOT" .
	fi
}

zxfer_vm_qemu_write_ref_archive() {
	l_ref=$1

	case "$l_ref" in
	'' | -* | *'
'*)
		zxfer_vm_die "ZXFER_VM_PERF_BASELINE_REF must be a non-empty ref name, tag, or commit and must not begin with '-' or contain newlines: $l_ref"
		;;
	esac
	git -C "$ZXFER_ROOT" rev-parse --verify "$l_ref^{tree}" >/dev/null 2>&1 ||
		zxfer_vm_die "ZXFER_VM_PERF_BASELINE_REF does not name a tree in this repository: $l_ref"
	git -C "$ZXFER_ROOT" archive --format=tar "$l_ref" ||
		zxfer_vm_die "Failed to archive ZXFER_VM_PERF_BASELINE_REF: $l_ref"
}

zxfer_vm_qemu_log_cached_download_state() {
	l_log_prefix=$1
	l_subject=$2
	l_action=$3
	l_size_bytes=$4
	l_size=

	l_size=$(zxfer_vm_format_bytes "$l_size_bytes")
	case "$l_action" in
	cached)
		zxfer_vm_log "==> [$l_log_prefix] reusing cached $l_subject ($l_size)"
		;;
	downloaded)
		zxfer_vm_log "==> [$l_log_prefix] downloaded $l_subject ($l_size)"
		;;
	esac
}

zxfer_vm_qemu_log_base_image_state() {
	l_log_prefix=$1
	l_action=$2
	l_size_bytes=$3
	l_size=

	l_size=$(zxfer_vm_format_bytes "$l_size_bytes")
	case "$l_action" in
	cached)
		zxfer_vm_log "==> [$l_log_prefix] reusing prepared base image ($l_size)"
		;;
	prepared)
		zxfer_vm_log "==> [$l_log_prefix] prepared base image ($l_size)"
		;;
	esac
}

zxfer_vm_qemu_resize_overlay_if_needed() {
	l_log_prefix=$1
	l_overlay_path=$2
	l_min_disk_size=$3

	if [ -z "$l_min_disk_size" ]; then
		return 0
	fi

	zxfer_vm_log "==> [$l_log_prefix] resizing writable overlay to $l_min_disk_size"
	qemu-img resize "$l_overlay_path" "$l_min_disk_size" >/dev/null ||
		zxfer_vm_die "Failed to resize writable overlay for [$l_log_prefix] to $l_min_disk_size"
}

zxfer_vm_qemu_copy_repo_to_guest() {
	zxfer_vm_qemu_write_repo_archive |
		zxfer_vm_qemu_ssh "$1" "$2" "$3" root@127.0.0.1 \
			"rm -rf '$4' && mkdir -p '$4' && tar xf - -C '$4'" ||
		zxfer_vm_die "Failed to copy repository contents into the guest"
}

zxfer_vm_qemu_copy_ref_to_guest() {
	zxfer_vm_qemu_write_ref_archive "$5" |
		zxfer_vm_qemu_ssh "$1" "$2" "$3" root@127.0.0.1 \
			"rm -rf '$4' && mkdir -p '$4' && tar xf - -C '$4'" ||
		zxfer_vm_die "Failed to copy baseline ref [$5] into the guest"
}

zxfer_vm_qemu_run_remote_script() {
	l_port=$1
	l_known_hosts=$2
	l_identity=$3
	l_script_file=$4
	l_stdout_file=$5
	l_stderr_file=$6
	l_status=0

	zxfer_vm_run_command_with_captured_output \
		"$l_stdout_file" \
		"$l_stderr_file" \
		"[$g_vm_guest_name stdout]" \
		"[$g_vm_guest_name stderr]" \
		zxfer_vm_qemu_ssh "$l_port" "$l_known_hosts" "$l_identity" \
		root@127.0.0.1 /bin/sh -s <"$l_script_file" || l_status=$?

	return "$l_status"
}

zxfer_vm_qemu_collect_remote_artifacts() {
	zxfer_vm_mkdir_p "$5/guest-artifacts"
	zxfer_vm_qemu_ssh "$1" "$2" "$3" root@127.0.0.1 \
		"if [ -d '$4' ]; then tar cf - -C '$4' .; fi" |
		tar xf - -C "$5/guest-artifacts" 2>/dev/null || true
}

zxfer_vm_backend_qemu_run_guest() {
	l_guest=$1
	l_artifact_dir=$2
	l_guest_cache_dir=
	l_checksum_file=
	l_checksum=
	l_archive_path=
	l_base_image_path=
	l_overlay_path=
	l_state_dir=
	l_seed_dir=
	l_seed_image_path=
	l_identity_path=
	l_http_port=
	l_ssh_port=
	l_accel=
	l_test_script=
	l_prepare_script=
	l_remote_dir=/root/zxfer
	l_remote_baseline_dir=/root/zxfer-baseline
	l_remote_artifact_dir=/var/tmp/zxfer-vm-matrix
	l_status=0
	l_saved_traps=

	zxfer_vm_load_guest "$l_guest" ||
		zxfer_vm_die "No qemu guest architecture is defined for guest [$l_guest]"
	l_guest_display_name="$g_vm_guest_label/$g_vm_guest_arch"
	l_prepare_script=$(zxfer_vm_guest_prepare_script qemu "$ZXFER_VM_TEST_LAYER") ||
		zxfer_vm_die "No qemu guest preparation script is defined for guest [$l_guest]"
	l_accel=$(zxfer_vm_qemu_select_accel "$g_vm_guest_arch")

	if [ "$l_accel" = "tcg" ]; then
		case ",$g_vm_guest_strict_qemu_profiles," in
		*",$ZXFER_VM_PROFILE,"*)
			zxfer_vm_die "Guest [$l_guest/$g_vm_guest_arch] requires hardware virtualization for strict isolation, but only TCG emulation is available on $ZXFER_VM_HOST_OS/$ZXFER_VM_HOST_ARCH"
			;;
		esac
		zxfer_vm_warn "Guest [$l_guest_display_name] is running under best-effort TCG emulation; this is not a strict isolation gate."
	fi
	l_test_script=$(zxfer_vm_render_guest_test_script "$l_remote_dir" "$l_remote_artifact_dir")

	zxfer_vm_log "==> [$l_guest_display_name] resolving image via qemu backend"
	zxfer_vm_mkdir_p "$ZXFER_VM_CACHE_DIR"
	zxfer_vm_mkdir_p "$l_artifact_dir"

	l_guest_cache_dir=$(zxfer_vm_join_path "$ZXFER_VM_CACHE_DIR" "$l_guest")
	l_guest_cache_dir=$(zxfer_vm_join_path "$l_guest_cache_dir" "$g_vm_guest_arch")
	zxfer_vm_mkdir_p "$l_guest_cache_dir"
	l_checksum_file=$(zxfer_vm_join_path "$l_guest_cache_dir" "$(basename "$g_vm_guest_checksum_url")")
	l_archive_path=$(zxfer_vm_join_path "$l_guest_cache_dir" "$g_vm_guest_image_filename")
	l_base_image_path=$(zxfer_vm_join_path "$l_guest_cache_dir" "$g_vm_guest_base_image_name")

	zxfer_vm_log "==> [$l_guest_display_name] refreshing checksum manifest $(basename "$l_checksum_file")"
	zxfer_vm_refresh_cached_download \
		"$g_vm_guest_checksum_url" \
		"$l_checksum_file" \
		"$l_guest_display_name" \
		"checksum manifest $(basename "$l_checksum_file")"
	zxfer_vm_log "==> [$l_guest_display_name] checksum manifest ready ($(zxfer_vm_format_bytes "$(zxfer_vm_file_size_bytes "$l_checksum_file")"))"
	l_checksum=$(zxfer_vm_resolve_expected_checksum "$l_checksum_file" "$g_vm_guest_image_filename") || return 1

	zxfer_vm_log "==> [$l_guest_display_name] ensuring guest image cache $(basename "$l_archive_path")"
	zxfer_vm_download_and_verify_file "$g_vm_guest_image_url" "$l_archive_path" "$l_checksum" || return 1
	zxfer_vm_qemu_log_cached_download_state \
		"$l_guest_display_name" \
		"guest image" \
		"$ZXFER_VM_LAST_DOWNLOAD_ACTION" \
		"$ZXFER_VM_LAST_DOWNLOAD_SIZE_BYTES"

	zxfer_vm_log "==> [$l_guest_display_name] preparing base image $(basename "$l_base_image_path")"
	zxfer_vm_decompress_archive "$l_archive_path" "$l_base_image_path" "$g_vm_guest_archive_compression"
	zxfer_vm_qemu_log_base_image_state \
		"$l_guest_display_name" \
		"$ZXFER_VM_LAST_DECOMPRESS_ACTION" \
		"$ZXFER_VM_LAST_DECOMPRESS_SIZE_BYTES"

	l_state_dir=$(mktemp -d -t "zxfer_vm_${l_guest}.XXXXXX") ||
		zxfer_vm_die "Unable to create state directory for guest [$l_guest]"
	ZXFER_VM_QEMU_STATE_DIR=$l_state_dir
	ZXFER_VM_QEMU_PRESERVE_STATE=0
	l_saved_traps=$(trap)
	trap 'zxfer_vm_qemu_guest_state_cleanup' EXIT
	trap 'zxfer_vm_qemu_handle_signal HUP' HUP
	trap 'zxfer_vm_qemu_handle_signal INT' INT
	trap 'zxfer_vm_qemu_handle_signal TERM' TERM

	l_seed_dir=$(zxfer_vm_join_path "$l_state_dir" "seed")
	l_seed_image_path=$(zxfer_vm_join_path "$l_state_dir" "seed-cidata.iso")
	l_identity_path=$(zxfer_vm_join_path "$l_state_dir" "id_ed25519")
	l_ssh_port=$(zxfer_vm_allocate_tcp_port)
	l_overlay_path=$(zxfer_vm_join_path "$l_state_dir" "overlay.qcow2")
	l_http_port=0

	ssh-keygen -q -t ed25519 -N "" -f "$l_identity_path" >/dev/null ||
		zxfer_vm_die "Unable to generate temporary SSH key for guest [$l_guest]"
	zxfer_vm_qemu_render_cloud_init "$l_seed_dir" "$l_identity_path.pub"
	case "$g_vm_guest_seed_transport" in
	disk-cidata)
		zxfer_vm_log "==> [$l_guest_display_name] building cidata seed image"
		zxfer_vm_qemu_create_seed_image "$l_seed_dir" "$l_seed_image_path"
		;;
	*)
		l_http_port=$(zxfer_vm_allocate_tcp_port)
		(
			cd "$l_seed_dir" &&
				exec python3 -m http.server "$l_http_port" --bind 127.0.0.1
		) >"$l_artifact_dir/http.stdout" 2>"$l_artifact_dir/http.stderr" &
		ZXFER_VM_QEMU_HTTP_PID=$!
		;;
	esac

	qemu-img create -f qcow2 -F "$g_vm_guest_base_format" -b "$l_base_image_path" "$l_overlay_path" >/dev/null ||
		zxfer_vm_die "Failed to create writable overlay for guest [$l_guest]"
	zxfer_vm_qemu_resize_overlay_if_needed "$l_guest_display_name" "$l_overlay_path" "$g_vm_guest_min_disk_size"
	ZXFER_VM_QEMU_PID_FILE=$(zxfer_vm_join_path "$l_state_dir" "qemu.pid")

	zxfer_vm_qemu_start_guest "$g_vm_guest_arch" "$l_accel" "$l_http_port" "$l_ssh_port" \
		"$l_overlay_path" "$l_artifact_dir" "$g_vm_guest_seed_transport" "$l_seed_image_path" ||
		zxfer_vm_die "Failed to start qemu guest [$l_guest]"

	zxfer_vm_log "==> [$l_guest_display_name] waiting for SSH readiness on 127.0.0.1:$l_ssh_port"
	if ! zxfer_vm_qemu_wait_for_ssh 127.0.0.1 "$l_ssh_port" "$l_state_dir/known_hosts" "$l_identity_path" "$g_vm_guest_ssh_ready_timeout_seconds" "$l_guest_display_name" "$g_vm_guest_ssh_ready_probe_count"; then
		ZXFER_VM_QEMU_PRESERVE_STATE=$ZXFER_VM_PRESERVE_FAILED_GUESTS
		if [ "${ZXFER_VM_QEMU_WAIT_FAILURE_REASON:-}" = "qemu_exited" ]; then
			zxfer_vm_die "Guest [$l_guest] qemu exited before SSH readiness; inspect $l_artifact_dir/serial.log"
		fi
		zxfer_vm_die "Timed out waiting for guest [$l_guest] SSH readiness; inspect $l_artifact_dir/serial.log"
	fi

	zxfer_vm_log "==> [$l_guest_display_name] copying repository into guest"
	zxfer_vm_qemu_prepare_remote_ssh_step \
		127.0.0.1 "$l_ssh_port" "$l_state_dir/known_hosts" "$l_identity_path" \
		"$l_guest_display_name" "copying the repository" ||
		zxfer_vm_die "Guest [$l_guest] did not accept SSH on its pinned host key before copying the repository"
	zxfer_vm_qemu_copy_repo_to_guest "$l_ssh_port" "$l_state_dir/known_hosts" "$l_identity_path" "$l_remote_dir"
	if [ "$ZXFER_VM_TEST_LAYER" = "perf-compare" ]; then
		zxfer_vm_log "==> [$l_guest_display_name] copying baseline ref ${ZXFER_VM_PERF_BASELINE_REF:-upstream-compat-final} into guest"
		zxfer_vm_qemu_prepare_remote_ssh_step \
			127.0.0.1 "$l_ssh_port" "$l_state_dir/known_hosts" "$l_identity_path" \
			"$l_guest_display_name" "copying the performance baseline" ||
			zxfer_vm_die "Guest [$l_guest] did not accept SSH on its pinned host key before copying the performance baseline"
		zxfer_vm_qemu_copy_ref_to_guest \
			"$l_ssh_port" \
			"$l_state_dir/known_hosts" \
			"$l_identity_path" \
			"$l_remote_baseline_dir" \
			"${ZXFER_VM_PERF_BASELINE_REF:-upstream-compat-final}"
	fi
	if [ "$ZXFER_VM_STREAM_GUEST_OUTPUT" != "1" ]; then
		zxfer_vm_log "==> [$l_guest_display_name] guest logs: $l_artifact_dir/prepare.stdout, $l_artifact_dir/prepare.stderr, $l_artifact_dir/harness.stdout, $l_artifact_dir/harness.stderr"
	fi

	cat <<EOF >"$l_artifact_dir/prepare-guest.sh"
#!/bin/sh
set -eu
$l_prepare_script
EOF
	chmod 700 "$l_artifact_dir/prepare-guest.sh"

	zxfer_vm_log "==> [$l_guest_display_name] running guest preparation"
	zxfer_vm_qemu_prepare_remote_ssh_step \
		127.0.0.1 "$l_ssh_port" "$l_state_dir/known_hosts" "$l_identity_path" \
		"$l_guest_display_name" "guest preparation" ||
		zxfer_vm_die "Guest [$l_guest] did not accept SSH on its pinned host key before guest preparation"
	if zxfer_vm_qemu_run_remote_script "$l_ssh_port" "$l_state_dir/known_hosts" "$l_identity_path" \
		"$l_artifact_dir/prepare-guest.sh" "$l_artifact_dir/prepare.stdout" "$l_artifact_dir/prepare.stderr"; then
		:
	else
		ZXFER_VM_QEMU_PRESERVE_STATE=$ZXFER_VM_PRESERVE_FAILED_GUESTS
		zxfer_vm_die "Guest preparation failed for [$l_guest]; inspect $l_artifact_dir/prepare.stderr"
	fi

	cat <<EOF >"$l_artifact_dir/run-harness.sh"
#!/bin/sh
set -eu
rm -rf "$l_remote_artifact_dir"
$l_test_script
EOF
	chmod 700 "$l_artifact_dir/run-harness.sh"

	zxfer_vm_log "==> [$l_guest_display_name] running $(zxfer_vm_test_layer_run_label "$ZXFER_VM_TEST_LAYER")"
	zxfer_vm_qemu_prepare_remote_ssh_step \
		127.0.0.1 "$l_ssh_port" "$l_state_dir/known_hosts" "$l_identity_path" \
		"$l_guest_display_name" "the selected guest test layer" ||
		zxfer_vm_die "Guest [$l_guest] did not accept SSH on its pinned host key before the selected guest test layer"
	if zxfer_vm_qemu_run_remote_script "$l_ssh_port" "$l_state_dir/known_hosts" "$l_identity_path" \
		"$l_artifact_dir/run-harness.sh" "$l_artifact_dir/harness.stdout" "$l_artifact_dir/harness.stderr"; then
		l_status=0
	else
		l_status=$?
		zxfer_vm_report_guest_command_failure \
			"$l_guest_display_name" \
			"$(zxfer_vm_test_layer_run_label "$ZXFER_VM_TEST_LAYER")" \
			"$l_status" \
			"$l_artifact_dir/harness.stdout" \
			"$l_artifact_dir/harness.stderr"
		ZXFER_VM_QEMU_PRESERVE_STATE=$ZXFER_VM_PRESERVE_FAILED_GUESTS
	fi

	zxfer_vm_log "==> [$l_guest_display_name] collecting guest artifacts"
	zxfer_vm_qemu_collect_remote_artifacts "$l_ssh_port" "$l_state_dir/known_hosts" "$l_identity_path" \
		"$l_remote_artifact_dir" "$l_artifact_dir"

	trap - EXIT HUP INT TERM
	zxfer_vm_qemu_guest_state_cleanup
	eval "$l_saved_traps"
	return "$l_status"
}
