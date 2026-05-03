#!/bin/sh
#
# QEMU backend for local disposable guest execution.
#

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
		l_guest_arch=$(zxfer_vm_guest_qemu_preferred_arch "$l_guest") ||
			zxfer_vm_die "No qemu guest architecture is defined for guest [$l_guest]"
		l_seed_transport=$(zxfer_vm_guest_qemu_seed_transport "$l_guest") ||
			zxfer_vm_die "No qemu seed transport is defined for guest [$l_guest]"
		l_qemu_cmd=$(zxfer_vm_qemu_system_binary "$l_guest_arch") ||
			zxfer_vm_die "No qemu system binary is defined for guest architecture [$l_guest_arch]"
		if ! zxfer_vm_list_contains "$l_required_qemu_commands" "$l_qemu_cmd"; then
			zxfer_vm_require_command "$l_qemu_cmd"
			l_required_qemu_commands=$(zxfer_vm_append_word "$l_required_qemu_commands" "$l_qemu_cmd")
		fi
		if [ "$l_guest_arch" = "arm64" ]; then
			zxfer_vm_qemu_resolve_aarch64_efi >/dev/null
		fi
		if [ "$l_seed_transport" = "disk-cidata" ]; then
			zxfer_vm_qemu_require_seed_image_builder
		fi
	done

	if command -v xz >/dev/null 2>&1; then
		:
	else
		zxfer_vm_warn "xz is not installed; FreeBSD guest downloads will fail until it is available."
	fi
	if command -v zstd >/dev/null 2>&1; then
		:
	else
		zxfer_vm_warn "zstd is not installed; .zst guest image archives would fail until it is available."
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
	l_machine=

	case "$l_guest_arch" in
	amd64)
		l_machine=$(zxfer_vm_qemu_machine_arg "$l_guest_arch" "$l_accel") ||
			zxfer_vm_die "Unsupported qemu machine selection for guest architecture: $l_guest_arch"
		case "$l_seed_transport" in
		disk-cidata)
			qemu-system-x86_64 \
				-daemonize \
				-pidfile "$ZXFER_VM_QEMU_PID_FILE" \
				-display none \
				-monitor none \
				-serial file:"$l_artifact_dir/serial.log" \
				-machine "$l_machine" \
				-cpu max \
				-smp 4 \
				-m 4096 \
				-device virtio-rng-pci \
				-nic user,model=virtio-net-pci,hostfwd=tcp:127.0.0.1:"$l_ssh_port"-:22 \
				-drive if=virtio,format=qcow2,file="$l_overlay_path" \
				-drive if=virtio,media=cdrom,format=raw,readonly=on,file="$l_seed_image_path"
			;;
		*)
			qemu-system-x86_64 \
				-daemonize \
				-pidfile "$ZXFER_VM_QEMU_PID_FILE" \
				-display none \
				-monitor none \
				-serial file:"$l_artifact_dir/serial.log" \
				-machine "$l_machine" \
				-cpu max \
				-smp 4 \
				-m 4096 \
				-device virtio-rng-pci \
				-nic user,model=virtio-net-pci,hostfwd=tcp:127.0.0.1:"$l_ssh_port"-:22 \
				-drive if=virtio,format=qcow2,file="$l_overlay_path" \
				-smbios type=1,serial="ds=nocloud-net;s=http://10.0.2.2:$l_http_port/"
			;;
		esac
		;;
	arm64)
		l_efi_firmware=$(zxfer_vm_qemu_resolve_aarch64_efi)
		l_machine=$(zxfer_vm_qemu_machine_arg "$l_guest_arch" "$l_accel") ||
			zxfer_vm_die "Unsupported qemu machine selection for guest architecture: $l_guest_arch"
		case "$l_seed_transport" in
		disk-cidata)
			qemu-system-aarch64 \
				-daemonize \
				-pidfile "$ZXFER_VM_QEMU_PID_FILE" \
				-display none \
				-monitor none \
				-serial file:"$l_artifact_dir/serial.log" \
				-machine "$l_machine" \
				-cpu max \
				-smp 4 \
				-m 4096 \
				-bios "$l_efi_firmware" \
				-device virtio-rng-pci \
				-nic user,model=virtio-net-pci,hostfwd=tcp:127.0.0.1:"$l_ssh_port"-:22 \
				-drive if=virtio,format=qcow2,file="$l_overlay_path" \
				-drive if=virtio,media=cdrom,format=raw,readonly=on,file="$l_seed_image_path"
			;;
		*)
			qemu-system-aarch64 \
				-daemonize \
				-pidfile "$ZXFER_VM_QEMU_PID_FILE" \
				-display none \
				-monitor none \
				-serial file:"$l_artifact_dir/serial.log" \
				-machine "$l_machine" \
				-cpu max \
				-smp 4 \
				-m 4096 \
				-bios "$l_efi_firmware" \
				-device virtio-rng-pci \
				-nic user,model=virtio-net-pci,hostfwd=tcp:127.0.0.1:"$l_ssh_port"-:22 \
				-drive if=virtio,format=qcow2,file="$l_overlay_path" \
				-smbios type=1,serial="ds=nocloud-net;s=http://10.0.2.2:$l_http_port/"
			;;
		esac
		;;
	*)
		zxfer_vm_die "Unsupported qemu guest architecture: $l_guest_arch"
		;;
	esac
}

zxfer_vm_qemu_wait_for_ssh() {
	l_host=$1
	l_port=$2
	l_known_hosts=$3
	l_identity=$4
	l_timeout_seconds=${5:-900}
	l_log_prefix=${6:-}
	l_min_successes=${7:-1}
	l_elapsed=0
	l_scan_file=$l_known_hosts.scan
	l_current_signature=
	l_last_signature=
	l_consecutive_successes=0

	[ "$l_min_successes" -gt 0 ] 2>/dev/null || l_min_successes=1

	rm -f "$l_scan_file"
	while [ "$l_elapsed" -lt "$l_timeout_seconds" ]; do
		if zxfer_vm_qemu_refresh_known_hosts "$l_host" "$l_port" "$l_known_hosts"; then
			l_current_signature=$(zxfer_vm_qemu_known_hosts_signature "$l_known_hosts")
			if [ "$l_current_signature" != "$l_last_signature" ]; then
				if [ -n "$l_last_signature" ] && [ -n "$l_log_prefix" ]; then
					zxfer_vm_log "==> [$l_log_prefix] detected a guest SSH host-key change during first boot; revalidating"
				fi
				l_last_signature=$l_current_signature
				l_consecutive_successes=0
			fi
			if zxfer_vm_qemu_ssh_probe "$l_host" "$l_port" "$l_known_hosts" "$l_identity"; then
				l_consecutive_successes=$((l_consecutive_successes + 1))
				if [ "$l_consecutive_successes" -ge "$l_min_successes" ]; then
					return 0
				fi
			else
				l_consecutive_successes=0
			fi
		else
			l_consecutive_successes=0
		fi
		sleep 5
		l_elapsed=$((l_elapsed + 5))
		if [ -n "$l_log_prefix" ] &&
			[ "$l_elapsed" -gt 0 ] &&
			[ $((l_elapsed % 30)) -eq 0 ]; then
			zxfer_vm_log "==> [$l_log_prefix] still waiting for SSH readiness (${l_elapsed}s elapsed)"
		fi
	done

	return 1
}

zxfer_vm_qemu_refresh_known_hosts() {
	l_host=$1
	l_port=$2
	l_known_hosts=$3
	l_scan_file=$l_known_hosts.scan

	rm -f "$l_scan_file"
	if ssh-keyscan -T 5 -p "$l_port" "$l_host" >"$l_scan_file" 2>/dev/null &&
		[ -s "$l_scan_file" ]; then
		mv "$l_scan_file" "$l_known_hosts"
		return 0
	fi

	rm -f "$l_scan_file"
	return 1
}

zxfer_vm_qemu_ssh_probe() {
	l_host=$1
	l_port=$2
	l_known_hosts=$3
	l_identity=$4

	ssh -i "$l_identity" \
		-o BatchMode=yes \
		-o IdentitiesOnly=yes \
		-o StrictHostKeyChecking=yes \
		-o UserKnownHostsFile="$l_known_hosts" \
		-o ConnectTimeout=10 \
		-p "$l_port" \
		root@"$l_host" true >/dev/null 2>&1
}

zxfer_vm_qemu_prepare_remote_ssh_step() {
	l_host=$1
	l_port=$2
	l_known_hosts=$3
	l_identity=$4
	l_log_prefix=${5:-}
	l_step_label=${6:-remote step}
	l_timeout_seconds=${7:-30}
	l_elapsed=0

	while [ "$l_elapsed" -lt "$l_timeout_seconds" ]; do
		if zxfer_vm_qemu_refresh_known_hosts "$l_host" "$l_port" "$l_known_hosts"; then
			if [ "$l_elapsed" -gt 0 ] && [ -n "$l_log_prefix" ]; then
				zxfer_vm_log "==> [$l_log_prefix] SSH host-key refresh recovered for $l_step_label"
			fi
			return 0
		fi
		if [ -r "$l_known_hosts" ] &&
			zxfer_vm_qemu_ssh_probe "$l_host" "$l_port" "$l_known_hosts" "$l_identity"; then
			if [ -n "$l_log_prefix" ]; then
				zxfer_vm_warn "[$l_log_prefix] ssh-keyscan did not refresh the guest host key before $l_step_label; reusing the existing validated known_hosts entry"
			fi
			return 0
		fi

		sleep 5
		l_elapsed=$((l_elapsed + 5))
		if [ -n "$l_log_prefix" ] &&
			[ "$l_elapsed" -gt 0 ] &&
			[ $((l_elapsed % 15)) -eq 0 ]; then
			zxfer_vm_log "==> [$l_log_prefix] still waiting for SSH host-key refresh before $l_step_label (${l_elapsed}s elapsed)"
		fi
	done

	return 1
}

zxfer_vm_qemu_known_hosts_signature() {
	l_known_hosts=$1

	[ -r "$l_known_hosts" ] || return 1
	cksum <"$l_known_hosts" 2>/dev/null
}

zxfer_vm_qemu_render_cloud_init() {
	l_guest=$1
	l_seed_dir=$2
	l_public_key_file=$3
	l_public_key=

	l_public_key=$(cat "$l_public_key_file") ||
		zxfer_vm_die "Unable to read generated SSH public key: $l_public_key_file"
	zxfer_vm_mkdir_p "$l_seed_dir"
	cat <<EOF >"$l_seed_dir/meta-data"
instance-id: zxfer-$l_guest
local-hostname: zxfer-$l_guest
EOF
	case "$l_guest" in
	freebsd)
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
	*)
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

zxfer_vm_qemu_copy_repo_to_guest() {
	l_port=$1
	l_known_hosts=$2
	l_identity=$3
	l_remote_dir=$4

	zxfer_vm_qemu_write_repo_archive |
		ssh -i "$l_identity" \
			-o BatchMode=yes \
			-o IdentitiesOnly=yes \
			-o StrictHostKeyChecking=yes \
			-o UserKnownHostsFile="$l_known_hosts" \
			-p "$l_port" \
			root@127.0.0.1 "rm -rf '$l_remote_dir' && mkdir -p '$l_remote_dir' && tar xf - -C '$l_remote_dir'" ||
		zxfer_vm_die "Failed to copy repository contents into the guest"
}

zxfer_vm_qemu_copy_ref_to_guest() {
	l_port=$1
	l_known_hosts=$2
	l_identity=$3
	l_remote_dir=$4
	l_ref=$5

	zxfer_vm_qemu_write_ref_archive "$l_ref" |
		ssh -i "$l_identity" \
			-o BatchMode=yes \
			-o IdentitiesOnly=yes \
			-o StrictHostKeyChecking=yes \
			-o UserKnownHostsFile="$l_known_hosts" \
			-p "$l_port" \
			root@127.0.0.1 "rm -rf '$l_remote_dir' && mkdir -p '$l_remote_dir' && tar xf - -C '$l_remote_dir'" ||
		zxfer_vm_die "Failed to copy baseline ref [$l_ref] into the guest"
}

zxfer_vm_qemu_run_remote_script() {
	l_port=$1
	l_known_hosts=$2
	l_identity=$3
	l_script_file=$4
	l_stdout_file=$5
	l_stderr_file=$6
	l_status=0

	if zxfer_vm_run_command_with_captured_output \
		"$l_stdout_file" \
		"$l_stderr_file" \
		"[$ZXFER_VM_CURRENT_GUEST stdout]" \
		"[$ZXFER_VM_CURRENT_GUEST stderr]" \
		ssh -i "$l_identity" \
		-o BatchMode=yes \
		-o IdentitiesOnly=yes \
		-o StrictHostKeyChecking=yes \
		-o UserKnownHostsFile="$l_known_hosts" \
		-p "$l_port" \
		root@127.0.0.1 /bin/sh -s <"$l_script_file"; then
		l_status=0
	else
		l_status=$?
	fi

	return "$l_status"
}

zxfer_vm_qemu_collect_remote_artifacts() {
	l_port=$1
	l_known_hosts=$2
	l_identity=$3
	l_remote_artifact_dir=$4
	l_artifact_dir=$5

	zxfer_vm_mkdir_p "$l_artifact_dir/guest-artifacts"
	ssh -i "$l_identity" \
		-o BatchMode=yes \
		-o IdentitiesOnly=yes \
		-o StrictHostKeyChecking=yes \
		-o UserKnownHostsFile="$l_known_hosts" \
		-p "$l_port" \
		root@127.0.0.1 "if [ -d '$l_remote_artifact_dir' ]; then tar cf - -C '$l_remote_artifact_dir' .; fi" |
		tar xf - -C "$l_artifact_dir/guest-artifacts" 2>/dev/null || true
}

zxfer_vm_backend_qemu_run_guest() {
	l_guest=$1
	l_artifact_dir=$2
	l_guest_label=$(zxfer_vm_guest_label "$l_guest") || return 1
	l_guest_image_url=
	l_guest_checksum_url=
	l_guest_image_filename=
	l_guest_base_name=
	l_guest_archive_compression=
	l_guest_base_format=
	l_guest_arch=
	l_seed_transport=
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
	l_ssh_ready_probe_count=1
	l_status=0
	l_saved_traps=

	l_guest_arch=$(zxfer_vm_guest_qemu_preferred_arch "$l_guest") ||
		zxfer_vm_die "No qemu guest architecture is defined for guest [$l_guest]"
	l_guest_image_url=$(zxfer_vm_guest_qemu_image_url "$l_guest" "$l_guest_arch") ||
		zxfer_vm_die "No qemu image URL is defined for guest [$l_guest]"
	l_guest_checksum_url=$(zxfer_vm_guest_qemu_checksum_url "$l_guest" "$l_guest_arch") ||
		zxfer_vm_die "No qemu checksum URL is defined for guest [$l_guest]"
	l_guest_image_filename=$(zxfer_vm_guest_qemu_image_filename "$l_guest" "$l_guest_arch") ||
		zxfer_vm_die "No qemu image filename is defined for guest [$l_guest]"
	l_guest_base_name=$(zxfer_vm_guest_qemu_base_image_name "$l_guest" "$l_guest_arch") ||
		zxfer_vm_die "No qemu base image name is defined for guest [$l_guest]"
	l_guest_archive_compression=$(zxfer_vm_guest_qemu_archive_compression "$l_guest" "$l_guest_arch") ||
		zxfer_vm_die "No qemu archive compression is defined for guest [$l_guest]"
	l_guest_base_format=$(zxfer_vm_guest_qemu_base_format "$l_guest" "$l_guest_arch") ||
		zxfer_vm_die "No qemu base image format is defined for guest [$l_guest]"
	l_seed_transport=$(zxfer_vm_guest_qemu_seed_transport "$l_guest") ||
		zxfer_vm_die "No qemu seed transport is defined for guest [$l_guest]"
	zxfer_vm_guest_qemu_shell "$l_guest" >/dev/null ||
		zxfer_vm_die "No guest shell is defined for guest [$l_guest]"
	l_ssh_ready_probe_count=$(zxfer_vm_guest_qemu_ssh_ready_probe_count "$l_guest") ||
		zxfer_vm_die "No qemu SSH readiness threshold is defined for guest [$l_guest]"
	l_prepare_script=$(zxfer_vm_guest_prepare_script "$l_guest" "qemu" "$ZXFER_VM_TEST_LAYER") ||
		zxfer_vm_die "No qemu guest preparation script is defined for guest [$l_guest]"
	l_accel=$(zxfer_vm_qemu_select_accel "$l_guest_arch")

	if [ "$l_accel" = "tcg" ]; then
		if [ "$ZXFER_VM_STRICT_ISOLATION_REQUIRED" = "1" ]; then
			zxfer_vm_die "Guest [$l_guest/$l_guest_arch] requires hardware virtualization for strict isolation, but only TCG emulation is available on $ZXFER_VM_HOST_OS/$ZXFER_VM_HOST_ARCH"
		fi
		zxfer_vm_warn "Guest [$l_guest_label/$l_guest_arch] is running under best-effort TCG emulation; this is not a strict isolation gate."
	fi
	l_test_script=$(zxfer_vm_render_guest_test_script "$l_guest" "$l_remote_dir" "$l_remote_artifact_dir")

	zxfer_vm_log "==> [$l_guest_label/$l_guest_arch] resolving image via qemu backend"
	zxfer_vm_mkdir_p "$ZXFER_VM_CACHE_DIR"
	zxfer_vm_mkdir_p "$l_artifact_dir"

	l_guest_cache_dir=$(zxfer_vm_join_path "$ZXFER_VM_CACHE_DIR" "$l_guest")
	l_guest_cache_dir=$(zxfer_vm_join_path "$l_guest_cache_dir" "$l_guest_arch")
	zxfer_vm_mkdir_p "$l_guest_cache_dir"
	l_checksum_file=$(zxfer_vm_join_path "$l_guest_cache_dir" "$(basename "$l_guest_checksum_url")")
	l_archive_path=$(zxfer_vm_join_path "$l_guest_cache_dir" "$l_guest_image_filename")
	l_base_image_path=$(zxfer_vm_join_path "$l_guest_cache_dir" "$l_guest_base_name")

	zxfer_vm_log "==> [$l_guest_label/$l_guest_arch] refreshing checksum manifest $(basename "$l_checksum_file")"
	zxfer_vm_refresh_cached_download \
		"$l_guest_checksum_url" \
		"$l_checksum_file" \
		"$l_guest_label/$l_guest_arch" \
		"checksum manifest $(basename "$l_checksum_file")"
	zxfer_vm_log "==> [$l_guest_label/$l_guest_arch] checksum manifest ready ($(zxfer_vm_format_bytes "$(zxfer_vm_file_size_bytes "$l_checksum_file")"))"
	l_checksum=$(zxfer_vm_resolve_expected_checksum "$l_checksum_file" "$l_guest_image_filename")

	zxfer_vm_log "==> [$l_guest_label/$l_guest_arch] ensuring guest image cache $(basename "$l_archive_path")"
	zxfer_vm_download_and_verify_file "$l_guest_image_url" "$l_archive_path" "$l_checksum"
	zxfer_vm_qemu_log_cached_download_state \
		"$l_guest_label/$l_guest_arch" \
		"guest image" \
		"$ZXFER_VM_LAST_DOWNLOAD_ACTION" \
		"$ZXFER_VM_LAST_DOWNLOAD_SIZE_BYTES"

	zxfer_vm_log "==> [$l_guest_label/$l_guest_arch] preparing base image $(basename "$l_base_image_path")"
	zxfer_vm_decompress_archive "$l_archive_path" "$l_base_image_path" "$l_guest_archive_compression"
	zxfer_vm_qemu_log_base_image_state \
		"$l_guest_label/$l_guest_arch" \
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
	zxfer_vm_qemu_render_cloud_init "$l_guest" "$l_seed_dir" "$l_identity_path.pub"
	case "$l_seed_transport" in
	disk-cidata)
		zxfer_vm_log "==> [$l_guest_label/$l_guest_arch] building cidata seed image"
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

	qemu-img create -f qcow2 -F "$l_guest_base_format" -b "$l_base_image_path" "$l_overlay_path" >/dev/null ||
		zxfer_vm_die "Failed to create writable overlay for guest [$l_guest]"
	ZXFER_VM_QEMU_PID_FILE=$(zxfer_vm_join_path "$l_state_dir" "qemu.pid")

	zxfer_vm_qemu_start_guest "$l_guest_arch" "$l_accel" "$l_http_port" "$l_ssh_port" \
		"$l_overlay_path" "$l_artifact_dir" "$l_seed_transport" "$l_seed_image_path" ||
		zxfer_vm_die "Failed to start qemu guest [$l_guest]"

	zxfer_vm_log "==> [$l_guest_label/$l_guest_arch] waiting for SSH readiness on 127.0.0.1:$l_ssh_port"
	if ! zxfer_vm_qemu_wait_for_ssh 127.0.0.1 "$l_ssh_port" "$l_state_dir/known_hosts" "$l_identity_path" 900 "$l_guest_label/$l_guest_arch" "$l_ssh_ready_probe_count"; then
		ZXFER_VM_QEMU_PRESERVE_STATE=$ZXFER_VM_PRESERVE_FAILED_GUESTS
		zxfer_vm_die "Timed out waiting for guest [$l_guest] SSH readiness; inspect $l_artifact_dir/serial.log"
	fi

	zxfer_vm_log "==> [$l_guest_label/$l_guest_arch] copying repository into guest"
	zxfer_vm_qemu_prepare_remote_ssh_step \
		127.0.0.1 "$l_ssh_port" "$l_state_dir/known_hosts" "$l_identity_path" \
		"$l_guest_label/$l_guest_arch" "copying the repository" ||
		zxfer_vm_die "Failed to refresh the guest SSH host key before copying the repository for [$l_guest]"
	zxfer_vm_qemu_copy_repo_to_guest "$l_ssh_port" "$l_state_dir/known_hosts" "$l_identity_path" "$l_remote_dir"
	if [ "$ZXFER_VM_TEST_LAYER" = "perf-compare" ]; then
		zxfer_vm_log "==> [$l_guest_label/$l_guest_arch] copying baseline ref ${ZXFER_VM_PERF_BASELINE_REF:-upstream-compat-final} into guest"
		zxfer_vm_qemu_prepare_remote_ssh_step \
			127.0.0.1 "$l_ssh_port" "$l_state_dir/known_hosts" "$l_identity_path" \
			"$l_guest_label/$l_guest_arch" "copying the performance baseline" ||
			zxfer_vm_die "Failed to refresh the guest SSH host key before copying the performance baseline for [$l_guest]"
		zxfer_vm_qemu_copy_ref_to_guest \
			"$l_ssh_port" \
			"$l_state_dir/known_hosts" \
			"$l_identity_path" \
			"$l_remote_baseline_dir" \
			"${ZXFER_VM_PERF_BASELINE_REF:-upstream-compat-final}"
	fi
	if [ "$ZXFER_VM_STREAM_GUEST_OUTPUT" != "1" ]; then
		zxfer_vm_log "==> [$l_guest_label/$l_guest_arch] guest logs: $l_artifact_dir/prepare.stdout, $l_artifact_dir/prepare.stderr, $l_artifact_dir/harness.stdout, $l_artifact_dir/harness.stderr"
	fi

	cat <<EOF >"$l_artifact_dir/prepare-guest.sh"
#!/bin/sh
set -eu
$l_prepare_script
EOF
	chmod 700 "$l_artifact_dir/prepare-guest.sh"

	zxfer_vm_log "==> [$l_guest_label/$l_guest_arch] running guest preparation"
	zxfer_vm_qemu_prepare_remote_ssh_step \
		127.0.0.1 "$l_ssh_port" "$l_state_dir/known_hosts" "$l_identity_path" \
		"$l_guest_label/$l_guest_arch" "guest preparation" ||
		zxfer_vm_die "Failed to refresh the guest SSH host key before guest preparation for [$l_guest]"
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

	zxfer_vm_log "==> [$l_guest_label/$l_guest_arch] running $(zxfer_vm_test_layer_run_label "$ZXFER_VM_TEST_LAYER")"
	zxfer_vm_qemu_prepare_remote_ssh_step \
		127.0.0.1 "$l_ssh_port" "$l_state_dir/known_hosts" "$l_identity_path" \
		"$l_guest_label/$l_guest_arch" "the selected guest test layer" ||
		zxfer_vm_die "Failed to refresh the guest SSH host key before the selected guest test layer for [$l_guest]"
	if zxfer_vm_qemu_run_remote_script "$l_ssh_port" "$l_state_dir/known_hosts" "$l_identity_path" \
		"$l_artifact_dir/run-harness.sh" "$l_artifact_dir/harness.stdout" "$l_artifact_dir/harness.stderr"; then
		l_status=0
	else
		l_status=$?
		zxfer_vm_report_guest_command_failure \
			"$l_guest_label/$l_guest_arch" \
			"$(zxfer_vm_test_layer_run_label "$ZXFER_VM_TEST_LAYER")" \
			"$l_status" \
			"$l_artifact_dir/harness.stdout" \
			"$l_artifact_dir/harness.stderr"
		ZXFER_VM_QEMU_PRESERVE_STATE=$ZXFER_VM_PRESERVE_FAILED_GUESTS
	fi

	zxfer_vm_log "==> [$l_guest_label/$l_guest_arch] collecting guest artifacts"
	zxfer_vm_qemu_refresh_known_hosts 127.0.0.1 "$l_ssh_port" "$l_state_dir/known_hosts" >/dev/null 2>&1 || true
	zxfer_vm_qemu_collect_remote_artifacts "$l_ssh_port" "$l_state_dir/known_hosts" "$l_identity_path" \
		"$l_remote_artifact_dir" "$l_artifact_dir"

	trap - EXIT HUP INT TERM
	zxfer_vm_qemu_guest_state_cleanup
	eval "$l_saved_traps"
	return "$l_status"
}
