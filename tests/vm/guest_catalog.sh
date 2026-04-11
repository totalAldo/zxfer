#!/bin/sh
#
# Guest metadata for the VM-backed integration runner.
#

zxfer_vm_guest_exists() {
	case "$1" in
	ubuntu | freebsd | omnios) return 0 ;;
	*) return 1 ;;
	esac
}

zxfer_vm_guest_label() {
	case "$1" in
	ubuntu) printf '%s\n' "Ubuntu 24.04" ;;
	freebsd) printf '%s\n' "FreeBSD 15.0" ;;
	omnios) printf '%s\n' "OmniOS r151056" ;;
	*) return 1 ;;
	esac
}

zxfer_vm_profile_guests() {
	case "$1" in
	smoke)
		printf '%s\n' "ubuntu"
		;;
	local)
		printf '%s\n' "ubuntu freebsd"
		;;
	full | ci)
		printf '%s\n' "ubuntu freebsd omnios"
		;;
	*)
		return 1
		;;
	esac
}

zxfer_vm_guest_qemu_supports_arch() {
	case "$1/$2" in
	ubuntu/amd64 | ubuntu/arm64 | freebsd/amd64 | freebsd/arm64 | omnios/amd64)
		return 0
		;;
	*)
		return 1
		;;
	esac
}

zxfer_vm_guest_qemu_preferred_arch() {
	l_guest=$1

	if [ "${ZXFER_VM_HOST_ARCH:-}" = "arm64" ] &&
		zxfer_vm_guest_qemu_supports_arch "$l_guest" "arm64"; then
		printf '%s\n' "arm64"
		return 0
	fi
	if zxfer_vm_guest_qemu_supports_arch "$l_guest" "amd64"; then
		printf '%s\n' "amd64"
		return 0
	fi

	return 1
}

zxfer_vm_guest_qemu_image_filename() {
	l_guest=$1
	l_arch=${2:-$(zxfer_vm_guest_qemu_preferred_arch "$l_guest")} || return 1

	case "$l_guest/$l_arch" in
	ubuntu/amd64)
		printf '%s\n' "ubuntu-24.04-server-cloudimg-amd64.img"
		;;
	ubuntu/arm64)
		printf '%s\n' "ubuntu-24.04-server-cloudimg-arm64.img"
		;;
	freebsd/amd64)
		printf '%s\n' "FreeBSD-15.0-RELEASE-amd64-BASIC-CLOUDINIT-zfs.qcow2.xz"
		;;
	freebsd/arm64)
		printf '%s\n' "FreeBSD-15.0-RELEASE-arm64-aarch64-BASIC-CLOUDINIT-zfs.qcow2.xz"
		;;
	omnios/amd64)
		printf '%s\n' "omnios-r151056.cloud.qcow2"
		;;
	*)
		return 1
		;;
	esac
}

zxfer_vm_guest_qemu_image_url() {
	l_guest=$1
	l_arch=${2:-$(zxfer_vm_guest_qemu_preferred_arch "$l_guest")} || return 1
	l_file_name=$(zxfer_vm_guest_qemu_image_filename "$l_guest" "$l_arch") || return 1

	case "$l_guest/$l_arch" in
	ubuntu/amd64 | ubuntu/arm64)
		printf '%s\n' "https://cloud-images.ubuntu.com/releases/noble/release/$l_file_name"
		;;
	freebsd/amd64)
		printf '%s\n' "https://download.freebsd.org/releases/VM-IMAGES/15.0-RELEASE/amd64/Latest/$l_file_name"
		;;
	freebsd/arm64)
		printf '%s\n' "https://download.freebsd.org/releases/VM-IMAGES/15.0-RELEASE/aarch64/Latest/$l_file_name"
		;;
	omnios/amd64)
		printf '%s\n' "https://downloads.omnios.org/media/stable/$l_file_name"
		;;
	*)
		return 1
		;;
	esac
}

zxfer_vm_guest_qemu_checksum_url() {
	l_guest=$1
	l_arch=${2:-$(zxfer_vm_guest_qemu_preferred_arch "$l_guest")} || return 1

	case "$l_guest/$l_arch" in
	ubuntu/amd64 | ubuntu/arm64)
		printf '%s\n' "https://cloud-images.ubuntu.com/releases/noble/release/SHA256SUMS"
		;;
	freebsd/amd64)
		printf '%s\n' "https://download.freebsd.org/releases/VM-IMAGES/15.0-RELEASE/amd64/Latest/CHECKSUM.SHA256"
		;;
	freebsd/arm64)
		printf '%s\n' "https://download.freebsd.org/releases/VM-IMAGES/15.0-RELEASE/aarch64/Latest/CHECKSUM.SHA256"
		;;
	omnios/amd64)
		printf '%s\n' "https://downloads.omnios.org/media/stable/omnios-r151056.cloud.qcow2.sha256"
		;;
	*)
		return 1
		;;
	esac
}

zxfer_vm_guest_qemu_archive_compression() {
	l_guest=$1
	l_arch=${2:-$(zxfer_vm_guest_qemu_preferred_arch "$l_guest")} || return 1

	case "$l_guest/$l_arch" in
	ubuntu/amd64 | ubuntu/arm64 | omnios/amd64)
		printf '%s\n' "none"
		;;
	freebsd/amd64 | freebsd/arm64)
		printf '%s\n' "xz"
		;;
	*)
		return 1
		;;
	esac
}

zxfer_vm_guest_qemu_base_image_name() {
	l_guest=$1
	l_arch=${2:-$(zxfer_vm_guest_qemu_preferred_arch "$l_guest")} || return 1
	l_file_name=$(zxfer_vm_guest_qemu_image_filename "$l_guest" "$l_arch") || return 1
	l_compression=$(zxfer_vm_guest_qemu_archive_compression "$l_guest" "$l_arch") || return 1

	case "$l_compression" in
	xz)
		printf '%s\n' "${l_file_name%.xz}"
		;;
	zst)
		printf '%s\n' "${l_file_name%.zst}"
		;;
	none)
		printf '%s\n' "$l_file_name"
		;;
	*)
		return 1
		;;
	esac
}

zxfer_vm_guest_qemu_base_format() {
	l_guest=$1
	l_arch=${2:-$(zxfer_vm_guest_qemu_preferred_arch "$l_guest")} || return 1

	case "$l_guest/$l_arch" in
	ubuntu/amd64 | ubuntu/arm64 | freebsd/amd64 | freebsd/arm64 | omnios/amd64)
		printf '%s\n' "qcow2"
		;;
	*)
		return 1
		;;
	esac
}

zxfer_vm_guest_qemu_shell() {
	case "$1" in
	omnios)
		printf '%s\n' "/usr/xpg4/bin/sh"
		;;
	ubuntu | freebsd)
		printf '%s\n' "/bin/sh"
		;;
	*)
		return 1
		;;
	esac
}

zxfer_vm_guest_qemu_seed_transport() {
	case "$1" in
	freebsd)
		printf '%s\n' "disk-cidata"
		;;
	ubuntu | omnios)
		printf '%s\n' "smbios-nocloud-net"
		;;
	*)
		return 1
		;;
	esac
}

zxfer_vm_guest_qemu_ssh_ready_probe_count() {
	case "$1" in
	omnios)
		printf '%s\n' "3"
		;;
	ubuntu | freebsd)
		printf '%s\n' "1"
		;;
	*)
		return 1
		;;
	esac
}

zxfer_vm_guest_shunit_jobs() {
	case "$1" in
	ubuntu)
		printf '%s\n' "4"
		;;
	freebsd | omnios)
		printf '%s\n' "2"
		;;
	*)
		return 1
		;;
	esac
}

zxfer_vm_guest_prepare_script() {
	l_guest=$1
	l_backend=$2
	l_test_layer=$3

	case "$l_test_layer/$l_backend/$l_guest" in
	integration/ci-managed/ubuntu)
		cat <<'EOF'
export PATH="/usr/sbin:/usr/bin:/sbin:/bin:$PATH"
apt-get update
apt-get install -y csh zfsutils-linux parallel zstd
modprobe zfs
zfs version
zpool version
EOF
		;;
	integration/ci-managed/freebsd)
		cat <<'EOF'
pkg install -y parallel zstd
kldload zfs || true
EOF
		;;
	integration/ci-managed/omnios)
		cat <<'EOF'
PKG_SUCCESS_ON_NOP=1 pkg install zstd
EOF
		;;
	integration/qemu/ubuntu)
		cat <<'EOF'
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y csh zfsutils-linux parallel zstd
modprobe zfs
EOF
		;;
	integration/qemu/freebsd)
		cat <<'EOF'
ASSUME_ALWAYS_YES=yes pkg bootstrap -f
pkg install -y parallel zstd
kldload zfs || true
EOF
		;;
	integration/qemu/omnios)
		cat <<'EOF'
PKG_SUCCESS_ON_NOP=1 pkg install zstd
EOF
		;;
	shunit2/ci-managed/ubuntu | shunit2/qemu/ubuntu)
		cat <<'EOF'
:
EOF
		;;
	shunit2/ci-managed/freebsd | shunit2/qemu/freebsd)
		cat <<'EOF'
ASSUME_ALWAYS_YES=yes pkg bootstrap -f
pkg install -y bash
EOF
		;;
	shunit2/ci-managed/omnios | shunit2/qemu/omnios)
		cat <<'EOF'
PKG_SUCCESS_ON_NOP=1 pkg install bash
EOF
		;;
	*)
		return 1
		;;
	esac
}
