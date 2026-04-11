#!/bin/sh
#
# Shared helpers for the VM-backed integration runner.
#

zxfer_vm_log() {
	printf '%s\n' "$*"
}

zxfer_vm_warn() {
	printf 'WARNING: %s\n' "$*" >&2
}

zxfer_vm_die() {
	printf 'ERROR: %s\n' "$*" >&2
	exit 1
}

zxfer_vm_require_command() {
	l_cmd=$1

	command -v "$l_cmd" >/dev/null 2>&1 ||
		zxfer_vm_die "Required command not found: $l_cmd"
}

zxfer_vm_compute_root_from_path() {
	l_script_path=$1

	case "$l_script_path" in
	/*)
		l_abs_path=$l_script_path
		;;
	*)
		l_abs_path=${PWD:-.}/$l_script_path
		;;
	esac

	l_script_dir=$(cd "$(dirname "$l_abs_path")/.." 2>/dev/null && pwd -P) ||
		zxfer_vm_die "Unable to resolve repository root from $l_script_path"
	printf '%s\n' "$l_script_dir"
}

# shellcheck disable=SC2034  # Shared state written here is consumed across sourced vm helpers and tests.
zxfer_vm_reset_shared_state() {
	ZXFER_VM_HOST_OS=
	ZXFER_VM_HOST_ARCH=
	ZXFER_VM_HOST_CAN_ACCELERATE_AMD64=0
	ZXFER_VM_HOST_CAN_ACCELERATE_ARM64=0
	ZXFER_VM_HOST_CAN_ACCELERATE_X86_64=0
	ZXFER_VM_CACHE_DIR=
	ZXFER_VM_ARTIFACT_ROOT=
	ZXFER_VM_SELECTED_GUESTS=
	ZXFER_VM_FAILED_GUESTS=
	ZXFER_VM_CURRENT_GUEST=
	ZXFER_VM_STRICT_ISOLATION_REQUIRED=0
	ZXFER_VM_BACKEND=
	ZXFER_VM_PROFILE=
	ZXFER_VM_PRESERVE_FAILED_GUESTS=0
	ZXFER_VM_ACTIVE_BACKGROUND_PIDS=
	ZXFER_VM_PARALLEL_STATE_DIR=
	ZXFER_VM_SHUTTING_DOWN=0
	ZXFER_VM_LAST_BACKGROUND_PID=
	ZXFER_VM_LAST_WAIT_GUEST=
	ZXFER_VM_LAST_WAIT_PID=
	ZXFER_VM_LAST_WAIT_STATUS=0
	ZXFER_VM_LAST_DOWNLOAD_ACTION=
	ZXFER_VM_LAST_DOWNLOAD_SIZE_BYTES=
	ZXFER_VM_LAST_DECOMPRESS_ACTION=
	ZXFER_VM_LAST_DECOMPRESS_SIZE_BYTES=
}

zxfer_vm_detect_host_platform() {
	l_kernel=${ZXFER_VM_UNAME_S:-$(uname -s)}
	l_machine=${ZXFER_VM_UNAME_M:-$(uname -m)}
	l_osrelease_file=${ZXFER_VM_PROC_OSRELEASE_FILE:-/proc/sys/kernel/osrelease}
	l_version_file=${ZXFER_VM_PROC_VERSION_FILE:-/proc/version}

	case "$l_kernel" in
	Linux)
		if [ -r "$l_osrelease_file" ] &&
			grep -Eqi '(microsoft|wsl)' "$l_osrelease_file" 2>/dev/null; then
			ZXFER_VM_HOST_OS=wsl2
		elif [ -r "$l_version_file" ] &&
			grep -Eqi '(microsoft|wsl)' "$l_version_file" 2>/dev/null; then
			ZXFER_VM_HOST_OS=wsl2
		else
			ZXFER_VM_HOST_OS=linux
		fi
		;;
	Darwin)
		ZXFER_VM_HOST_OS=darwin
		;;
	*)
		zxfer_vm_die "Unsupported VM-matrix host OS: $l_kernel"
		;;
	esac

	case "$l_machine" in
	x86_64 | amd64)
		ZXFER_VM_HOST_ARCH=amd64
		;;
	arm64 | aarch64)
		ZXFER_VM_HOST_ARCH=arm64
		;;
	*)
		zxfer_vm_die "Unsupported VM-matrix host architecture: $l_machine"
		;;
	esac
}

zxfer_vm_set_default_paths() {
	if [ -n "${ZXFER_VM_CACHE_DIR:-}" ]; then
		:
	elif [ -n "${XDG_CACHE_HOME:-}" ]; then
		ZXFER_VM_CACHE_DIR=$XDG_CACHE_HOME/zxfer/vm-images
	elif [ -n "${HOME:-}" ]; then
		ZXFER_VM_CACHE_DIR=$HOME/.cache/zxfer/vm-images
	else
		ZXFER_VM_CACHE_DIR=${TMPDIR:-/tmp}/zxfer-vm-images
	fi

	if [ -n "${ZXFER_VM_ARTIFACT_ROOT:-}" ]; then
		:
	elif [ -n "${XDG_STATE_HOME:-}" ]; then
		ZXFER_VM_ARTIFACT_ROOT=$XDG_STATE_HOME/zxfer/vm-matrix
	elif [ -n "${HOME:-}" ]; then
		ZXFER_VM_ARTIFACT_ROOT=$HOME/.local/state/zxfer/vm-matrix
	else
		ZXFER_VM_ARTIFACT_ROOT=${TMPDIR:-/tmp}/zxfer-vm-matrix
	fi
}

zxfer_vm_mkdir_p() {
	mkdir -p "$1" || zxfer_vm_die "Unable to create directory: $1"
}

zxfer_vm_append_word() {
	l_current=$1
	l_word=$2

	if [ -z "$l_current" ]; then
		printf '%s\n' "$l_word"
	else
		printf '%s %s\n' "$l_current" "$l_word"
	fi
}

zxfer_vm_remove_word() {
	l_current=$1
	l_word=$2
	l_result=

	for l_item in $l_current; do
		if [ "$l_item" = "$l_word" ]; then
			continue
		fi
		l_result=$(zxfer_vm_append_word "$l_result" "$l_item")
	done

	printf '%s\n' "$l_result"
}

zxfer_vm_positive_integer_p() {
	case "$1" in
	'' | *[!0-9]* | 0)
		return 1
		;;
	*)
		return 0
		;;
	esac
}

zxfer_vm_signal_exit_status() {
	case "$1" in
	HUP)
		printf '%s\n' "129"
		;;
	INT)
		printf '%s\n' "130"
		;;
	TERM)
		printf '%s\n' "143"
		;;
	*)
		printf '%s\n' "1"
		;;
	esac
}

zxfer_vm_stderr_is_tty() {
	if [ -n "${ZXFER_VM_STDERR_IS_TTY:-}" ]; then
		[ "$ZXFER_VM_STDERR_IS_TTY" = "1" ]
		return $?
	fi

	[ -t 2 ]
}

zxfer_vm_should_show_progress_bar() {
	if ! zxfer_vm_stderr_is_tty; then
		return 1
	fi

	[ "${ZXFER_VM_JOBS:-1}" -eq 1 ]
}

zxfer_vm_list_contains() {
	l_list=$1
	l_word=$2

	case " $l_list " in
	*" $l_word "*) return 0 ;;
	*) return 1 ;;
	esac
}

zxfer_vm_count_words() {
	l_list=$1

	if [ -z "$l_list" ]; then
		printf '0\n'
		return 0
	fi

	# shellcheck disable=SC2086  # Intentional word splitting of space-delimited guest lists.
	set -- $l_list
	printf '%s\n' "$#"
}

zxfer_vm_sha256_file() {
	l_file=$1

	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$l_file" | awk '{print $1}'
		return 0
	fi
	if command -v shasum >/dev/null 2>&1; then
		shasum -a 256 "$l_file" | awk '{print $1}'
		return 0
	fi

	zxfer_vm_die "A SHA-256 tool is required (sha256sum or shasum)."
}

zxfer_vm_file_size_bytes() {
	l_file=$1

	wc -c <"$l_file" 2>/dev/null | awk '{print $1}'
}

zxfer_vm_format_bytes() {
	l_bytes=${1:-0}

	awk -v bytes="$l_bytes" '
		BEGIN {
			split("B KiB MiB GiB TiB PiB", units, " ")
			value = bytes + 0
			unit_index = 1
			while (value >= 1024 && unit_index < 6) {
				value /= 1024
				unit_index++
			}
			if (unit_index == 1) {
				printf "%d %s\n", value, units[unit_index]
			} else {
				printf "%.1f %s\n", value, units[unit_index]
			}
		}
	'
}

zxfer_vm_resolve_expected_checksum() {
	l_checksum_file=$1
	l_file_name=$2

	awk -v file_name="$l_file_name" '
		$0 ~ ("^SHA256 \\(" file_name "\\) = ") {
			print $NF
			found = 1
			exit
		}
		NF >= 2 && ($2 == file_name || $2 == ("*" file_name)) {
			print $1
			found = 1
			exit
		}
		NR == 1 && NF >= 1 && length($1) == 64 {
			first_hash = $1
			line_count = 1
			next
		}
		{
			line_count = NR
		}
		END {
			if (!found && line_count == 1 && length(first_hash) == 64) {
				print first_hash
				found = 1
			}
			if (!found) {
				exit 1
			}
		}
	' "$l_checksum_file" ||
		zxfer_vm_die "Unable to find SHA-256 for $l_file_name in $l_checksum_file"
}

zxfer_vm_download_file() {
	l_url=$1
	l_dest=$2
	l_tmp=$l_dest.tmp.$$

	zxfer_vm_mkdir_p "$(dirname "$l_dest")"
	rm -f "$l_tmp"
	if zxfer_vm_should_show_progress_bar; then
		curl -fL --progress-bar "$l_url" -o "$l_tmp" ||
			zxfer_vm_die "Failed to download $l_url"
	else
		curl -fsSL "$l_url" -o "$l_tmp" ||
			zxfer_vm_die "Failed to download $l_url"
	fi
	mv "$l_tmp" "$l_dest" ||
		zxfer_vm_die "Failed to move downloaded file into place: $l_dest"
}

zxfer_vm_download_and_verify_file() {
	l_url=$1
	l_dest=$2
	l_expected_sha256=$3
	l_actual_sha256=

	ZXFER_VM_LAST_DOWNLOAD_ACTION=
	ZXFER_VM_LAST_DOWNLOAD_SIZE_BYTES=

	if [ -f "$l_dest" ]; then
		l_actual_sha256=$(zxfer_vm_sha256_file "$l_dest")
		if [ "$l_actual_sha256" = "$l_expected_sha256" ]; then
			ZXFER_VM_LAST_DOWNLOAD_ACTION=cached
			ZXFER_VM_LAST_DOWNLOAD_SIZE_BYTES=$(zxfer_vm_file_size_bytes "$l_dest")
			return 0
		fi
		rm -f "$l_dest"
	fi

	zxfer_vm_download_file "$l_url" "$l_dest"
	l_actual_sha256=$(zxfer_vm_sha256_file "$l_dest")
	[ "$l_actual_sha256" = "$l_expected_sha256" ] ||
		zxfer_vm_die "Checksum mismatch for $(basename "$l_dest"): expected $l_expected_sha256, got $l_actual_sha256"
	# shellcheck disable=SC2034  # Cached for later logging and shunit assertions.
	ZXFER_VM_LAST_DOWNLOAD_ACTION=downloaded
	# shellcheck disable=SC2034  # Cached for later logging and shunit assertions.
	ZXFER_VM_LAST_DOWNLOAD_SIZE_BYTES=$(zxfer_vm_file_size_bytes "$l_dest")
}

zxfer_vm_decompress_archive() {
	l_archive=$1
	l_dest=$2
	l_compression=$3
	l_tmp=$l_dest.tmp.$$

	ZXFER_VM_LAST_DECOMPRESS_ACTION=
	ZXFER_VM_LAST_DECOMPRESS_SIZE_BYTES=

	if [ -f "$l_dest" ]; then
		ZXFER_VM_LAST_DECOMPRESS_ACTION=cached
		ZXFER_VM_LAST_DECOMPRESS_SIZE_BYTES=$(zxfer_vm_file_size_bytes "$l_dest")
		return 0
	fi

	rm -f "$l_tmp"
	case "$l_compression" in
	xz)
		xz -dc "$l_archive" >"$l_tmp" ||
			zxfer_vm_die "Failed to decompress $(basename "$l_archive") with xz"
		;;
	zst)
		zstd -dc "$l_archive" >"$l_tmp" ||
			zxfer_vm_die "Failed to decompress $(basename "$l_archive") with zstd"
		;;
	none)
		cp "$l_archive" "$l_tmp" ||
			zxfer_vm_die "Failed to copy $(basename "$l_archive") into $l_dest"
		;;
	*)
		zxfer_vm_die "Unsupported archive compression: $l_compression"
		;;
	esac

	mv "$l_tmp" "$l_dest" ||
		zxfer_vm_die "Failed to finalize decompressed image: $l_dest"
	# shellcheck disable=SC2034  # Cached for later logging and shunit assertions.
	ZXFER_VM_LAST_DECOMPRESS_ACTION=prepared
	# shellcheck disable=SC2034  # Cached for later logging and shunit assertions.
	ZXFER_VM_LAST_DECOMPRESS_SIZE_BYTES=$(zxfer_vm_file_size_bytes "$l_dest")
}

zxfer_vm_allocate_tcp_port() {
	python3 - <<'PY'
import socket

with socket.socket() as s:
    s.bind(("127.0.0.1", 0))
    print(s.getsockname()[1])
PY
}

zxfer_vm_detect_acceleration_capabilities() {
	ZXFER_VM_HOST_CAN_ACCELERATE_AMD64=0
	ZXFER_VM_HOST_CAN_ACCELERATE_ARM64=0

	case "$ZXFER_VM_HOST_OS/$ZXFER_VM_HOST_ARCH" in
	linux/amd64)
		if [ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ]; then
			ZXFER_VM_HOST_CAN_ACCELERATE_AMD64=1
		fi
		;;
	linux/arm64)
		if [ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ]; then
			ZXFER_VM_HOST_CAN_ACCELERATE_ARM64=1
		fi
		;;
	darwin/amd64)
		ZXFER_VM_HOST_CAN_ACCELERATE_AMD64=1
		;;
	darwin/arm64)
		ZXFER_VM_HOST_CAN_ACCELERATE_ARM64=1
		;;
	esac

	# shellcheck disable=SC2034  # Cross-file alias for x86_64-specific qemu helpers.
	ZXFER_VM_HOST_CAN_ACCELERATE_X86_64=$ZXFER_VM_HOST_CAN_ACCELERATE_AMD64
}

zxfer_vm_detect_x86_64_acceleration() {
	zxfer_vm_detect_acceleration_capabilities
}

zxfer_vm_host_can_accelerate_guest_arch() {
	case "$1" in
	amd64)
		[ "$ZXFER_VM_HOST_CAN_ACCELERATE_AMD64" = "1" ]
		;;
	arm64)
		[ "$ZXFER_VM_HOST_CAN_ACCELERATE_ARM64" = "1" ]
		;;
	*)
		return 1
		;;
	esac
}

zxfer_vm_join_path() {
	l_base=$1
	l_leaf=$2

	case "$l_base" in
	*/)
		printf '%s%s\n' "$l_base" "$l_leaf"
		;;
	*)
		printf '%s/%s\n' "$l_base" "$l_leaf"
		;;
	esac
}

zxfer_vm_stream_to_file_and_console() {
	l_input=$1
	l_output_file=$2
	l_prefix=$3
	l_stream=${4:-stdout}
	l_line=

	: >"$l_output_file" ||
		return 1
	while IFS= read -r l_line || [ -n "$l_line" ]; do
		printf '%s\n' "$l_line" >>"$l_output_file" ||
			return 1
		case "$l_stream" in
		stderr)
			printf '%s %s\n' "$l_prefix" "$l_line" >&2
			;;
		*)
			printf '%s %s\n' "$l_prefix" "$l_line"
			;;
		esac
	done <"$l_input"
}

zxfer_vm_run_command_with_captured_output() {
	l_stdout_file=$1
	l_stderr_file=$2
	l_stdout_prefix=$3
	l_stderr_prefix=$4
	shift 4
	l_status=0
	l_stdout_pipe=
	l_stderr_pipe=
	l_stdout_pid=
	l_stderr_pid=

	if [ "${ZXFER_VM_STREAM_GUEST_OUTPUT:-0}" != "1" ]; then
		"$@" >"$l_stdout_file" 2>"$l_stderr_file"
		return $?
	fi

	l_stdout_pipe=$l_stdout_file.stdout.pipe.$$
	l_stderr_pipe=$l_stderr_file.stderr.pipe.$$
	rm -f "$l_stdout_pipe" "$l_stderr_pipe"
	mkfifo "$l_stdout_pipe" "$l_stderr_pipe" ||
		zxfer_vm_die "Unable to create streaming fifos for guest output"

	zxfer_vm_stream_to_file_and_console "$l_stdout_pipe" "$l_stdout_file" "$l_stdout_prefix" stdout &
	l_stdout_pid=$!
	zxfer_vm_stream_to_file_and_console "$l_stderr_pipe" "$l_stderr_file" "$l_stderr_prefix" stderr &
	l_stderr_pid=$!

	if "$@" >"$l_stdout_pipe" 2>"$l_stderr_pipe"; then
		l_status=0
	else
		l_status=$?
	fi

	wait "$l_stdout_pid" >/dev/null 2>&1 || true
	wait "$l_stderr_pid" >/dev/null 2>&1 || true
	rm -f "$l_stdout_pipe" "$l_stderr_pipe"

	return "$l_status"
}

zxfer_vm_warn_file_matches() {
	l_file=$1
	l_pattern=$2
	l_prefix=$3
	l_limit=${4:-5}

	[ -r "$l_file" ] || return 1
	awk -v pattern="$l_pattern" -v prefix="$l_prefix" -v limit="$l_limit" '
		$0 ~ pattern {
			printf "WARNING: %s %s\n", prefix, $0 > "/dev/stderr"
			count++
			if (count >= limit) {
				exit
			}
		}
		END {
			exit(count == 0)
		}
	' "$l_file"
}

zxfer_vm_warn_last_nonempty_file_line() {
	l_file=$1
	l_prefix=$2

	[ -r "$l_file" ] || return 1
	awk -v prefix="$l_prefix" '
		NF {
			last = $0
		}
		END {
			if (last == "") {
				exit 1
			}
			printf "WARNING: %s %s\n", prefix, last > "/dev/stderr"
		}
	' "$l_file"
}

zxfer_vm_report_guest_command_failure() {
	l_log_prefix=$1
	l_step_label=$2
	l_status=$3
	l_stdout_file=$4
	l_stderr_file=$5

	zxfer_vm_warn "[$l_log_prefix] $l_step_label failed with exit status $l_status"
	if [ "${ZXFER_VM_STREAM_GUEST_OUTPUT:-0}" = "1" ]; then
		return 0
	fi

	if zxfer_vm_warn_file_matches "$l_stderr_file" '^!! ' "[$l_log_prefix]" 5; then
		:
	elif zxfer_vm_warn_file_matches "$l_stdout_file" '^ASSERT:' "[$l_log_prefix]" 5; then
		:
	elif zxfer_vm_warn_last_nonempty_file_line "$l_stderr_file" "[$l_log_prefix]"; then
		:
	else
		zxfer_vm_warn_last_nonempty_file_line "$l_stdout_file" "[$l_log_prefix]" || true
	fi

	zxfer_vm_warn "[$l_log_prefix] guest logs: $l_stdout_file, $l_stderr_file"
}
