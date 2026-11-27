#!/bin/sh
#
# Shared POSIX shell primitives for zxfer validation, lint, coverage, perf,
# integration, and VM matrix runners. No eval is used.
#

# Purpose: Succeed when VALUE is a non-empty ASCII decimal integer (>= 0).
# Explicit digits avoid bash 3.2 / ksh93 UTF-8 collation range matches.
# Usage: zxfer_runner_nonnegative_integer_p VALUE
zxfer_runner_nonnegative_integer_p() {
	case "${1:-}" in
	'' | *[!0123456789]*)
		return 1
		;;
	esac
	return 0
}

# Purpose: Succeed when VALUE is a positive ASCII decimal integer (>= 1).
# Usage: zxfer_runner_positive_integer_p VALUE
zxfer_runner_positive_integer_p() {
	zxfer_runner_nonnegative_integer_p "${1:-}" && [ "$1" -gt 0 ]
}

# Purpose: Succeed when space-delimited LIST contains exact token WORD.
# Usage: zxfer_runner_list_contains LIST WORD
zxfer_runner_list_contains() {
	[ -n "${2:-}" ] || return 1
	case " ${1:-} " in
	*" $2 "*) return 0 ;;
	esac
	return 1
}

# Purpose: Print space-delimited LIST with WORD appended if not already present.
# Usage: zxfer_runner_append_unique_word LIST WORD
zxfer_runner_append_unique_word() {
	l_runner_list=${1:-}
	l_runner_word=${2:-}

	[ -n "$l_runner_word" ] || {
		printf '%s\n' "$l_runner_list"
		return 0
	}
	if zxfer_runner_list_contains "$l_runner_list" "$l_runner_word"; then
		printf '%s\n' "$l_runner_list"
	elif [ -n "$l_runner_list" ]; then
		printf '%s %s\n' "$l_runner_list" "$l_runner_word"
	else
		printf '%s\n' "$l_runner_word"
	fi
}

# Purpose: Split comma- or whitespace-delimited SPEC and append unique tokens
# to space-delimited LIST without pathname expansion or caller IFS sensitivity.
# Usage: zxfer_runner_append_csv_words LIST SPEC
zxfer_runner_append_csv_words() (
	l_csv_result=${1:-}
	l_csv_spec=${2:-}

	[ -n "$l_csv_spec" ] || {
		printf '%s\n' "$l_csv_result"
		exit 0
	}
	unset IFS
	set -f
	l_csv_words=$(printf '%s\n' "$l_csv_spec" | tr ',' ' ')
	for l_csv_item in $l_csv_words; do
		[ -n "$l_csv_item" ] || continue
		l_csv_result=$(zxfer_runner_append_unique_word "$l_csv_result" "$l_csv_item")
	done
	printf '%s\n' "$l_csv_result"
)

# Purpose: Print the number of space-delimited words in LIST.
# Usage: zxfer_runner_count_words LIST
zxfer_runner_count_words() (
	l_count_words_list=${1:-}
	if [ -z "$l_count_words_list" ]; then
		printf '0\n'
		exit 0
	fi
	unset IFS
	set -f
	# shellcheck disable=SC2086  # Intentional word splitting with default IFS and noglob.
	set -- $l_count_words_list
	printf '%s\n' "$#"
)

# Purpose: Map signal name HUP/INT/QUIT/TERM to conventional 128+N exit code.
# Usage: zxfer_runner_signal_exit_status SIGNAL [DEFAULT_STATUS]
zxfer_runner_signal_exit_status() {
	case "${1:-}" in
	HUP) printf '%s\n' 129 ;;
	INT) printf '%s\n' 130 ;;
	QUIT) printf '%s\n' 131 ;;
	TERM) printf '%s\n' 143 ;;
	*) printf '%s\n' "${2:-1}" ;;
	esac
}

# Purpose: Resolve a cache/state root using explicit OVERRIDE, XDG base dir,
# HOME fallback, or TMPDIR fallback.
# Usage: zxfer_runner_resolve_cache_dir OVERRIDE XDG_BASE XDG_SUBPATH HOME_SUBPATH TMP_LEAF
zxfer_runner_resolve_cache_dir() {
	if [ -n "${1:-}" ]; then
		printf '%s\n' "$1"
	elif [ -n "${2:-}" ]; then
		printf '%s/%s\n' "$2" "${3:-}"
	elif [ -n "${HOME:-}" ]; then
		printf '%s/%s\n' "$HOME" "${4:-}"
	else
		printf '%s/%s\n' "${TMPDIR:-/tmp}" "${5:-}"
	fi
}

# Purpose: Compute lowercase hex SHA-256 digest of regular FILE using sha256sum
# or shasum. Reads via stdin so backslash/newline filenames never emit a leading
# backslash escape. Returns 0 on success, 127 when neither tool is available,
# and 1 when FILE is missing, unreadable, non-regular, or hashing fails.
# Usage: zxfer_runner_sha256_file FILE
zxfer_runner_sha256_file() {
	l_sha_file=${1:-}
	if command -v sha256sum >/dev/null 2>&1; then
		[ -n "$l_sha_file" ] && [ -f "$l_sha_file" ] && [ -r "$l_sha_file" ] || return 1
		l_sha_out=$(sha256sum <"$l_sha_file") || return 1
		printf '%s\n' "${l_sha_out%%[[:space:]]*}"
		return 0
	fi
	if command -v shasum >/dev/null 2>&1; then
		[ -n "$l_sha_file" ] && [ -f "$l_sha_file" ] && [ -r "$l_sha_file" ] || return 1
		l_sha_out=$(shasum -a 256 <"$l_sha_file") || return 1
		printf '%s\n' "${l_sha_out%%[[:space:]]*}"
		return 0
	fi
	return 127
}

# Purpose: Detect normalized host OS (linux, darwin, or wsl2 when ALLOW_WSL2=1)
# and architecture (amd64 or arm64), publishing ZXFER_RUNNER_HOST_OS and
# ZXFER_RUNNER_HOST_ARCH. Returns 1 for unsupported OS and 2 for unsupported arch.
# Usage: zxfer_runner_detect_host_os_arch [ALLOW_WSL2] [KERNEL_OVERRIDE] [ARCH_OVERRIDE]
# shellcheck disable=SC2034  # Published for callers sourcing runner_common.sh.
zxfer_runner_detect_host_os_arch() {
	l_detect_allow_wsl=${1:-0}
	l_detect_kernel=${2:-$(uname -s)}
	l_detect_machine=${3:-$(uname -m)}
	ZXFER_RUNNER_HOST_OS=
	ZXFER_RUNNER_HOST_ARCH=

	case "$l_detect_kernel" in
	Linux)
		ZXFER_RUNNER_HOST_OS=linux
		if [ "$l_detect_allow_wsl" = "1" ]; then
			l_osrelease_file=${ZXFER_VM_PROC_OSRELEASE_FILE:-/proc/sys/kernel/osrelease}
			l_version_file=${ZXFER_VM_PROC_VERSION_FILE:-/proc/version}
			if { [ -r "$l_osrelease_file" ] && grep -Eqi '(microsoft|wsl)' "$l_osrelease_file" 2>/dev/null; } ||
				{ [ -r "$l_version_file" ] && grep -Eqi '(microsoft|wsl)' "$l_version_file" 2>/dev/null; }; then
				ZXFER_RUNNER_HOST_OS=wsl2
			fi
		fi
		;;
	Darwin)
		ZXFER_RUNNER_HOST_OS=darwin
		;;
	*)
		return 1
		;;
	esac

	case "$l_detect_machine" in
	x86_64 | amd64)
		ZXFER_RUNNER_HOST_ARCH=amd64
		;;
	arm64 | aarch64)
		ZXFER_RUNNER_HOST_ARCH=arm64
		;;
	*)
		return 2
		;;
	esac
	return 0
}
