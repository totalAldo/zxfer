#!/bin/sh
# Shared host command and platform-path setup for ZFS-backed test harnesses.

require_cmd() {
	l_cmd=$1
	if ! command -v "$l_cmd" >/dev/null 2>&1; then
		echo "Missing required command: $l_cmd" >&2
		exit 1
	fi
}

append_path_entry() {
	l_entry=$1

	[ -n "$l_entry" ] || return
	case ":$PATH:" in
	*:"$l_entry":*) ;;
	*)
		PATH="$l_entry:$PATH"
		export PATH
		;;
	esac
}

append_secure_path_entry() {
	l_entry=$1

	[ -n "$l_entry" ] || return
	case ":${ZXFER_SECURE_PATH_APPEND-}:" in
	*:"$l_entry":*) ;;
	*)
		if [ -n "${ZXFER_SECURE_PATH_APPEND-}" ]; then
			ZXFER_SECURE_PATH_APPEND="$ZXFER_SECURE_PATH_APPEND:$l_entry"
		else
			ZXFER_SECURE_PATH_APPEND="$l_entry"
		fi
		export ZXFER_SECURE_PATH_APPEND
		;;
	esac
}

configure_platform_tool_paths() {
	if [ "$OS_NAME" = "Darwin" ] && [ -x "$MACOS_OPENZFS_ZFS_BIN" ]; then
		l_zfs_dir=${MACOS_OPENZFS_ZFS_BIN%/*}
		append_path_entry "$l_zfs_dir"
		append_secure_path_entry "$l_zfs_dir"
	fi
}

compute_absolute_path() {
	l_path=$1

	case "$l_path" in
	/*)
		printf '%s\n' "$l_path"
		;;
	*)
		l_dir=${l_path%/*}
		l_base=${l_path##*/}
		if [ "$l_dir" = "$l_path" ]; then
			l_dir=.
		fi
		l_abs_dir=$(cd "$l_dir" 2>/dev/null && pwd -P) || return 1
		printf '%s/%s\n' "$l_abs_dir" "$l_base"
		;;
	esac
}

resolve_host_command() {
	l_cmd=$1
	l_search_path=$PATH
	l_oldifs=$IFS

	if [ -n "${ZXFER_CONFIRM_WRAPPER_DIR:-}" ]; then
		l_search_path=
		IFS=:
		for l_entry in $PATH; do
			[ "$l_entry" = "$ZXFER_CONFIRM_WRAPPER_DIR" ] && continue
			if [ "$l_search_path" = "" ]; then
				l_search_path=$l_entry
			else
				l_search_path="$l_search_path:$l_entry"
			fi
		done
		IFS=$l_oldifs
	else
		IFS=$l_oldifs
	fi

	PATH=$l_search_path command -v "$l_cmd" 2>/dev/null || true
}

require_platform_permissions() {
	:
}
