#!/bin/sh
#
# Prepare a vmactions FreeBSD guest for zxfer integration tests.
#

set -u

ZXFER_CI_FREEBSD_REQUIRED_PACKAGES=${ZXFER_CI_FREEBSD_REQUIRED_PACKAGES:-"parallel zstd"}
ZXFER_CI_FREEBSD_PKG_REPOS_DIR=${ZXFER_CI_FREEBSD_PKG_REPOS_DIR:-/var/db/pkg/repos}
ZXFER_CI_FREEBSD_PKG_CACHE_DIR=${ZXFER_CI_FREEBSD_PKG_CACHE_DIR:-/var/cache/pkg}
ZXFER_CI_FREEBSD_ALLOW_DEGRADED_PACKAGES=${ZXFER_CI_FREEBSD_ALLOW_DEGRADED_PACKAGES:-1}

zxfer_ci_freebsd_log() {
	printf '%s\n' "$*"
}

zxfer_ci_freebsd_warn() {
	printf 'WARNING: %s\n' "$*" >&2
}

zxfer_ci_freebsd_clear_pkg_state() {
	l_status=0

	if [ -n "${ZXFER_CI_FREEBSD_PKG_REPOS_DIR:-}" ]; then
		case "$ZXFER_CI_FREEBSD_PKG_REPOS_DIR" in
		/)
			zxfer_ci_freebsd_warn "Refusing to clear package repository state at /."
			return 1
			;;
		esac
		rm -rf "${ZXFER_CI_FREEBSD_PKG_REPOS_DIR:?}"/* || l_status=$?
	fi
	if [ -n "${ZXFER_CI_FREEBSD_PKG_CACHE_DIR:-}" ]; then
		case "$ZXFER_CI_FREEBSD_PKG_CACHE_DIR" in
		/)
			zxfer_ci_freebsd_warn "Refusing to clear package cache state at /."
			return 1
			;;
		esac
		rm -rf "${ZXFER_CI_FREEBSD_PKG_CACHE_DIR:?}"/* || l_status=$?
	fi

	return "$l_status"
}

zxfer_ci_freebsd_pkg_bootstrap() {
	if ! command -v pkg >/dev/null 2>&1; then
		zxfer_ci_freebsd_warn "pkg is not available; FreeBSD package prerequisites cannot be installed."
		return 1
	fi

	ASSUME_ALWAYS_YES=yes pkg bootstrap -f
}

zxfer_ci_freebsd_pkg_update() {
	zxfer_ci_freebsd_clear_pkg_state || return $?
	pkg update -f
}

zxfer_ci_freebsd_pkg_install_required() {
	# Intentional split of the controlled package list.
	# shellcheck disable=SC2086
	set -- $ZXFER_CI_FREEBSD_REQUIRED_PACKAGES
	[ "$#" -gt 0 ] || return 0

	pkg install -y "$@"
}

zxfer_ci_freebsd_install_required_packages() {
	l_status=0

	zxfer_ci_freebsd_log "Preparing FreeBSD package prerequisites: $ZXFER_CI_FREEBSD_REQUIRED_PACKAGES"

	zxfer_ci_freebsd_pkg_bootstrap
	l_status=$?
	if [ "$l_status" -eq 0 ]; then
		zxfer_ci_freebsd_pkg_update
		l_status=$?
	fi
	if [ "$l_status" -eq 0 ]; then
		zxfer_ci_freebsd_pkg_install_required
		l_status=$?
	fi

	if [ "$l_status" -eq 0 ]; then
		return 0
	fi

	zxfer_ci_freebsd_warn "FreeBSD package prerequisite install failed with status $l_status; clearing pkg state and retrying once."

	zxfer_ci_freebsd_pkg_update
	l_status=$?
	if [ "$l_status" -eq 0 ]; then
		zxfer_ci_freebsd_pkg_install_required
		l_status=$?
	fi
	if [ "$l_status" -eq 0 ]; then
		return 0
	fi

	if [ "$ZXFER_CI_FREEBSD_ALLOW_DEGRADED_PACKAGES" = "1" ]; then
		zxfer_ci_freebsd_warn "FreeBSD package prerequisites are unavailable after retry; continuing with reduced integration coverage."
		return 0
	fi

	return "$l_status"
}

zxfer_ci_freebsd_report_optional_tool() {
	l_tool=$1

	if command -v "$l_tool" >/dev/null 2>&1; then
		zxfer_ci_freebsd_log "FreeBSD integration optional tool available: $l_tool"
	else
		zxfer_ci_freebsd_warn "FreeBSD integration optional tool unavailable: $l_tool"
	fi
}

zxfer_ci_freebsd_load_zfs() {
	kldload zfs >/dev/null 2>&1 || true
}

zxfer_ci_freebsd_install_required_packages
l_prepare_status=$?
zxfer_ci_freebsd_load_zfs
zxfer_ci_freebsd_report_optional_tool parallel
zxfer_ci_freebsd_report_optional_tool zstd

exit "$l_prepare_status"
