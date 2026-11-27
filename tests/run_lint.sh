#!/bin/sh
#
# Run zxfer lint targets with pinned tool versions so local runs mirror CI.
#
# shellcheck shell=sh disable=SC2016

set -eu

ZXFER_ROOT=$(cd "$(dirname "$0")/.." && pwd)
if [ -f "$ZXFER_ROOT/tests/helpers/runner_common.sh" ]; then
	# shellcheck source=tests/helpers/runner_common.sh
	. "$ZXFER_ROOT/tests/helpers/runner_common.sh"
else
	l_lint_self=$0
	while [ -L "$l_lint_self" ] && command -v readlink >/dev/null 2>&1; do
		l_lint_target=$(readlink "$l_lint_self") || break
		case "$l_lint_target" in
		/*) l_lint_self=$l_lint_target ;;
		*) l_lint_self="$(dirname "$l_lint_self")/$l_lint_target" ;;
		esac
	done
	l_lint_real_tests_dir=$(cd -P "$(dirname "$l_lint_self")" 2>/dev/null && pwd) ||
		l_lint_real_tests_dir=
	if [ -z "$l_lint_real_tests_dir" ] ||
		[ ! -f "$l_lint_real_tests_dir/helpers/runner_common.sh" ]; then
		printf '%s\n' "Unable to locate tests/helpers/runner_common.sh from $0" >&2
		exit 1
	fi
	# shellcheck source=tests/helpers/runner_common.sh
	. "$l_lint_real_tests_dir/helpers/runner_common.sh"
fi

ACTIONLINT_VERSION=1.7.12
ACTIONLINT_BASE_URL=https://github.com/rhysd/actionlint/releases/download/v1.7.12
SHFMT_VERSION=3.13.0
SHFMT_BASE_URL=https://github.com/mvdan/sh/releases/download/v3.13.0
SHELLCHECK_VERSION=0.11.0
SHELLCHECK_BASE_URL=https://github.com/koalaman/shellcheck/releases/download/v0.11.0
CODESPELL_VERSION=2.4.2
CODESPELL_WHEEL_URL=https://files.pythonhosted.org/packages/42/a1/52fa05533e95fe45bcc09bcf8a503874b1c08f221a4e35608017e0938f55/codespell-2.4.2-py3-none-any.whl
CODESPELL_WHEEL_SHA256=97e0c1060cf46bd1d5db89a936c98db8c2b804e1fdd4b5c645e82a1ec6b1f886
DEVSCRIPTS_VERSION=2.25.33
CHECKBASHISMS_DEB_URL=https://snapshot.debian.org/archive/debian/20251229T143714Z/pool/main/d/devscripts/devscripts_2.25.33_all.deb
CHECKBASHISMS_DEB_SHA256=f1fae3aad11d4d8c3565eafdc15ee8bbf95f7452ede52f4b4bc372dd2a38b54f

LINT_TOOL_ROOT=
DOWNLOAD_DIR=
HOST_OS=
HOST_ARCH=

print_usage() {
	cat <<'EOF'
Usage: tests/run_lint.sh [target ...]

Run zxfer lint targets with the same pinned toolchain used by GitHub Actions.
With no target arguments, all lint targets run.

Targets:
  actionlint
  checkbashisms
  shfmt
  codespell
  shellcheck
  budget
  manpages
  all

Options:
  --bootstrap-only
               download/install the selected tools without running the lint checks
  --list        print the available lint targets
  -h, --help    show this help

Environment:
  ZXFER_LINT_TOOL_DIR  override the bootstrap/cache directory
EOF
}

die() {
	printf '%s\n' "$*" >&2
	exit 1
}

require_command() {
	command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

sha256_file() {
	l_lint_sha_status=0
	zxfer_runner_sha256_file "$1" || l_lint_sha_status=$?
	case "$l_lint_sha_status" in
	0) return 0 ;;
	127) die "A SHA-256 tool is required (sha256sum or shasum)." ;;
	*) die "Failed to compute SHA-256 checksum for $1" ;;
	esac
}

download_and_verify() {
	l_url=$1
	l_dest=$2
	l_expected_sha256=$3
	l_actual_sha256=
	l_tmp=

	require_command curl
	require_command awk

	mkdir -p "$(dirname "$l_dest")"
	if [ -f "$l_dest" ]; then
		l_actual_sha256=$(sha256_file "$l_dest") || return 1
		if [ "$l_actual_sha256" = "$l_expected_sha256" ]; then
			return 0
		fi
		rm -f "$l_dest"
	fi

	l_tmp=$l_dest.tmp.$$
	rm -f "$l_tmp"
	curl -fsSL "$l_url" -o "$l_tmp"
	l_actual_sha256=$(sha256_file "$l_tmp") || return 1
	if [ "$l_actual_sha256" != "$l_expected_sha256" ]; then
		rm -f "$l_tmp"
		die "Checksum mismatch for $(basename "$l_dest"): expected $l_expected_sha256, got $l_actual_sha256"
	fi
	mv "$l_tmp" "$l_dest"
}

set_tool_root() {
	LINT_TOOL_ROOT=$(zxfer_runner_resolve_cache_dir \
		"${ZXFER_LINT_TOOL_DIR:-}" "${XDG_CACHE_HOME:-}" \
		"zxfer/lint-tools" ".cache/zxfer/lint-tools" "zxfer-lint-tools")
	DOWNLOAD_DIR=$LINT_TOOL_ROOT/downloads
	mkdir -p "$DOWNLOAD_DIR"
}

ensure_tool_root() {
	[ -n "$LINT_TOOL_ROOT" ] && return 0
	set_tool_root
}

detect_host_platform() {
	l_detect_status=0
	zxfer_runner_detect_host_os_arch 0 || l_detect_status=$?
	case "$l_detect_status" in
	0)
		HOST_OS=$ZXFER_RUNNER_HOST_OS
		HOST_ARCH=$ZXFER_RUNNER_HOST_ARCH
		;;
	1) die "Unsupported host OS for lint bootstrap: $(uname -s)" ;;
	*) die "Unsupported host architecture for lint bootstrap: $(uname -m)" ;;
	esac
}

ensure_host_platform() {
	[ -n "$HOST_OS" ] && [ -n "$HOST_ARCH" ] && return 0
	detect_host_platform
}

resolve_pinned_tool_asset() {
	l_pinned_tool=$1
	case "$l_pinned_tool:$HOST_OS/$HOST_ARCH" in
	actionlint:linux/amd64)
		PINNED_ASSET=actionlint_1.7.12_linux_amd64.tar.gz
		PINNED_SHA256=8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8
		;;
	actionlint:linux/arm64)
		PINNED_ASSET=actionlint_1.7.12_linux_arm64.tar.gz
		PINNED_SHA256=325e971b6ba9bfa504672e29be93c24981eeb1c07576d730e9f7c8805afff0c6
		;;
	actionlint:darwin/amd64)
		PINNED_ASSET=actionlint_1.7.12_darwin_amd64.tar.gz
		PINNED_SHA256=5b44c3bc2255115c9b69e30efc0fecdf498fdb63c5d58e17084fd5f16324c644
		;;
	actionlint:darwin/arm64)
		PINNED_ASSET=actionlint_1.7.12_darwin_arm64.tar.gz
		PINNED_SHA256=aba9ced2dee8d27fecca3dc7feb1a7f9a52caefa1eb46f3271ea66b6e0e6953f
		;;
	shfmt:linux/amd64)
		PINNED_ASSET=shfmt_v3.13.0_linux_amd64
		PINNED_SHA256=70aa99784703a8d6569bbf0b1e43e1a91906a4166bf1a79de42050a6d0de7551
		;;
	shfmt:linux/arm64)
		PINNED_ASSET=shfmt_v3.13.0_linux_arm64
		PINNED_SHA256=2091a31afd47742051a77bf7cfd175533ab07e924c20ef3151cd108fa1cab5b0
		;;
	shfmt:darwin/amd64)
		PINNED_ASSET=shfmt_v3.13.0_darwin_amd64
		PINNED_SHA256=b6890a0009abf71d36d7c536ad56e3132c547ceb77cd5d5ee62b3469ab4e9417
		;;
	shfmt:darwin/arm64)
		PINNED_ASSET=shfmt_v3.13.0_darwin_arm64
		PINNED_SHA256=650970603b5946dc6041836ddcfa7a19d99b5da885e4687f64575508e99cf718
		;;
	shellcheck:linux/amd64)
		PINNED_ASSET=shellcheck-v0.11.0.linux.x86_64.tar.xz
		PINNED_SHA256=8c3be12b05d5c177a04c29e3c78ce89ac86f1595681cab149b65b97c4e227198
		;;
	shellcheck:linux/arm64)
		PINNED_ASSET=shellcheck-v0.11.0.linux.aarch64.tar.xz
		PINNED_SHA256=12b331c1d2db6b9eb13cfca64306b1b157a86eb69db83023e261eaa7e7c14588
		;;
	shellcheck:darwin/amd64)
		PINNED_ASSET=shellcheck-v0.11.0.darwin.x86_64.tar.xz
		PINNED_SHA256=3c89db4edcab7cf1c27bff178882e0f6f27f7afdf54e859fa041fca10febe4c6
		;;
	shellcheck:darwin/arm64)
		PINNED_ASSET=shellcheck-v0.11.0.darwin.aarch64.tar.xz
		PINNED_SHA256=56affdd8de5527894dca6dc3d7e0a99a873b0f004d7aabc30ae407d3f48b0a79
		;;
	*)
		die "No pinned $l_pinned_tool asset for $HOST_OS/$HOST_ARCH"
		;;
	esac
}

ensure_pinned_host_tool() {
	l_tool_name=$1
	l_tool_version=$2
	l_base_url=$3
	l_extract_mode=$4
	l_archive_member=${5:-}

	ensure_tool_root
	ensure_host_platform
	resolve_pinned_tool_asset "$l_tool_name"
	l_install_dir=$LINT_TOOL_ROOT/$l_tool_name/$l_tool_version/$HOST_OS-$HOST_ARCH
	PINNED_BIN=$l_install_dir/$l_tool_name
	if [ -x "$PINNED_BIN" ]; then
		return 0
	fi

	l_download=$DOWNLOAD_DIR/$PINNED_ASSET
	download_and_verify "$l_base_url/$PINNED_ASSET" "$l_download" "$PINNED_SHA256"
	if [ "$l_extract_mode" = "raw" ]; then
		rm -rf "$l_install_dir"
		mkdir -p "$l_install_dir"
		cp "$l_download" "$PINNED_BIN"
		chmod 755 "$PINNED_BIN"
		return 0
	fi

	require_command tar
	l_stage=$l_install_dir.stage.$$
	rm -rf "$l_stage" "$l_install_dir"
	mkdir -p "$l_stage" "$l_install_dir"
	case "$l_extract_mode" in
	tgz) tar -xzf "$l_download" -C "$l_stage" "$l_archive_member" ;;
	txz) tar -xJf "$l_download" -C "$l_stage" ;;
	esac
	mv "$l_stage/$l_archive_member" "$PINNED_BIN"
	chmod 755 "$PINNED_BIN"
	rm -rf "$l_stage"
}

ensure_actionlint() {
	ensure_pinned_host_tool actionlint "$ACTIONLINT_VERSION" "$ACTIONLINT_BASE_URL" tgz actionlint
	ACTIONLINT_BIN=$PINNED_BIN
}

ensure_checkbashisms() {
	ensure_tool_root
	l_install_dir=$LINT_TOOL_ROOT/checkbashisms/$DEVSCRIPTS_VERSION
	CHECKBASHISMS_BIN=$l_install_dir/checkbashisms
	if [ -x "$CHECKBASHISMS_BIN" ]; then
		return 0
	fi

	require_command ar
	require_command perl
	require_command tar
	l_deb=$DOWNLOAD_DIR/devscripts_$DEVSCRIPTS_VERSION"_all.deb"
	l_stage=$l_install_dir.stage.$$

	download_and_verify "$CHECKBASHISMS_DEB_URL" "$l_deb" "$CHECKBASHISMS_DEB_SHA256"
	rm -rf "$l_stage" "$l_install_dir"
	mkdir -p "$l_stage" "$l_install_dir"
	(
		cd "$l_stage"
		ar x "$l_deb"
		tar -xJf data.tar.xz ./usr/bin/checkbashisms
	)
	mv "$l_stage/usr/bin/checkbashisms" "$CHECKBASHISMS_BIN"
	chmod 755 "$CHECKBASHISMS_BIN"
	rm -rf "$l_stage"
}

ensure_shfmt() {
	ensure_pinned_host_tool shfmt "$SHFMT_VERSION" "$SHFMT_BASE_URL" raw
	SHFMT_BIN=$PINNED_BIN
}

ensure_codespell() {
	ensure_tool_root
	l_install_dir=$LINT_TOOL_ROOT/codespell/$CODESPELL_VERSION
	CODESPELL_BIN=$l_install_dir/venv/bin/codespell
	if [ -x "$CODESPELL_BIN" ] && "$CODESPELL_BIN" --version >/dev/null 2>&1; then
		return 0
	fi

	require_command python3
	l_wheel=$DOWNLOAD_DIR/$(basename "$CODESPELL_WHEEL_URL")

	download_and_verify "$CODESPELL_WHEEL_URL" "$l_wheel" "$CODESPELL_WHEEL_SHA256"
	rm -rf "$l_install_dir"
	mkdir -p "$l_install_dir"
	python3 -m venv "$l_install_dir/venv"
	"$l_install_dir/venv/bin/python" -m ensurepip --upgrade >/dev/null 2>&1 || true
	"$l_install_dir/venv/bin/python" -m pip install --disable-pip-version-check --no-deps --no-index "$l_wheel" >/dev/null
}

ensure_shellcheck() {
	ensure_pinned_host_tool shellcheck "$SHELLCHECK_VERSION" "$SHELLCHECK_BASE_URL" \
		txz "shellcheck-v$SHELLCHECK_VERSION/shellcheck"
	SHELLCHECK_BIN=$PINNED_BIN
}

run_actionlint() {
	ensure_actionlint
	printf '==> actionlint %s\n' "$ACTIONLINT_VERSION"
	(
		cd "$ZXFER_ROOT"
		"$ACTIONLINT_BIN"
	)
}

list_lint_shell_files() {
	# Keep tracked and non-ignored untracked sources NUL-delimited, once each.
	# Git also lists unstaged deletions; lint only files still in the worktree.
	git ls-files -z --cached --others --exclude-standard -- \
		'zxfer' '*.sh' ':!:tests/shunit2/shunit2' |
		xargs -0 sh -c '
			for l_file do
				[ ! -f "$l_file" ] || printf "%s\000" "$l_file"
			done
		' sh
}

run_shell_lint_tool() {
	require_command git
	require_command xargs
	(
		cd "$ZXFER_ROOT"
		list_lint_shell_files |
			xargs -0 sh -c '
				l_tool_argc=$1
				shift
				[ "$#" -gt "$l_tool_argc" ] || exit 0
				exec "$@"
			' sh "$#" "$@"
	)
}

run_checkbashisms() {
	ensure_checkbashisms
	printf '==> checkbashisms (devscripts %s)\n' "$DEVSCRIPTS_VERSION"
	run_shell_lint_tool "$CHECKBASHISMS_BIN" --posix
}

run_shfmt() {
	ensure_shfmt
	printf '==> shfmt %s\n' "$SHFMT_VERSION"
	run_shell_lint_tool "$SHFMT_BIN" -d
}

run_codespell() {
	ensure_codespell
	printf '==> codespell %s\n' "$CODESPELL_VERSION"
	(
		cd "$ZXFER_ROOT"
		"$CODESPELL_BIN" --config .codespellrc .
	)
}

# Purpose: Keep eval out of shared test helpers (tests/*_helper.sh and
# tests/helpers/*.sh). Capture helpers execute commands through literal argv.
# Usage: run_test_helper_eval_policy; prints each eval line and fails.
run_test_helper_eval_policy() {
	require_command awk
	printf '==> test-helper eval policy\n'
	set --
	for l_eval_helper in "$ZXFER_ROOT"/tests/*_helper.sh \
		"$ZXFER_ROOT"/tests/helpers/*.sh; do
		[ ! -f "$l_eval_helper" ] || set -- "$@" "$l_eval_helper"
	done
	[ "$#" -gt 0 ] || return 0
	LC_ALL=C awk -v root="$ZXFER_ROOT/" '
		$0 !~ /^[[:space:]]*#/ && $0 ~ /(^|[^[:alnum:]_])eval([^[:alnum:]_]|$)/ {
			printf "%s:%d: eval in a shared test helper; execute a command or scenario function instead\n",
				substr(FILENAME, length(root) + 1), FNR
			bad = 1
		}
		END { exit bad }
	' "$@"
}

run_shellcheck() {
	run_test_helper_eval_policy
	ensure_shellcheck
	printf '==> shellcheck %s\n' "$SHELLCHECK_VERSION"
	run_shell_lint_tool "$SHELLCHECK_BIN" --external-sources --source-path=.:src
}

run_budget() {
	printf '==> budget (tests/budget_policy.tsv)\n'
	"$ZXFER_ROOT/tests/run_budget_check.sh"
}

run_manpages() {
	printf '==> manpages (man/zxfer.8 -> man/zxfer.1m)\n'
	"$ZXFER_ROOT/tests/generate_solaris_manpage.sh" --check
}

run_target() {
	case "$1" in
	actionlint)
		run_actionlint
		;;
	checkbashisms)
		run_checkbashisms
		;;
	shfmt)
		run_shfmt
		;;
	codespell)
		run_codespell
		;;
	shellcheck)
		run_shellcheck
		;;
	budget)
		run_budget
		;;
	manpages)
		run_manpages
		;;
	*)
		die "Unknown lint target: $1"
		;;
	esac
}

bootstrap_target() {
	case "$1" in
	actionlint)
		printf '==> bootstrap actionlint %s\n' "$ACTIONLINT_VERSION"
		ensure_actionlint
		;;
	checkbashisms)
		printf '==> bootstrap checkbashisms (devscripts %s)\n' "$DEVSCRIPTS_VERSION"
		ensure_checkbashisms
		;;
	shfmt)
		printf '==> bootstrap shfmt %s\n' "$SHFMT_VERSION"
		ensure_shfmt
		;;
	codespell)
		printf '==> bootstrap codespell %s\n' "$CODESPELL_VERSION"
		ensure_codespell
		;;
	shellcheck)
		printf '==> bootstrap shellcheck %s\n' "$SHELLCHECK_VERSION"
		ensure_shellcheck
		;;
	budget)
		printf '==> bootstrap budget (no downloads required)\n'
		;;
	manpages)
		printf '==> bootstrap manpages (no downloads required)\n'
		;;
	*)
		die "Unknown lint target: $1"
		;;
	esac
}

append_target() {
	l_target=$1

	if [ -n "${TARGET_LIST:-}" ]; then
		TARGET_LIST="$TARGET_LIST
$l_target"
	else
		TARGET_LIST=$l_target
	fi
}

append_default_targets() {
	append_target actionlint
	append_target checkbashisms
	append_target shfmt
	append_target codespell
	append_target shellcheck
	append_target budget
	append_target manpages
}

print_default_targets() {
	cat <<'EOF'
actionlint
checkbashisms
shfmt
codespell
shellcheck
budget
manpages
EOF
}

TARGET_LIST=
BOOTSTRAP_ONLY=0
if [ "$#" -eq 0 ]; then
	append_default_targets
else
	for l_arg in "$@"; do
		case "$l_arg" in
		-h | --help)
			print_usage
			exit 0
			;;
		--list)
			print_default_targets
			exit 0
			;;
		--bootstrap-only)
			BOOTSTRAP_ONLY=1
			;;
		all)
			TARGET_LIST=
			append_default_targets
			;;
		actionlint | checkbashisms | shfmt | codespell | shellcheck | budget | manpages)
			append_target "$l_arg"
			;;
		*)
			die "Unknown lint target: $l_arg"
			;;
		esac
	done
fi

while IFS= read -r l_target; do
	[ -n "$l_target" ] || continue
	if [ "$BOOTSTRAP_ONLY" -eq 1 ]; then
		bootstrap_target "$l_target"
	else
		run_target "$l_target"
	fi
done <<EOF
$TARGET_LIST
EOF
