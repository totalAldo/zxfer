#!/bin/sh
#
# Run zxfer lint targets with pinned tool versions so local runs mirror CI.
#

set -eu

ZXFER_ROOT=$(cd "$(dirname "$0")/.." && pwd)

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
	l_file=$1
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$l_file" | awk '{print $1}'
		return
	fi
	if command -v shasum >/dev/null 2>&1; then
		shasum -a 256 "$l_file" | awk '{print $1}'
		return
	fi
	die "A SHA-256 tool is required (sha256sum or shasum)."
}

download_and_verify() {
	l_url=$1
	l_dest=$2
	l_expected_sha256=$3
	l_actual_sha256=
	l_tmp=

	mkdir -p "$(dirname "$l_dest")"
	if [ -f "$l_dest" ]; then
		l_actual_sha256=$(sha256_file "$l_dest")
		if [ "$l_actual_sha256" = "$l_expected_sha256" ]; then
			return 0
		fi
		rm -f "$l_dest"
	fi

	l_tmp=$l_dest.tmp.$$
	rm -f "$l_tmp"
	curl -fsSL "$l_url" -o "$l_tmp"
	l_actual_sha256=$(sha256_file "$l_tmp")
	if [ "$l_actual_sha256" != "$l_expected_sha256" ]; then
		rm -f "$l_tmp"
		die "Checksum mismatch for $(basename "$l_dest"): expected $l_expected_sha256, got $l_actual_sha256"
	fi
	mv "$l_tmp" "$l_dest"
}

set_tool_root() {
	if [ -n "${ZXFER_LINT_TOOL_DIR:-}" ]; then
		LINT_TOOL_ROOT=$ZXFER_LINT_TOOL_DIR
	elif [ -n "${XDG_CACHE_HOME:-}" ]; then
		LINT_TOOL_ROOT=$XDG_CACHE_HOME/zxfer/lint-tools
	elif [ -n "${HOME:-}" ]; then
		LINT_TOOL_ROOT=$HOME/.cache/zxfer/lint-tools
	else
		LINT_TOOL_ROOT=${TMPDIR:-/tmp}/zxfer-lint-tools
	fi

	DOWNLOAD_DIR=$LINT_TOOL_ROOT/downloads
	mkdir -p "$DOWNLOAD_DIR"
}

detect_host_platform() {
	case "$(uname -s)" in
	Linux)
		HOST_OS=linux
		;;
	Darwin)
		HOST_OS=darwin
		;;
	*)
		die "Unsupported host OS for lint bootstrap: $(uname -s)"
		;;
	esac

	case "$(uname -m)" in
	x86_64 | amd64)
		HOST_ARCH=amd64
		;;
	arm64 | aarch64)
		HOST_ARCH=arm64
		;;
	*)
		die "Unsupported host architecture for lint bootstrap: $(uname -m)"
		;;
	esac
}

set_actionlint_asset() {
	case "$HOST_OS/$HOST_ARCH" in
	linux/amd64)
		ACTIONLINT_ASSET=actionlint_1.7.12_linux_amd64.tar.gz
		ACTIONLINT_SHA256=8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8
		;;
	linux/arm64)
		ACTIONLINT_ASSET=actionlint_1.7.12_linux_arm64.tar.gz
		ACTIONLINT_SHA256=325e971b6ba9bfa504672e29be93c24981eeb1c07576d730e9f7c8805afff0c6
		;;
	darwin/amd64)
		ACTIONLINT_ASSET=actionlint_1.7.12_darwin_amd64.tar.gz
		ACTIONLINT_SHA256=5b44c3bc2255115c9b69e30efc0fecdf498fdb63c5d58e17084fd5f16324c644
		;;
	darwin/arm64)
		ACTIONLINT_ASSET=actionlint_1.7.12_darwin_arm64.tar.gz
		ACTIONLINT_SHA256=aba9ced2dee8d27fecca3dc7feb1a7f9a52caefa1eb46f3271ea66b6e0e6953f
		;;
	*)
		die "No pinned actionlint asset for $HOST_OS/$HOST_ARCH"
		;;
	esac
}

set_shfmt_asset() {
	case "$HOST_OS/$HOST_ARCH" in
	linux/amd64)
		SHFMT_ASSET=shfmt_v3.13.0_linux_amd64
		SHFMT_SHA256=70aa99784703a8d6569bbf0b1e43e1a91906a4166bf1a79de42050a6d0de7551
		;;
	linux/arm64)
		SHFMT_ASSET=shfmt_v3.13.0_linux_arm64
		SHFMT_SHA256=2091a31afd47742051a77bf7cfd175533ab07e924c20ef3151cd108fa1cab5b0
		;;
	darwin/amd64)
		SHFMT_ASSET=shfmt_v3.13.0_darwin_amd64
		SHFMT_SHA256=b6890a0009abf71d36d7c536ad56e3132c547ceb77cd5d5ee62b3469ab4e9417
		;;
	darwin/arm64)
		SHFMT_ASSET=shfmt_v3.13.0_darwin_arm64
		SHFMT_SHA256=650970603b5946dc6041836ddcfa7a19d99b5da885e4687f64575508e99cf718
		;;
	*)
		die "No pinned shfmt asset for $HOST_OS/$HOST_ARCH"
		;;
	esac
}

set_shellcheck_asset() {
	case "$HOST_OS/$HOST_ARCH" in
	linux/amd64)
		SHELLCHECK_ASSET=shellcheck-v0.11.0.linux.x86_64.tar.xz
		SHELLCHECK_SHA256=8c3be12b05d5c177a04c29e3c78ce89ac86f1595681cab149b65b97c4e227198
		;;
	linux/arm64)
		SHELLCHECK_ASSET=shellcheck-v0.11.0.linux.aarch64.tar.xz
		SHELLCHECK_SHA256=12b331c1d2db6b9eb13cfca64306b1b157a86eb69db83023e261eaa7e7c14588
		;;
	darwin/amd64)
		SHELLCHECK_ASSET=shellcheck-v0.11.0.darwin.x86_64.tar.xz
		SHELLCHECK_SHA256=3c89db4edcab7cf1c27bff178882e0f6f27f7afdf54e859fa041fca10febe4c6
		;;
	darwin/arm64)
		SHELLCHECK_ASSET=shellcheck-v0.11.0.darwin.aarch64.tar.xz
		SHELLCHECK_SHA256=56affdd8de5527894dca6dc3d7e0a99a873b0f004d7aabc30ae407d3f48b0a79
		;;
	*)
		die "No pinned ShellCheck asset for $HOST_OS/$HOST_ARCH"
		;;
	esac
}

ensure_actionlint() {
	l_install_dir=$LINT_TOOL_ROOT/actionlint/$ACTIONLINT_VERSION/$HOST_OS-$HOST_ARCH
	ACTIONLINT_BIN=$l_install_dir/actionlint
	if [ -x "$ACTIONLINT_BIN" ]; then
		return 0
	fi

	l_archive=$DOWNLOAD_DIR/$ACTIONLINT_ASSET
	l_stage=$l_install_dir.stage.$$

	download_and_verify "$ACTIONLINT_BASE_URL/$ACTIONLINT_ASSET" "$l_archive" "$ACTIONLINT_SHA256"
	rm -rf "$l_stage" "$l_install_dir"
	mkdir -p "$l_stage" "$l_install_dir"
	tar -xzf "$l_archive" -C "$l_stage" actionlint
	mv "$l_stage/actionlint" "$ACTIONLINT_BIN"
	chmod 755 "$ACTIONLINT_BIN"
	rm -rf "$l_stage"
}

ensure_checkbashisms() {
	l_install_dir=$LINT_TOOL_ROOT/checkbashisms/$DEVSCRIPTS_VERSION
	CHECKBASHISMS_BIN=$l_install_dir/checkbashisms
	if [ -x "$CHECKBASHISMS_BIN" ]; then
		return 0
	fi

	require_command ar
	require_command perl
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
	l_install_dir=$LINT_TOOL_ROOT/shfmt/$SHFMT_VERSION/$HOST_OS-$HOST_ARCH
	SHFMT_BIN=$l_install_dir/shfmt
	if [ -x "$SHFMT_BIN" ]; then
		return 0
	fi

	l_binary=$DOWNLOAD_DIR/$SHFMT_ASSET
	download_and_verify "$SHFMT_BASE_URL/$SHFMT_ASSET" "$l_binary" "$SHFMT_SHA256"
	rm -rf "$l_install_dir"
	mkdir -p "$l_install_dir"
	cp "$l_binary" "$SHFMT_BIN"
	chmod 755 "$SHFMT_BIN"
}

ensure_codespell() {
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
	l_install_dir=$LINT_TOOL_ROOT/shellcheck/$SHELLCHECK_VERSION/$HOST_OS-$HOST_ARCH
	SHELLCHECK_BIN=$l_install_dir/shellcheck
	if [ -x "$SHELLCHECK_BIN" ]; then
		return 0
	fi

	l_archive=$DOWNLOAD_DIR/$SHELLCHECK_ASSET
	l_stage=$l_install_dir.stage.$$

	download_and_verify "$SHELLCHECK_BASE_URL/$SHELLCHECK_ASSET" "$l_archive" "$SHELLCHECK_SHA256"
	rm -rf "$l_stage" "$l_install_dir"
	mkdir -p "$l_stage" "$l_install_dir"
	tar -xJf "$l_archive" -C "$l_stage"
	mv "$l_stage/shellcheck-v$SHELLCHECK_VERSION/shellcheck" "$SHELLCHECK_BIN"
	chmod 755 "$SHELLCHECK_BIN"
	rm -rf "$l_stage"
}

run_actionlint() {
	ensure_actionlint
	printf '==> actionlint %s\n' "$ACTIONLINT_VERSION"
	(
		cd "$ZXFER_ROOT"
		"$ACTIONLINT_BIN"
	)
}

run_checkbashisms() {
	ensure_checkbashisms
	printf '==> checkbashisms (devscripts %s)\n' "$DEVSCRIPTS_VERSION"
	(
		cd "$ZXFER_ROOT"
		git ls-files -z -- 'zxfer' '*.sh' ':!:tests/shunit2/shunit2' |
			xargs -0 "$CHECKBASHISMS_BIN" --posix
	)
}

run_shfmt() {
	ensure_shfmt
	printf '==> shfmt %s\n' "$SHFMT_VERSION"
	(
		cd "$ZXFER_ROOT"
		git ls-files -z '*.sh' |
			xargs -0 "$SHFMT_BIN" -d zxfer
	)
}

run_codespell() {
	ensure_codespell
	printf '==> codespell %s\n' "$CODESPELL_VERSION"
	(
		cd "$ZXFER_ROOT"
		"$CODESPELL_BIN" --config .codespellrc .
	)
}

run_shellcheck() {
	ensure_shellcheck
	printf '==> shellcheck %s\n' "$SHELLCHECK_VERSION"
	(
		cd "$ZXFER_ROOT"
		git ls-files -z '*.sh' |
			xargs -0 "$SHELLCHECK_BIN" --external-sources --source-path=.:src zxfer
	)
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
}

print_default_targets() {
	cat <<'EOF'
actionlint
checkbashisms
shfmt
codespell
shellcheck
EOF
}

set_tool_root
detect_host_platform
set_actionlint_asset
set_shfmt_asset
set_shellcheck_asset
require_command curl
require_command git
require_command tar

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
		actionlint | checkbashisms | shfmt | codespell | shellcheck)
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
