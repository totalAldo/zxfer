#!/bin/sh
# Focused fake executable writers for suites that explicitly opt in.
# shellcheck disable=SC2317,SC2329

# Purpose: Write an environment-driven ssh stand-in for unit tests.
# Usage: Suites set only the FAKE_SSH_* variables needed by each case, then
# call zxfer_test_write_env_fake_ssh with a path inside their private temp dir.
zxfer_test_write_env_fake_ssh() {
	l_zxfer_test_fake_ssh_path=$1

	cat >"$l_zxfer_test_fake_ssh_path" <<'EOF' || return "$?"
#!/bin/sh
if [ -n "${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "$@" >>"$FAKE_SSH_LOG"
fi
if [ -n "${FAKE_SSH_STDOUT:-}" ] && [ -z "${FAKE_SSH_SUPPRESS_STDOUT:-}" ]; then
	printf '%s' "$FAKE_SSH_STDOUT"
fi
if [ -n "${FAKE_SSH_STDERR:-}" ]; then
	printf '%s' "$FAKE_SSH_STDERR" >&2
fi
exit "${FAKE_SSH_EXIT_STATUS:-0}"
EOF
	chmod +x "$l_zxfer_test_fake_ssh_path"
}
