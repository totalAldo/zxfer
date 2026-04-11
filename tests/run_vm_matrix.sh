#!/bin/sh
#
# Run zxfer guest-backed test workflows inside disposable VM guests.
#

set -eu

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/vm/lib.sh
. "$TESTS_DIR/vm/lib.sh"

zxfer_vm_main "$@"
