#!/bin/sh

# Template: replace the origin host and dataset names before use.

set -eu

REPO_ROOT=$(
	CDPATH=
	cd -- "$(dirname "$0")/.." && pwd
)
ORIGIN_HOST="user@example.com"
SRC_DATASET="zroot"
DEST_DATASET="backup/zroot"

exec "$REPO_ROOT/zxfer" -v -O "$ORIGIN_HOST" -R "$SRC_DATASET" "$DEST_DATASET"
