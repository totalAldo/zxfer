#!/bin/sh

# Template: replace the target host and dataset names before use.

set -eu

REPO_ROOT=$(
	CDPATH=
	cd -- "$(dirname "$0")/.." && pwd
)
TARGET_HOST="backup@example.com"
SRC_DATASET="tank/src"
DEST_DATASET="backup/dst"

exec "$REPO_ROOT/zxfer" -v -z -T "$TARGET_HOST" -R "$SRC_DATASET" "$DEST_DATASET"
