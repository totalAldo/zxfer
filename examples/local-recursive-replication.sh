#!/bin/sh

# Template: replace the dataset names before use.

set -eu

REPO_ROOT=$(
	CDPATH=
	cd -- "$(dirname "$0")/.." && pwd
)
SRC_DATASET="tank/data"
DEST_DATASET="backup/data"

exec "$REPO_ROOT/zxfer" -v -R "$SRC_DATASET" "$DEST_DATASET"
