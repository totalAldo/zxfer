#!/bin/sh

# Template: replace the dataset names and backup directory before use.

set -eu

REPO_ROOT=$(
	CDPATH=
	cd -- "$(dirname "$0")/.." && pwd
)
BACKUP_DIR="/var/db/zxfer"
SRC_DATASET="tank/src"
DEST_DATASET="backup/dst"
RESTORE_DESTINATION="backup/restore"

ZXFER_BACKUP_DIR="$BACKUP_DIR" "$REPO_ROOT/zxfer" -v -k -R "$SRC_DATASET" "$DEST_DATASET"
ZXFER_BACKUP_DIR="$BACKUP_DIR" "$REPO_ROOT/zxfer" -v -e -R "$SRC_DATASET" "$RESTORE_DESTINATION"
