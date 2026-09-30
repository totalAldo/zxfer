#!/bin/sh

# Template: replace the dataset names and backup directory before use.
# The -k/-e flow uses current-format v2 backup metadata only. -e reads only the
# metadata that -k recorded for the same source and destination, so set
# RESTORE_DESTINATION to the destination of the -k run (DEST_DATASET); any
# other destination stops with "Cannot find backup property file".

set -eu

REPO_ROOT=$(
	CDPATH=
	cd -- "$(dirname "$0")/.." && pwd
)
BACKUP_DIR="/var/db/zxfer"
SRC_DATASET="tank/src"
DEST_DATASET="backup/dst"
RESTORE_DESTINATION="$DEST_DATASET"

ZXFER_BACKUP_DIR="$BACKUP_DIR" "$REPO_ROOT/zxfer" -v -k -R "$SRC_DATASET" "$DEST_DATASET"
ZXFER_BACKUP_DIR="$BACKUP_DIR" "$REPO_ROOT/zxfer" -v -e -R "$SRC_DATASET" "$RESTORE_DESTINATION"
