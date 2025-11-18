# TODO – Logical Bugs

- **Recursive runs skip property/backup work when datasets are already in sync** – `set_g_recursive_source_list()` only tracks datasets with missing snapshots (`src/zxfer_get_zfs_list.sh:223-265`), and `copy_filesystems()` iterates exclusively over that list (`src/zxfer_zfs_mode.sh:253-294`). When recursion finds no deltas, `transfer_properties()` never runs and `g_backup_file_contents` stays empty, yet `write_backup_properties()` still overwrites `.zxfer_backup_info.*` with just the header (`src/zxfer_globals.sh:959-980`). As a result `-P/-o` flags cannot enforce property changes on already-synced datasets and `-k` wipes prior metadata. The property/backup pass should still run (or file writes should be skipped) even when no snapshots need sending.


# TODO – Security Review
