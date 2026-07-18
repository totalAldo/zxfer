#!/bin/sh
# Compatibility aliases for reporting tests whose implementation boundary
# changed while their externally selectable names remain supported.

# Runtime cleanup is now an unconditional dependency of error-log storage, so
# the old missing-helper fallback maps to the owner-operation coverage that
# validates registration and cleanup through the supported boundary.
test_zxfer_cleanup_error_log_stage_dir_falls_back_without_runtime_cleanup_helper_in_current_shell() {
	test_zxfer_create_secure_staging_dir_for_path_registers_and_cleanup_unregisters_error_log_stage_dirs
}
