#!/bin/sh
# BSD HEADER START
# This file is part of zxfer project.

# Copyright (c) 2024-2026 Aldo Gonzalez
# Copyright (c) 2013-2019 Allan Jude <allanjude@freebsd.org>
# Copyright (c) 2010,2011 Ivan Nash Dreckman
# Copyright (c) 2007,2008 Constantin Gonzalez
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:

#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright notice,
#       this list of conditions and the following disclaimer in the documentation
#       and/or other materials provided with the distribution.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# BSD HEADER END

# Loader contract:
# owns globals: none.
# reads globals: ZXFER_SOURCE_MODULES_ROOT.
# mutates caches: none; loading defines functions but performs no initialization.
# returns via stdout: none.

# Canonical newline-delimited source order. Keep this list as data so loading,
# partial-load validation, architecture checks, and release concatenation can
# consume one manifest instead of maintaining parallel module inventories.
ZXFER_SOURCE_MODULE_MANIFEST='zxfer_path_security.sh
zxfer_quoting.sh
zxfer_locking.sh
zxfer_reporting.sh
zxfer_profile.sh
zxfer_exec.sh
zxfer_dependencies.sh
zxfer_runtime.sh
zxfer_secure_staging.sh
zxfer_error_log.sh
zxfer_background_jobs.sh
zxfer_ssh_transport.sh
zxfer_remote_hosts.sh
zxfer_cli.sh
zxfer_operation_state.sh
zxfer_snapshot_state.sh
zxfer_backup_storage.sh
zxfer_backup_metadata.sh
zxfer_property_state.sh
zxfer_property_policy.sh
zxfer_property_reconcile.sh
zxfer_snapshot_producers.sh
zxfer_remote_snapshot_discovery.sh
zxfer_snapshot_discovery.sh
zxfer_migration_services.sh
zxfer_send_jobs.sh
zxfer_send_receive.sh
zxfer_snapshot_reconcile.sh
zxfer_replication.sh
zxfer_session.sh'

# Purpose: Source one `src/` module through the canonical loader path.
# Usage: Called during module loading and bootstrap sequencing so the launcher
# and tests share one source-order authority.
zxfer_source_module() {
	l_zxfer_source_module_name=$1
	l_zxfer_source_module_root=${ZXFER_SOURCE_MODULES_ROOT:-.}
	# shellcheck source=/dev/null
	. "$l_zxfer_source_module_root/src/$l_zxfer_source_module_name"
}

# Purpose: Validate a requested partial-load boundary against the manifest.
# Usage: Called before loading so an invalid boundary cannot partially mutate
# the current shell by sourcing an unintended prefix.
zxfer_is_source_module_name() {
	l_zxfer_source_module_candidate=${1:-}
	[ -n "$l_zxfer_source_module_candidate" ] || return 1

	while IFS= read -r l_zxfer_source_module_manifest_name; do
		[ "$l_zxfer_source_module_manifest_name" = \
			"$l_zxfer_source_module_candidate" ] && return 0
	done <<EOF
$ZXFER_SOURCE_MODULE_MANIFEST
EOF

	return 1
}

# Purpose: Source the canonical module sequence, optionally through one module.
# Usage: The launcher loads the complete sequence; focused tests pass a final
# module name without relying on source-time returns or initialization effects.
# Side effects: Defines functions from each selected module in the current shell.
zxfer_load_modules() {
	l_zxfer_load_modules_through=${1:-}
	if [ -n "$l_zxfer_load_modules_through" ] &&
		! zxfer_is_source_module_name "$l_zxfer_load_modules_through"; then
		printf '%s\n' "zxfer: unknown source module boundary: $l_zxfer_load_modules_through" >&2
		return 2
	fi

	while IFS= read -r l_zxfer_load_modules_name; do
		zxfer_source_module "$l_zxfer_load_modules_name" || return $?
		if [ -n "$l_zxfer_load_modules_through" ] &&
			[ "$l_zxfer_load_modules_name" = "$l_zxfer_load_modules_through" ]; then
			return 0
		fi
	done <<EOF
$ZXFER_SOURCE_MODULE_MANIFEST
EOF
}
