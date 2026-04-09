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
# reads globals: ZXFER_SOURCE_MODULES_ROOT and ZXFER_SOURCE_MODULES_THROUGH.
# mutates caches: dependency defaults via zxfer_initialize_dependency_defaults.
# returns via stdout: none.

: "${ZXFER_SOURCE_MODULES_ROOT:=.}"

zxfer_source_module() {
	l_module=$1
	# shellcheck source=/dev/null
	. "$ZXFER_SOURCE_MODULES_ROOT/src/$l_module"
}

# Foundation: path validation, reporting, command rendering, and local dependency resolution.
zxfer_source_module zxfer_path_security.sh
[ "${ZXFER_SOURCE_MODULES_THROUGH:-}" = "zxfer_path_security.sh" ] && return 0

zxfer_source_module zxfer_reporting.sh
[ "${ZXFER_SOURCE_MODULES_THROUGH:-}" = "zxfer_reporting.sh" ] && return 0

zxfer_source_module zxfer_exec.sh
[ "${ZXFER_SOURCE_MODULES_THROUGH:-}" = "zxfer_exec.sh" ] && return 0

zxfer_source_module zxfer_dependencies.sh
zxfer_initialize_dependency_defaults
[ "${ZXFER_SOURCE_MODULES_THROUGH:-}" = "zxfer_dependencies.sh" ] && return 0

# Runtime/session state and remote/bootstrap layers.
zxfer_source_module zxfer_runtime.sh
[ "${ZXFER_SOURCE_MODULES_THROUGH:-}" = "zxfer_runtime.sh" ] && return 0

zxfer_source_module zxfer_remote_hosts.sh
[ "${ZXFER_SOURCE_MODULES_THROUGH:-}" = "zxfer_remote_hosts.sh" ] && return 0

zxfer_source_module zxfer_cli.sh
[ "${ZXFER_SOURCE_MODULES_THROUGH:-}" = "zxfer_cli.sh" ] && return 0

# Cached dataset/property state before replication planning.
zxfer_source_module zxfer_snapshot_state.sh
[ "${ZXFER_SOURCE_MODULES_THROUGH:-}" = "zxfer_snapshot_state.sh" ] && return 0

zxfer_source_module zxfer_backup_metadata.sh
[ "${ZXFER_SOURCE_MODULES_THROUGH:-}" = "zxfer_backup_metadata.sh" ] && return 0

zxfer_source_module zxfer_property_cache.sh
[ "${ZXFER_SOURCE_MODULES_THROUGH:-}" = "zxfer_property_cache.sh" ] && return 0

zxfer_source_module zxfer_property_reconcile.sh
[ "${ZXFER_SOURCE_MODULES_THROUGH:-}" = "zxfer_property_reconcile.sh" ] && return 0

# Snapshot discovery, transfer, reconciliation, and top-level orchestration.
zxfer_source_module zxfer_snapshot_discovery.sh
[ "${ZXFER_SOURCE_MODULES_THROUGH:-}" = "zxfer_snapshot_discovery.sh" ] && return 0

zxfer_source_module zxfer_send_receive.sh
[ "${ZXFER_SOURCE_MODULES_THROUGH:-}" = "zxfer_send_receive.sh" ] && return 0

zxfer_source_module zxfer_snapshot_reconcile.sh
[ "${ZXFER_SOURCE_MODULES_THROUGH:-}" = "zxfer_snapshot_reconcile.sh" ] && return 0

zxfer_source_module zxfer_replication.sh
