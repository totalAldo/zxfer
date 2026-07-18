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
# shellcheck shell=sh disable=SC2034,SC2154

################################################################################
# SECURE PATH-ADJACENT STAGING
################################################################################

# Module contract:
# owns globals: secure-staging result scratch.
# reads globals: path-security validators and runtime-artifact registration.
# mutates caches: runtime artifact registry when registration is available.
# returns via stdout: unpredictable staging entry and directory paths.

# Purpose: Create one unpredictably named staging entry (file or directory)
# from a randomized temp-name template under a caller-validated parent.
# Usage: zxfer_create_unpredictable_staging_entry <template> <file|dir>.
# Validated staging parents may still be shared sticky directories (a
# /tmp-style ZXFER_ERROR_LOG parent), where predictable pid+attempt slot names
# would let a local process-table reader pre-create every slot and deny
# staging; the randomized names close that squat window and the forced 077
# umask keeps entries 0600 (files) or 0700 (directories).
zxfer_create_unpredictable_staging_entry() {
	l_template=$1
	l_entry_kind=$2

	l_dir_flag=""
	if [ "$l_entry_kind" = "dir" ]; then
		l_dir_flag="-d"
	fi
	# l_dir_flag intentionally expands unquoted: empty means no extra word.
	# shellcheck disable=SC2086
	(umask 077 && exec mktemp $l_dir_flag "$l_template") 2>/dev/null
}

# Purpose: Create the secure staging directory for path using the safety checks
# owned by this module.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# zxfer needs a fresh staged resource or persistent helper state.
zxfer_create_secure_staging_dir_for_path() {
	l_path=$1
	l_prefix=${2:-zxfer.stage}

	g_zxfer_secure_staging_dir_result=""
	l_parent=$(zxfer_get_path_parent_dir "$l_path") || return 1
	l_parent=$(zxfer_validate_temp_root_candidate "$l_parent") || return 1

	l_stage_dir=$(zxfer_create_unpredictable_staging_entry "$l_parent/.$l_prefix.XXXXXX" dir) || return 1
	# Register same-directory staging so trap cleanup reaps it on aborts.
	if ! zxfer_register_runtime_artifact_path "$l_stage_dir"; then
		rmdir "$l_stage_dir" 2>/dev/null || :
		return 1
	fi
	g_zxfer_secure_staging_dir_result=$l_stage_dir
	printf '%s\n' "$l_stage_dir"
	return 0
}
