#!/bin/sh
# BSD HEADER START
# This file is part of zxfer project.

# Copyright (c) 2024 Aldo Gonzalez
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

################################################################################
# COMMON FUNCTIONS FOR ZXFER
#
# Global variables that must be defined in the calling script:
#  g_option_n_dryrun
#  g_option_v_verbose
#  g_option_V_very_verbose
#  g_option_b_beep_always
#  g_option_B_beep_on_success
################################################################################

#
# Add the debug_start() and debug_end() functions to enable/disable debugging
# between code blocks.
debug_start() {
    set -x
}

debug_end() {
    set +x
}

#
# Create a temporary file and return the filename.
#
get_temp_file() {
    l_timestamp=$(date +%s)
    l_file=$(mktemp -t "zxfer.$l_timestamp") || throw_error "Error creating temporary file."
    echoV "New temporary file: $l_file"
    echo "$l_file"
}

#
# Gets a $(uname), i.e. the operating system, for origin or target, if remote.
# Takes: $1=either $g_option_O_origin_host or $g_option_T_target_host
#
get_os() {
    l_input_options=$1
    l_output_os=""

    # Get uname of the destination (target) machine, local or remote
    if [ "$l_input_options" = "" ]; then
        l_output_os=$(uname)
    else
        l_output_os=$($l_input_options uname)
    fi

    echo "$l_output_os"
}

#
# Function to handle errors
#
# ext status
# 0 - success
# 1 - general error
# 2 - usage error
# 3 - error that prevents the script from continuing
throw_error() {
    l_msg=$1
    l_exit_status=${2:-1} # global used by beep
    echo "$l_msg"
    beep "$l_exit_status"
    exit "$l_exit_status"
}

throw_usage_error() {
    l_msg=$1
    l_exit_status=${2:-2} # global used by beep
    if [ "$l_msg" != "" ]; then
        echo "Error: $l_msg"
    fi
    usage
    beep "$l_exit_status"
    exit "$l_exit_status"
}

# sample usage:
# execute_command "ls -l" 1
# l_cmd: command to execute
# l_is_continue_on_fail: 1 to continue on fail, 0 to stop on fail
execute_command() {
    l_cmd=$1
    l_is_continue_on_fail=${2:-0}

    if [ $g_option_n_dryrun -eq 1 ]; then
        echov "Dry run: $l_cmd"
        return
    fi

    echov "$l_cmd"
    if [ "$l_is_continue_on_fail" -eq 1 ]; then
        eval "$l_cmd" || {
            echo "Non-critical error when executing command. Continuing."
        }
    else
        eval "$l_cmd" || throw_error "Error when executing command."
    fi
}

#
# Execute a command in the background and write the output to a file.
#
# l_cmd: command to execute
# l_output_file: file to write the output to
#
execute_background_cmd() {
    l_cmd=$1
    l_output_file=$2
    echoV "Executing command in the background: $l_cmd"
    $l_cmd >"$l_output_file" &
}

#
# Print out information if in verbose mode
#
echov() {
    if [ "$g_option_v_verbose" -eq 1 ]; then
        echo "$@"
    fi
}

#
# Very verbose mode - print mesage to standard error
#
echoV() {
    if [ "$g_option_V_very_verbose" -eq 1 ]; then
        echo "$@" >&2
    fi
}

#
# Beeps a success sound if -B enabled, and a failure sound if -b or -B enabled.
#
beep() {
    l_exit_status=${1:-1} # default to 1 (failure)

    if [ $g_option_b_beep_always -eq 1 ] || [ $g_option_B_beep_on_success -eq 1 ]; then
        # load the speaker kernel module if not loaded already
        l_speaker_km_loaded=$(kldstat | grep -c speaker.ko)
        if [ "$l_speaker_km_loaded" = "0" ]; then
            kldload "speaker"
        fi

        # play the appropriate beep
        if [ "$l_exit_status" -eq 0 ]; then
            if [ $g_option_B_beep_on_success -eq 1 ]; then
                echo "T255CCMLEG~EG..." >/dev/speaker # success sound
            fi
        else
            echo "T150A<C.." >/dev/speaker # failure sound
        fi
    fi
}
