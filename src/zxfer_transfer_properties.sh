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

# for shellcheck linting, uncomment this line
#. ./zxfer_globals.sh; . ./zxfer_get_zfs_list.sh; . ./zxfer_get_zfs_list.sh; . ./zxfer_rsync_mode.sh;

# module variables
m_new_rmvs_pv=""
m_new_rmv_pvs=""
m_new_mc_pvs=""
m_only_supported_properties=""

#
# Strips the sources from a list of properties=values=sources,
# e.g. output is properties=values,
# output is in $m_new_rmvs_pv
#
remove_sources() {
    m_new_rmvs_pv=""

    l_rmvs_list=$1

    for l_rmvs_line in $l_rmvs_list; do
        l_rmvs_property=$(echo "$l_rmvs_line" | cut -f1 -d=)
        l_rmvs_value=$(echo "$l_rmvs_line" | cut -f2 -d=)
        m_new_rmvs_pv="$m_new_rmvs_pv$l_rmvs_property=$l_rmvs_value,"
    done

    # remove trailing comma
    m_new_rmvs_pv=${m_new_rmvs_pv%,}
}

#
# Selects only the specified properties
# and values in the format property1=value1=source,...
# Used to select the "must create" properties
#
select_mc() {
    m_new_mc_pvs=""

    l_mc_list=$1          # target list of properties, values
    l_mc_property_list=$2 # list of properties to select

    # remove readonly properties from the override list
    for l_mc_line in $l_mc_list; do
        l_found_mc=0

        l_mc_property=$(echo "$l_mc_line" | cut -f1 -d=)
        l_mc_value=$(echo "$l_mc_line" | cut -f2 -d=)
        l_mc_source=$(echo "$l_mc_line" | cut -f3 -d=)

        # test for readonly properties
        for l_property in $l_mc_property_list; do
            if [ "$l_property" = "$l_mc_property" ]; then
                l_found_mc=1
                #since the property was matched let's not waste time looking for it again
                l_mc_property_list=$(echo "$l_mc_property_list" | tr -s "," "\n" |
                    grep -v ^"$l_property"$ | tr -s "\n" ",")
                break
            fi
        done

        if [ $l_found_mc -eq 1 ]; then
            m_new_mc_pvs="$m_new_mc_pvs$l_mc_property=$l_mc_value=$l_mc_source,"
        fi
    done

    m_new_mc_pvs=${m_new_mc_pvs%,}
}

#
# Removes the readonly properties and values from a list of properties
# values and sources in the format property1=value1=source1,...
# output is in m_new_rmv_pvs
#
remove_properties() {
    m_new_rmv_pvs="" # global

    _rmv_list=$1    # the list of properties=values=sources,...
    _remove_list=$2 # list of properties to remove

    for rmv_line in $_rmv_list; do
        found_readonly=0
        rmv_property=$(echo "$rmv_line" | cut -f1 -d=)
        rmv_value=$(echo "$rmv_line" | cut -f2 -d=)
        rmv_source=$(echo "$rmv_line" | cut -f3 -d=)
        # test for readonly properties
        for property in $_remove_list; do
            if [ "$property" = "$rmv_property" ]; then
                if [ "$rmv_source" = "override" ]; then
                    # The user has specifically required we set this property
                    continue
                fi
                found_readonly=1
                #since the property was matched let's not waste time looking for it again
                _remove_list=$(echo "$_remove_list" | tr -s "," "\n" | grep -v ^"$property"$)
                _remove_list=$(echo "$_remove_list" | tr -s "\n" ",")
                break
            fi
        done
        if [ $found_readonly -eq 0 ]; then
            m_new_rmv_pvs="$m_new_rmv_pvs$rmv_property=$rmv_value=$rmv_source,"
        fi
    done

    m_new_rmv_pvs=${m_new_rmv_pvs%,}
}

#
# Removes the readonly properties and values from a list of properties
# values and sources in the format property1=value1=source1,...
# output is in m_new_rmv_pvs
#
remove_unsupported_properties() {
    l_orig_set_list=$1 # the list of properties=values=sources,...
    l_FUNCIFS=$IFS
    IFS=","

    m_only_supported_properties=""
    for l_orig_line in $l_orig_set_list; do
        l_found_unsup=0
        l_orig_set_property=$(echo "$l_orig_line" | cut -f1 -d=)
        l_orig_set_value=$(echo "$l_orig_line" | cut -f2 -d=)
        l_orig_set_source=$(echo "$l_orig_line" | cut -f3 -d=)
        for l_property in $unsupported_properties; do
            if [ "$l_property" = "$l_orig_set_property" ]; then
                l_found_unsup=1
                break
            fi
        done
        if [ $l_found_unsup -eq 0 ]; then
            m_only_supported_properties="$m_only_supported_properties$l_orig_set_property=$l_orig_set_value=$l_orig_set_source,"
        else
            echov "Destination does not support property ${l_orig_set_property}=${l_orig_set_value}"
        fi
    done
    m_only_supported_properties=${m_only_supported_properties%,}
    IFS=$l_FUNCIFS
}

#
# Normalize the list of properties to set by using a mix of human-readable and
# machine-readable values
#
resolve_human_vars() {
    _machine_vars=$1
    _human_vars=$2
    _FUNCIFS=$IFS
    IFS=","

    human_results=
    for h_var in $_human_vars; do
        h_prop=${h_var%%=*}
        for m_var in $_machine_vars; do
            m_prop=${m_var%%=*}
            if [ "$h_prop" = "$m_prop" ]; then
                machine_property=$(echo "$m_var" | cut -f1 -d=)
                machine_value=$(echo "$m_var" | cut -f2 -d=)
                machine_source=$(echo "$m_var" | cut -f3 -d=)
                human_value=$(echo "$h_var" | cut -f2 -d=)
                if [ "$human_value" = "none" ]; then
                    machine_value=$human_value
                fi
                human_results="${human_results}$machine_property=$machine_value=$machine_source,"
            fi
        done
    done
    human_results=${human_results%,}
    IFS=$_FUNCIFS
}

#
# Transfers properties from any source to destination.
# Either creates the filesystem if it doesn't exist,
# or sets it after the fact.
# Also, checks to see if the override properties given as options are valid.
# Needs: $initial_source, $g_actual_dest, $g_recursive_dest_list
# $g_dont_write_backup
# $g_ensure_writable
#
transfer_properties() {
    echoV "transfer_properties: $1"
    echoV "initial_source: $initial_source"

    l_source=$1

    # We have chosen to set all source properties in the case of -P
    # Any -o values will be set too, and will override any values from -P.
    # Where the destination does not exist, it will be created.

    # Get the list of properties to enforce on the destination. This will be an
    # amalgam of -o options, and if -P exists, a list of the source properties.

    # get source properties,values,sources in form
    # property1=value1=source1,property2=value2=source2,...
    # Use -p to remove units, since localization can make these incompatible
    l_source_pvs=$($g_LZFS get -Hpo property,value,source all "$l_source" |
        tr "\t" "=" | tr "\n" ",")
    l_source_pvs=${l_source_pvs%,}
    l_source_human_pvs=$($g_LZFS get -Ho property,value,source all "$l_source" |
        tr "\t" "=" | tr "\n" ",")
    l_source_human_pvs=${l_source_human_pvs%,}

    # Check if the source is a zvol, which requires some special handling
    l_source_dstype=$($g_LZFS get -Hpo value type "$l_source")
    l_source_volsize=$($g_LZFS get -Hpo value volsize "$l_source")

    # Some localizations (German, French) use a comma as a decimal separator which
    # changes the interpretation of the value on machines with a different
    # localization.  Some OSes (OS X) use odd units (Ki) instead of K, and the
    # values cannot be interpretted properly on other localizations
    # Resolve this issue by passing -p which generates 'script parsable' output
    # However, some properties have a value of 'none', that in -p ends up having
    # a different value (filesystem_count=18446744073709551615)
    # clean these up by comparing the two outputs and making the right decision
    resolve_human_vars "$l_source_pvs" "$l_source_human_pvs"
    l_source_pvs="$human_results"

    # add to the details to allow backup of properties
    # unless $g_dont_write_backup non-zero, as with first rsync transfer
    # of properties
    if [ "$g_option_k_backup_property_mode" -eq 1 ] && [ "$g_dont_write_backup" -eq 0 ]; then
        g_backup_file_contents="$g_backup_file_contents;\
$l_source,$g_actual_dest,$l_source_pvs"
    fi

    # If we are restoring properties, then get l_source_pvs from the backup file
    if [ "$g_option_e_restore_property_mode" -eq 1 ]; then
        l_source_pvs=$(echo "$g_restored_backup_file_contents" | grep "^[^,]*,$l_source," |
            sed -e 's/^[^,]*,[^,]*,//g')
        if [ "$l_source_pvs" = "" ]; then
            throw_usage_error "Can't find the properties for the filesystem $l_source"
        fi
    fi

    # Just using g_option_o_override_property_pv so that we can modify it
    g_option_o_override_property_pv=$g_option_o_override_property

    # Now to ensure writable, if that is set.
    if [ "$g_ensure_writable" -eq 1 ]; then
        # make sure that the g_option_o_override_property_pv includes only readonly=off
        g_option_o_override_property_pv=$(echo "$g_option_o_override_property_pv" | sed -e 's/readonly=on/readonly=off/g')

        # make sure that the l_source_pvs includes only readonly=off
        l_source_pvs=$(echo "$l_source_pvs" | sed -e 's/readonly=on/readonly=off/g')
    fi

    valid_option_property=0
    #change the field separator to a ","
    OLDIFS=$IFS
    IFS=","

    # Test to see if each -o property is valid; leave value testing to zfs.
    # Note that this only needs to be done once and this is a good place.
    if [ "$initial_source" = "$l_source" ]; then
        for op_line in $g_option_o_override_property_pv; do
            op_property=$(echo "$op_line" | cut -f1 -d=)
            for sp_line in $l_source_pvs; do
                sp_property=$(echo "$sp_line" | cut -f1 -d=)
                if [ "$op_property" = "$sp_property" ]; then
                    valid_option_property=1
                    break # break out of the loop, we found what we wanted
                fi
            done
            if [ $valid_option_property -eq 0 ]; then
                throw_usage_error "Invalid option property - check -o list for syntax errors."
            else
                valid_option_property=0
            fi
        done
    fi

    # Create the override_pvs list and creation_pvs list.
    # creation_pvs will be used in the instance where we need to create the
    # destination. override_pvs will be used in the instance where we need
    # to set/inherit destination properties.
    override_pvs=""
    creation_pvs=""
    # note that if this function is executed, either option P or o must
    # have been invoked
    if [ "$g_option_P_transfer_property" -eq 0 ]; then # i.e. option o contains something
        for op_line in $g_option_o_override_property_pv; do
            op_property=$(echo "$op_line" | cut -f1 -d=)
            op_value=$(echo "$op_line" | cut -f2 -d=)
            override_source="override"
            override_pvs="$override_pvs$override_property=\
$override_value=$override_source,"
        done
    else
        # Get a list of properties and values to override the destination's.
        # Note that the overrides need to be removed from the creation list as
        # they will be auto-inherited from the initial source. Note also that
        # only "local" options need to be specified in the creation list, as
        # all others will be auto-inherited.
        #
        for sp_line in $l_source_pvs; do
            override_property=$(echo "$sp_line" | cut -f1 -d=)
            override_value=$(echo "$sp_line" | cut -f2 -d=)
            override_source=$(echo "$sp_line" | cut -f3 -d=)
            creation_property=$override_property
            creation_value=$override_value
            creation_source=$override_source
            for op_line in $g_option_o_override_property_pv; do
                op_property=$(echo "$op_line" | cut -f1 -d=)
                op_value=$(echo "$op_line" | cut -f2 -d=)
                if [ "$op_property" = "$override_property" ]; then
                    override_property=$op_property
                    override_value=$op_value
                    override_source="override"
                    creation_property="NULL"
                    break # break out of the loop, we found what we wanted
                fi
            done
            override_pvs="$override_pvs$override_property=$override_value=$override_source,"
            if [ "$creation_property" != "NULL" ] && [ "$creation_source" = "local" ]; then
                creation_pvs="$creation_pvs$creation_property=$creation_value=$creation_source,"
            elif [ "$l_source_dstype" = "volume" ] && [ "$creation_property" = "refreservation" ]; then
                creation_pvs="$creation_pvs$creation_property=$creation_value=$creation_source,"
            fi
        done
    fi

    # Remove several properties not supported on FreeBSD.
    if [ "$g_destination_operating_system" = "FreeBSD" ]; then
        g_readonly_properties="$g_readonly_properties,$g_fbsd_readonly_properties"
    fi

    # Remove several properties not supported on Solaris Express.
    if [ "$g_destination_operating_system" = "SunOS" ] && [ "$g_source_operating_system" = "FreeBSD" ]; then
        g_readonly_properties="$g_readonly_properties,$g_solexp_readonly_properties"
    fi

    # Remove the readonly properties and values.
    remove_properties "$override_pvs" "$g_readonly_properties"
    # Remove the properties the user has asked us to ignore
    if [ -n "$g_option_I_ignore_properties" ]; then
        remove_properties "$m_new_rmv_pvs" "$g_option_I_ignore_properties"
    fi
    override_pvs="$m_new_rmv_pvs"

    # Remove any properties that are not supported by the destination
    if [ -n "$unsupported_properties" ]; then
        remove_unsupported_properties "$override_pvs"
        override_pvs="$m_only_supported_properties"
    fi

    dest_exist=$(echo "$g_recursive_dest_list" | grep -c "^$g_actual_dest$")

    # This is where we actually create or modify the destination filesystem.
    # Is the destination absent? If so, just create with correct option list.
    if [ "$dest_exist" = "0" ]; then
        if [ "$initial_source" = "$l_source" ]; then
            # as this is the initial source, we want to transfer all properties from
            # the source, overridden with g_option_o_override_property values as necessary
            remove_sources "$override_pvs"
            override_option_list=$(echo "$m_new_rmvs_pv" | tr "," "\n" | sed "s/\(.*\)=\(.*\)/\1=\'\2\'/g" | tr "\n" "," | sed 's/,$//' | sed -e 's/,/ -o /g')
            if [ "$override_option_list" != "" ]; then
                override_option_list=" -o $override_option_list"
            fi
            if [ "$l_source_dstype" = "volume" ]; then
                override_option_list=" -V $l_source_volsize $override_option_list"
            fi

            # If not, create it with the override list and be done with it -
            # we have now transferred all properties
            echov "Creating destination filesystem \"$g_actual_dest\" \
with specified properties."

            # revert to old field separator
            # (This and reversion back is so that $g_RZFS command works with -r)
            IFS=$OLDIFS

            if [ "$g_option_n_dryrun" -eq 0 ]; then
                eval "$g_RZFS create $override_option_list $g_actual_dest" ||
                    throw_error "Error when creating destination filesystem."
            else
                echo "$g_RZFS create $override_option_list $g_actual_dest"
            fi

            #change the field separator to a ","
            OLDIFS=$IFS
            IFS=","

        else
            # for non-initial source, all the overrides will be inherited, hence
            # create with creation_pvs list
            remove_properties "$creation_pvs" "$g_readonly_properties"
            # Remove the properties the user has asked us to ignore
            if [ -n "$g_option_I_ignore_properties" ]; then
                remove_properties "$m_new_rmv_pvs" "$g_option_I_ignore_properties"
            fi
            creation_pvs="$m_new_rmv_pvs"

            remove_sources "$creation_pvs"
            creation_option_list=$(echo "$m_new_rmvs_pv" | tr "," "\n" | sed "s/\(.*\)=\(.*\)/\1=\'\2\'/" | tr "\n" "," | sed 's/,$//' | sed -e 's/,/ -o /g')

            if [ "$creation_option_list" != "" ]; then
                creation_option_list=" -o $creation_option_list"
            fi
            if [ "$l_source_dstype" = "volume" ]; then
                creation_option_list=" -V $l_source_volsize $creation_option_list"
            fi

            # revert to old field separator
            # (This and reversion back is so that $g_RZFS command works with -r)
            IFS=$OLDIFS

            echov "Creating destination filesystem \"$g_actual_dest\" \
with specified properties."
            if [ "$g_option_n_dryrun" -eq 0 ]; then
                eval "$g_RZFS create -p $creation_option_list $g_actual_dest" ||
                    throw_error "Error when creating destination filesystem."
            else
                echo "$g_RZFS create -p $creation_option_list $g_actual_dest"
            fi

            #change the field separator to a ","
            OLDIFS=$IFS
            IFS=","
        fi
    else # it does exist, need to create.

        # For the child, we need to do:
        # If the destination list does exist, we need to do the following:
        # 1. Check that the "must create" properties are the same, otherwise exit.
        # 2. Check that all the remaining values and sources are appropriate on the
        #      destination, or are required to be set or inherited.

        # For the initial source, we need to do:
        # If the destination list does exist, we need to do the following:
        # 1. Check that the "must create" properties are the same, otherwise exit.
        # 2. Check that all the remaining values are the same, and that each source
        #      is "local". This applies both to -P properties and -o properties
        # 3. If either of those are different, we need to set them.

        # revert to old field separator
        # (This and reversion back is so that $g_RZFS command works with -r)
        IFS=$OLDIFS

        dest_pvs=$($g_RZFS get -Hpo property,value,source all "$g_actual_dest")
        dest_human_pvs=$($g_RZFS get -Ho property,value,source all "$g_actual_dest")

        #change the field separator to a ","
        OLDIFS=$IFS
        IFS=","

        dest_pvs=$(echo "$dest_pvs" | tr -s "\t" "=" | tr -s "\n" ",")
        dest_pvs=${dest_pvs%,}
        dest_human_pvs=${dest_human_pvs%,}
        resolve_human_vars "$dest_pvs" "$dest_human_pvs"
        dest_pvs="$human_results"

        # remove the readonly properties and values as we are not comparing to them
        remove_properties "$dest_pv" "$g_readonly_properties"
        # Remove the properties the user has asked us to ignore
        if [ -n "$g_option_I_ignore_properties" ]; then
            remove_properties "$m_new_rmv_pvs" "$g_option_I_ignore_properties"
        fi
        dest_pv="$m_new_rmv_pvs"

        # Test to see if any of the four properties that must be specified at
        # creation time differ from destination to the overrides, if so
        # terminate with an error.

        must_create_properties="casesensitivity,normalization,jailed,utf8only"

        select_mc "$override_pvs" "$must_create_properties"
        mc_override_pvs="$m_new_mc_pvs"

        select_mc "$dest_pvs" "$must_create_properties"
        mc_dest_pvs="$m_new_mc_pvs"

        # this for loop tests for a "must create" property that we can't set
        for ov_line in $mc_override_pvs; do
            ov_property=$(echo "$ov_line" | cut -f1 -d=)
            ov_value=$(echo "$ov_line" | cut -f2 -d=)
            for dest_line in $mc_dest_pvs; do
                found_dest=0
                dest_property=$(echo "$dest_line" | cut -f1 -d=)
                dest_value=$(echo "$dest_line" | cut -f2 -d=)
                for l_mc_property in $must_create_properties; do
                    if [ "$l_mc_property" = "$dest_property" ] && [ "$l_mc_property" = "$ov_property" ]; then
                        if [ "$ov_value" != "$dest_value" ]; then
                            echo "Error: The property \"$dest_property\" may only be set"
                            echo "       at filesystem creation time. To modify this property"
                            echo "       you will need to first destroy target filesystem."
                            usage
                            beep
                            exit 1
                        fi
                        # we've matched the must create property, remove it.
                        must_create_properties=$(echo "$must_create_properties" | tr -s "," "\n")
                        must_create_properties=$(echo "$must_create_properties" | grep -v ^"$l_mc_property"$ | tr -s "\n" ",")
                        found_dest=1
                        break # break out of the loop, we found what we wanted
                    fi
                done
                if [ $found_dest -eq 1 ]; then
                    break
                fi
            done
        done

        # At this stage, the "must create" properties are fine.
        # Now we need to compare destination values and sources for each
        # property from the $override_pv list. If the destination's source field
        # is not "local" and the value field from both source and destination
        # do not match, we will need to set the destination property.
        #

        # remove the "must create" properties from the $override_pvs list
        must_create_properties="casesensitivity,normalization,jailed,utf8only"
        remove_properties "$override_pvs" "$must_create_properties"
        override_pvs="$m_new_rmv_pvs"

        remove_properties "$dest_pvs" "$must_create_properties,$g_readonly_properties"
        # Remove the properties the user has asked us to ignore
        if [ -n "$g_option_I_ignore_properties" ]; then
            remove_properties "$m_new_rmv_pvs" "$g_option_I_ignore_properties"
        fi

        dest_pvs="$m_new_rmv_pvs"

        # zfs set takes a long time; let's only set the properties we need to set
        # or inherit the properties we need to inherit

        # changes begin here

        ov_initsrc_set_list="" # for initial source only
        ov_set_list=""         # for child sources
        ov_inherit_list=""     # for child sources
        for ov_line in $override_pvs; do
            ov_property=$(echo "$ov_line" | cut -f1 -d=)
            ov_value=$(echo "$ov_line" | cut -f2 -d=)
            ov_source=$(echo "$ov_line" | cut -f3 -d=)
            for dest_line in $dest_pvs; do
                dest_property=$(echo "$dest_line" | cut -f1 -d=)
                dest_value=$(echo "$dest_line" | cut -f2 -d=)
                dest_source=$(echo "$dest_line" | cut -f3 -d=)
                if [ "$ov_property" = "$dest_property" ]; then
                    if [ "$dest_value" != "$ov_value" ] || [ "$dest_source" != "local" ]; then
                        ov_initsrc_set_list="$ov_initsrc_set_list$ov_property=$ov_value,"
                    fi
                    # Now we decide whether to leave, set, or inherit on the destination
                    if [ "$ov_value" != "$dest_value" ]; then
                        # value needs to be set or inherited, which one?
                        if [ "$ov_source" = "local" ]; then
                            #value needs to be set
                            ov_set_list="$ov_set_list$ov_property=$ov_value,"
                        else
                            # source is not local and value needs to be force inherited
                            ov_inherit_list="$ov_inherit_list$ov_property=$ov_value,"
                        fi
                    # at this stage, the src and dest values are the same, just need
                    # to figure out whether to set or inherit
                    elif [ "$ov_source" = "local" ] && [ "$dest_source" != "local" ]; then
                        # value needs to be set
                        ov_set_list="$ov_set_list$ov_property=$ov_value,"
                    elif [ "$ov_source" != "local" ] && [ "$dest_source" = "local" ]; then
                        # need to force inherit
                        ov_inherit_list="$ov_inherit_list$ov_property=$ov_value,"
                    fi
                fi
                # we've matched the dest_line, remove it.
                dest_pvs=$(echo "$dest_pvs" | tr -s "," "\n" | grep -v ^"$dest_line"$)
                dest_pvs=$(echo "$dest_pvs" | tr -s "\n" ",")
                break
            done
        done

        # remove commas from end of line
        ov_initsrc_set_list=${ov_initsrc_set_list%,}
        ov_set_list=${ov_set_list%,}
        ov_inherit_list=${ov_inherit_list%,}

        # Now we have a list of only changes to make using zfs set.
        # Let's make the changes.

        # First notify the user
        if [ "$ov_set_list" != "" ] ||
            [ "$ov_inherit_list" != "" ] ||
            [ "$ov_initsrc_set_list" != "" ]; then
            echov "Setting properties/sources on destination filesystem \"$g_actual_dest\"."
        fi

        if [ "$initial_source" = "$l_source" ]; then
            ov_set_list="$ov_initsrc_set_list"
        fi

        # set properties that need setting
        for ov_line in $ov_set_list; do
            ov_property=$(echo "$ov_line" | cut -f1 -d=)
            ov_value=$(echo "$ov_line" | cut -f2 -d=)

            # revert to old field separator
            # (This and reversion back is so that $g_RZFS command works with -r)
            IFS=$OLDIFS

            if [ "$g_option_n_dryrun" -eq 0 ]; then
                $g_RZFS set "${ov_property}=${ov_value}" "$g_actual_dest" ||
                    trhow_error "Error when setting properties on destination filesystem."
            else
                echo "$g_RZFS set $ov_property=$ov_value $g_actual_dest"
            fi

            #change the field separator to a ","
            OLDIFS=$IFS
            IFS=","

        done

        if [ "$initial_source" != "$l_source" ]; then
            # Now we have a list of only changes to make using zfs inherit.
            # Let's make the changes.
            for ov_line in $ov_inherit_list; do
                ov_property=$(echo "$ov_line" | cut -f1 -d=)
                ov_value=$(echo "$ov_line" | cut -f2 -d=)

                # revert to old field separator
                # (This and reversion back is so that $g_RZFS command works with -r)
                IFS=$OLDIFS

                if [ "$g_option_n_dryrun" -eq 0 ]; then
                    $g_RZFS inherit "$ov_property" "$g_actual_dest" ||
                        throw_error "Error when inheriting properties on destination filesystem."
                else
                    echo "$g_RZFS inherit $ov_property $g_actual_dest"
                fi

                #change the field separator to a ","
                OLDIFS=$IFS
                IFS=","

            done
        fi
    fi

    # revert to old field separator
    IFS=$OLDIFS
}
