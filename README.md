zxfer
=====

2024 - This fork of zxfer aims at optimizing zfs replication by refactoring the code for readability and maintainability, adding error handling functions, and adding new options for very verbose mode and
zstd compression.

A continuation of development on zxfer, a popular script for managing ZFS snapshot replication

The Original author seems to have abandoned the project, there have been no updates since May 2011 and the script fails to work correctly in FreeBSD versions after 8.2 and 9.0 due to new ZFS properties.

[Original Project Home](http://code.google.com/p/zxfer/)

Changes
=======

2024.03.14
+ refactor script into multiple files, grouping files by function usage
+ bump to version 2.0.0

2024.03.12
+ add -V option for (V)ery verbose mode using echoV()
+ optimize set_last_common_snapshot() by using grep instead of nested loops
+ add echoV statements for debugging

2024.03.11
+ refactor inspect_delete_snap() into multiple functions with the primary goal
  of optimizing the snapshot deletion logic. Convert that from nested loops
  to using temp files and comm for a more efficient comparison.

2024.03.08
+ refactor code for readability and maintanability
+ add error handling functions
+ rename variables for clarity
+ add -z option to compress zfs send using zstd -3
+ add -Z option to specify the full zstd compress command
---

+ Implement new -D parameter, allows you to put a progress indicator app between the zfs send and zfs receive. Provides macros %%size%% and %%title%%.
	Example usage:

		-D 'bar -s %%size%% -bl 1m -bs 256m'
+ Ignore new read-only properties added in FreeBSD 9.1: 'written' and 'refcompressratio'
+ Ignore new read-only properties added in FreeBSD 9.2/8.4: 'logicalused' and 'logicalreferenced'
+ "Unsupported Properties" support, do not copy properties that are unsupported by the destination pool. Allows replication from 11-CURRENT to 9.2 etc, by automatically ignoring new properties such as: volmode, filesystem_limit, snapshot_limit, filesystem_count, snapshot_count, redundant_metadata
+ Fixed -o mountpoint=foo , it is no longer ignored as readonly if explicitly requested by the user
+ Implemented new -I parameter, ignore these properties and do not try to set them
+ Implemented new -U parameter, do not try to replicate unsupported properties, to skip properties that the destination does not understand

# Installation


You will need to be root before starting.

	$ su

## FreeBSD:

### Via pkg (Recommended)
	pkg install zxfer


### Via Ports
#### Auto

a) Go to ports directory.

	# cd /usr/ports/sysutils/zxfer
b) Install

	# make install
##### Manual
Here are the directions for those who want to do it manually.
a) Copy zxfer to /usr/local/sbin.

	# cp zxfer /usr/local/sbin
b) Copy zxfer.8.gz to /usr/local/man/man8

	# cp zxfer.8.gz /usr/local/man/man8

### FreeNAS

As the freenas file system is not persistent for user changes, we will need to;

a) Create a standard jail via the freenas UI

b) Add the datasets required for the transfer to the jail via the jails storage manager in the UI

c) Use either the pkg or port methods above to install zxfer

all instructions for the above can be found here

http://doc.freenas.org/9.3/freenas_jails.html

### OpenSolaris, Solaris 11 Express:
a) Copy zxfer to /usr/sfw/bin.

	# cp zxfer /usr/sfw/bin

b) Set the path to include this.

	# PATH=$PATH:/usr/sfw/bin

c) Copy zxfer.1m to /usr/share/man/man1m

	# cp zxfer.1m /usr/share/man/man1m

d) Delete the old catman page, if you are updating.

	# rm /usr/share/man/cat1m/zxfer.1m

e) Set the MANPATH variable correctly.

	# MANPATH=$MANPATH:/usr/sfw/share/man

**Note that this will not set the paths permanently.**

(I don't know how. If you know how, please inform me.)
