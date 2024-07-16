zxfer (turbo)
=====

2024 - This is a refactored version of zxfer, with the goal of optimizing ZFS replication. Enhancements include improved code readability and performance, additional error handling functions, and new options.

These changes were motivated by the lengthy replication times experienced when transferring large dataset snapshots, primarily composed of log entries. As a result, the modifications have significantly decreased the time required for both ssh and local replication.

## Ideas for further improvements
+ if the delete option is specified, list the source snapshots without
  creation time, and begin deleting the destination snapshots as soon as
  they are listed. This will reduce the time required to delete snapshots
  after the full snapshot list has been generated. While the deletions are
  in progress, list the source snapshots with creation time which takes longer.
  Generating the source snapshot list with creation time can take several seconds
  depednding on the number of snapshots and this time can be used effectively
  to perform delete operations on the destination.
+ explore using parallel sends which may help maximize network bandwidth
  utilization and reduce overall replication time

## New Options
+ `-V`: Enables very verbose mode.
+ `-w`: Activates raw send.
+ `-x`: specify the number of parallel zfs list snaphot commands to run via gnu parallel (this can improve the performance when listing source snapshots that are cpu-bound)
+ `-Y`: Yields when there are no more snapshots to send or destroy, or after 8 iterations, whichever comes first.
+ `-z`: pipe ssh transfers through zstd default compression
+ `-Z`: custom zstd compression supporting higher compression levels or multiple threads

## Performance Improvements
In addition, wherever the code path lends itself to parallelization, it is implemented
as background processes. This includes:
+ Executing `zfs list` commands concurrently.
+ Running `zfs destroy` commands as background processes.
+ Use `zfs send -I` instead of `zfs send -i` for incremental replication to send the entire snapshot chain in one go.
+ The `inspect_delete_snap()` function has been refactored to use the `comm` command instead of nested loops. Previously, the function used nested loops to identify which destination snapshots should be deleted. This process was executed even when the `-d` option was not in use. For instance, if both the source and destination contained 1,000 snapshots, the loop would iterate 1,000,000 times. Each iteration would spawn at least two `grep` and two `cut` commands to compare snapshot names. The new implementation with `comm` is more efficient and readable.
+ Reduce I/O load by listing only the names of the destination snapshots.
  Previously, the destination snapshots were listed by creation time which
  caused the snapshot metadata to be fetched.
+ combine multiple `zfs destroy` commands into a single command to reduce the number of
  processes spawned
+ optimize `get_zfs_list()` by only checking the snapshots of the intended
  destination dataset if it exists. Previous snapshot lists used the parent dataset
  which potentially doubled the number of snapshots to check and may have included
  destination datasets that did not need to be checked
+ compress the output of the source snapshot list when using `ssh` via `zstd -9`.
  When there are many snapshots, the output of the `zfs list` command can be several
  megabytes and it is highly compressible. While `ssh` offers compression options
  including the use of `zstd`, compressesion is now explicity set by piping the
  `zfs list` output through `zstd -9`

## Code Refactoring
The code has been refactored for better readability and maintainability, which includes:
+ Dividing the code into smaller, more manageable functions.
+ Incorporating error handling and debugging functions.
+ Segmenting the code into smaller files, grouped by functionality.
+ Renaming variables to indicate whether they are global, modular, or local references.

## Feedback Welcome
If you use this script and have any suggestions or feedback, please open an issue or a pull request. I hope this script will be beneficial to others and that useful features can be incorporated into the main project.

## Testing
This fork has been tested with FreeBSD 14.0, FreeBSD 14.1

## Acknowledgements
A big thank you to everyone who contributed to this script over the past 16+ years, and to all its users.

Best Wishes, Aldo

---
## Original README Contents:
A continuation of development on zxfer, a popular script for managing ZFS snapshot replication

The Original author seems to have abandoned the project, there have been no updates since May 2011 and the script fails to work correctly in FreeBSD versions after 8.2 and 9.0 due to new ZFS properties.

[Original Project Home](http://code.google.com/p/zxfer)

Changes
=======
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
