CONTENTS
========
1. INTRODUCTION
2. FILES
3. INSTALLATION
4. READING THE FEATURED MANUAL

1. INTRODUCTION
===============
zxfer is a utility for transferring ZFS filesystems and their properties. It
can also transfer selected files and directories on a ZFS pool from a single
atomic snapshot. For more info consult the man page, which has extensive
usage examples.

You MUST read and understand the disclaimer before use.
Refer to the man page, and the file COPYING.

2. FILES
========
zxfer
a) README.txt   - this file
b) COPYING      - BSD license
c) zxfer        - zxfer
d) zxfer.8      - man page, gzipped - FreeBSD version
e) zxfer.1m     - man page, (Open)Solaris version
f) zxfer.spec   - rpm specification file
g) CHANGELOG.txt- list of what has changed and when
h) src/*        - src directory with modularized sh files referenced by zxfer

3. INSTALLATION
===============
You will need to be root before starting.
$ su

FreeBSD:
For the most recent version of this script
git clone https://github.com/totalAldo/zxfer ./zxfer
chmod +x ./zxfer/zxfer
Add zxfer to your path or invoke with relative file path

Older versions may be installed from FreeBSD ports
The best way is to simply install via ports. Recommended.
a) Go to ports directory.
# cd /usr/ports/sysutils/zxfer
b) Install
# make install

Here are the directions for those who want to do it manually.
a) Copy zxfer to /usr/local/sbin.
# cp zxfer /usr/local/sbin
b) Copy zxfer.8.gz to /usr/local/man/man8
# cp zxfer.8.gz /usr/local/man/man8

OpenSolaris, Solaris 11 Express:
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

Note that this will not set the paths permanently.

4. READING THE FEATURED MANUAL
==============================
You can read the man page after you have followed the above steps by:

# man zxfer

