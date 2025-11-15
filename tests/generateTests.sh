#!/bin/sh

# This script generates the tests for zxfer.sh
# it generates local zfs dataset source and dest and creates multiple
# snapshots. It then uses zxfer in various combinations to replicate the
# datasets to the destination

# pass the pool name as the first argument
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <pool_name>"
    exit 1
fi

pool_name=$1

test_dataset="$pool_name/zxfer_tests"

zfs destroy -r "$test_dataset"

zfs create "$test_dataset"

zfs create "$test_dataset"/src

# create child datasets
for i in 1 2 3; do
    zfs create "$test_dataset"/src/child$i

    # create snapshots
    for j in 1 2 3 4; do
        zfs snap -r "$test_dataset"/src/child$i@snap$j
    done
done

# some other snapshots
zfs snap -r "$test_dataset"/src/child1@snap1_1
zfs snap -r "$test_dataset"/src/child1@snap2_1

zfs create "$test_dataset"/dest

../zxfer -v -R "$test_dataset"/src "$test_dataset"/dest

zfs list -r "$test_dataset"/dest