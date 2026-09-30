zxfer README.txt
================

This file is retained for packaging and compatibility with older distributions
that still expect a plain-text README.

The current project documentation lives in:

- README.md      - primary project overview, testing notes, and current fork behavior
- docs/external-tools.md - runtime, optional, and test tool inventory for packaging work
- man/zxfer.8    - canonical CLI reference and examples for section 8 installs
- man/zxfer.1m   - generated Solaris/illumos section 1M rendering; do not edit directly
- CHANGELOG.txt  - release history
- COPYING        - license

For current usage and operational guidance, read README.md first.
For the complete command reference, install the man page and run:

    man zxfer

	or read man/zxfer.8 / man/zxfer.1m directly from this repository.

Building the RPM
----------------

packaging/zxfer.spec is a starting point for downstream packages, and CI
builds it on every push (.github/workflows/packaging.yml). To build it by hand
from a checkout, with rpmbuild installed:

    version=$(sed -n 's/^Version:[[:space:]]*//p' packaging/zxfer.spec)
    mkdir -p ~/rpmbuild/SOURCES
    git archive --format=tar.gz --prefix="zxfer-$version/" \
        --output="$HOME/rpmbuild/SOURCES/v$version.tar.gz" HEAD
    rpmbuild -bb packaging/zxfer.spec

The version is MAJOR.MINOR.YYYYMMDD (see CONTRIBUTING.md); rpmbuild rejects a
"-" in Version.
