Name:           zxfer
Version:        2.0.0-20260413
Release:        0.1%{?dist}
Summary:        Optimized ZFS snapshot replication script

License:        BSD-2-Clause
URL:            https://github.com/totalAldo/zxfer
Source0:        https://github.com/totalAldo/zxfer/archive/refs/tags/v%{version}.tar.gz

BuildArch:      noarch

# Keep the core dependency set portable. zxfer resolves runtime helpers through
# its secure-PATH model, so downstream packagers may swap these generic
# requirements for distro-specific package names or virtual provides.
Requires:       /bin/sh
Requires:       /usr/bin/awk
Requires:       /usr/bin/ssh
Requires:       zfs

# Optional accelerators (GNU parallel for -j discovery, zstd for -z compression
# and remote snapshot-discovery metadata compression when ssh compression is active)
Recommends:     parallel
Recommends:     zstd

%description
zxfer is a maintained release of the long-standing zxfer utility. It adds
high-performance ZFS replication, dataset property synchronization, and
additional safety checks while retaining the original one-command workflow.
Optional features use GNU parallel for `-j` snapshot discovery and zstd for
`-z` / `-Z` compressed ssh streams, including remote snapshot-discovery
metadata compression when that ssh-compression path is active.

%prep
%autosetup

%build
# Nothing to build, this is a shell script collection.

%install
rm -rf %{buildroot}

# Install the project into a private libexec tree so the helper modules can be
# located relative to the launcher.
install -d %{buildroot}%{_libexecdir}/%{name}
install -m0755 zxfer %{buildroot}%{_libexecdir}/%{name}/zxfer
for helper in src/*.sh; do
    install -Dm0644 "$helper" %{buildroot}%{_libexecdir}/%{name}/$helper
done

# Public wrapper script.
install -d %{buildroot}%{_bindir}
cat > %{buildroot}%{_bindir}/zxfer <<'EOF'
#!/bin/sh
exec %{_libexecdir}/%{name}/zxfer "$@"
EOF
chmod 0755 %{buildroot}%{_bindir}/zxfer

# Manual page
install -Dm0644 man/zxfer.8 %{buildroot}%{_mandir}/man8/zxfer.8

%files
%license COPYING
%doc README.md CHANGELOG.txt KNOWN_ISSUES.md OPTIMIZATION.md SECURITY.md CONTRIBUTING.md
%doc packaging/README.txt
%doc docs/
%doc examples/
%{_bindir}/zxfer
%{_libexecdir}/%{name}
%{_mandir}/man8/zxfer.8*

%changelog
* Mon Apr 13 2026 Aldo Gonzalez - 2.0.0-20260413-0.1
- Track zxfer 2.0.0-20260413 release and modernize Source URL (see CHANGELOG.txt for upstream details).
- Make platform/security docs clearer about remote helper hardening and macOS caveats, and loosen the RPM spec away from a path-locked `/sbin/zfs` dependency so downstream packagers can adapt it more easily.
