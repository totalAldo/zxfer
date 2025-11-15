Name:           zxfer
Version:        2.0.1
Release:        0.1%{?dist}
Summary:        Optimized ZFS snapshot replication script with rsync mode

License:        BSD-2-Clause
URL:            https://github.com/totalAldo/zxfer
Source0:        https://github.com/totalAldo/zxfer/archive/refs/tags/v%{version}.tar.gz

BuildArch:      noarch

Requires:       /bin/sh
Requires:       /sbin/zfs
Requires:       /usr/bin/gawk
Requires:       /usr/bin/parallel
Requires:       /usr/bin/rsync
Requires:       /usr/bin/ssh
Requires:       /usr/bin/zstd

Provides:       zxfer-turbo

%description
zxfer turbo is a refactored release of the long-standing zxfer utility.  It
adds GNU parallel powered snapshot discovery, optional rsync replication, raw
send support, zstd-compressed ssh streams, dataset property synchronization and
additional safety checks.  The script is aimed at power users who need to
replicate or prune large OpenZFS installations quickly while retaining the
original one-command workflow.

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

# Manual page and documentation.
install -Dm0644 zxfer.8 %{buildroot}%{_mandir}/man8/zxfer.8
install -Dm0644 README.md %{buildroot}%{_docdir}/%{name}-%{version}/README.md
install -Dm0644 README.txt %{buildroot}%{_docdir}/%{name}-%{version}/README.txt
install -Dm0644 CHANGELOG.txt %{buildroot}%{_docdir}/%{name}-%{version}/CHANGELOG.txt
install -Dm0644 COPYING %{buildroot}%{_docdir}/%{name}-%{version}/COPYING

%files
%license %{_docdir}/%{name}-%{version}/COPYING
%doc %{_docdir}/%{name}-%{version}/README.md
%doc %{_docdir}/%{name}-%{version}/README.txt
%doc %{_docdir}/%{name}-%{version}/CHANGELOG.txt
%{_bindir}/zxfer
%{_libexecdir}/%{name}
%{_mandir}/man8/zxfer.8*

%changelog
* Fri Nov 14 2025 Aldo Gonzalez - 2.0.1-0.1
- Track zxfer turbo 2.0.1 release and modernize Source URL (see CHANGELOG.txt for upstream details).
