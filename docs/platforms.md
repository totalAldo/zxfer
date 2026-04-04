# Platform Support

## Supported Platforms

zxfer is intended to work with:

- FreeBSD with OpenZFS
- Linux with OpenZFS
- illumos / Solaris-family systems
- OpenZFS on macOS, with known property-reconciliation caveats

The project targets POSIX `/bin/sh`, so portability depends more on shell and
tool behavior than on GNU-specific scripting features.

## Tool Resolution

zxfer resolves required tools through a trusted secure-PATH model instead of
blindly inheriting the caller's `PATH`.

Important environment variables:

- `ZXFER_SECURE_PATH`: replace the default allowlist entirely
- `ZXFER_SECURE_PATH_APPEND`: append extra absolute directories

Default allowlist:

```text
/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin
```

On macOS, the integration harness also prepends `/usr/local/zfs/bin` when that
OpenZFS-on-macOS path exists.

## Remote Hosts

Remote helper resolution is platform-aware for the hardened paths below and no
longer assumes the same local absolute binary path exists remotely. This
matters especially when:

- `zfs` lives in different directories between source and destination hosts
- wrapped host specs are used, for example `user@host pfexec`
- restore mode (`-e`) needs a remote `cat`
- `-j` requires GNU `parallel` on the origin host

Remote `uname` detection and the remote `zstd` path used by `-O ... -j ... -z`
are still tracked as open hardening gaps in [../KNOWN_ISSUES.md](../KNOWN_ISSUES.md).

## Service Management

`-c` and migration-related service handling remain Solaris / illumos oriented.
These paths assume `svcadm` semantics and fail fast when the service manager is
not available.

## Current Caveats

See [../KNOWN_ISSUES.md](../KNOWN_ISSUES.md) for the current cross-platform
limitations, including:

- remote `uname` / OS detection hardening gaps
- remote `zstd` lookup in source snapshot listing
- Darwin-specific property behavior that is not yet strict enough for full
  end-to-end certification
