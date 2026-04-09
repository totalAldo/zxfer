FROM mcr.microsoft.com/devcontainers/base:ubuntu-24.04 AS kcov-builder

ARG DEBIAN_FRONTEND=noninteractive
ARG KCOV_VERSION=v43
ARG KCOV_TARBALL_SHA256=4cbba86af11f72de0c7514e09d59c7927ed25df7cebdad087f6d3623213b95bf

RUN apt-get update && apt-get install -y --no-install-recommends \
	binutils-dev \
	build-essential \
	ca-certificates \
	cmake \
	curl \
	libcurl4-openssl-dev \
	libdw-dev \
	libelf-dev \
	libiberty-dev \
	libssl-dev \
	libstdc++-12-dev \
	ninja-build \
	python3 \
	zlib1g-dev \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /tmp

RUN set -eux; \
	curl -fsSL "https://github.com/SimonKagstrom/kcov/archive/refs/tags/${KCOV_VERSION}.tar.gz" -o kcov.tar.gz; \
	echo "${KCOV_TARBALL_SHA256}  kcov.tar.gz" | sha256sum -c -; \
	tar -xzf kcov.tar.gz; \
	cd "kcov-${KCOV_VERSION#v}"; \
	cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local; \
	cmake --build build; \
	DESTDIR=/tmp/kcov-install cmake --install build; \
	rm -rf /tmp/kcov.tar.gz "/tmp/kcov-${KCOV_VERSION#v}"

FROM mcr.microsoft.com/devcontainers/base:ubuntu-24.04

ARG DEBIAN_FRONTEND=noninteractive

ENV ZXFER_LINT_TOOL_DIR=/usr/local/share/zxfer/lint-tools

RUN apt-get update && apt-get install -y --no-install-recommends \
	bash \
	binutils \
	busybox \
	ca-certificates \
	curl \
	dash \
	git \
	libbfd-dev \
	libcurl4 \
	libdw1 \
	perl \
	posh \
	python-is-python3 \
	python3 \
	python3-venv \
	tar \
	xz-utils \
	zfsutils-linux \
	zlib1g \
	&& rm -rf /var/lib/apt/lists/*

COPY --from=kcov-builder /tmp/kcov-install/usr/local/ /usr/local/

WORKDIR /tmp/zxfer-devcontainer-bootstrap

COPY tests/run_lint.sh ./tests/run_lint.sh

RUN set -eux; \
	mkdir -p "$ZXFER_LINT_TOOL_DIR"; \
	./tests/run_lint.sh --bootstrap-only all; \
	actionlint_bin=$(find "$ZXFER_LINT_TOOL_DIR/actionlint" -type f -name actionlint -print -quit); \
	checkbashisms_bin=$(find "$ZXFER_LINT_TOOL_DIR/checkbashisms" -type f -name checkbashisms -print -quit); \
	shfmt_bin=$(find "$ZXFER_LINT_TOOL_DIR/shfmt" -type f -name shfmt -print -quit); \
	codespell_bin=$(find "$ZXFER_LINT_TOOL_DIR/codespell" -type f -path '*/venv/bin/codespell' -print -quit); \
	shellcheck_bin=$(find "$ZXFER_LINT_TOOL_DIR/shellcheck" -type f -name shellcheck -print -quit); \
	bash_bin=$(command -v bash); \
	busybox_bin=$(command -v busybox); \
	[ -n "$actionlint_bin" ]; \
	[ -n "$checkbashisms_bin" ]; \
	[ -n "$shfmt_bin" ]; \
	[ -n "$codespell_bin" ]; \
	[ -n "$shellcheck_bin" ]; \
	[ -n "$bash_bin" ]; \
	[ -n "$busybox_bin" ]; \
	ln -sf "$actionlint_bin" /usr/local/bin/actionlint; \
	ln -sf "$checkbashisms_bin" /usr/local/bin/checkbashisms; \
	ln -sf "$shfmt_bin" /usr/local/bin/shfmt; \
	ln -sf "$codespell_bin" /usr/local/bin/codespell; \
	ln -sf "$shellcheck_bin" /usr/local/bin/shellcheck; \
	printf '%s\n' '#!/bin/sh' "exec \"$bash_bin\" --posix \"\$@\"" >/usr/local/bin/bash-posix; \
	printf '%s\n' '#!/bin/sh' "exec \"$busybox_bin\" ash \"\$@\"" >/usr/local/bin/busybox-ash; \
	chmod 755 /usr/local/bin/bash-posix /usr/local/bin/busybox-ash; \
	kcov --version >/dev/null; \
	actionlint -version >/dev/null; \
	checkbashisms --help >/dev/null 2>&1; \
	shfmt --version >/dev/null; \
	codespell --version >/dev/null; \
	shellcheck --version >/dev/null; \
	chown -R vscode:vscode /usr/local/share/zxfer; \
	rm -rf /tmp/zxfer-devcontainer-bootstrap

WORKDIR /workspaces/zxfer
