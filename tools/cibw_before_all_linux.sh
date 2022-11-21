#!/bin/bash
set -e -x

yum install -y openssl-devel
curl https://sh.rustup.rs --proto '=https' --tlsv1.2 -sSf | sh -s -- --default-toolchain stable -y

ZIG_VERSION=0.10.0
ZIG_DIRECTORY="zig-linux-aarch64-${ZIG_VERSION}"
ZIG_ZIP_FILE=${ZIG_DIRECTORY}.tar.xz"

curl "https://ziglang.org/download/${ZIG_VERSION}/${ZIG_ZIP_FILE}" -o ./${ZIG_ZIP_FILE}
tar xvf ./${ZIG_ZIP_FILE}
ls -lah ./${ZIG_DIRECTORY}/zig
echo $PATH
mkdir -p $HOME/.bin
cp ./${ZIG_DIRECTORY}/zig $HOME/.bin
zig --help
