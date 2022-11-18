#!/bin/bash
set -e -x

yum install -y openssl-devel
curl https://sh.rustup.rs --proto '=https' --tlsv1.2 -sSf | sh -s -- --default-toolchain stable -y

curl 'https://ziglang.org/download/0.10.0/zig-linux-aarch64-0.10.0.tar.xz' -o ./zig-linux-aarch64-0.10.0.tar.xz
tar xvf ./zig-linux-aarch64-0.10.0.tar.xz
ls -lah ./zig-linux-aarch64-0.10.0/zig
echo $PATH
mkdir -p $HOME/.bin
cp ./zig-linux-aarch64-0.10.0/zig $HOME/.bin
zig --help
