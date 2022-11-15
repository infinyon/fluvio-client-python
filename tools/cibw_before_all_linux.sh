#!/bin/bash
set -e -x

yum install -y openssl-devel zig
curl https://sh.rustup.rs --proto '=https' --tlsv1.2 -sSf | sh -s -- --default-toolchain stable -y
