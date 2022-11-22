#!/bin/bash
set -e -x

yum install -y openssl-devel
curl https://sh.rustup.rs --proto '=https' --tlsv1.2 -sSf | sh -s -- --default-toolchain stable -y
cp $HOME/.cargo/bin/* /bin/
