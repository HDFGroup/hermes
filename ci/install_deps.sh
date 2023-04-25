#!/bin/bash

# Hermes dependency installation script
#
# Hermes depends on the following packages (in alphabetical order):
#
# Catch2
# GLOG
# GLPK
# HDF5
# IOR (for performance testing)
# Thallium
# yaml-cpp
#
# This script will build and install them via Spack from source
# because Hermes requires a very specific version and configuration options
# for each package.

set -x
set -e
set -o pipefail

sudo apt-get install -y pkg-config

# Change this especially when your $HOME doesn't have enough disk space.
#LOCAL=local
INSTALL_DIR="${HOME}/${LOCAL}"
SPACK_DIR=${INSTALL_DIR}/spack
SPACK_VERSION=0.18.1

# Install spack
echo "Installing dependencies at ${INSTALL_DIR}"
mkdir -p ${INSTALL_DIR}
git clone https://github.com/spack/spack ${SPACK_DIR}
pushd ${SPACK_DIR}
git checkout v${SPACK_VERSION}
popd

set +x
. ${SPACK_DIR}/share/spack/setup-env.sh
set -x

# This will allow Spack to skip building some packages that are directly
# available from the system. For example, autoconf, cmake, m4, etc.
# Modify ci/pckages.yaml to skip building compilers or build tools via Spack.
cp ci/packages.yaml ${SPACK_DIR}/etc/spack/packages.yaml

# This will override Spack's default package repository to allow building
# a custom package when the same package is available from Spack.
spack repo add ./ci/hermes

# This will build our small python library for running unit tests
cd ci/jarvis-util
python3 -m pip install -r requirements.txt
python3 -m pip install -e .

# NOTE(llogan): Modify version string per release.
HERMES_VERSION=1.0.0
spack install hermes +vfd