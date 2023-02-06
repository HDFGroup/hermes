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
INSTALL_DIR="${HOME}/${LOCAL}"

SPACK_DIR=${INSTALL_DIR}/spack
MOCHI_REPO_DIR=${INSTALL_DIR}/mochi-spack-packages
THALLIUM_VERSION=0.10.0
CATCH2_VERSION=3.0.1
SPACK_VERSION=0.18.1
HDF5_VERSION=1_13_1

echo "Installing dependencies at ${INSTALL_DIR}"
mkdir -p ${INSTALL_DIR}

# Spack
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

MOCHI_REPO=https://github.com/mochi-hpc/mochi-spack-packages.git
# TODO(chogan): We pin this commit because in the past using the HEAD of 'main'
# has been unstable. We update at controlled intervals rather than putting out
# fires. The fewer moving pieces we have in CI, the easier it is to diagnose
# errors.
MOCHI_SPACK_PACKAGES_COMMIT=3c3d64d5e7265b44cb68433be061604c9e0ed739
git clone ${MOCHI_REPO} ${MOCHI_REPO_DIR}
pushd ${MOCHI_REPO_DIR}
git checkout ${MOCHI_SPACK_PACKAGES_COMMIT}
popd

# This will override Spack's default package repository to allow building
# a custom package when the same package is available from Spack.
spack repo add ${MOCHI_REPO_DIR}
spack repo add ./ci/hermes

THALLIUM_SPEC="mochi-thallium@${THALLIUM_VERSION} ^mercury~boostsys"
CATCH2_SPEC="catch2@${CATCH2_VERSION}"
GLPK_SPEC="glpk"
GLOG_SPEC="glog"
HDF5_SPEC="hdf5@${HDF5_VERSION}"
YAML_CPP_SPEC="yaml-cpp"
ALL_SPECS="${THALLIUM_SPEC} ${CATCH2_SPEC} ${GLPK_SPEC} ${GLOG_SPEC} ${HDF5_SPEC} ${YAML_CPP_SPEC}"

spack install ${ALL_SPECS}
SPACK_STAGING_DIR=~/spack_staging
mkdir -p ${SPACK_STAGING_DIR}

# Spack installation directory has hash value.
# This will simplify and consolidate the installation path.
spack view --verbose symlink ${SPACK_STAGING_DIR} ${ALL_SPECS}

# Copy what Spack installed in a temporary location to your desired location.
cp -LRnv ${SPACK_STAGING_DIR}/* ${INSTALL_DIR}

# Install a custom IOR that has patches for Hermes for performance testing.
pushd ~
git clone https://github.com/ChristopherHogan/ior
pushd ior
git checkout chogan/hermes
./bootstrap
mkdir -p build
pushd build
CPPFLAGS=-I${INSTALL_DIR}/include \
        LDFLAGS="-L${INSTALL_DIR}/lib -Wl,-rpath,${INSTALL_DIR}/lib" \
        ../configure --prefix=${INSTALL_DIR} --with-hdf5=yes
make -j 4 && make install
popd
popd
popd
