#!/bin/bash

set -x
set -e
set -o pipefail

INSTALL_DIR="${HOME}/${LOCAL}"
SPACK_DIR=${INSTALL_DIR}/spack
MOCHI_REPO_DIR=${INSTALL_DIR}/mochi-spack-packages
THALLIUM_VERSION=0.8.3
GOTCHA_VERSION=develop
CATCH2_VERSION=2.13.3
ORTOOLS_VERSION=7.7
SPACK_VERSION=0.16.3
HDF5_VERSION=1_13_0

echo "Installing dependencies at ${INSTALL_DIR}"
mkdir -p ${INSTALL_DIR}

# HDF5
# TODO: Use spack package once 1.13.0 is available in a release (currently
# only on the develop branch)
git clone https://github.com/HDFGroup/hdf5
pushd hdf5
git checkout hdf5-${HDF5_VERSION}
./autogen.sh
mkdir -p build
pushd build
CXXFLAGS=-I"${INSTALL_DIR}/include" LDFLAGS="-L${INSTALL_DIR}/lib -Wl,-rpath,${INSTALL_DIR}/lib" \
        ../configure --prefix=${INSTALL_DIR}
make && make install
popd
popd

# Spack
git clone https://github.com/spack/spack ${SPACK_DIR}
pushd ${SPACK_DIR}
git checkout v${SPACK_VERSION}
popd

set +x
. ${SPACK_DIR}/share/spack/setup-env.sh
set -x

cp ci/packages.yaml ${SPACK_DIR}/etc/spack/packages.yaml
MOCHI_REPO=https://github.com/mochi-hpc/mochi-spack-packages.git
# TODO(chogan): We pin this commit because in future commits they add features
# of spack that are not included in the version we use. Next time there's a new
# spack release, we can tryb using the head of `main` in mochi-spack-packages.
MOCHI_SPACK_PACKAGES_COMMIT=f015ae93717ac3b81972c55116c3b91aa9c645e4
git clone ${MOCHI_REPO} ${MOCHI_REPO_DIR}
pushd ${MOCHI_REPO_DIR}
git checkout ${MOCHI_SPACK_PACKAGES_COMMIT}
popd

spack repo add ${MOCHI_REPO_DIR}
spack repo add ./ci/hermes

THALLIUM_SPEC="mochi-thallium~cereal@${THALLIUM_VERSION} ^mercury~boostsys"
CATCH2_SPEC="catch2@${CATCH2_VERSION}"
ORTOOLS_SPEC="gortools@${ORTOOLS_VERSION}"

spack install ${THALLIUM_SPEC} ${CATCH2_SPEC} ${ORTOOLS_SPEC}
SPACK_STAGING_DIR=~/spack_staging
mkdir -p ${SPACK_STAGING_DIR}
spack view --verbose symlink ${SPACK_STAGING_DIR} ${THALLIUM_SPEC} ${CATCH2_SPEC} ${ORTOOLS_SPEC}

cp -LRnv ${SPACK_STAGING_DIR}/* ${INSTALL_DIR}
