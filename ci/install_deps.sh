#!/bin/bash

set -x
set -e
set -o pipefail

INSTALL_DIR="${HOME}/${LOCAL}"
SPACK_DIR=${INSTALL_DIR}/spack
MOCHI_REPO_DIR=${INSTALL_DIR}/mochi-spack-packages
THALLIUM_VERSION=0.10.0
GOTCHA_VERSION=develop
CATCH2_VERSION=2.13.3
SPACK_VERSION=0.17.2
HDF5_VERSION=1_13_0

echo "Installing dependencies at ${INSTALL_DIR}"
mkdir -p ${INSTALL_DIR}

# HDF5
# TODO: Use spack package once 1.13.0 is available in a release (currently
# only on the develop branch)
git clone https://github.com/HDFGroup/hdf5
pushd hdf5
git checkout hdf5-${HDF5_VERSION}
bash autogen.sh
mkdir -p build
pushd build
CXXFLAGS=-I"${INSTALL_DIR}/include" LDFLAGS="-L${INSTALL_DIR}/lib -Wl,-rpath,${INSTALL_DIR}/lib" \
        ../configure --prefix="${INSTALL_DIR}"
make -j 4 && make install
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
# TODO(chogan): We pin this commit because in the past using the HEAD of 'main'
# has been unstable. We update at controlled intervals rather than putting out
# fires. The fewer moving pieces we have in CI, the easier it is to diagnose
# errors.
MOCHI_SPACK_PACKAGES_COMMIT=3c3d64d5e7265b44cb68433be061604c9e0ed739
git clone ${MOCHI_REPO} ${MOCHI_REPO_DIR}
pushd ${MOCHI_REPO_DIR}
git checkout ${MOCHI_SPACK_PACKAGES_COMMIT}
popd

spack repo add ${MOCHI_REPO_DIR}
spack repo add ./ci/hermes

THALLIUM_SPEC="mochi-thallium@${THALLIUM_VERSION} ^mercury~boostsys"
CATCH2_SPEC="catch2@${CATCH2_VERSION}"
GLPK_SPEC="glpk"
GLOG_SPEC="glog"

spack install ${THALLIUM_SPEC} ${CATCH2_SPEC} ${GLPK_SPEC} ${GLOG_SPEC}
SPACK_STAGING_DIR=~/spack_staging
mkdir -p ${SPACK_STAGING_DIR}
spack view --verbose symlink ${SPACK_STAGING_DIR} ${THALLIUM_SPEC} ${CATCH2_SPEC} ${GLPK_SPEC} ${GLOG_SPEC}

cp -LRnv ${SPACK_STAGING_DIR}/* ${INSTALL_DIR}
