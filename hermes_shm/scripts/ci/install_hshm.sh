#!/bin/bash

# CD into git workspace
cd ${GITHUB_WORKSPACE}

set -x
set -e
set -o pipefail

# Set spack env
INSTALL_DIR="${HOME}"
SPACK_DIR=${INSTALL_DIR}/spack
. ${SPACK_DIR}/share/spack/setup-env.sh

mkdir -p "${HOME}/install"
mkdir build
pushd build
spack load --only dependencies hermes_shm
cmake ../ \
-DCMAKE_BUILD_TYPE=Debug \
-DHERMES_ENABLE_COVERAGE=ON \
-DHERMES_ENABLE_DOXYGEN=ON \
-DBUILD_HSHM_BENCHMARKS=ON \
-DBUILD_HSHM_TESTS=ON \
-DCMAKE_INSTALL_PREFIX=${HOME}/install
make -j8
make install

export CXXFLAGS=-Wall
ctest -VV
popd

# Set proper flags for cmake to find Hermes
INSTALL_PREFIX="${HOME}/install"
export LIBRARY_PATH="${INSTALL_PREFIX}/lib:${LIBRARY_PATH}"
export LD_LIBRARY_PATH="${INSTALL_PREFIX}/lib:${LD_LIBRARY_PATH}"
export LDFLAGS="-L${INSTALL_PREFIX}/lib:${LDFLAGS}"
export CFLAGS="-I${INSTALL_PREFIX}/include:${CFLAGS}"
export CPATH="${INSTALL_PREFIX}/include:${CPATH}"
export CMAKE_PREFIX_PATH="${INSTALL_PREFIX}:${CMAKE_PREFIX_PATH}"
export CXXFLAGS="-I${INSTALL_PREFIX}/include:${CXXFLAGS}"

# Run make install unit test
cd scripts/ci/external
mkdir build
cd build
cmake ../
make -j8
