#!/bin/bash

# THIS SCRIPT IS EXECUTED BY CONTAINER!!!
# CD into Hermes directory in container
cd ${GITHUB_WORKSPACE}
git submodule update --init

set -x
set -e
set -o pipefail

# Load hermes_shm
. ${SPACK_DIR}/share/spack/setup-env.sh
spack load hermes_shm

# Build Hermes
mkdir -p "${HOME}/install"
mkdir build
cd build
cmake ../ \
-DCMAKE_BUILD_TYPE=Debug \
-DCMAKE_INSTALL_PREFIX="${HOME}/install" \
-DHERMES_ENABLE_MPIIO_ADAPTER=ON \
-DHERMES_MPICH=ON \
-DHERMES_ENABLE_STDIO_ADAPTER=ON \
-DHERMES_ENABLE_POSIX_ADAPTER=ON \
-DHERMES_ENABLE_COVERAGE=ON
make -j8
make install

# Test Hermes
export CXXFLAGS=-Wall
ctest -VV

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
cd test/unit/external
mkdir build
cd build
cmake ../
make -j8
