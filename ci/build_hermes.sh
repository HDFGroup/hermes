#!/bin/bash

# CD into git workspace
cd ${GITHUB_WORKSPACE}
git submodule update --init

set -x
set -e
set -o pipefail

# Download from dockerhub
docker pull lukemartinlogan/hermes_deps:latest
docker run lukemartinlogan/hermes_deps:latest

# Set spack env
INSTALL_DIR="${HOME}"
SPACK_DIR=${INSTALL_DIR}/spack
. ${SPACK_DIR}/share/spack/setup-env.sh

# Load hermes_shm
mkdir -p "${HOME}/install"
mkdir build
cd build
spack load hermes_shm

# Install jarvis-cd
git clone https://github.com/grc-iit/jarvis-cd.git
pushd jarvis-cd
pip install -e . -r requirements.txt
popd

# Build Hermes
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
