#!/bin/bash

# ARGS:
# SPACK_DIR: the path to spack

# Ensure environment modules are loaded
. /hermes/ci/module_load.sh

# THIS SCRIPT IS EXECUTED BY CONTAINER!!!
set -x
set -e
set -o pipefail

# Update jarvis-cd
pushd jarvis-cd
git pull
pip install -e . -r requirements.txt
popd

# Update scspkg
pushd scspkg
git pull
pip install -e . -r requirements.txt
popd

# Load scspkg environment
module use "$(scspkg module dir)"

# Load hermes_shm
. "${SPACK_DIR}/share/spack/setup-env.sh"
spack module tcl refresh --delete-tree -y
spack load hermes_shm
# module use "${SPACK_DIR}/share/spack/modules/linux-ubuntu22.04-zen2"

# Create Hermes module
scspkg create hermes
scspkg env prepend hermes PATH /hermes/build/bin
scspkg env prepend hermes LIBRARY_PATH /hermes/build/bin
scspkg env prepend hermes LD_LIBRARY_PATH /hermes/build/bin
module load hermes

# Initialize the Jarvis testing Hermes environment
jarvis init \
"${HOME}/jarvis-config" \
"${HOME}/jarvis-priv" \
"${HOME}/jarvis-shared"
cp /hermes/ci/resource_graph.yaml /jarvis-cd/config/resource_graph.yaml
jarvis env build hermes

# CD into Hermes directory in container
cd /hermes
git config --global --add safe.directory '*'
git submodule update --init

# Build Hermes
mkdir -p build
cd build
cmake ../ \
-DCMAKE_BUILD_TYPE=Debug \
-DCMAKE_INSTALL_PREFIX="$(scspkg pkg root hermes)" \
-DHERMES_ENABLE_MPIIO_ADAPTER=ON \
-DHERMES_MPICH=ON \
-DHERMES_ENABLE_STDIO_ADAPTER=ON \
-DHERMES_ENABLE_POSIX_ADAPTER=ON \
-DHERMES_ENABLE_COVERAGE=ON
make -j4
make install

# Test Hermes
export CXXFLAGS=-Wall
ctest -VV

# Run make install unit test
cd /hermes/test/unit/external
mkdir -p build
cd build
cmake ../
make -j4
