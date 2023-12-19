#!/bin/bash

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
if ! shopt -q login_shell; then
  if [ -d /etc/profile.d ]; then
    for i in /etc/profile.d/*.sh; do
      if [ -r $i ]; then
        . $i
      fi
    done
  fi
fi
module use "$(scspkg module dir)"

# CD into Hermes directory in container
git config --global --add safe.directory '*'
cd hermes
git submodule update --init

# Load hermes_shm
. ${SPACK_DIR}/share/spack/setup-env.sh
spack load hermes_shm

# Create Hermes module
scspkg create hermes
scspkg env prepend hermes PATH /hermes/build
scspkg env prepend hermes LIBRARY_PATH /hermes/build
scspkg env prepend hermes LD_LIBRARY_PATH /hermes/build
module load hermes

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
make -j8
make install

# Initialize the Jarvis testing Hermes environment
jarvis init \
"${HOME}/jarvis-config" \
"${HOME}/jarvis-priv" \
"${HOME}/jarvis-shared"
cp /hermes/ci/resource_graph.yaml /jarvis-cd/config/resource_graph.yaml
jarvis env build hermes

# Test Hermes
export CXXFLAGS=-Wall
ctest -VV

# Run make install unit test
cd test/unit/external
mkdir build
cd build
cmake ../
make -j8
