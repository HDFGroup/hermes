#!/bin/bash

set -x
set -e
set -o pipefail

mkdir build
pushd build

INSTALL_PREFIX="${HOME}/${LOCAL}"

export CXXFLAGS="${CXXFLAGS} -std=c++17"
cmake                                                      \
    -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}               \
    -DCMAKE_PREFIX_PATH=${INSTALL_PREFIX}                  \
    -DCMAKE_BUILD_RPATH=${INSTALL_PREFIX}/lib              \
    -DCMAKE_INSTALL_RPATH=${INSTALL_PREFIX}/lib            \
    -DCMAKE_BUILD_TYPE=${BUILD_TYPE}                       \
    -DCMAKE_CXX_COMPILER=`which mpicxx`                    \
    -DCMAKE_C_COMPILER=`which mpicc`                       \
    -DBUILD_SHARED_LIBS=ON                                 \
    -DHERMES_INTERCEPT_IO=ON                               \
    -DHERMES_COMMUNICATION_MPI=ON                          \
    -DBUILD_BUFFER_POOL_VISUALIZER=ON                      \
    -DORTOOLS_DIR=${INSTALL_PREFIX}                        \
    -DUSE_ADDRESS_SANITIZER=ON                             \
    -DUSE_THREAD_SANITIZER=OFF                             \
    -DHERMES_RPC_THALLIUM=ON                               \
    -DHERMES_DEBUG_HEAP=OFF                                \
    ..

cmake --build . -- -j4 && ctest -VV

popd
