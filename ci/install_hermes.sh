#!/bin/bash

set -x

if [[ "$#" -ne 1 ]]; then
   echo "$0 Expected the dependencies directory as its first argument"
   exit 1
fi

LOCAL=${1}
mkdir build
pushd build

export CXXFLAGS="${CXXFLAGS} -std=c++17"
cmake                                                      \
    -DCMAKE_INSTALL_PREFIX=${LOCAL}                        \
    -DCMAKE_PREFIX_PATH=${LOCAL}                           \
    -DCMAKE_BUILD_RPATH="${LOCAL}/lib"                     \
    -DCMAKE_INSTALL_RPATH="${LOCAL}/lib"                   \
    -DCMAKE_BUILD_TYPE=Release                             \
    -DCMAKE_CXX_COMPILER=`which mpicxx`                    \
    -DCMAKE_C_COMPILER=`which mpicc`                       \
    -DBUILD_SHARED_LIBS=ON                                 \
    -DHERMES_INTERCEPT_IO=ON                               \
    -DHERMES_COMMUNICATION_MPI=ON                          \
    -DBUILD_BUFFER_POOL_VISUALIZER=ON                      \
    -DORTOOLS_DIR=${LOCAL}                                 \
    -DUSE_ADDRESS_SANITIZER=ON                             \
    -DUSE_THREAD_SANITIZER=OFF                             \
    -DHERMES_RPC_THALLIUM=ON                               \
    -DHERMES_DEBUG_HEAP=OFF                                \
    ..

cmake --build . -- -j4 && ctest -VV

popd
