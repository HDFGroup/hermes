#!/bin/bash

LOCAL=${HOME}/local
mkdir build
pushd build

export CXXFLAGS="${CXXFLAGS} -std=c++17"
cmake                                                      \
    -DCMAKE_INSTALL_PREFIX=${LOCAL}                        \
    -DCMAKE_BUILD_RPATH="${LOCAL}/lib"                     \
    -DCMAKE_INSTALL_RPATH="${LOCAL}/lib"                   \
    -DCMAKE_BUILD_TYPE=Release                             \
    -DCMAKE_CXX_COMPILER=`which mpicxx`                    \
    -DCMAKE_C_COMPILER=`which mpicc`                       \
    -DBUILD_SHARED_LIBS=ON                                 \
    -DHERMES_INTERCEPT_IO=${HERMES_INTERCEPT_IO}           \
    -DHERMES_COMMUNICATION_MPI=ON                          \
    -DBUILD_BUFFER_POOL_VISUALIZER=OFF                     \
    -DUSE_ADDRESS_SANITIZER=ON                             \
    -DUSE_THREAD_SANITIZER=OFF                             \
    -DHERMES_USE_TCP=ON                                    \
    -DHERMES_RPC_THALLIUM=ON                               \
    -DHERMES_DEBUG_HEAP=OFF                                \
    ..

cmake --build . -- -j4 && ctest -VV

popd
