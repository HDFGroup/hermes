#!/bin/bash

# set -x
# set -e
# set -o pipefail

mkdir build
pushd build

#LOCAL=local
INSTALL_PREFIX="${HOME}/${LOCAL}"

export CXXFLAGS="${CXXFLAGS} -std=c++17 -Werror -Wall -Wextra"
cmake                                                      \
    -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}               \
    -DCMAKE_PREFIX_PATH=${INSTALL_PREFIX}                  \
    -DCMAKE_BUILD_RPATH=${INSTALL_PREFIX}/lib              \
    -DCMAKE_INSTALL_RPATH=${INSTALL_PREFIX}/lib            \
    -DCMAKE_BUILD_TYPE=${BUILD_TYPE}                       \
    -DCMAKE_CXX_COMPILER=`which mpicxx`                    \
    -DCMAKE_C_COMPILER=`which mpicc`                       \
    -DBUILD_SHARED_LIBS=ON                                 \
    -DHERMES_ENABLE_DOXYGEN=ON                             \
    -DHERMES_ENABLE_COVERAGE=ON                            \
    -DHERMES_INTERCEPT_IO=OFF                              \
    -DHERMES_BUILD_BENCHMARKS=ON                           \
    -DHERMES_COMMUNICATION_MPI=ON                          \
    -DHERMES_BUILD_BUFFER_POOL_VISUALIZER=ON               \
    -DORTOOLS_DIR=${INSTALL_PREFIX}                        \
    -DHERMES_USE_ADDRESS_SANITIZER=ON                      \
    -DHERMES_USE_THREAD_SANITIZER=OFF                      \
    -DHERMES_RPC_THALLIUM=ON                               \
    -DHERMES_ENABLE_VFD=ON                                 \
    -DBUILD_TESTING=ON                                     \
    ..
make dox >& out.txt
# cat out.txt | grep warning | grep -v "ignoring unsupported tag"
popd
rec="$( grep warning build/out.txt | grep -v "ignoring unsupported tag" |  wc -l )"
echo "$rec"
exit "$rec"

