#!/bin/bash

# Run main unit tests
pushd build
ctest -VV -R test_hermes_posix_basic_small
popd build

# Set proper flags for cmake to find Hermes
INSTALL_PREFIX="${HOME}/hermes_install"
export LIBRARY_PATH="${INSTALL_PREFIX}/lib:${LIBRARY_PATH}"
export LD_LIBRARY_PATH="${INSTALL_PREFIX}/lib:${LD_LIBRARY_PATH}"
export LDFLAGS="-L${INSTALL_PREFIX}/lib:${LDFLAGS}"
export CFLAGS="-I${INSTALL_PREFIX}/include:${CFLAGS}"
export CPATH="${INSTALL_PREFIX}/include:${CPATH}"
export CMAKE_PREFIX_PATH="${INSTALL_PREFIX}:${CMAKE_PREFIX_PATH}"
export CXXFLAGS="-I${INSTALL_PREFIX}/include:${CXXFLAGS}"

unset -f LDFLAGS
unset -f CFLAGS
unset -f CXXFLAGS
unset -f CMAKE_PREFIX_PATH
unset -f CMAKE_MODULE_PATH

# Run make install unit test
cd ci/external
mkdir build
cd build
cmake ../
make -j8

