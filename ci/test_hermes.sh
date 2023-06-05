#!/bin/bash

# Run main unit tests
pushd build
ctest -VV -R test_hermes_posix_basic_small
popd build

# Run make install unit test
cd ci/external
mkdir build
cd build
cmake ../
make -j8
