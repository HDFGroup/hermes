#!/bin/bash

. ci/build_hermes.sh
pushd build
ctest
popd
