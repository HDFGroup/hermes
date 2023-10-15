#!/bin/bash

HRUN_ROOT=$1

cpplint --recursive \
"${HRUN_ROOT}/src" "${HRUN_ROOT}/include" "${HRUN_ROOT}/test"
