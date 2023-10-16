#!/bin/bash

HRUN_ROOT=$1

echo "RUNNING CPPLINT"
cpplint --recursive \
"${HRUN_ROOT}/src" "${HRUN_ROOT}/include" "${HRUN_ROOT}/test" \
--exclude="${HRUN_ROOT}/test/unit/external"