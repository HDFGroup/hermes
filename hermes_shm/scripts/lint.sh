#!/bin/bash

HERMES_ROOT=$1

cpplint --recursive \
--exclude="${HERMES_ROOT}/include/hermes_shm/constants/singleton_macros.h" \
--exclude="${HERMES_ROOT}/include/hermes_shm/constants/data_structure_singleton_macros.h" \
--exclude="${HERMES_ROOT}/src/singleton.cc" \
--exclude="${HERMES_ROOT}/src/data_structure_singleton.cc" \
--exclude="${HERMES_ROOT}/include/hermes_shm/util/formatter.h" \
--exclude="${HERMES_ROOT}/include/hermes_shm/util/errors.h" \
"${HERMES_ROOT}/src" "${HERMES_ROOT}/include" "${HERMES_ROOT}/test"