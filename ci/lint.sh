#!/bin/bash
HERMES_ROOT=$1
ADAPTER=${HERMES_ROOT}/adapter

cpplint --recursive \
--exclude="${HERMES_ROOT}/src/config_server_default.h" \
--exclude="${HERMES_ROOT}/src/config_client_default.h" \
--exclude="${ADAPTER}/posix/posix_api.h" \
--exclude="${ADAPTER}/stdio/stdio_api.h" \
--exclude="${ADAPTER}/mpiio/mpiio_api.h" \
"${HERMES_ROOT}/adapter" "${HERMES_ROOT}/benchmarks" "${HERMES_ROOT}/data_stager" \
"${HERMES_ROOT}/src" "${HERMES_ROOT}/test"
