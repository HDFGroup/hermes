#!/bin/bash
CMAKE_BINARY_DIR=$1
CMAKE_SOURCE_DIR=$2
EXEC=$3
TAG_NAME=$4
TAGS=$5
MODE=$6
DO_PATH_EXCLUDE=$7

LSAN_OPTIONS=suppressions="${CMAKE_SOURCE_DIR}/test/data/asan.supp" \
ADAPTER_MODE="${MODE}" \
SET_PATH="${DO_PATH_EXCLUDE}" \
HERMES_CONF="${CMAKE_SOURCE_DIR}/adapter/test/data/hermes.yaml" \
HERMES_CLIENT_CONF="${CMAKE_SOURCE_DIR}/adapter/test/data/hermes.yaml" \
COMMAND="${CMAKE_BINARY_DIR}/bin/${exec}" \
"${COMMAND}" "${TAGS}" --reporter compact -d yes
