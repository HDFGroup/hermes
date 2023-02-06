#!/bin/bash
CMAKE_BINARY_DIR=$1
CMAKE_SOURCE_DIR=$2
EXEC=$3
TAG_NAME=$4
TAGS=$5
MODE=$6
path

LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/test/data/asan.supp
ADAPTER_MODE=${mode}
SET_PATH=${path}
HERMES_CONF=${CMAKE_SOURCE_DIR}/adapter/test/data/hermes.yaml

COMMAND="${CMAKE_BINARY_DIR}/bin/${exec}"