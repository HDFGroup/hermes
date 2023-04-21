#!/bin/bash
CMAKE_SOURCE_DIR=$1
CMAKE_BINARY_DIR=$2
EXEC_NAME=$3
ARGS=$4
SLEEP_TIME=3

mkdir /tmp/test_hermes

# Run the program
echo "RUNNING PROGRAM"
export LSAN_OPTIONS=suppressions="${CMAKE_SOURCE_DIR}/test/data/asan.supp"
export COMMAND="${CMAKE_BINARY_DIR}/bin/${EXEC_NAME}"
echo "RUNNING PROGRAM"
echo "${COMMAND}" "${ARGS}"
"${COMMAND}" "${ARGS}"
status=$?

# Exit with status
exit $status