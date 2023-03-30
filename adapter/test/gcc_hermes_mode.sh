#!/bin/bash
CMAKE_SOURCE_DIR=$1
CMAKE_BINARY_DIR=$2
EXEC_NAME=$3
TAG_NAME=$4
TAGS=$5
MODE=$6
DO_PATH_EXCLUDE=$7
SLEEP_TIME=3

mkdir /tmp/test_hermes

export HERMES_CLIENT_CONF="${CMAKE_SOURCE_DIR}/test/data/hermes_client.yaml"
export HERMES_CONF="${CMAKE_SOURCE_DIR}/test/data/hermes_server.yaml"

# Start the Hermes daemon
echo "STARTING DAEMON"
${CMAKE_BINARY_DIR}/bin/hermes_daemon &
echo "WAITING FOR DAEMON"
sleep ${SLEEP_TIME}

# Run the program
echo "RUNNING PROGRAM"
export LSAN_OPTIONS=suppressions="${CMAKE_SOURCE_DIR}/test/data/asan.supp"
export HERMES_ADAPTER_MODE="${MODE}"
export SET_PATH="${DO_PATH_EXCLUDE}"
export COMMAND="${CMAKE_BINARY_DIR}/bin/${EXEC_NAME}"

echo ${MODE}
echo ${HERMES_CLIENT_CONF}
echo ${HERMES_CONF}
echo "${COMMAND}" "${TAGS}" --reporter compact -d yes
${COMMAND} ${TAGS} --reporter compact -d yes
status=$?
echo "COMMAND FINISHED"

# Finalize the Hermes daemon
${CMAKE_BINARY_DIR}/bin/finalize_hermes

# Exit with status
exit $status