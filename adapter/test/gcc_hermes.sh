#!/bin/bash
CMAKE_SOURCE_DIR=$1
CMAKE_BINARY_DIR=$2
EXEC_NAME=$3
TAG_NAME=$4
TAGS=$5
CONF=$6
ASYNC=$7
SLEEP_TIME=3

export HERMES_CLIENT_CONF="${CMAKE_SOURCE_DIR}/test/data/${CONF}_client.yaml"
export HERMES_CONF="${CMAKE_SOURCE_DIR}/test/data/${CONF}_server.yaml"

# Start the Hermes daemon
echo "STARTING DAEMON"
${CMAKE_BINARY_DIR}/bin/hermes_daemon &
echo "WAITING FOR DAEMON"
sleep ${SLEEP_TIME}

# Run the program
echo "RUNNING PROGRAM"
export LSAN_OPTIONS=suppressions="${CMAKE_SOURCE_DIR}/test/data/asan.supp"
export COMMAND="${CMAKE_BINARY_DIR}/bin/${EXEC_NAME}"
echo ""${COMMAND}" "${TAGS}" --reporter compact -d yes"
"${COMMAND}" "${TAGS}" --reporter compact -d yes
status=$?

# Finalize the Hermes daemon
${CMAKE_BINARY_DIR}/bin/finalize_hermes

# Exit with status
exit $status