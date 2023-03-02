#!/bin/bash
CMAKE_SOURCE_DIR=$1
CMAKE_BINARY_DIR=$2
PRELOAD_NAME=$3
FULL_EXEC=$4
SLEEP_TIME=3

export HERMES_CONF="${CMAKE_SOURCE_DIR}/test/data/hermes_server.yaml"
export HERMES_CLIENT_CONF="${CMAKE_SOURCE_DIR}/test/data/hermes_client_specific.yaml"

# Start the Hermes daemon
echo "STARTING DAEMON"
${CMAKE_BINARY_DIR}/bin/hermes_daemon &
echo "WAITING FOR DAEMON"
sleep ${SLEEP_TIME}

# Run the program
echo "RUNNING PROGRAM"
export LSAN_OPTIONS=suppressions="${CMAKE_SOURCE_DIR}/test/data/asan.supp"
LD_PRELOAD=${CMAKE_BINARY_DIR}/bin/lib${PRELOAD_NAME}.so \
${FULL_EXEC}
status=$?

# Finalize the Hermes daemon
${CMAKE_BINARY_DIR}/bin/finalize_hermes

# Exit with status
exit $status