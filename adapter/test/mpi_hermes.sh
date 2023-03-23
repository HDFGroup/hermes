#!/bin/bash

CMAKE_SOURCE_DIR=$1
CMAKE_BINARY_DIR=$2
MPI_EXEC=$3
DAEMON_PROCS=$4
TEST_PROCS=$5
EXEC_NAME=$6
TEST_ARGS=$7
SLEEP_TIME=3

export HERMES_CLIENT_CONF="${CMAKE_SOURCE_DIR}/test/data/hermes_client.yaml"
export HERMES_CONF="${CMAKE_SOURCE_DIR}/test/data/hermes_server.yaml"
export TEST_EXEC="${CMAKE_BINARY_DIR}/bin/${EXEC_NAME}"
export DAEMON_EXEC="${CMAKE_BINARY_DIR}/bin/hermes_daemon"

# Spawn the hermes daemon
echo "STARTING HERMES DAEMON: ${DAEMON_EXEC}"
${MPI_EXEC} -n ${DAEMON_PROCS} ${DAEMON_EXEC} ${HERMES_CONF} &

# Wait for daemon
echo "Started hermes daemon with ${DAEMON_PROCS} procs. sleeping for ${SLEEP_TIME} seconds"
sleep ${SLEEP_TIME}

# Run program
echo "RUNNING PROGRAM: ${EXEC_NAME}"
${MPI_EXEC} -n ${TEST_PROCS} ${TEST_EXEC} ${TEST_ARGS}
status=$?

# Finalize the Hermes daemon
${CMAKE_BINARY_DIR}/bin/finalize_hermes

exit $status
