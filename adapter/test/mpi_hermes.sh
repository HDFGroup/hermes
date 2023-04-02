#!/bin/bash

CMAKE_SOURCE_DIR=$1
CMAKE_BINARY_DIR=$2
MPI_EXEC=$3
TEST_PROCS=$4
EXEC_NAME=$5
TEST_ARGS=$6
SLEEP_TIME=3

mkdir /tmp/test_hermes

export HERMES_CLIENT_CONF="${CMAKE_SOURCE_DIR}/test/data/hermes_client.yaml"
export HERMES_CONF="${CMAKE_SOURCE_DIR}/test/data/hermes_server.yaml"
export HERMES_TRAIT_PATH=${CMAKE_BINARY_DIR}/bin
TEST_EXEC="${CMAKE_BINARY_DIR}/bin/${EXEC_NAME}"
DAEMON_EXEC="${CMAKE_BINARY_DIR}/bin/hermes_daemon"

# Spawn the hermes daemon
echo "STARTING HERMES DAEMON: ${DAEMON_EXEC}"
${DAEMON_EXEC} ${HERMES_CONF} &

# Wait for daemon
echo "Started hermes daemon. sleeping for ${SLEEP_TIME} seconds"
sleep ${SLEEP_TIME}

# Run program
echo "RUNNING PROGRAM: ${TEST_EXEC}"
${MPI_EXEC} -n ${TEST_PROCS} \
-genv HERMES_CLIENT_CONF=${HERMES_CLIENT_CONF} \
-genv HERMES_CONF=${HERMES_CONF} \
-genv HERMES_TRAIT_PATH=${HERMES_TRAIT_PATH} \
${TEST_EXEC} ${TEST_ARGS}
status=$?

# Finalize the Hermes daemon
${CMAKE_BINARY_DIR}/bin/finalize_hermes

exit $status
