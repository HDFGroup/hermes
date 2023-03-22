#!/bin/bash

CMAKE_SOURCE_DIR=$1
CMAKE_BINARY_DIR=$2
$MPI_EXEC=$3
DAEMON_PROCS=$4
TEST_PROCS=$5
EXEC_NAME=$6
TEST_ARGS=$7
SLEEP_TIME=3

export HERMES_CLIENT_CONF="${CMAKE_SOURCE_DIR}/test/data/hermes_client.yaml"
export HERMES_CONF="${CMAKE_SOURCE_DIR}/test/data/hermes_server.yaml"
export TEST_EXEC="${CMAKE_BINARY_DIR}/bin/${EXEC_NAME}"
export DAEMON_EXEC="${CMAKE_BINARY_DIR}/bin/hermes_daemon"

error_ct=0
if [[ ! -f "$MPI_EXEC" ]]; then
  echo "MPI_EXEC ${MPI_EXEC} does not exists." >&2
  error_ct=$((error_ct + 1))
fi
if [[ ! -f "${TEST_EXEC}" ]]; then
  echo "TEST_EXEC ${TEST_EXEC} does not exists." >&2
  error_ct=$((error_ct + 1))
fi
if [[ ! -f "${DAEMON_EXEC}" ]]; then
  echo "DAEMON_EXEC ${DAEMON_EXEC} does not exists." >&2
  error_ct=$((error_ct + 1))
fi
if [[ ! -f "${HERMES_CONF}" ]]; then
  echo "HERMES_CONF ${HERMES_CONF} does not exists." >&2
  error_ct=$((error_ct + 1))
fi
if [ $error_ct -gt 0 ]; then
  echo "Arguments are wrong !!!" >&2
  exit $error_ct
fi

# Spawn the hermes daemon
echo "STARTING HERMES DAEMON"
echo "${MPI_EXEC} -n ${HERMES_PROCS} ${DAEMON_EXEC} ${HERMES_CONF} &"
${MPI_EXEC} -n ${HERMES_PROCS} ${DAEMON_EXEC} ${HERMES_CONF} &
DAEMON_EXEC_PID=$!
echo "process spawned ${DAEMON_EXEC_PID}"

# Wait for daemon
echo "Started hermes daemon with ${HERMES_PROCS} procs. sleeping for ${SLEEP_TIME} seconds"
sleep ${SLEEP_TIME}

# Run program
echo "RUNNING PROGRAM"
echo "${MPI_EXEC} -n ${TEST_PROCS} ${TEST_EXEC} ${TEST_ARGS}"
${MPI_EXEC} -n ${TEST_PROCS} ${TEST_EXEC} ${TEST_ARGS}
status=$?
echo "Killing Hermes daemon with PID ${DAEMON_EXEC_PID}"
kill ${DAEMON_EXEC_PID}
if [ $status -gt 0 ]; then
  echo "Test failed with code $status!" >&2
  exit $status
fi
echo "Finishing test."
exit 0
