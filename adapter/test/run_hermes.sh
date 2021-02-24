#!/bin/bash

MPI_EXEC=$1
TEST_EXEC=$2
TEST_PROCS=$3
HERMES_EXEC=$4
HERMES_PROCS=$5
HERMES_CONF=$6
TEST_ARGS="${@:7}"
SLEEP_TIME=3

error_ct=0
if [[ ! -f "$MPI_EXEC" ]]; then
  echo "MPI_EXEC ${MPI_EXEC} does not exists." >&2
  error_ct=$((error_ct + 1))
fi
if [[ ! -f "${TEST_EXEC}" ]]; then
  echo "TEST_EXEC ${TEST_EXEC} does not exists." >&2
  error_ct=$((error_ct + 1))
fi
if [[ ! -f "${HERMES_EXEC}" ]]; then
  echo "HERMES_EXEC ${HERMES_EXEC} does not exists." >&2
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

echo "${MPI_EXEC} -n ${HERMES_PROCS} ${HERMES_EXEC} ${HERMES_CONF} &"
${MPI_EXEC} -n ${HERMES_PROCS} ${HERMES_EXEC} ${HERMES_CONF} &
HERMES_EXEC_PID=$!
echo "process spawned ${HERMES_EXEC_PID}"

echo "Started hermes daemon with ${HERMES_PROCS} procs. sleeping for ${SLEEP_TIME} seconds"
sleep ${SLEEP_TIME}

echo "${MPI_EXEC} -n ${TEST_PROCS} ${TEST_EXEC} ${TEST_ARGS}"
${MPI_EXEC} -n ${TEST_PROCS} ${TEST_EXEC} ${TEST_ARGS}
status=$?
echo "Killing Hermes daemon with PID ${HERMES_EXEC_PID}"
kill ${HERMES_EXEC_PID}
if [ $status -gt 0 ]; then
  echo "Test failed with code $status!" >&2
  exit $status
fi
echo "Finishing test."
exit 0
