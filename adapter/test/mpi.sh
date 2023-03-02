#!/bin/bash
CMAKE_SOURCE_DIR=$1
CMAKE_BINARY_DIR=$2
MPIEXEC_EXECUTABLE=$3
MPIEXEC_NUMPROC_FLAG=$4
MPI_PROC=$5
EXEC_NAME=$6
ARGS=$7
SLEEP_TIME=3

export HERMES_CONF="${CMAKE_SOURCE_DIR}/test/data/hermes_server.yaml"
export HERMES_CLIENT_CONF="${CMAKE_SOURCE_DIR}/test/data/hermes_client.yaml"

# Start the Hermes daemon
echo "STARTING DAEMON"
${CMAKE_BINARY_DIR}/bin/hermes_daemon &
echo "WAITING FOR DAEMON"
sleep ${SLEEP_TIME}

# Run the program
echo "RUNNING PROGRAM"
export LSAN_OPTIONS=suppressions="${CMAKE_SOURCE_DIR}/test/data/asan.supp"
${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} ${MPI_PROC} \
${CMAKE_BINARY_DIR}/bin/${EXEC_NAME} "${ARGS}" -d yes
status=$?

# Finalize the Hermes daemon
${CMAKE_BINARY_DIR}/bin/finalize_hermes

# Exit with status
exit $status