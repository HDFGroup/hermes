#!/bin/bash
CMAKE_SOURCE_DIR=$1
CMAKE_BINARY_DIR=$2
MPIEXEC_EXECUTABLE=$3
MPIEXEC_NUMPROC_FLAG=$4
MPI_PROC=$5
EXEC_NAME=$6
ARGS=$7
SLEEP_TIME=3

# Run the program
echo "RUNNING PROGRAM (MPI)"
export LSAN_OPTIONS=suppressions="${CMAKE_SOURCE_DIR}/test/data/asan.supp"
${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} ${MPI_PROC} \
${CMAKE_BINARY_DIR}/bin/${EXEC_NAME} "${ARGS}" -d yes
status=$?

# Exit with status
exit $status