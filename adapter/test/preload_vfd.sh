#!/bin/bash
CMAKE_SOURCE_DIR=$1
CMAKE_BINARY_DIR=$2
ADAPTER_MODE=$3
FULL_EXEC=$4
ARGS=$5
SLEEP_TIME=3

mkdir /tmp/test_hermes

export HERMES_CLIENT_CONF="${CMAKE_SOURCE_DIR}/test/data/hermes_client_specific.yaml"
export HERMES_CONF="${CMAKE_SOURCE_DIR}/test/data/hermes_server.yaml"
export HERMES_TRAIT_PATH=${CMAKE_BINARY_DIR}/bin

# Start the Hermes daemon
echo "STARTING DAEMON"
${CMAKE_BINARY_DIR}/bin/hermes_daemon &
echo "WAITING FOR DAEMON"
sleep ${SLEEP_TIME}

# Run the program
echo "RUNNING PROGRAM"
export LSAN_OPTIONS=suppressions="${CMAKE_SOURCE_DIR}/test/data/asan.supp"
# HDF5_PLUGIN_PATH=${HERMES_VFD_LIBRARY_DIR} \
HERMES_ADAPTER_MODE="${ADAPTER_MODE}" \
HDF5_DRIVER=hermes \
LD_PRELOAD=${CMAKE_BINARY_DIR}/bin/libhdf5_hermes_vfd.so \
${FULL_EXEC} ${ARGS}
status=$?

# Finalize the Hermes daemon
${CMAKE_BINARY_DIR}/bin/finalize_hermes

# Exit with status
exit $status