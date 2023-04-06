#!/bin/bash

CMAKE_BINARY_DIR=$1

# Start thallium server
echo "Starting thallium server: ${CMAKE_BINARY_DIR}/bin/test_thallium_server"
${CMAKE_BINARY_DIR}/bin/test_thallium_server &

# Wait for it to start
sleep 1

# Run test
echo "Thallium test: ${CMAKE_BINARY_DIR}/bin/test_thallium_exec"
${CMAKE_BINARY_DIR}/bin/test_thallium_exec

