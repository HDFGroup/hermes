#!/bin/bash

CMAKE_CURRENT_BINARY_DIR=$1

# Start thallium server
echo "Starting thallium server: ${CMAKE_CURRENT_BINARY_DIR}/test_thallium_server"
${CMAKE_CURRENT_BINARY_DIR}/test_thallium_server &

# Wait for it to start
sleep 1

# Run test
echo "Thallium test: ${CMAKE_CURRENT_BINARY_DIR}/test_thallium_exec"
${CMAKE_CURRENT_BINARY_DIR}/test_thallium_exec

