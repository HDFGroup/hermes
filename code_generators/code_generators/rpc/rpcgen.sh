#!/bin/bash

CMAKE_SOURCE_DIR=$1
CMAKE_BINARY_DIR=$2
COMPILER_INCLUDES=${PWD}/compile_includes.txt
COMPILER_COMMANDS=${PWD}/compile_commands.json
RPC_GEN_DIR=${CMAKE_SOURCE_DIR}/code_generators/code_generators/rpc

clang -### "${RPC_GEN_DIR}/empty.cc" &> ${COMPILER_INCLUDES}
python3 "${CMAKE_SOURCE_DIR}/code_generators/code_generators/rpc/clang_doc.py" \
  "${CMAKE_SOURCE_DIR}" \
  "${CMAKE_BINARY_DIR}" \
  "${COMPILER_INCLUDES}" \
  "${COMPILER_COMMANDS}"
