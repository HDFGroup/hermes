#!/bin/bash

LABSTOR_ROOT=$1

cpplint --recursive \
"${LABSTOR_ROOT}/src" "${LABSTOR_ROOT}/include" "${LABSTOR_ROOT}/test"
