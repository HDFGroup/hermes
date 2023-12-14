#!/bin/bash
COVERAGE_DIR="$1"
BUILD_DIR="$2"
mkdir -p "${COVERAGE_DIR}"
cd "${BUILD_DIR}"
echo $BUILD_DIR
lcov -c -d . -o "${COVERAGE_DIR}/tmp.info"
lcov --remove "${COVERAGE_DIR}/tmp.info" \
              "/usr/*" \
              "*/spack/*" \
              -o "${COVERAGE_DIR}/lcov.info"
genhtml "${COVERAGE_DIR}/lcov.info" --output-directory coverage_report

echo "Placed coverage info in ${COVERAGE_DIR}/lcov.info"
cd ${COVERAGE_DIR}
`pwd`
ls

#lcov -c -d . -o "tmp.info"
#lcov --remove "tmp.info" \
#              "/usr/*" \
#              "*/spack/*" \
#              -o "lcov.info"
#genhtml "lcov.info" --output-directory coverage_report
