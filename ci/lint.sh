HERMES_ROOT=$1
ADAPTER=${HERMES_ROOT}/adapter

cpplint --recursive \
--exclude=${HERMES_ROOT}/src/stb_ds.h \
--exclude=${ADAPTER}/posix/real_api.h \
--exclude=${ADAPTER}/stdio/real_api.h \
--exclude=${ADAPTER}/mpiio/real_api.h \
${HERMES_ROOT}/adapter ${HERMES_ROOT}/benchmarks ${HERMES_ROOT}/data_stager \
${HERMES_ROOT}/src ${HERMES_ROOT}/test
