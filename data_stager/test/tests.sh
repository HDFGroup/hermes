HERMES_ROOT=$1
HERMES_BUILD=$2
HERMES_CONF=$3
N_IOR=$4
N_STAGE=$5
STAGE_URL=$6
STAGE_OFF=$7
FILE_PER_PROCESS=$8
IOR_BLOCK_SIZE=1048576

mkdir -p /tmp/staging

mpirun -n 1 \
-genv HERMES_CONF ${HERMES_CONF} \
-genv LD_LIBRARY_PATH ${HERMES_BUILD}:$LD_LIBRARY_PATH \
${HERMES_BUILD}/hermes_daemon &
sleep 1

mpirun -n ${N_IOR} ior -k -w ${FILE_PER_PROCESS} -b ${IOR_BLOCK_SIZE} -o /tmp/test_staging.txt

mpirun -n ${N_STAGE} \
-genv HERMES_CONF ${HERMES_CONF} \
-genv LD_LIBRARY_PATH $LD_LIBRARY_PATH \
-genv LSAN_OPTIONS=suppressions=${HERMES_ROOT}/test/data/asan.supp \
-genv ADAPTER_MODE=DEFAULT \
-genv SET_PATH=0 \
${HERMES_BUILD}/stage_in ${STAGE_URL} ${STAGE_OFF} 0 kMinimizeIoTime

mpirun -n ${N_STAGE} ior -k -r ${FILE_PER_PROCESS} -b ${IOR_BLOCK_SIZE}  -o /tmp/test_staging.txt

rm -rf /tmp/staging