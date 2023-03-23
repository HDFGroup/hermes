set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHERMES_PRELOAD -DHERMES_RPC_THALLIUM")
set(ADAPTER_COMMON ${CMAKE_CURRENT_SOURCE_DIR}/catch_config.h)
set(HERMES_ADAPTER_TEST_DIR ${HERMES_ADAPTER_DIR}/test)

find_package(Catch2 REQUIRED)
find_program(BASH_PROGRAM bash)
find_package(OpenMP REQUIRED COMPONENTS C CXX)

function(gcc exec args)
    set(test_name Test${exec} COMMAND)
    set(script ${CMAKE_SOURCE_DIR}/adapter/test/gcc.sh)
    add_test(NAME ${test_name} COMMAND ${BASH_PROGRAM} ${script}
            "${CMAKE_SOURCE_DIR}" "${CMAKE_BINARY_DIR}"
            "${exec}" "${args}")
endfunction()

function(mpi exec mpi_proc args)
    set(test_name Test${exec}_${mpi_proc} COMMAND)
    set(script ${CMAKE_SOURCE_DIR}/adapter/test/mpi.sh)
    add_test(NAME ${test_name} COMMAND ${BASH_PROGRAM} ${script}
            "${CMAKE_SOURCE_DIR}" "${CMAKE_BINARY_DIR}"
            "${MPIEXEC_EXECUTABLE}" "${MPIEXEC_NUMPROC_FLAG}" "${mpi_proc}"
            "${exec}" "${args}")
endfunction()

function(gcc_hermes exec tag_name tags conf async)
    set(test_name Test${exec}_${tag_name}_${async} COMMAND)
    set(script ${CMAKE_SOURCE_DIR}/adapter/test/gcc_hermes.sh)
    add_test(NAME ${test_name} COMMAND ${BASH_PROGRAM} ${script}
            "${CMAKE_SOURCE_DIR}" "${CMAKE_BINARY_DIR}"
            "${exec}" "${tag_name}" "${tags}" "${conf}" "${async}")
endfunction()

function(gcc_hermes_mode exec tag_name tags mode path)
    set(test_name Test${exec}_${tag_name}_${mode}_${path})
    set(script ${CMAKE_SOURCE_DIR}/adapter/test/gcc_hermes_mode.sh)
    add_test(NAME ${test_name} COMMAND ${BASH_PROGRAM} ${script}
            "${CMAKE_SOURCE_DIR}" "${CMAKE_BINARY_DIR}"
            "${exec}" "${tag_name}" "${tags}" "${mode}" "${path}")
endfunction()

function(mpi_daemon test_exec test_mpi_proc test_args arg_name daemon_procs)
    #MPI_EXEC=$1 TEST_EXEC=$2 TEST_PROCS=$3 HERMES_EXEC=$4 HERMES_PROCS=$5 HERMES_CONF=$6
    set(script ${CMAKE_SOURCE_DIR}/adapter/test/mpi_hermes.sh)
    add_test(NAME Test${test_exec}_${test_mpi_proc}_${arg_name} COMMAND
            ${BASH_PROGRAM} ${script}
            "${CMAKE_SOURCE_DIR}" "${CMAKE_BINARY_DIR}"
            "${MPIEXEC_EXECUTABLE}"
            "${daemon_procs}"
            "${test_mpi_proc}"
            "${test_exec}"
            "${test_args}")
endfunction()

function(preload_hermes name preload exec)
    set(test_name Test_${preload}_${name})
    set(script ${CMAKE_SOURCE_DIR}/adapter/test/preload.sh)
    add_test(NAME ${test_name} COMMAND ${BASH_PROGRAM} ${script}
            "${CMAKE_SOURCE_DIR}" "${CMAKE_BINARY_DIR}"
            "${preload}" "${exec}")
endfunction()

enable_testing()

if(HERMES_ENABLE_POSIX_ADAPTER)
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/posix)
endif()

if(HERMES_ENABLE_STDIO_ADAPTER)
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/stdio)
endif()

if(HERMES_ENABLE_MPIIO_ADAPTER)
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/mpiio)
endif()

if(HERMES_ENABLE_PUBSUB_ADAPTER)
  # add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/pubsub)
endif()

if(HERMES_ENABLE_VFD)
  # add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/vfd)
endif()