set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHERMES_PRELOAD -DHERMES_RPC_THALLIUM")
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
set(ADAPTER_COMMON ${CMAKE_CURRENT_SOURCE_DIR}/catch_config.h)
#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
include_directories(
        ${CMAKE_SOURCE_DIR}/adapter/include
        ${CMAKE_SOURCE_DIR}/adapter/src)

find_package(Catch2 REQUIRED)
add_executable(hermes_daemon hermes_daemon.cc)
target_link_libraries(hermes_daemon -ldl -lc MPI::MPI_CXX)
target_link_libraries(hermes_daemon hermes)
add_dependencies(hermes_daemon hermes)


function(gcc exec args)
    add_test(NAME Test${exec} COMMAND "${CMAKE_BINARY_DIR}/bin/${exec}" ${args})
    set_property(TEST Test${exec}
            PROPERTY ENVIRONMENT LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/test/data/asan.supp)
endfunction()

function(mpi exec mpi_proc args)
    add_test(NAME Test${exec}_${mpi_proc}
            COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} ${mpi_proc}
            "${CMAKE_BINARY_DIR}/bin/${exec}" ${args})
    set_property(TEST Test${exec}_${mpi_proc}
            PROPERTY ENVIRONMENT HERMES_CONF=${CMAKE_SOURCE_DIR}/adapter/test/data/hermes.conf)
    set_property(TEST Test${exec}_${mpi_proc} APPEND
            PROPERTY ENVIRONMENT LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/test/data/asan.supp)
endfunction()

find_program(BASH_PROGRAM bash)

function(mpi_daemon test_exec test_mpi_proc test_args arg_name daemon_procs)
    #MPI_EXEC=$1 TEST_EXEC=$2 TEST_PROCS=$3 HERMES_EXEC=$4 HERMES_PROCS=$5 HERMES_CONF=$6
    add_test(Test${test_exec}_${test_mpi_proc}_${arg_name} ${BASH_PROGRAM}
            ${CMAKE_SOURCE_DIR}/adapter/test/run_hermes.sh ${MPIEXEC_EXECUTABLE}
            ${CMAKE_BINARY_DIR}/bin/${test_exec} ${test_mpi_proc}
            ${CMAKE_BINARY_DIR}/bin/hermes_daemon ${daemon_procs}
            ${CMAKE_SOURCE_DIR}/adapter/test/data/hermes.conf)
    set_property(TEST Test${test_exec}_${test_mpi_proc}_${arg_name}
            PROPERTY ENVIRONMENT HERMES_CONF=${CMAKE_SOURCE_DIR}/adapter/test/data/hermes.conf)
    set_property(TEST Test${test_exec}_${test_mpi_proc}_${arg_name} APPEND
            PROPERTY ENVIRONMENT LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/test/data/asan.supp)
endfunction()


add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/posix)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/stdio)